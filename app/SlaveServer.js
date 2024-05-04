const net = require("net");
const Encoder = require("./Encoder");
const RequestParser = require("./RequestParser");
const HashTable = require("./HashTable");

/**
 * Generates a unique identifier for a socket using its remote address and port.
 * @param {net.Socket} socket - The socket object.
 * @returns {string} The unique identifier for the socket.
 */
function getUid(socket) {
  return `${socket.remoteAddress}:${socket.remotePort}`;
}

/**
 * Class representing a slave server that connects to a master server for data replication.
 */
class SlaveServer {
  /**
   * Creates an instance of the SlaveServer.
   * @param {string} host - The local server host.
   * @param {number|string} port - The local server port.
   * @param {string} masterHost - The master server host.
   * @param {number|string} masterPort - The master server port.
   */
  constructor(host, port, masterHost, masterPort) {
    this.host = host;
    this.port = port;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.dataStore = new HashTable();
    this.clientBuffers = {};

    this.masterBuffer = "";
    this.masterSocket = null;
    this.masterOffset = 0;
  }

  /**
   * Starts the slave server and connects to the master server.
   */
  startServer() {
    this.performHandshake();
    const server = net.createServer((socket) => {
      this.clientBuffers[getUid(socket)] = "";

      socket.on(`data`, (data) => {
        this.clientBuffers[getUid(socket)] += data.toString();
        this.processClientBuffer(socket);
      });

      socket.on("error", (err) => {
        console.log(`Socket Error: ${err}`);
        delete this.clientBuffers[getUid(socket)];
      });

      socket.on(`close`, () => {
        console.log(`Disconnecting client: ${getUid(socket)}`);
        delete this.clientBuffers[getUid(socket)];
      });
    });

    server.listen(this.port, this.host, () => {
      console.log(`Slave Server Listening on ${this.host}:${this.port}`);
    });
  }

  /**
   * Performs the handshake process with the master server to establish replication capabilities.
   */
  performHandshake() {
    const socket = net.createConnection(
      { host: this.masterHost, port: this.masterPort },
      () => {
        console.log(
          `Connected to master server on ${this.masterHost}:${this.masterPort}`
        );
      }
    );

    this.masterSocket = socket;

    socket.write(Encoder.createArray([Encoder.createBulkString("PING")]));
    this.handshakeStep = 1;

    socket.on("data", (data) => {
      let masterResponse = data.toString().toLowerCase();
      if (this.handshakeStep === 1) {
        if (masterResponse !== Encoder.createSimpleString("pong")) return;
        this.handshakeStep = 2;
        socket.write(
          Encoder.createArray([
            Encoder.createBulkString("REPLCONF"),
            Encoder.createBulkString("listening-port"),
            Encoder.createBulkString(`${this.port}`),
          ])
        );
      } else if (this.handshakeStep === 2) {
        if (masterResponse !== Encoder.createSimpleString("ok")) return;
        this.handshakeStep = 3;
        socket.write(
          Encoder.createArray([
            Encoder.createBulkString("REPLCONF"),
            Encoder.createBulkString("capa"),
            Encoder.createBulkString("psync2"),
          ])
        );
      } else if (this.handshakeStep === 3) {
        if (masterResponse !== Encoder.createSimpleString("ok")) return;
        this.handshakeStep = 4;
        socket.write(
          Encoder.createArray([
            Encoder.createBulkString("PSYNC"),
            Encoder.createBulkString("?"),
            Encoder.createBulkString("-1"),
          ])
        );
      } else if (this.handshakeStep === 4) {
        if (!masterResponse.startsWith("+fullresync")) return;
        let idx = masterResponse.indexOf("\r\n");
        idx += 3;
        let sizeOfRDB = 0;
        while (masterResponse[idx] !== "\r") {
          sizeOfRDB = sizeOfRDB * 10 + (masterResponse[idx] - "0");
          idx++;
        }
        idx += 2;
        let rdbFileData = masterResponse.slice(idx, idx + sizeOfRDB);
        idx += sizeOfRDB - 1;
        masterResponse = data.toString().slice(idx);
        this.masterBuffer = "";
        this.handshakeStep = 5;
      }
      if (this.handshakeStep === 5) {
        if (masterResponse === "") return;
        this.masterBuffer += masterResponse;
        this.processMasterBuffer();
      }
    });

    socket.on(`error`, (err) => {
      console.log(`Error from master server connection : ${err}`);
    });

    socket.on("close", () => {
      console.log("Connection closed");
    });
  }

  /**
   * Processes the buffer for a specific client socket, parsing commands and handling them.
   * @param {net.Socket} socket - The client socket whose buffer is being processed.
   */
  processClientBuffer(socket) {
    const clientKey = getUid(socket);
    const buffer = this.clientBuffers[clientKey];
    const requestParser = new RequestParser(buffer);
    while (true) {
      const args = requestParser.parse();
      if (args.length === 0) break;
      const currentRequest = requestParser.currentRequest;
      this.handleCommand(socket, args, currentRequest);
    }

    this.clientBuffers[clientKey] = requestParser.getRemainingRequest();
  }

  /**
   * Processes the buffer received from the master server, parsing and handling replication commands.
   */
  processMasterBuffer() {
    const buffer = this.masterBuffer;
    const requestParser = new RequestParser(buffer);
    while (true) {
      const args = requestParser.parse();
      if (args.length === 0) break;
      const currentRequest = requestParser.currentRequest;
      this.handleCommand(this.masterSocket, args, currentRequest);
      this.masterOffset += currentRequest.length;
    }
    this.masterBuffer = requestParser.getRemainingRequest();
  }

  /**
   * Handles commands parsed from the client or master buffer based on their arguments.
   * @param {net.Socket} socket - The socket on which the command was received.
   * @param {string[]} args - The arguments of the command.
   * @param {string} request - The full string of the command request.
   */
  handleCommand(socket, args, request) {
    const command = args[0].toLowerCase();
    switch (command) {
      case "info":
        socket.write(this.handleInfo(args.slice(1)));
        break;
      case "set":
        this.handleSet(args.slice(1));
        break;
      case "get":
        socket.write(this.handleGet(args.slice(1)));
        break;
      case "replconf":
        socket.write(this.handleReplconf(args.slice(1)));
        break;
    }
  }

  /**
   * Handles the 'info' command, providing details about the server's state.
   * @param {string[]} args - Arguments following the command.
   * @returns {string} The server information formatted as a bulk string.
   */
  handleInfo(args) {
    const section = args[0].toLowerCase();
    let response;
    if (section === "replication") {
      response = Encoder.createBulkString("role:slave");
    }
    return response;
  }

  /**
   * Handles the 'set' command to insert a key-value pair into the data store.
   * @param {string[]} args - Arguments containing the key and value, and optionally the expiry time.
   */
  handleSet(args) {
    const key = args[0];
    const value = args[1];
    if (args.length == 2) {
      this.dataStore.insert(key, value);
    } else {
      const arg = args[2];
      const expiryTime = args[3];
      this.dataStore.insertWithExpiry(key, value, expiryTime);
    }
  }

  /**
   * Handles the 'get' command to retrieve a value by key from the data store.
   * @param {string[]} args - Arguments containing the key.
   * @returns {string} The value associated with the key or a null bulk string if not found.
   */
  handleGet(args) {
    const key = args[0];
    const value = this.dataStore.get(key);
    if (value === null) {
      return Encoder.createBulkString("", true);
    }
    return Encoder.createBulkString(value);
  }

  /**
   * Handles the 'replconf' command, typically used to acknowledge the state of replication.
   * @param {string[]} args - Arguments for the replconf command.
   * @returns {string} An array encoded as a bulk string, acknowledging the replication offset.
   */
  handleReplconf(args) {
    return Encoder.createArray([
      Encoder.createBulkString("REPLCONF"),
      Encoder.createBulkString("ACK"),
      Encoder.createBulkString(`${this.masterOffset}`),
    ]);
  }
}

module.exports = SlaveServer;
