const net = require("net");
const fs = require("fs");
const path = require("path");
const Encoder = require("./Encoder");
const RequestParser = require("./RequestParser");
const HashTable = require("./HashTable");
const RDBParser = require("./RDBParser");

/**
 * Helper function to generate a unique identifier for a socket based on its address and port.
 * @param {net.Socket} socket - The socket instance.
 * @returns {string} A unique identifier for the socket.
 */
function getUid(socket) {
  return `${socket.remoteAddress}:${socket.remotePort}`;
}

/**
 * Class representing a master server handling commands and managing replication.
 */
class MasterServer {
  /**
   * Constructs a master server.
   * @param {string} host - The host IP address or hostname the server will listen on.
   * @param {number|string} port - The port number on which the server will listen.
   * @param {Object} [config=null] - Configuration options for the server.
   */
  constructor(host, port, config = null) {
    this.host = host;
    this.port = port;
    this.dataStore = new HashTable(); // Initialize the data store as a hash table.
    this.clientBuffers = {}; // To store buffers for each connected client.

    // Replication related properties.
    this.masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    this.masterReplOffset = 0;
    this.replicas = {}; // Track connected replica servers.
    this.config = config; // Server configuration.
  }

  /**
   * Starts the TCP server and handles incoming connections and data.
   */
  startServer() {
    this.loadRDBFile();
    const server = net.createServer((socket) => {
      this.clientBuffers[getUid(socket)] = "";

      // Data event handler to concatenate incoming data chunks.
      socket.on(`data`, (data) => {
        this.clientBuffers[getUid(socket)] += data.toString();
        this.processClientBuffer(socket);
      });

      // Error event handler.
      socket.on("error", (err) => {
        console.log(`Socket Error: ${err}`);
        delete this.clientBuffers[getUid(socket)];
      });

      // Close event handler.
      socket.on(`close`, () => {
        console.log(`Disconnecting client: ${getUid(socket)}`);
        delete this.clientBuffers[getUid(socket)];
      });
    });

    // Start listening on the specified host and port.
    server.listen(this.port, this.host, () => {
      console.log(`Server Listening on ${this.host}:${this.port}`);
    });
  }

  /**
   * Loads the RDB file and parses its contents into the data store.
   */
  loadRDBFile() {
    if (!this.config) return;
    const filePath = path.join(this.config["dir"], this.config["dbFilename"]);
    if (!fs.existsSync(filePath)) return;
    const fileBuffer = fs.readFileSync(filePath);
    const rdbParser = new RDBParser(fileBuffer);
    rdbParser.parse();
    this.dataStore = rdbParser.dataStore;
  }

  /**
   * Processes buffered commands from a client socket.
   * @param {net.Socket} socket - The client socket.
   */
  processClientBuffer(socket) {
    const clientKey = getUid(socket);
    const buffer = this.clientBuffers[clientKey];
    const requestParser = new RequestParser(buffer);
    while (true) {
      const args = requestParser.parse(); // Parse arguments from the buffer.
      if (args.length === 0) break;
      const currentRequest = requestParser.currentRequest;
      this.handleCommand(socket, args, currentRequest); // Handle parsed commands.
    }

    // Store remaining buffer data back into the clientBuffers.
    this.clientBuffers[clientKey] = requestParser.getRemainingRequest();
  }

  /**
   * Handles commands received from clients.
   * @param {net.Socket} socket - The client socket.
   * @param {Array<string>} args - Arguments of the command.
   * @param {string} request - The raw request string.
   */
  handleCommand(socket, args, request) {
    const command = args[0].toLowerCase();
    switch (command) {
      case "ping":
        socket.write(this.handlePing());
        break;
      case "echo":
        socket.write(this.handleEcho(args.slice(1)));
        break;
      case "set":
        socket.write(this.handleSet(args.slice(1)));
        this.propagate(request);
        break;
      case "get":
        socket.write(this.handleGet(args.slice(1)));
        break;
      case "info":
        socket.write(this.handleInfo(args.slice(1)));
        break;
      case "replconf":
        this.handleReplconf(args.slice(1), socket);
        break;
      case "psync":
        socket.write(this.handlePsync(args.slice(1), socket));
        this.replicas[getUid(socket)] = { socket, state: "connected" }; // Register the replica
        break;
      case "wait":
        this.handleWait(args.slice(1), socket, request);
        break;
      case "config":
        socket.write(this.handleConfig(args.slice(1)));
        break;
      case "keys":
        socket.write(this.handleKeys(args.slice(1)));
        break;
      case "type":
        socket.write(this.handleType(args.slice(1)));
        break;
      case "xadd":
        this.handleXadd(args.slice(1), socket);
        break;
      case "xrange":
        socket.write(this.handleXrange(args.slice(1)));
        break;
      case "xread":
        this.handleXread(args.slice(1), socket);
        break;
    }
  }

  /**
   * Handles the 'ping' command by returning a standard response.
   * @returns {string} Encoded simple string "PONG".
   */
  handlePing() {
    return Encoder.createSimpleString("PONG");
  }

  /**
   * Handles the 'echo' command by returning the input argument as a bulk string.
   * @param {string[]} args - Array containing the string to echo.
   * @returns {string} Encoded bulk string of the echoed argument.
   */
  handleEcho(args) {
    return Encoder.createBulkString(args[0]);
  }

  /**
   * Handles the 'set' command to store a key-value pair, optionally with an expiry.
   * @param {string[]} args - Arguments containing the key, value, and optional expiry time.
   * @returns {string} Confirmation of the operation as an encoded simple string "OK".
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
    return Encoder.createSimpleString("OK");
  }

  /**
   * Handles the 'get' command to retrieve a value by key.
   * @param {string[]} args - Array containing the key.
   * @returns {string} The value associated with the key as a bulk string, or null bulk string if key not found.
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
   * Handles the 'info' command to provide server status information.
   * @param {string[]} args - Array containing the section to return information about.
   * @returns {string} Encoded bulk string containing the requested information.
   */
  handleInfo(args) {
    const section = args[0].toLowerCase();
    let response = "";
    if (section === "replication") {
      response = "role:master\n";
      response += `master_replid:${this.masterReplId}\n`;
      response += `master_repl_offset:${this.masterReplOffset}`;
    }
    return Encoder.createBulkString(response);
  }

  /**
   * Handles the 'replconf' command for replica configuration.
   * @param {string[]} args - Array containing configuration arguments.
   * @param {net.Socket} socket - The socket to which the response should be sent.
   */
  handleReplconf(args, socket) {
    const arg = args[0].toLowerCase();
    if (arg === "ack") {
      this.acknowledgeReplica(parseInt(args[1]));
    } else {
      socket.write(Encoder.createSimpleString("OK"));
    }
  }

  /**
   * Handles the 'psync' command for initializing synchronization with a replica.
   * @param {string[]} args - Array containing synchronization arguments.
   * @param {net.Socket} socket - The socket to which the response should be sent.
   * @returns {Buffer} Final buffer containing synchronization data.
   */
  handlePsync(args, socket) {
    socket.write(
      Encoder.createSimpleString(
        `FULLRESYNC ${this.masterReplId} ${this.masterReplOffset}`
      )
    );
    const emptyRDB =
      "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    const buffer = Buffer.from(emptyRDB, "hex");
    const finalBuffer = Buffer.concat([
      Buffer.from(`$${buffer.length}\r\n`),
      buffer,
    ]);
    return finalBuffer;
  }

  /**
   * Propagates a request to all connected replicas.
   * @param {string} request - The raw request string to be propagated.
   */
  propagate(request) {
    for (const replica of Object.values(this.replicas)) {
      const socket = replica.socket;
      socket.write(request);
    }
    this.masterReplOffset += request.length;
  }

  /**
   * Handles the 'wait' command for synchronization wait logic.
   * @param {string[]} args - Arguments containing number of required replicas and timeout.
   * @param {net.Socket} socket - The socket on which to perform the wait.
   * @param {string} request - The request associated with the wait.
   */
  handleWait(args, socket, request) {
    if (Object.keys(this.replicas).length === 0) {
      socket.write(Encoder.createInteger(0));
      return;
    }
    if (this.masterReplOffset === 0) {
      socket.write(Encoder.createInteger(Object.keys(this.replicas).length));
      return;
    }

    let numOfReqReplicas = args[0];
    let timeoutTime = args[1];

    // Register a wait
    this.wait = {};
    this.wait.numOfAckReplicas = 0;
    this.wait.numOfReqReplicas = numOfReqReplicas;
    this.wait.socket = socket;
    this.wait.isDone = false;
    this.wait.request = request;
    this.wait.timeout = setTimeout(() => {
      this.respondToWait();
    }, timeoutTime);

    for (const replica of Object.values(this.replicas)) {
      const socket = replica.socket;
      socket.write(
        Encoder.createArray([
          Encoder.createBulkString("REPLCONF"),
          Encoder.createBulkString("GETACK"),
          Encoder.createBulkString("*"),
        ])
      );
    }
  }

  /**
   * Responds to a wait request after the timeout or when conditions are met.
   */
  respondToWait() {
    clearTimeout(this.wait.timeout);
    this.masterReplOffset += this.wait.request.length;
    this.wait.socket.write(Encoder.createInteger(this.wait.numOfAckReplicas));
    this.wait.isDone = true;
  }

  /**
   * Acknowledges the reception of data from a replica up to the specified offset.
   * @param {number} replicaOffset - The offset up to which data has been received.
   */
  acknowledgeReplica(replicaOffset) {
    if (this.wait.isDone) return;
    if (replicaOffset >= this.masterReplOffset) {
      this.wait.numOfAckReplicas++;
      if (this.wait.numOfAckReplicas >= this.wait.numOfReqReplicas)
        this.respondToWait();
    }
  }

  /**
   * Handles the configuration based on the provided arguments.
   *
   * @param {Array} args - The arguments passed to the function.
   * @returns {Array} - An array containing the encoded configuration.
   */
  handleConfig(args) {
    const arg = args[1].toLowerCase();
    return Encoder.createArray([
      Encoder.createBulkString(arg),
      Encoder.createBulkString(this.config[arg]),
    ]);
  }

  /**
   * Handles the keys command.
   *
   * @param {Array} args - The arguments passed to the keys command.
   * @returns {string|Array} - The encoded response for the keys command.
   */
  handleKeys(args) {
    const key = args[0];
    if (key === "*") {
      const arr = this.dataStore.getAllKeys().map((value) => {
        return Encoder.createBulkString(value);
      });
      return Encoder.createArray(arr);
    } else {
      const value = this.dataStore.get(key);
      if (value === null) {
        return Encoder.createBulkString("", true);
      }
      return Encoder.createBulkString(value);
    }
  }

  /**
   * Handles the type command.
   *
   * @param {Array} args - The command arguments.
   * @returns {string} - The response string.
   */
  handleType(args) {
    const key = args[0];
    const type = this.dataStore.getType(key);
    if (type) return Encoder.createSimpleString(type);
    return Encoder.createSimpleString("none");
  }

  /**
   * Handles the XADD command by inserting a new entry into the specified stream.
   *
   * @param {Array} args - The arguments passed to the XADD command.
   * @param {Socket} socket - The socket object for communication.
   * @returns {void}
   */
  handleXadd(args, socket) {
    const streamKey = args[0];
    const streamEntry = {};
    const streamEntryId = args[1];
    streamEntry.id = streamEntryId;

    for (let i = 2; i < args.length; i += 2) {
      let entryKey = args[i];
      let entryValue = args[i + 1];
      streamEntry[entryKey] = entryValue;
    }

    if (streamEntryId === "0-0") {
      socket.write(
        Encoder.createSimpleError(
          "ERR The ID specified in XADD must be greater than 0-0"
        )
      );
      return;
    }

    const entryId = this.dataStore.insertStream(streamKey, streamEntry);
    if (entryId === null) {
      socket.write(
        Encoder.createSimpleError(
          "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
      );
      return;
    }

    socket.write(Encoder.createBulkString(entryId));
    this.checkBlock();
  }

  /**
   * Handles the XRANGE command.
   *
   * @param {Array} args - The arguments passed to the XRANGE command.
   * @returns {Array|BulkString} - The response to the XRANGE command.
   */
  handleXrange(args) {
    const streamKey = args[0];
    const startId = args[1];
    const endId = args[2];
    const entries = this.dataStore.getStreamBetween(streamKey, startId, endId);

    if (entries.length === 0) {
      return Encoder.createBulkString("nil");
    }

    const toReturn = [];
    for (const entry of entries) {
      const id = entry[0];
      const keyValues = entry[1];
      toReturn.push(
        Encoder.createArray([
          Encoder.createBulkString(id),
          Encoder.createArray(
            keyValues.map((value) => Encoder.createBulkString(value))
          ),
        ])
      );
    }

    return Encoder.createArray(toReturn);
  }

  /**
   * Handles the XREAD command.
   *
   * @param {Array} args - The arguments passed to the XREAD command.
   * @param {Socket} socket - The socket object for communication.
   * @returns {void}
   */
  handleXread(args, socket) {
    if (args[0].toLowerCase() !== 'block') {
      args = args.slice(1);
      const mid = Math.ceil(args.length / 2);
      const streamKeys = args.slice(0, mid);
      const startIds = args.slice(mid);
      const entries = this.dataStore.getStreamAfter(streamKeys, startIds);
      const response = this.getXreadResponse(entries);
      socket.write(response);
      return;
    }

    const timeoutTime = Number.parseInt(args[1], 10);
    args = args.slice(3);
    const mid = Math.ceil(args.length / 2);
    const streamKeys = args.slice(0, mid);
    startIds = args.slice(mid);
    startIds = this.processStartIds(streamKeys, startIds);
    this.block = { streamKeys, startIds, isDone: false };
    this.block.socket = socket;
    this.block.timeout = -1;
    if (timeoutTime !== 0) {
      this.block.timeout = setTimeout(() => {
        const entries = this.dataStore.getStreamAfter(this.block.streamKeys, this.block.startIds);
        const response = this.getXreadResponse(entries);
        this.block.socket.write(response);
        this.block.isDone = true;
      }, timeoutTime);
    }
    this.checkBlock();
  }

  /**
   * Returns the XREAD response for the given entries.
   *
   * @param {Array} entries - The entries to process.
   * @returns {Array|String} - The XREAD response.
   */
  getXreadResponse(entries) {
    if (entries.length === 0) {
      return Encoder.createBulkString('nil', true);
    }
    const ret = [];
    for (const keyEntries of entries) {
      const key = keyEntries[0];
      const arr = [Encoder.createBulkString(key)];
      const entriesForKey = [];
      for (const entries of keyEntries[1]) {
        const id = entries[0];
        const keyValues = entries[1];
        entriesForKey.push(
          Encoder.createArray([
            Encoder.createBulkString(id),
            Encoder.createArray(
              keyValues.map((value) => Encoder.createBulkString(value))
            ),
          ])
        );
      }
      arr.push(Encoder.createArray(entriesForKey));
      ret.push(Encoder.createArray(arr));
    }
    return Encoder.createArray(ret);
  }

  /**
   * Processes the start IDs for the given stream keys.
   *
   * @param {Array<string>} streamKeys - The array of stream keys.
   * @param {Array<string>} startIds - The array of start IDs.
   * @returns {Array<string>} - The updated array of start IDs.
   */
  processStartIds(streamKeys, startIds) {
    for (let i = 0; i < streamKeys.length; i++) {
      const key = streamKeys[i];
      let startId = startIds[i];
      if (startId !== '$') continue;

      const entries = this.dataStore.get(key);
      if (entries === null || entries.length === 0) startId = '0-0';
      const lastEntryId = entries.slice(-1)[0].id;
      const lastEntryIdMS = lastEntryId.split('-')[0];
      const lastEntryIdSeq = lastEntryId.split('-')[1];
      startId = lastEntryIdMS + '-' + `${Number.parseInt(lastEntryIdSeq)}`
      startIds[i] = startId
    }
    return startIds;
  }

  /**
   * Checks the block and sends the appropriate response if it is not done.
   */
  checkBlock() {
    if (!this.block || this.block.isDone) return;

    const entries = this.dataStore.getStreamAfter(
      this.block.streamKeys,
      this.block.startIds
    );

    if (entries.length === 0) return;

    const response = this.getXreadResponse(entries);
    this.block.socket.write(response);
    this.block.isDone = true;

    if (this.block.timeout !== -1) {
      clearTimeout(this.block.timeout);
    }
  }
}

module.exports = MasterServer;
