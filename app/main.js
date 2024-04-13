const net = require("net");

const args = process.argv;
const cache = new Map();

const globalConfig = {
  PORT: 0,
  MASTER_HOST: '',
  MASTER_PORT: 0,
}

const replicationInfo = {
  connected_slaves: 0,
  master_replid: '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
  master_repl_offset: 0
};

// Utility to find and parse the port from command-line arguments
function getportFromArgs(args, defaultPort = 6379) {
  // looking for the '--port' argument in command line
  const portIndex = args.indexOf('--port');

  // check if '--port' is present and has a following argument
  if (portIndex !== -1 && args[portIndex + 1]) {
    return Number(args[portIndex + 1]);
  }

  // return default port if '--port' is not specified
  return defaultPort;
}

globalConfig.PORT = getportFromArgs(args);

function configureReplication(args) {
  const replicaOfIndex = args.indexOf('--replicaof');

  if (replicaOfIndex !== -1 && args[replicaOfIndex + 1] && args[replicaOfIndex + 2]) {
    replicationInfo['role'] = 'slave';
    globalConfig.MASTER_HOST = args[replicaOfIndex + 1];
    globalConfig.MASTER_PORT = Number(args[replicaOfIndex + 2]);
  } else {
    replicationInfo['role'] = 'master';
  }
}

function replicaConnection() {
  let stage = 'PING';
  const replicaSocket = net.createConnection({
    host: globalConfig.MASTER_HOST,
    port: globalConfig.MASTER_PORT,
  });

  replicaSocket.on('connect', () => {
    console.log(`Connected to master at ${globalConfig.MASTER_HOST}:${globalConfig.MASTER_PORT}`);
    const command = getStringArray('ping');
    replicaSocket.write(command);
  });

  replicaSocket.on('data', (data) => {
    const commands = cmdParser(data);
    console.log(`\nReponse to replica: ${commands}`);

    let command = commands.shift().toUpperCase();

    switch (stage) {
      case 'PING':
        if (command === 'PONG') {
          const cmd = getStringArray('REPLCONF', 'listening-port', `${globalConfig.PORT}`);
          replicaSocket.write(cmd);
          stage = 'REPLCONF1';
        }
        break;
      case 'REPLCONF1':
        if (command === 'OK') {
          const cmd = getStringArray('REPLCONF', 'capa', 'psync2');
          replicaSocket.write(cmd);
          stage = 'REPLCONF2';
        }
        break;
      case 'REPLCONF2':
        if (command === 'OK') {
          const cmd = getStringArray('PSYNC', '?', '-1');
          replicaSocket.write(cmd);
          stage = 'PSYNC';
        }
        break;
      default:
        return;
    }
  });
}

// helper function to parse commands received
function cmdParser(data) {
  let currentParameter = '',
    results = [];

  // Convert input data to string and split it into lines, excluding the last empty line
  let dataLines = data.toString().split('\r\n').slice(0, -1);
  console.log(`\nParsed Data Lines: ${dataLines}`);

  // Extracting the first line which might specify the number of parameters to process
  currentParameter = dataLines.shift();
  let parameterCount = 1; // Default to processing one parameter if not specified

  // Check if the first parameter is '*', which means the line indicates the count of parameters
  if (currentParameter[0] !== '*') {
    // if it's not '*', consider it as regular data and re-add it to dataLines
    dataLines.unshift(currentParameter);
  } else {
    // extract the number from the line ('*3' means three parameters to process)
    parameterCount = Number(currentParameter.slice(1));
  }

  // process each parameter as specified by parameterCount
  for (let i = 0; i < parameterCount; i++) {
    currentParameter = dataLines.shift();
    if (!currentParameter) {
      console.log('Error: Missing Parameter');
      return;
    }
    switch (currentParameter[0]) {
      case '+': // simple string
        results.push(currentParameter.slice(1));
        break;
      case '$': // bulk string
        if (currentParameter.length < 2) {
          console.log('Error: Malformed parameter');
          return;
        }
        let expectedLength = Number(currentParameter.slice(1));
        if (expectedLength === -1) {
          results.push(null); // 'null' for Redis bulk strings that are null
          break;
        } else {
          let actualString = dataLines.shift();
          if (actualString.length !== expectedLength) {
            console.log('Error: String length mismatch');
            return;
          }
          results.push(actualString);
        }
        break;
      default:
        console.log(`Warning: Unrecognized parameter type - ${currentParameter[0]}`);
        return;
    }
  }
  return results;
}

function getBulkString(str) {
  if (str === null) {
    return `\$-1\r\n`;
  } else {
    return `\$${str.length}\r\n${str}\r\n`;
  }
}

function getStringArray(cmd) {
  const args = [...arguments];
  let result = '';
  result += `\*${args.length}\r\n`
  for (const arg of args) {
    result += getBulkString(arg);
  }
  return result;
}

function formatSimpleError(message) {
  return `-${message}\r\n`;
}

function formatSimpleString(message) {
  return `+${message}\r\n`;
}

function formatBulkString(message) {
  if (message === null) {
    return '$-1\r\n';
  }
  return `$${message.length}\r\n${message}\r\n`;
}

function handleEchoCommand(commands) {
  if (commands.length < 1) {
    return formatSimpleError('Syntax: ECHO message');
  }
  return formatBulkString(commands.shift());
}

function handleSetCommand(commands) {
  if (commands.length < 2) {
    return formatSimpleError('Syntax: SET key value [PX milliseconds]');
  }
  
  let key = commands.shift();
  let value = commands.shift();
  let pxTime = 0;

  while (commands.length > 0) {
    let parameter = commands.shift().toUpperCase();
    if (parameter === 'PX') {
      if (commands.length === 0) {
        return formatSimpleError('Syntax: SET key value [PX milliseconds]');
      }
      pxTime = Number(commands.shift());
    }
  }

  cache.set(key, value);
  if (pxTime > 0) {
    setTimeout(() => cache.delete(key), pxTime);
  }

  return formatSimpleString('OK');
}

function handleGetCommand(commands) {
  if (commands.length < 1) {
    return formatSimpleError('Syntax: GET key');
  }
  
  let key = commands.shift();
  if (cache.has(key)) {
    return formatBulkString(cache.get(key));
  }

  return formatBulkString(null);
}

function handleInfoCommand(commands) {
  if (commands.length < 1) {
    return formatSimpleError('Syntax: INFO [section]');
  }
  let section = commands.shift();
  if (section === 'replication') {
    let infoString = Object.entries(replicationInfo)
      .map(([key, value]) => `${key}:${value}\r\n`)
      .join('');
    return formatBulkString(infoString);
  }
  return formatSimpleError('Invalid section specified');
}

// main code
configureReplication(args);

if (replicationInfo.role === 'slave') replicaConnection();

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Parsing incoming data into commands
    const commands = cmdParser(data);
    console.log(`\nReceived Commands: ${commands}`);

    // Extracting first command and converting it to uppercase
    let command = commands.shift().toUpperCase();
    let response = '';

    switch(command) {
      case 'PING':
        response = formatSimpleString('PONG');
        break;
      case 'ECHO':
        response = handleEchoCommand(commands);
        break;
      case 'SET':
        response = handleSetCommand(commands);
        break;
      case 'GET':
        response = handleGetCommand(commands);
        break;
      case 'INFO':
        response = handleInfoCommand(commands);
        break;
      case 'REPLCONF':
        response = formatSimpleString('OK');
        break;
      case 'PSYNC':
        response = formatSimpleString(`FULLRESYNC ${replicationInfo.master_replid} 0`);
        break;
      default:
        response = formatSimpleError(`Command ${command} not managed`);
    }
    connection.write(response);
  });
});

server.listen(globalConfig.PORT, "127.0.0.1");
