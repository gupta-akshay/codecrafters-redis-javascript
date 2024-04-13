const net = require("net");

const cache = new Map();

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
      case '$':
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
        console.log('Warning: Unrecognized parameter type');
        return;
    }
  }
  return results;
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

// main code
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
      default:
        response = formatSimpleError(`Command ${command} not managed`);
    }
    connection.write(response);
  });
});

server.listen(6379, "127.0.0.1");
