const { replicationInfo } = require('./config');

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
          if (actualString && actualString.length !== expectedLength) {
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

function parseCommandChunks(data) {
  let currentIndex = 0; // start at the beginning of the data string
  const commandChunks = []; // this will store each parsed command chunk

  // loop throught the entire string to find all command chunks
  while (currentIndex < data.length) {
    // find the start index of the next command, indicated by '*'
    const nextCommandStart = data.indexOf('*', currentIndex + 1);
    // determine the end of the current chunk: either the start of the next command chunk or the end of the data
    const currentChunkEnd = nextCommandStart === -1 ? data.length : nextCommandStart;
    // extract the command chunk from currentIndex to the determined end
    if (currentIndex !== currentChunkEnd) { // ensure that we do no include empty command
      commandChunks.push(data.substring(currentIndex, currentChunkEnd));
    }
    // move the currentIndex to the start of the next command
    // if no next command, break the loop by setting currentIndex to data.length
    currentIndex = nextCommandStart === -1 ? data.length : nextCommandStart;
  }

  if (commandChunks[commandChunks.length - 1] === '*\r\n') {
    commandChunks.pop();
    commandChunks[commandChunks.length - 1] = commandChunks[commandChunks.length - 1] + '*\r\n';
  }

  return commandChunks;
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

function generateCommandToPropagate(commands) {
  if (commands.length === 0) return '$-1\r\n';

  const formattedCommands = [];

  for (let i = 0; i < commands.length; i++) {
    const element = commands[i];

    if (element[0] === '+') {
      formattedCommands.push(element);
    } else if (element[0] === '*') {
      formattedCommands.push(`*${commands.length - 1}`);
    } else if (element[0] !== '$') {
      formattedCommands.push('$' + element.length.toString());
      formattedCommands.push(element);
    } else {
      formattedCommands.push(element);
      formattedCommands.push(commands[i + 1]);
      i++;
    }
  }

  return formattedCommands.join('\r\n') + '\r\n';
}

function encodeArray(data) {
  const payload = data.map(line => `$${line.length}\r\n${line}`).join('\r\n');
  return `*${data.length}\r\n${payload}\r\n`;
}

function sendMessage(connection, message) {
  if (connection && connection.write) {
    connection.write(message);
  }
}

function updateBytesProcessed(data) {
  const commandLength = Buffer.byteLength(data.toString(), 'utf8');
  replicationInfo.bytesProcessed += commandLength;
}

module.exports = {
  cmdParser,
  encodeArray,
  getStringArray,
  formatSimpleString,
  formatSimpleError,
  formatBulkString,
  generateCommandToPropagate,
  sendMessage,
  parseCommandChunks,
  updateBytesProcessed,
};
