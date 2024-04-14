const {
  cmdParser,
  formatSimpleString,
  formatSimpleError,
  formatBulkString,
  encodeArray,
  generateCommandToPropagate,
  sendMessage,
  updateBytesProcessed
} = require('./utils');
const { replicationInfo, globalConfig } = require('./config');
const cache = new Map();

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
  
  let key = commands.shift().toLowerCase();
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
  
  let key = commands.shift().toLowerCase();
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
      .filter(([key, value]) => key !== 'bytesProcessed')
      .map(([key, value]) => `${key}:${value}\r\n`)
      .join('');
    return formatBulkString(infoString);
  }
  return formatSimpleError('Invalid section specified');
}

function handleReplConfCommand(commands) {
  let subcommand = commands.shift().toLowerCase();
  if (subcommand === 'getack') {
    const offsetResponse = encodeArray(['REPLCONF', 'ACK', `${replicationInfo.bytesProcessed}`]);
    // replicationInfo.bytesProcessed = 0;
    return offsetResponse;
  }
  return formatSimpleString('OK');
}

function handlePsyncCommand(connection) {
  const response = formatSimpleString(`FULLRESYNC ${replicationInfo.master_replid} 0`);
  sendMessage(connection, response);
  const rdbFileBase64 = 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==';
  const rdbBuffer = Buffer.from(rdbFileBase64, 'base64');
  const rdbHead = Buffer.from(`$${rdbBuffer.length}\r\n`);
  sendMessage(connection, Buffer.concat([rdbHead, rdbBuffer]));
  globalConfig.REPLICAS.push(connection);
}

function replicateCommand(command, commands) {
  const commandsCopy = [...commands];
  globalConfig.REPLICAS.forEach(replica => {
    const commandToPropagate = generateCommandToPropagate(['*', command, ...commandsCopy]);
    sendMessage(replica, commandToPropagate);
  });
}

function processCommand(command, commands, connection, fromReplica) {
  switch(command) {
    case 'PING':
      return formatSimpleString('PONG');
    case 'ECHO':
      return handleEchoCommand(commands);
    case 'SET':
      const commandsCopy = [...commands];
      const response = handleSetCommand(commands);
      if (replicationInfo.role === 'master' && !fromReplica) {
        replicateCommand('SET', commandsCopy);
      }
      return response;
    case 'GET':
      return handleGetCommand(commands);
    case 'INFO':
      return handleInfoCommand(commands);
  }
}

function handleCommands(connection, data, fromReplica = false) {
  // Parsing incoming data into commands
  const commands = cmdParser(data);
  console.log(`\nReceived Commands: ${commands}`);

  // Extracting first command and converting it to uppercase
  let command = commands.shift().toUpperCase();
  let response = '';

  switch(command) {
    case 'PING':
    case 'ECHO':
    case 'SET':
    case 'GET':
    case 'INFO':
      response = processCommand(command, commands, connection, fromReplica);
      if (!fromReplica) sendMessage(connection, response);
      break;
    case 'REPLCONF':
      response = handleReplConfCommand(commands);
      sendMessage(connection, response);
      break;
    case 'PSYNC':
      handlePsyncCommand(connection);
      break;
    default:
      response = formatSimpleError(`Command ${command} not managed`);
  }

  // Update bytes processed at the entry point of command processing
  if (fromReplica && !command.includes('FULLRESYNC')) {
    updateBytesProcessed(data);
  }
}

module.exports = {
  handleEchoCommand,
  handleSetCommand,
  handleGetCommand,
  handleInfoCommand,
  handleCommands,
};
