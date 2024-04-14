const {
  cmdParser,
  formatSimpleString,
  formatSimpleError,
  formatBulkString,
  generateCommandToPropagate,
  sendMessage
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
      .map(([key, value]) => `${key}:${value}\r\n`)
      .join('');
    return formatBulkString(infoString);
  }
  return formatSimpleError('Invalid section specified');
}

function handleCommands(connection, data) {
  // Parsing incoming data into commands
  const commands = cmdParser(data);
  console.log(`\nReceived Commands: ${commands}`);

  // Extracting first command and converting it to uppercase
  let command = commands.shift().toUpperCase();
  let response = '';

  switch(command) {
    case 'PING':
      response = formatSimpleString('PONG');
      sendMessage(connection, response);
      break;
    case 'ECHO':
      response = handleEchoCommand(commands);
      sendMessage(connection, response);
      break;
    case 'SET':
      const commandsCopy = [...commands];
      response = handleSetCommand(commands);
      sendMessage(connection, response);
      if (replicationInfo.role === 'master') {
        globalConfig.REPLICAS.forEach(replica => {
          const commandToPropagate = generateCommandToPropagate(['*', 'SET', ...commandsCopy]);
          sendMessage(replica, commandToPropagate);
        });
      }
      break;
    case 'GET':
      response = handleGetCommand(commands);
      sendMessage(connection, response);
      break;
    case 'INFO':
      response = handleInfoCommand(commands);
      sendMessage(connection, response);
      break;
    case 'REPLCONF':
      response = formatSimpleString('OK');
      sendMessage(connection, response);
      break;
    case 'PSYNC':
      response = formatSimpleString(`FULLRESYNC ${replicationInfo.master_replid} 0`);
      sendMessage(connection, response);
      const rdbFileBase64 = 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==';
      const rdbBuffer = Buffer.from(rdbFileBase64, 'base64');
      const rdbHead = Buffer.from(`$${rdbBuffer.length}\r\n`);
      sendMessage(connection, Buffer.concat([rdbHead, rdbBuffer]));
      globalConfig.REPLICAS.push(connection);
      break;
    default:
      response = formatSimpleError(`Command ${command} not managed`);
  }
}

module.exports = {
  handleEchoCommand,
  handleSetCommand,
  handleGetCommand,
  handleInfoCommand,
  handleCommands,
};
