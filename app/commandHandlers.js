const { formatSimpleString, formatSimpleError, formatBulkString } = require('./utils');
const { replicationInfo } = require('./config');
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

module.exports = { handleEchoCommand, handleSetCommand, handleGetCommand, handleInfoCommand };
