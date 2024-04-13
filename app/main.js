const net = require("net");
const { globalConfig, args, replicationInfo, getPortFromArgs } = require('./config');
const { configureReplication, replicaConnection } = require('./replication');
const { cmdParser, formatSimpleString, formatSimpleError } = require('./utils');
const { handleEchoCommand, handleSetCommand, handleGetCommand, handleInfoCommand } = require('./commandHandlers');

globalConfig.PORT = getPortFromArgs(args);
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

    if (command === 'PSYNC') {
      const rdbFileBase64 = 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==';
      const rdbBuffer = Buffer.from(rdbFileBase64, 'base64');
      const rdbHead = Buffer.from(`$${rdbBuffer.length}\r\n`);
      connection.write(Buffer.concat([rdbHead, rdbBuffer]));
    }
  });
});

server.listen(globalConfig.PORT, "127.0.0.1");
