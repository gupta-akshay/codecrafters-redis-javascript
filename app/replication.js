const net = require('net');
const { globalConfig, replicationInfo } = require('./config');
const { cmdParser, getStringArray, sendMessage, parseCommandChunks } = require('./utils');
const { handleCommands } = require('./commandHandlers');

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
    sendMessage(replicaSocket, command);
  });

  replicaSocket.on('data', (data) => {
    const commands = cmdParser(data);
    console.log(`\nReponse to replica: ${commands}`);

    if (!commands) return;
    let command = commands.shift();

    if (!command) return;
    command = command.toUpperCase();

    switch (stage) {
      case 'PING':
        if (command === 'PONG') {
          const cmd = getStringArray('REPLCONF', 'listening-port', `${globalConfig.PORT}`);
          sendMessage(replicaSocket, cmd);
          stage = 'REPLCONF1';
        }
        break;
      case 'REPLCONF1':
        if (command === 'OK') {
          const cmd = getStringArray('REPLCONF', 'capa', 'psync2');
          sendMessage(replicaSocket, cmd);
          stage = 'REPLCONF2';
        }
        break;
      case 'REPLCONF2':
        if (command === 'OK') {
          const cmd = getStringArray('PSYNC', '?', '-1');
          sendMessage(replicaSocket, cmd);
          stage = 'PSYNC';
        }
        break;
      case 'PSYNC':
        if (command.indexOf('FULLRESYNC') > 0) {
          stage = 'SYNCDONE';
          break;
        }
      case 'SYNCDONE':
        // Now ready to receive propagated commands and process them without responding
        const requests = parseCommandChunks(data.toString());
        console.log('requests --', requests);
        requests.forEach(request => {
          handleCommands(replicaSocket, request, true);
        });
        break;
      default:
        return;
    }
  });

  // replicaSocket.on('error', (error) => {
  //   console.log('Error Occurred: ', error);
  // });
}

module.exports = { configureReplication, replicaConnection };
