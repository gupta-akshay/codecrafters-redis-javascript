const net = require('net');
const { globalConfig, replicationInfo } = require('./config');
const { cmdParser, getStringArray } = require('./utils');

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

module.exports = { configureReplication, replicaConnection };
