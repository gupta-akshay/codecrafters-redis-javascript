const net = require("net");
const { globalConfig, args, replicationInfo, getPortFromArgs } = require('./config');
const { configureReplication, replicaConnection } = require('./replication');
const { handleCommands } = require('./commandHandlers');

globalConfig.PORT = getPortFromArgs(args);
configureReplication(args);
if (replicationInfo.role === 'slave') replicaConnection();

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    handleCommands(connection, data);
  });
});

server.listen(globalConfig.PORT, "127.0.0.1");
