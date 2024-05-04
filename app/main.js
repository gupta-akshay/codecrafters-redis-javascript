const MasterServer = require('./MasterServer');
const SlaveServer = require('./SlaveServer');

const HOST = 'localhost';
const PORT = '6379';

function init(args) {
  if (args.length === 0) {
    const server = new MasterServer(HOST, PORT);
    server.startServer();
    return;
  }

  const flag = args[0];
  if (flag === '--port') {
    if (args.length === 2) {
      const port = args[1];
      const server = new MasterServer(HOST, port);
      return server.startServer();
    }

    if (args.length === 5) {
      const port = args[1];
      const replicaFlag = args[2];
      const masterHost = args[3];
      const masterPort = args[4];
      const server = new SlaveServer(HOST, port, masterHost, masterPort);
      return server.startServer();
    }
  }
}

init(process.argv.slice(2));
