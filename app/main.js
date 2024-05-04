const MasterServer = require('./MasterServer');
const SlaveServer = require('./SlaveServer');

// Default host and port configuration for the server.
const HOST = 'localhost';
const PORT = '6379';

/**
 * Initializes and starts a Master or Slave server based on the provided command line arguments.
 * 
 * @param {string[]} args - The command line arguments passed to the script.
 */
function init(args) {
  // Check if no arguments were provided and start a MasterServer on default port.
  if (args.length === 0) {
    const server = new MasterServer(HOST, PORT);
    server.startServer();
    return;
  }

  // First argument should be a flag indicating the operation mode or configuration.
  const flag = args[0];

  // Check for '--port' flag to configure the port.
  if (flag === '--port') {
    // If only port is provided, start a MasterServer on the specified port.
    if (args.length === 2) {
      const port = args[1];
      const server = new MasterServer(HOST, port);
      return server.startServer();
    }

    // If replication configuration is provided, start a SlaveServer with the specified settings.
    if (args.length === 5) {
      const port = args[1];
      const replicaFlag = args[2]; // Expected to be a specific flag, like '--replica'.
      const masterHost = args[3];
      const masterPort = args[4];
      const server = new SlaveServer(HOST, port, masterHost, masterPort);
      return server.startServer();
    }
  }
}

init(process.argv.slice(2));
