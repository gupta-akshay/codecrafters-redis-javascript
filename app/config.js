const args = process.argv;

const globalConfig = {
  PORT: 0,
  MASTER_HOST: '',
  MASTER_PORT: 0,
}

const replicationInfo = {
  connected_slaves: 0,
  master_replid: '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
  master_repl_offset: 0
};

// Utility to find and parse the port from command-line arguments
function getPortFromArgs(args, defaultPort = 6379) {
  // looking for the '--port' argument in command line
  const portIndex = args.indexOf('--port');

  // check if '--port' is present and has a following argument
  if (portIndex !== -1 && args[portIndex + 1]) {
    return Number(args[portIndex + 1]);
  }

  // return default port if '--port' is not specified
  return defaultPort;
}

module.exports = { args, globalConfig, replicationInfo, getPortFromArgs };
