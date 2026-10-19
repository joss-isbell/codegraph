// Exercise the public constructor/start seam after a caller changes its environment.
const { MCPServer } = require('../../dist/mcp');
const server = new MCPServer(process.argv[2]);
process.env.CODEGRAPH_QUERY_POOL_SIZE = '0';
process.env.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS = '45000';
server.start().catch(error => { console.error(error); process.exitCode = 1; });
