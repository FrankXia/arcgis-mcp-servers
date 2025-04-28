# arcgis-mcp-servers
This repo contains MCP servers for ArcGIS services

### Steps to create a new MCP server under `src` folder

1. `uv init <mcp-server-name>`, it will create a folder with given name and default to Python 3.12
2. `cd <mcp-server-name>`
3. `uv venv --python=python3.11 --seed`, change to Pyhon 3.11 to make it compatible with ArcGIS Python API 
4. `source .venv/bin/activate`
5. `uv pip install arcgis`, install ArcGIS Python API
6. `uv pip install "mcp[cli]"`, install MCP Python SDK

