## Esri Network Analysis  & Geocoding MCP Server

#### Build Docker image
`docker build -t  esri/arcgis-mcp-server/geocode-network-analysis -f Dockerfile .`

#### Test Geocoding MCP Server with Docker image 
`docker run -it --rm -e ArcGIS_MAPS_API_KEY="xxxxxxx" -p 8090:8090  esri/arcgis-mcp-server/geocode-network-analysis`

#### Test locally with 
`uv run uvicorn main:app --port 8090`

#### MCP Configure JSON for Claude Desktop App

To make it work, we need to use a proxy gateway to connect Claude App to a remote MCP server. I found this one works for us: https://github.com/supercorp-ai/supergateway

Here is the MCP Proxy Server configuration for Claude Desktop App: 

```json
{
  "mcpServers": {
    "Esri Network Analysis": {
      "command": "npx",
      "args": [
        "-y",
        "supergateway",
        "--sse",
        "https://remote-host-name/mcp/sse"
      ]
    }
  }
}
```

