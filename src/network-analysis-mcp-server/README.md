## Esri Network Analysis  & Geocoding MCP Server

#### Build Docker image
`docker build -t esri-mcp/network-analysis-mcp-server -f Dockerfile .`

#### Test Geocoding MCP Server with Docker image 
`docker run -it --rm -e ArcGIS_MAPS_API_KEY="xxxxxxx" -p 8090:8090 arcgis-mcp-server/geocode-network-analysis`

#### Test locally with 
`uv run uvicorn main:app --port 8090`