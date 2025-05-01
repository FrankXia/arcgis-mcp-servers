from fastapi import FastAPI
from server import mcp
from server import create_starlette_app

app = FastAPI()

@app.get("/health")
async def health_check():
    """
    Health check endpoint for the MCP server.
    Returns a status message indicating the server is running.
    """
    return {"status": "OK"}

# Mount the Starlette SSE server onto the FastAPI app
app.mount("/", create_starlette_app(mcp, "/mcp"))
