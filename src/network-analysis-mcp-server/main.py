from fastapi import FastAPI
from server import mcp
from server import create_starlette_app

app = FastAPI()

# Mount the Starlette SSE server onto the FastAPI app
app.mount("/", create_starlette_app(mcp))
