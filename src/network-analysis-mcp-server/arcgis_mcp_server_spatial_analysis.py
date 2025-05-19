
import os
import json
import asyncio
from typing import Any, Dict, List, Union, Optional
import httpx
from fastapi import FastAPI, Request
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport # Ensure this is the correct import
from starlette.applications import Starlette
from starlette.routing import Mount, Route

# --- ArcGIS Analysis Helper Functions ---
# (These functions remain unchanged)
ARCGIS_BASE_URL = "https://www.arcgis.com/sharing/rest"
DEFAULT_ANALYSIS_SERVER_URL = "https://analysis3.arcgis.com/arcgis/rest/services/tasks/GPServer" # Placeholder

async def get_arcgis_token(username, password, portal_url=ARCGIS_BASE_URL):
    token_req_url = f"{portal_url}/generateToken"
    params = {
        "username": username,
        "password": password,
        "referer": "http://www.arcgis.com",
        "f": "json",
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(token_req_url, data=params)
            response.raise_for_status()
            token_response = response.json()
            if "token" in token_response:
                return token_response["token"]
            elif "error" in token_response:
                error_mess = token_response.get("error", {}).get("message", "Unknown error")
                if "This request needs to be made over https." in error_mess and portal_url.startswith("http://"):
                    secure_portal_url = portal_url.replace("http://", "https://")
                    return await get_arcgis_token(username, password, secure_portal_url)
                raise Exception(f"ArcGIS Token Error: {error_mess}")
            else:
                raise Exception("Failed to get ArcGIS token, unknown response.")
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error while getting token: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error getting ArcGIS token: {e}")

async def get_analysis_service_url(token: str, portal_url: str = ARCGIS_BASE_URL):
    portals_self_url = f"{portal_url}/portals/self?f=json&token={token}"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(portals_self_url)
            response.raise_for_status()
            portal_response = response.json()
            helper_services = portal_response.get("helperServices")
            if helper_services and "analysis" in helper_services:
                analysis_service_info = helper_services["analysis"]
                if "url" in analysis_service_info:
                    return analysis_service_info["url"]
            if portal_response.get("analysisServiceUrl"):
                 return portal_response["analysisServiceUrl"]
            print("Warning: Could not dynamically determine analysis server URL from /portals/self. Using default.")
            return DEFAULT_ANALYSIS_SERVER_URL
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error getting analysis URL: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error getting analysis URL: {e}")

async def submit_arcgis_job(analysis_task_url: str, task_params: Dict, token: str):
    submit_url = f"{analysis_task_url}/submitJob"
    params = {**task_params, "f": "json", "token": token}
    headers = {"Referer": "http://www.arcgis.com"}
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(submit_url, data=params, headers=headers)
            response.raise_for_status()
            job_submission_response = response.json()
            if "jobId" in job_submission_response:
                return job_submission_response
            else:
                error_info = job_submission_response.get("error", "Unknown error during job submission")
                raise Exception(f"ArcGIS Job Submission Error: {error_info}")
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error submitting job: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error submitting ArcGIS job: {e}")

async def check_arcgis_job_status(job_url: str, token: str):
    params = {"f": "json", "token": token}
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                response = await client.get(job_url, params=params)
                response.raise_for_status()
                job_status_response = response.json()
                status = job_status_response.get("jobStatus")
                print(f"Current Job Status: {status}")
                if status == "esriJobSucceeded":
                    return job_status_response
                elif status in ["esriJobFailed", "esriJobCancelled", "esriJobTimedOut"]:
                    error_messages = job_status_response.get("messages", [])
                    raise Exception(f"ArcGIS Job Error: {status}. Messages: {error_messages}")
                await asyncio.sleep(5)
            except httpx.HTTPStatusError as e:
                raise Exception(f"HTTP error checking job status: {e.response.status_code} - {e.response.text}")
            except Exception as e:
                raise Exception(f"Error checking ArcGIS job status: {e}")

async def get_arcgis_job_results(job_status_response: Dict, job_base_url: str, result_param_name: str, token: str):
    results = job_status_response.get("results")
    if not results or result_param_name not in results:
        raise Exception(f"Result parameter '{result_param_name}' not found in job response.")
    result_info = results[result_param_name]
    if "paramUrl" in result_info:
        result_fetch_url = f"{job_base_url}/{result_info['paramUrl']}?f=json&token={token}"
    elif "value" in result_info and isinstance(result_info["value"], dict) and "url" in result_info["value"]:
        result_fetch_url = f"{result_info['value']['url']}?f=json&token={token}"
    elif "value" in result_info:
         return result_info["value"]
    else:
        raise Exception("Could not determine URL to fetch job results.")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(result_fetch_url)
            response.raise_for_status()
            result_data = response.json()
            return result_data.get("value", result_data)
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error getting job results: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error getting ArcGIS job results: {e}")


# --- MCP Server Setup ---
mcp = FastMCP(
    "ArcGISAnalysisServer",
    description="MCP Server for ArcGIS Spatial Analysis Tools",
    version="0.1.0"
)

def create_sse_starlette_app(mcp_instance: FastMCP) -> Starlette:
    # ***** MODIFICATION HERE *****
    # SseServerTransport takes the message path prefix as a positional argument.
    # This path should be the *exact path from the host root* (e.g., /mcp/messages/)
    # that the transport will advertise to the client for POSTing messages.
    transport = SseServerTransport("/mcp/messages/") # Changed from path_prefix="/mcp/messages/"

    async def handle_sse_connection(request: Request):
        scope = request.scope
        receive = request.receive
        starlette_send = request._send
        async with transport.connect_sse(scope, receive, starlette_send) as streams:
            await mcp_instance._mcp_server.run(
                streams[0], streams[1], mcp_instance._mcp_server.create_initialization_options()
            )

    starlette_sse_app = Starlette(
        routes=[
            Route("/mcp/sse/", endpoint=handle_sse_connection), # Will be GET /mcp/sse/
            # This Mount path is relative to where starlette_sse_app is mounted ("/mcp").
            # To make the full path /mcp/messages/ for POSTs, this needs to be "/messages/".
            Mount("/mcp/messages/", app=transport.handle_post_message) # Will be POST /mcp/messages/
        ]
    )
    return starlette_sse_app

app = FastAPI()
sse_app = create_sse_starlette_app(mcp)
app.mount("/", sse_app) # Mount the Starlette app at /mcp

@app.get("/")
async def root():
    return {"message": "ArcGIS MCP Server with SSE is running. Connect via MCP client to /mcp/sse/"}

# --- MCP Tool Definition for Find Hot Spots ---
# (This tool definition remains unchanged)
@mcp.tool()
async def find_hot_spots(
    analysisLayer: Union[Dict, str],
    analysisField: str = None,
    divideByField: str = None,
    boundingPolygonLayer: Union[Dict, str] = None,
    aggregationPolygonLayer: Union[Dict, str] = None,
    shapeType: str = None,
    cellSize: float = None,
    cellSizeUnits: str = None,
    distanceBand: float = None,
    distanceBandUnits: str = None,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    # Manually added wrap for the generated mcp tool function (find_hot_spots)
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        find_hot_spots_internal(
            analysisLayer=analysisLayer,
            analysisField=analysisField,
            divideByField=divideByField,
            boundingPolygonLayer=boundingPolygonLayer,
            aggregationPolygonLayer=aggregationPolygonLayer,
            shapeType=shapeType,
            cellSize=cellSize,
            cellSizeUnits=cellSizeUnits,
            distanceBand=distanceBand,
            distanceBandUnits=distanceBandUnits,
            outputName=outputName,
            context=context,
        )
    )
    return results


async def find_hot_spots_internal(
    analysisLayer: Union[Dict, str],
    analysisField: str = None,
    divideByField: str = None,
    boundingPolygonLayer: Union[Dict, str] = None,
    aggregationPolygonLayer: Union[Dict, str] = None,
    shapeType: str = None,
    cellSize: float = None,
    cellSizeUnits: str = None,
    distanceBand: float = None,
    distanceBandUnits: str = None,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
             return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        find_hot_spots_task_url = f"{dynamic_analysis_url_base}/FindHotSpots"

        task_params = {
            "analysisLayer": json.dumps(analysisLayer) if isinstance(analysisLayer, dict) else analysisLayer,
        }
        if analysisField: task_params["analysisField"] = analysisField
        if divideByField: task_params["divideByField"] = divideByField
        if boundingPolygonLayer:
            task_params["boundingPolygonLayer"] = json.dumps(boundingPolygonLayer) if isinstance(boundingPolygonLayer, dict) else boundingPolygonLayer
        if aggregationPolygonLayer:
            task_params["aggregationPolygonLayer"] = json.dumps(aggregationPolygonLayer) if isinstance(aggregationPolygonLayer, dict) else aggregationPolygonLayer
        if shapeType: task_params["shapeType"] = shapeType
        if cellSize: task_params["cellSize"] = cellSize
        if cellSizeUnits: task_params["cellSizeUnits"] = cellSizeUnits
        if distanceBand: task_params["distanceBand"] = distanceBand
        if distanceBandUnits: task_params["distanceBandUnits"] = distanceBandUnits
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context: task_params["context"] = json.dumps(context)

        print(f"Submitting Find Hot Spots job to: {find_hot_spots_task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(find_hot_spots_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{find_hot_spots_task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")
        results = await get_arcgis_job_results(final_job_status, job_status_url, "hotSpotsResultLayer", token)
        return {"status": "success", "jobId": job_id, "results": results}
    except Exception as e:
        print(f"Error in find_hot_spots tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# MCP Tool Definition for Calculate Density
@mcp.tool()
async def calculate_density(
        inputLayer: Union[Dict, str],  # Required [cite: 163]
        field: str = None,  # Optional [cite: 166]
        cellSize: float = None,  # Optional [cite: 166]
        cellSizeUnits: str = None,  # Required if cellSize is set [cite: 166] (Miles, Feet, Kilometers, Meters)
        radius: float = None,  # Optional [cite: 166]
        radiusUnits: str = None,  # Required if radius is set [cite: 169] (Miles, Feet, Kilometers, Meters)
        boundingPolygonLayer: Union[Dict, str] = None,  # Optional [cite: 169]
        areaUnits: str = None,  # Optional [cite: 169] (SquareMiles, SquareKilometers)
        classificationType: str = "EqualInterval",  # Optional, default EqualInterval [cite: 172]
        numClasses: int = 10,  # Optional, default 10, max 32 [cite: 172]
        outputName: str = None,  # Optional [cite: 174, 175]
        context: Dict = None,  # Optional [cite: 180]
) -> Dict:
    """
    Creates a density map from point or line features. [cite: 147]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        calculate_density_internal(
            inputLayer=inputLayer,
            field=field,
            cellSize=cellSize,
            cellSizeUnits=cellSizeUnits,
            radius=radius,
            radiusUnits=radiusUnits,
            boundingPolygonLayer=boundingPolygonLayer,
            areaUnits=areaUnits,
            classificationType=classificationType,
            numClasses=numClasses,
            outputName=outputName,
            context=context
        )
    )
    return results


async def calculate_density_internal(
        inputLayer: Union[Dict, str],
        field: str = None,
        cellSize: float = None,
        cellSizeUnits: str = None,
        radius: float = None,
        radiusUnits: str = None,
        boundingPolygonLayer: Union[Dict, str] = None,
        areaUnits: str = None,
        classificationType: str = "EqualInterval",
        numClasses: int = 10,
        outputName: str = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        calculate_density_task_url = f"{dynamic_analysis_url_base}/CalculateDensity"  # [cite: 162]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "classificationType": classificationType,  # [cite: 172]
            "numClasses": numClasses,  # [cite: 172]
        }

        if field:  # [cite: 166]
            task_params["field"] = field
        if cellSize:  # [cite: 166]
            task_params["cellSize"] = cellSize
            if not cellSizeUnits:
                return {"error": "cellSizeUnits is required if cellSize is specified."}
            task_params["cellSizeUnits"] = cellSizeUnits  # [cite: 166]
        if radius:  # [cite: 166]
            task_params["radius"] = radius
            if radiusUnits:  # radiusUnits is optional if radius is set, but good practice to set. Default depends on profile. [cite: 169]
                task_params["radiusUnits"] = radiusUnits  # [cite: 169]
        if boundingPolygonLayer:  # [cite: 169]
            task_params["boundingPolygonLayer"] = json.dumps(boundingPolygonLayer) if isinstance(boundingPolygonLayer,
                                                                                                 dict) else boundingPolygonLayer
        if areaUnits:  # [cite: 169]
            task_params["areaUnits"] = areaUnits

        if outputName:  # [cite: 174, 175]
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and (
                    "itemId" in outputName and "overwrite" in outputName):  # For overwriting [cite: 177]
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {
                    "error": "outputName format is invalid. Provide a string for new service or a dict with itemId and overwrite keys."}

        if context:  # [cite: 180]
            task_params["context"] = json.dumps(context)

        print(f"Submitting Calculate Density job to: {calculate_density_task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(calculate_density_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{calculate_density_task_url}/jobs/{job_id}"  # [cite: 187]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")

        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Calculate Density is "resultLayer" [cite: 189]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "resultLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in calculate_density tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# MCP Tool Definition for Calculate Composite Index
@mcp.tool()
async def calculate_composite_index(
        inputLayer: Union[Dict, str],  # Required [cite: 114, 115]
        inputVariables: List[Dict],  # Required, list of dicts with "field", "reverseVariable", "weight" [cite: 117]
        indexMethod: str = "meanScaled",  # Optional, default "meanScaled" [cite: 117, 120]
        outputIndexReverse: bool = False,  # Optional, default False [cite: 120, 123]
        outputIndexMinMax: List[Dict] = None,  # Optional, e.g. [{"min": 0, "max": 100}] [cite: 123]
        outputName: str = None,  # Optional [cite: 123]
        context: Dict = None,  # Optional [cite: 126, 127]
) -> Dict:
    """
    Combines multiple numeric variables to create a single index. [cite: 112]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        calculate_composite_index_internal(
            inputLayer=inputLayer,
            inputVariables=inputVariables,
            indexMethod=indexMethod,
            outputIndexReverse=outputIndexReverse,
            outputIndexMinMax=outputIndexMinMax,
            outputName=outputName,
            context=context
        )
    )
    return results


async def calculate_composite_index_internal(
        inputLayer: Union[Dict, str],
        inputVariables: List[Dict],
        indexMethod: str = "meanScaled",
        outputIndexReverse: bool = False,
        outputIndexMinMax: List[Dict] = None,
        outputName: str = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        calculate_composite_index_task_url = f"{dynamic_analysis_url_base}/CalculateCompositeIndex"  # [cite: 113]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "inputVariables": json.dumps(inputVariables),  # [cite: 117]
            "indexMethod": indexMethod,  # [cite: 117, 120]
            "outputIndexReverse": str(outputIndexReverse).lower(),  # [cite: 120, 123]
        }

        if outputIndexMinMax:  # [cite: 123]
            task_params["outputIndexMinMax"] = json.dumps(outputIndexMinMax)

        if outputName:  # [cite: 123]
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and (
                    "itemId" in outputName and "overwrite" in outputName):  # For overwriting [cite: 123]
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {
                    "error": "outputName format is invalid. Provide a string for new service or a dict with itemId and overwrite keys."}

        if context:  # [cite: 126, 127]
            task_params["context"] = json.dumps(context)

        print(f"Submitting Calculate Composite Index job to: {calculate_composite_index_task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(calculate_composite_index_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{calculate_composite_index_task_url}/jobs/{job_id}"  # [cite: 134]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")

        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Calculate Composite Index is "indexResultLayer" [cite: 136]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "indexResultLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in calculate_composite_index tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# MCP Tool Definition for Aggregate Points
@mcp.tool()
async def aggregate_points(
        pointLayer: Union[Dict, str],  # Required [cite: 74]
        polygonLayer: Union[Dict, str] = None,  # Required unless binType is specified [cite: 74]
        keepBoundariesWithNoPoints: bool = True,  # [cite: 77]
        summaryFields: List[str] = None,  # [cite: 77]
        groupByField: str = None,  # [cite: 77]
        minorityMajority: bool = False,  # [cite: 80]
        percentPoints: bool = False,  # [cite: 80]
        outputName: str = None,  # [cite: 80]
        context: Dict = None,  # [cite: 86]
        binType: str = None,  # [cite: 86] (Square or Hexagon)
        binSize: float = None,  # Required if binType is used [cite: 89]
        binSizeUnit: str = None,
        # Required if binType is used [cite: 89] (Meters, Kilometers, Feet, Miles, NauticalMiles, Yards)
) -> Dict:
    """
    Aggregates point features within specified polygon features or bins. [cite: 67, 68]
    It calculates statistics for all points within each polygon or bin. [cite: 69]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        aggregate_points_internal(
            pointLayer=pointLayer,
            polygonLayer=polygonLayer,
            keepBoundariesWithNoPoints=keepBoundariesWithNoPoints,
            summaryFields=summaryFields,
            groupByField=groupByField,
            minorityMajority=minorityMajority,
            percentPoints=percentPoints,
            outputName=outputName,
            context=context,
            binType=binType,
            binSize=binSize,
            binSizeUnit=binSizeUnit
        )
    )
    return results


async def aggregate_points_internal(
        pointLayer: Union[Dict, str],
        polygonLayer: Union[Dict, str] = None,
        keepBoundariesWithNoPoints: bool = True,
        summaryFields: List[str] = None,
        groupByField: str = None,
        minorityMajority: bool = False,
        percentPoints: bool = False,
        outputName: str = None,
        context: Dict = None,
        binType: str = None,
        binSize: float = None,
        binSizeUnit: str = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL. Using default or check configuration."}

        # Ensure dynamic_analysis_url_base does not end with a slash for clean joining
        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        aggregate_points_task_url = f"{dynamic_analysis_url_base}/AggregatePoints"  # [cite: 73]

        task_params = {
            "pointLayer": json.dumps(pointLayer) if isinstance(pointLayer, dict) else pointLayer,
        }

        if polygonLayer:
            task_params["polygonLayer"] = json.dumps(polygonLayer) if isinstance(polygonLayer, dict) else polygonLayer
        elif not binType:  # polygonLayer is required if binType is not specified [cite: 74]
            return {"error": "Either polygonLayer or binType must be specified."}

        task_params["keepBoundariesWithNoPoints"] = str(
            keepBoundariesWithNoPoints).lower()  # [cite: 77] Boolean as string

        if summaryFields:  # [cite: 77]
            task_params["summaryFields"] = json.dumps(summaryFields)  # e.g. ["fieldName summaryType"]

        if groupByField:  # [cite: 77]
            task_params["groupByField"] = groupByField
            task_params["minorityMajority"] = str(minorityMajority).lower()  # [cite: 80]
            task_params["percentPoints"] = str(percentPoints).lower()  # [cite: 80]

        if binType:  # [cite: 86]
            task_params["binType"] = binType
            if binSize is None or binSizeUnit is None:
                return {"error": "binSize and binSizeUnit are required if binType is specified."}
            task_params["binSize"] = binSize  # [cite: 89]
            task_params["binSizeUnit"] = binSizeUnit  # [cite: 89]

        if outputName:  # [cite: 80, 83]
            # Check if outputName is a simple string for service creation or a dict for overwrite
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and (
                    "itemId" in outputName and "overwrite" in outputName):  # For overwriting [cite: 83]
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {
                    "error": "outputName format is invalid. Provide a string for new service or a dict with itemId and overwrite keys."}

        if context:  # [cite: 86]
            task_params["context"] = json.dumps(context)

        print(f"Submitting Aggregate Points job to: {aggregate_points_task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(aggregate_points_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        # job_status_url = f"{aggregate_points_task_url}/jobs/{job_id}" # This was in template
        # Per documentation, job URL for status checking is part of job_submission_info or constructed with base and jobId
        # The example shows: http://<analysis url>/AggregatePoints/jobs/<jobId> [cite: 94]
        job_status_url = f"{aggregate_points_task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")

        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Aggregate Points is "aggregatedLayer" [cite: 96]
        # If groupByField is used, an additional "groupSummary" table is created [cite: 99]
        results = {}
        aggregated_layer_results = await get_arcgis_job_results(final_job_status, job_status_url, "aggregatedLayer",
                                                                token)
        results["aggregatedLayer"] = aggregated_layer_results

        if groupByField and "groupSummary" in final_job_status.get("results", {}):
            group_summary_results = await get_arcgis_job_results(final_job_status, job_status_url, "groupSummary",
                                                                 token)
            results["groupSummary"] = group_summary_results

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in aggregate_points tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# Assume ArcGIS helper functions (get_arcgis_token, get_analysis_service_url,
# submit_arcgis_job, check_arcgis_job_status, get_arcgis_job_results)
# are defined elsewhere in your server script as per the template.

@mcp.tool()
async def choose_best_facilities(
        goal: str,
        # Required: Allocate, MinimizeImpedance, MaximizeCoverage, MaximizeCapacitatedCoverage, PercentCoverage
        demandLocationsLayer: Union[Dict, str],  # Required
        travelMode: Union[Dict, str],  # Required (JSON object for travel mode or string name if preconfigured)
        maxTravelRange: float = None,
        maxTravelRangeUnits: str = None,  # Seconds, Minutes, Hours, Days, Meters, Kilometers, Feet, Yards, Miles
        demand: float = 1.0,
        demandField: str = None,
        maxTravelRangeField: str = None,
        timeOfDay: int = None,  # Milliseconds since Unix epoch
        timeZoneForTimeOfDay: str = "GeoLocal",  # GeoLocal | UTC
        travelDirection: str = "FacilityToDemand",  # FacilityToDemand | DemandToFacility
        requiredFacilitiesLayer: Union[Dict, str] = None,  # Required if goal is Allocate
        requiredFacilitiesCapacity: float = None,
        requiredFacilitiesCapacityField: str = None,
        candidateFacilitiesLayer: Union[Dict, str] = None,  # Required for all goals except Allocate
        candidateCount: int = None,
        candidateFacilitiesCapacity: float = None,
        candidateFacilitiesCapacityField: str = None,
        percentDemandCoverage: float = None,  # Required if goal is PercentCoverage
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    """
    Finds the set of facilities that will best serve demand from surrounding areas.
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        choose_best_facilities_internal(
            goal=goal,
            demandLocationsLayer=demandLocationsLayer,
            travelMode=travelMode,
            maxTravelRange=maxTravelRange,
            maxTravelRangeUnits=maxTravelRangeUnits,
            demand=demand,
            demandField=demandField,
            maxTravelRangeField=maxTravelRangeField,
            timeOfDay=timeOfDay,
            timeZoneForTimeOfDay=timeZoneForTimeOfDay,
            travelDirection=travelDirection,
            requiredFacilitiesLayer=requiredFacilitiesLayer,
            requiredFacilitiesCapacity=requiredFacilitiesCapacity,
            requiredFacilitiesCapacityField=requiredFacilitiesCapacityField,
            candidateFacilitiesLayer=candidateFacilitiesLayer,
            candidateCount=candidateCount,
            candidateFacilitiesCapacity=candidateFacilitiesCapacity,
            candidateFacilitiesCapacityField=candidateFacilitiesCapacityField,
            percentDemandCoverage=percentDemandCoverage,
            outputName=outputName,
            pointBarrierLayer=pointBarrierLayer,
            lineBarrierLayer=lineBarrierLayer,
            polygonBarrierLayer=polygonBarrierLayer,
            context=context,
        )
    )
    return results


async def choose_best_facilities_internal(
        goal: str,
        demandLocationsLayer: Union[Dict, str],
        travelMode: Union[Dict, str],
        maxTravelRange: float = None,
        maxTravelRangeUnits: str = None,
        demand: float = 1.0,
        demandField: str = None,
        maxTravelRangeField: str = None,
        timeOfDay: int = None,
        timeZoneForTimeOfDay: str = "GeoLocal",
        travelDirection: str = "FacilityToDemand",
        requiredFacilitiesLayer: Union[Dict, str] = None,
        requiredFacilitiesCapacity: float = None,
        requiredFacilitiesCapacityField: str = None,
        candidateFacilitiesLayer: Union[Dict, str] = None,
        candidateCount: int = None,
        candidateFacilitiesCapacity: float = None,
        candidateFacilitiesCapacityField: str = None,
        percentDemandCoverage: float = None,
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/ChooseBestFacilities"

        task_params = {
            "goal": goal,
            "demandLocationsLayer": json.dumps(demandLocationsLayer) if isinstance(demandLocationsLayer,
                                                                                   dict) else demandLocationsLayer,
            "travelMode": json.dumps(travelMode) if isinstance(travelMode, dict) else travelMode,
        }

        if maxTravelRange is not None:
            task_params["maxTravelRange"] = maxTravelRange
        if maxTravelRangeUnits:
            task_params["maxTravelRangeUnits"] = maxTravelRangeUnits
        if demand is not None:  # Default is 1.0, so allow explicit setting
            task_params["demand"] = demand
        if demandField:
            task_params["demandField"] = demandField
        if maxTravelRangeField:
            task_params["maxTravelRangeField"] = maxTravelRangeField
        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay
        if timeZoneForTimeOfDay:
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay
        if travelDirection:
            task_params["travelDirection"] = travelDirection

        if requiredFacilitiesLayer:
            task_params["requiredFacilitiesLayer"] = json.dumps(requiredFacilitiesLayer) if isinstance(
                requiredFacilitiesLayer, dict) else requiredFacilitiesLayer
        if requiredFacilitiesCapacity is not None:
            task_params["requiredFacilitiesCapacity"] = requiredFacilitiesCapacity
        if requiredFacilitiesCapacityField:
            task_params["requiredFacilitiesCapacityField"] = requiredFacilitiesCapacityField

        if candidateFacilitiesLayer:
            task_params["candidateFacilitiesLayer"] = json.dumps(candidateFacilitiesLayer) if isinstance(
                candidateFacilitiesLayer, dict) else candidateFacilitiesLayer
        if candidateCount is not None:
            task_params["candidateCount"] = candidateCount
        if candidateFacilitiesCapacity is not None:
            task_params["candidateFacilitiesCapacity"] = candidateFacilitiesCapacity
        if candidateFacilitiesCapacityField:
            task_params["candidateFacilitiesCapacityField"] = candidateFacilitiesCapacityField

        if percentDemandCoverage is not None:
            task_params["percentDemandCoverage"] = percentDemandCoverage

        if pointBarrierLayer:
            task_params["pointBarrierLayer"] = json.dumps(pointBarrierLayer) if isinstance(pointBarrierLayer,
                                                                                           dict) else pointBarrierLayer
        if lineBarrierLayer:
            task_params["lineBarrierLayer"] = json.dumps(lineBarrierLayer) if isinstance(lineBarrierLayer,
                                                                                         dict) else lineBarrierLayer
        if polygonBarrierLayer:
            task_params["polygonBarrierLayer"] = json.dumps(polygonBarrierLayer) if isinstance(polygonBarrierLayer,
                                                                                               dict) else polygonBarrierLayer

        if outputName:
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context)

        print(f"Submitting Choose Best Facilities job to: {task_url}")
        print(
            f"With parameters: {json.dumps({k: v for k, v in task_params.items() if k != 'travelMode'}, indent=2)}")  # Avoid printing large travelMode

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        results_data = {}
        # Result parameters: allocatedDemandLocationsLayer, allocationLinesLayer, assignedFacilitiesLayer
        param_names = ["allocatedDemandLocationsLayer", "allocationLinesLayer", "assignedFacilitiesLayer"]
        for param_name in param_names:
            if param_name in final_job_status.get("results", {}):
                results_data[param_name] = await get_arcgis_job_results(final_job_status, job_status_url, param_name,
                                                                        token)

        return {"status": "success", "jobId": job_id, "results": results_data}

    except Exception as e:
        print(f"Error in choose_best_facilities tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def connect_origins_to_destinations(
        originsLayer: Union[Dict, str],  # Required
        destinationsLayer: Union[Dict, str],  # Required
        measurementType: Union[str, Dict],  # Required: "StraightLine" or travel mode JSON object
        originsLayerRouteIDField: str = None,
        destinationsLayerRouteIDField: str = None,
        timeOfDay: int = None,  # Milliseconds since Unix epoch
        timeZoneForTimeOfDay: str = "GeoLocal",  # GeoLocal | UTC
        includeRouteLayers: bool = False,
        routeShape: str = "FollowStreets",  # FollowStreets | StraightLine (only if measurementType is a travel mode)
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    """
    Measures the travel time or distance between pairs of origin and destination points.
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        connect_origins_to_destinations_internal(
            originsLayer=originsLayer,
            destinationsLayer=destinationsLayer,
            measurementType=measurementType,
            originsLayerRouteIDField=originsLayerRouteIDField,
            destinationsLayerRouteIDField=destinationsLayerRouteIDField,
            timeOfDay=timeOfDay,
            timeZoneForTimeOfDay=timeZoneForTimeOfDay,
            includeRouteLayers=includeRouteLayers,
            routeShape=routeShape,
            outputName=outputName,
            pointBarrierLayer=pointBarrierLayer,
            lineBarrierLayer=lineBarrierLayer,
            polygonBarrierLayer=polygonBarrierLayer,
            context=context,
        )
    )
    return results


async def connect_origins_to_destinations_internal(
        originsLayer: Union[Dict, str],
        destinationsLayer: Union[Dict, str],
        measurementType: Union[str, Dict],
        originsLayerRouteIDField: str = None,
        destinationsLayerRouteIDField: str = None,
        timeOfDay: int = None,
        timeZoneForTimeOfDay: str = "GeoLocal",
        includeRouteLayers: bool = False,
        routeShape: str = "FollowStreets",
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/ConnectOriginsToDestinations"

        task_params = {
            "originsLayer": json.dumps(originsLayer) if isinstance(originsLayer, dict) else originsLayer,
            "destinationsLayer": json.dumps(destinationsLayer) if isinstance(destinationsLayer,
                                                                             dict) else destinationsLayer,
            "measurementType": json.dumps(measurementType) if isinstance(measurementType, dict) else measurementType,
            "includeRouteLayers": str(includeRouteLayers).lower(),
        }

        if originsLayerRouteIDField:
            task_params["originsLayerRouteIDField"] = originsLayerRouteIDField
        if destinationsLayerRouteIDField:
            task_params["destinationsLayerRouteIDField"] = destinationsLayerRouteIDField
        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay
        if timeZoneForTimeOfDay:  # Default is GeoLocal
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay
        if isinstance(measurementType, dict):  # routeShape is only applicable for travel modes
            task_params["routeShape"] = routeShape

        if pointBarrierLayer:
            task_params["pointBarrierLayer"] = json.dumps(pointBarrierLayer) if isinstance(pointBarrierLayer,
                                                                                           dict) else pointBarrierLayer
        if lineBarrierLayer:
            task_params["lineBarrierLayer"] = json.dumps(lineBarrierLayer) if isinstance(lineBarrierLayer,
                                                                                         dict) else lineBarrierLayer
        if polygonBarrierLayer:
            task_params["polygonBarrierLayer"] = json.dumps(polygonBarrierLayer) if isinstance(polygonBarrierLayer,
                                                                                               dict) else polygonBarrierLayer

        if outputName:
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context)

        print(f"Submitting Connect Origins to Destinations job to: {task_url}")
        print(
            f"With parameters: {json.dumps({k: v for k, v in task_params.items() if k != 'measurementType' or isinstance(v, str)}, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        results_data = {}
        # Main output: routesLayer. Optional: unassignedOriginsLayer, unassignedDestinationsLayer
        param_names = ["routesLayer", "unassignedOriginsLayer", "unassignedDestinationsLayer"]
        if includeRouteLayers and "outputRouteLayers" in final_job_status.get("results",
                                                                              {}):  # Special name for created items
            param_names.append("outputRouteLayers")

        for param_name in param_names:
            if param_name in final_job_status.get("results", {}):
                try:  # Gracefully handle if a layer isn't present (e.g. no unassigned)
                    results_data[param_name] = await get_arcgis_job_results(final_job_status, job_status_url,
                                                                            param_name, token)
                except Exception as e:
                    print(f"Could not fetch result for {param_name}: {e}")

        return {"status": "success", "jobId": job_id, "results": results_data}

    except Exception as e:
        print(f"Error in connect_origins_to_destinations tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_buffers(
        inputLayer: Union[Dict, str],  # Required
        distances: List[float] = None,  # Required if Field is not provided
        field: str = None,  # Required if Distances is not provided
        units: str = "Meters",  # Meters, Kilometers, Feet, Miles, NauticalMiles, Yards
        dissolveType: str = "None",  # None | Dissolve
        ringType: str = "Disks",  # Disks | Rings (for multiple distances)
        sideType: str = "Full",  # Full | Right | Left | Outside (single distance only)
        endType: str = "Round",  # Round | Flat (for line inputs)
        outputName: str = None,
        context: Dict = None,
) -> Dict:
    """
    Creates buffer polygons around input features at specified distances.
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_buffers_internal(
            inputLayer=inputLayer,
            distances=distances,
            field=field,
            units=units,
            dissolveType=dissolveType,
            ringType=ringType,
            sideType=sideType,
            endType=endType,
            outputName=outputName,
            context=context,
        )
    )
    return results


async def create_buffers_internal(
        inputLayer: Union[Dict, str],
        distances: List[float] = None,
        field: str = None,
        units: str = "Meters",
        dissolveType: str = "None",
        ringType: str = "Disks",
        sideType: str = "Full",
        endType: str = "Round",
        outputName: str = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateBuffers"  #

        if not distances and not field:
            return {"error": "Either 'distances' or 'field' parameter must be provided."}
        if distances and field:
            return {"error": "Provide either 'distances' or 'field', not both."}

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "units": units,
        }

        if distances:
            task_params["distances"] = json.dumps(distances)  # List of doubles
        if field:
            task_params["field"] = field

        if dissolveType:  # Default is None
            task_params["dissolveType"] = dissolveType
        if ringType:  # Default is Disks
            task_params["ringType"] = ringType

        # sideType and endType have defaults Full and Round
        task_params["sideType"] = sideType
        task_params["endType"] = endType

        if outputName:
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context)

        print(f"Submitting Create Buffers job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Create Buffers is "bufferLayer"
        results = await get_arcgis_job_results(final_job_status, job_status_url, "bufferLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in create_buffers tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_drive_time_areas(
        inputLayer: Union[Dict, str],  # Required
        breakValues: List[float],  # Required, e.g., [5.0, 10.0, 15.0]
        breakUnits: str,  # Required: Seconds, Minutes, Hours, Feet, Yards, Meters, Kilometers, Miles
        travelMode: Union[Dict, str],  # Required: JSON object for travel mode settings
        timeOfDay: int = None,  # Milliseconds since Unix epoch
        timeZoneForTimeOfDay: str = "GeoLocal",  # GeoLocal | UTC
        overlapPolicy: str = "Overlap",  # Overlap | Dissolve | Split
        travelDirection: str = "AwayFromFacility",  # AwayFromFacility | TowardsFacility
        showHoles: bool = False,
        includeReachableStreets: bool = False,
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    """
    Creates areas that can be reached within a given drive time or drive distance.
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_drive_time_areas_internal(
            inputLayer=inputLayer,
            breakValues=breakValues,
            breakUnits=breakUnits,
            travelMode=travelMode,
            timeOfDay=timeOfDay,
            timeZoneForTimeOfDay=timeZoneForTimeOfDay,
            overlapPolicy=overlapPolicy,
            travelDirection=travelDirection,
            showHoles=showHoles,
            includeReachableStreets=includeReachableStreets,
            outputName=outputName,
            pointBarrierLayer=pointBarrierLayer,
            lineBarrierLayer=lineBarrierLayer,
            polygonBarrierLayer=polygonBarrierLayer,
            context=context,
        )
    )
    return results


async def create_drive_time_areas_internal(
        inputLayer: Union[Dict, str],
        breakValues: List[float],
        breakUnits: str,
        travelMode: Union[Dict, str],
        timeOfDay: int = None,
        timeZoneForTimeOfDay: str = "GeoLocal",
        overlapPolicy: str = "Overlap",
        travelDirection: str = "AwayFromFacility",
        showHoles: bool = False,
        includeReachableStreets: bool = False,
        outputName: str = None,
        pointBarrierLayer: Union[Dict, str] = None,
        lineBarrierLayer: Union[Dict, str] = None,
        polygonBarrierLayer: Union[Dict, str] = None,
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateDriveTimeAreas"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "breakValues": json.dumps(breakValues),  # Array of doubles
            "breakUnits": breakUnits,
            "travelMode": json.dumps(travelMode) if isinstance(travelMode, dict) else travelMode,  # JSON object
            "overlapPolicy": overlapPolicy,
            "travelDirection": travelDirection,
            "showHoles": str(showHoles).lower(),
            "includeReachableStreets": str(includeReachableStreets).lower(),
        }

        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay
        if timeZoneForTimeOfDay:  # Default is GeoLocal
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay

        if pointBarrierLayer:
            task_params["pointBarrierLayer"] = json.dumps(pointBarrierLayer) if isinstance(pointBarrierLayer,
                                                                                           dict) else pointBarrierLayer
        if lineBarrierLayer:
            task_params["lineBarrierLayer"] = json.dumps(lineBarrierLayer) if isinstance(lineBarrierLayer,
                                                                                         dict) else lineBarrierLayer
        if polygonBarrierLayer:
            task_params["polygonBarrierLayer"] = json.dumps(polygonBarrierLayer) if isinstance(polygonBarrierLayer,
                                                                                               dict) else polygonBarrierLayer

        if outputName:
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context)

        print(f"Submitting Create Drive-Time Areas job to: {task_url}")
        print(f"With parameters: {json.dumps({k: v for k, v in task_params.items() if k != 'travelMode'}, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        results_data = {}
        # Main output: driveTimeAreasLayer. Optional: reachableStreetsLayer
        main_result_param = "driveTimeAreasLayer"
        results_data[main_result_param] = await get_arcgis_job_results(final_job_status, job_status_url,
                                                                       main_result_param, token)

        if includeReachableStreets and "reachableStreetsLayer" in final_job_status.get("results", {}):
            results_data["reachableStreetsLayer"] = await get_arcgis_job_results(final_job_status, job_status_url,
                                                                                 "reachableStreetsLayer", token)

        return {"status": "success", "jobId": job_id, "results": results_data}

    except Exception as e:
        print(f"Error in create_drive_time_areas tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_route_layers(
        routeData: Dict,  # Required: {"itemId": "<item_id_of_route_data>"}
        deleteRouteData: bool,  # Required
        outputName: Dict,
        # Required: {"itemProperties": {"title": "<Prefix>", "tags": "<tag1>,...", "snippet": "...", "folderId": "..."}}
        context: Dict = None,  # Currently not honored by the service but included for completeness
) -> Dict:
    """
    Creates route layer items on the portal from the input route data item.
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_route_layers_internal(
            routeData=routeData,
            deleteRouteData=deleteRouteData,
            outputName=outputName,
            context=context,
        )
    )
    return results


async def create_route_layers_internal(
        routeData: Dict,
        deleteRouteData: bool,
        outputName: Dict,  # This is itemProperties for output, not serviceProperties
        context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateRouteLayers"

        task_params = {
            "routeData": json.dumps(routeData),  # {"itemId": "..."}
            "deleteRouteData": str(deleteRouteData).lower(),
            "outputName": json.dumps(outputName)  # {"itemProperties": {...}}
        }

        if context:  # Although not honored, include if provided
            task_params["context"] = json.dumps(context)

        print(f"Submitting Create Route Layers job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{task_url}/jobs/{job_id}"

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Create Route Layers is "routeLayers" (note plural)
        # The result itself is a JSON structure with item IDs and URLs
        results = await get_arcgis_job_results(final_job_status, job_status_url, "routeLayers", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in create_route_layers tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_viewshed(
    inputLayer: Union[Dict, str],  # Required
    demResolution: str = "FINEST",  # FINEST | 10m | 24m | 30m | 90m [cite: 150]
    maximumDistance: float = None,
    maxDistanceUnits: str = "Meters",  # Meters | Kilometers | Feet | Yards | Miles [cite: 153]
    observerHeight: float = 1.75,  # Default 1.75 [cite: 153]
    observerHeightUnits: str = "Meters",  # Meters | Kilometers | Feet | Yards | Miles [cite: 156]
    targetHeight: float = 0.0,  # Default 0.0 [cite: 159]
    targetHeightUnits: str = "Meters",  # Meters | Kilometers | Feet | Yards | Miles [cite: 159]
    generalize: bool = True,  # Default True [cite: 159]
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    """
    Identifies visible areas based on the observer locations provided.
    The results are areas where the observers can see the observed objects. [cite: 145]
    This task is a wrapper around the Viewshed task in Elevation Analysis Services. [cite: 146]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_viewshed_internal(
            inputLayer=inputLayer,
            demResolution=demResolution,
            maximumDistance=maximumDistance,
            maxDistanceUnits=maxDistanceUnits,
            observerHeight=observerHeight,
            observerHeightUnits=observerHeightUnits,
            targetHeight=targetHeight,
            targetHeightUnits=targetHeightUnits,
            generalize=generalize,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def create_viewshed_internal(
    inputLayer: Union[Dict, str],
    demResolution: str = "FINEST",
    maximumDistance: float = None,
    maxDistanceUnits: str = "Meters",
    observerHeight: float = 1.75,
    observerHeightUnits: str = "Meters",
    targetHeight: float = 0.0,
    targetHeightUnits: str = "Meters",
    generalize: bool = True,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateViewshed" # [cite: 148]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "demResolution": demResolution, #
            "observerHeight": observerHeight, #
            "observerHeightUnits": observerHeightUnits, #
            "targetHeight": targetHeight, #
            "targetHeightUnits": targetHeightUnits, #
            "generalize": str(generalize).lower(), #
        }

        if maximumDistance is not None:
            task_params["maximumDistance"] = maximumDistance #
            task_params["maxDistanceUnits"] = maxDistanceUnits #

        if outputName:
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}}) #
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName): #
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) #

        print(f"Submitting Create Viewshed job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"] #
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 170]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Create Viewshed is "viewshedLayer" [cite: 172]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "viewshedLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in create_viewshed tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_watersheds(
    inputLayer: Union[Dict, str],  # Required
    searchDistance: float = None, # Default is calculated by the tool [cite: 109]
    searchUnits: str = "Meters",  # Meters | Kilometers | Feet | Yards | Miles [cite: 112]
    sourceDatabase: str = "Finest", # Finest | 30m | 90m [cite: 112]
    generalize: bool = True,  # Default True [cite: 115]
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    """
    Determines the watershed, or upstream contributing area, for each point in your analysis layer. [cite: 104]
    This task is a wrapper around the Watershed task found in Elevation Analysis Services. [cite: 107]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_watersheds_internal(
            inputLayer=inputLayer,
            searchDistance=searchDistance,
            searchUnits=searchUnits,
            sourceDatabase=sourceDatabase,
            generalize=generalize,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def create_watersheds_internal(
    inputLayer: Union[Dict, str],
    searchDistance: float = None,
    searchUnits: str = "Meters",
    sourceDatabase: str = "Finest",
    generalize: bool = True,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateWatersheds" #

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer, #
            "sourceDatabase": sourceDatabase, #
            "generalize": str(generalize).lower(), #
        }

        if searchDistance is not None:
            task_params["searchDistance"] = searchDistance #
            task_params["searchUnits"] = searchUnits #
        elif searchUnits and searchDistance is None: # if units provided but not distance, still pass units
             task_params["searchUnits"] = searchUnits


        if outputName:
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}}) #
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName): #
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) #

        print(f"Submitting Create Watersheds job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"] #
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 126]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        results_data = {}
        # Result parameters: snapPourPtsLayer, watershedLayer [cite: 127, 133]
        if "snapPourPtsLayer" in final_job_status.get("results", {}):
            results_data["snapPourPtsLayer"] = await get_arcgis_job_results(final_job_status, job_status_url, "snapPourPtsLayer", token)
        if "watershedLayer" in final_job_status.get("results", {}):
            results_data["watershedLayer"] = await get_arcgis_job_results(final_job_status, job_status_url, "watershedLayer", token)

        return {"status": "success", "jobId": job_id, "results": results_data}

    except Exception as e:
        print(f"Error in create_watersheds tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def derive_new_locations(
    inputLayers: List[Union[Dict, str]],  # Required, list of layer definitions or URLs [cite: 51]
    expressions: List[Dict],  # Required, list of attribute and/or spatial expressions [cite: 54]
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    """
    Creates new features from the input layers that meet a query you specify.
    A query is made up of one or more expressions (attribute or spatial). [cite: 41, 42]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        derive_new_locations_internal(
            inputLayers=inputLayers,
            expressions=expressions,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def derive_new_locations_internal(
    inputLayers: List[Union[Dict, str]],
    expressions: List[Dict],
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/DeriveNewLocations" # [cite: 50]

        # inputLayers needs to be a list of JSON strings if they are dicts
        processed_input_layers = []
        for layer in inputLayers:
            if isinstance(layer, dict):
                processed_input_layers.append(json.dumps(layer))
            else:
                processed_input_layers.append(layer)

        task_params = {
            "inputLayers": json.dumps(processed_input_layers), # List of layer URLs or feature collections [cite: 51]
            "expressions": json.dumps(expressions), # List of expressions [cite: 54]
        }

        if outputName:
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}}) #
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName): #
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) #

        print(f"Submitting Derive New Locations job to: {task_url}")
        # Be cautious printing expressions if they are very large or sensitive
        print(f"With parameters (expressions omitted for brevity if large): {json.dumps({k: v for k,v in task_params.items() if k != 'expressions'}, indent=2)}")


        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"] #
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 93]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Derive New Locations is "resultLayer" [cite: 95]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "resultLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in derive_new_locations tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def dissolve_boundaries(
    inputLayer: Union[Dict, str],  # Required
    dissolveFields: List[str] = None,  # Optional, list of field names [cite: 9]
    summaryFields: List[str] = None,  # Optional, list of "fieldName summaryType" strings [cite: 12]
    multiPartFeatures: bool = True, # Specifies if multipart features are allowed. Default from doc example is not explicit, but implies True. [cite: 12]
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    """
    Finds polygons that overlap or share a common boundary and merges them
    together to form a single polygon. [cite: 3]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        dissolve_boundaries_internal(
            inputLayer=inputLayer,
            dissolveFields=dissolveFields,
            summaryFields=summaryFields,
            multiPartFeatures=multiPartFeatures,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def dissolve_boundaries_internal(
    inputLayer: Union[Dict, str],
    dissolveFields: List[str] = None,
    summaryFields: List[str] = None,
    multiPartFeatures: bool = True,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/DissolveBoundaries" #

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer, #
            "multiPartFeatures": str(multiPartFeatures).lower() # Boolean as string "True" or "False" [cite: 12]
        }

        if dissolveFields: # If not supplied or empty, polygons sharing common border or overlapping are dissolved [cite: 9]
            task_params["dissolveFields"] = json.dumps(dissolveFields) #
        if summaryFields:
            task_params["summaryFields"] = json.dumps(summaryFields) #

        if outputName:
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}}) # [cite: 16]
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName): # [cite: 19]
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) #

        print(f"Submitting Dissolve Boundaries job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"] #
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 28]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Dissolve Boundaries is "dissolvedLayer" [cite: 31]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "dissolvedLayer", token)

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in dissolve_boundaries tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def create_threshold_areas(
        inputLayer: Union[Dict, str],  # Required [cite: 190]
        idField: str,  # Required [cite: 193]
        thresholdVariable: str,  # Required, e.g., "KeyUSFacts.TOTPOP10" [cite: 193]
        thresholdValues: List[float],  # Required, e.g., [10000, 20000] [cite: 193]
        distanceType: Union[str, Dict],  # "StraightLine" or travel mode JSON object [cite: 196]
        thresholdExpression: str = None,  # Arcade expression [cite: 193]
        distanceUnits: str = None,  # Units for distanceType if it's StraightLine or for travel mode distance attribute
        maxIterations: int = None,  # Default is null (no limit) [cite: 202]
        travelDirection: str = "FromFacility",  # FromFacility | ToFacility [cite: 202]
        timeOfDay: int = None,  # Milliseconds since Unix epoch [cite: 202]
        timeZoneForTimeOfDay: str = "GeoLocal",  # GeoLocal | UTC [cite: 208]
        polygonDetail: str = "Standard",  # Standard | Generalized | High [cite: 211]
        minimumStep: float = None,  # Default is null [cite: 214]
        targetPercentDifference: float = 5.0,  # Default is 5 [cite: 214]
        outputName: str = None,
        context: Dict = None,
        useData: Dict = None,  # e.g., {"sourceCountry":"US", "hierarchy":"census"} [cite: 220]
        # resultLayer parameter in docs seems to be an output, not input. [cite: 223]
) -> Dict:
    """
    Creates rings or drive-time areas based on the value for a threshold variable
    such as population, income, or any demographic variable. [cite: 182]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        create_threshold_areas_internal(
            inputLayer=inputLayer,
            idField=idField,
            thresholdVariable=thresholdVariable,
            thresholdValues=thresholdValues,
            distanceType=distanceType,
            thresholdExpression=thresholdExpression,
            distanceUnits=distanceUnits,
            maxIterations=maxIterations,
            travelDirection=travelDirection,
            timeOfDay=timeOfDay,
            timeZoneForTimeOfDay=timeZoneForTimeOfDay,
            polygonDetail=polygonDetail,
            minimumStep=minimumStep,
            targetPercentDifference=targetPercentDifference,
            outputName=outputName,
            context=context,
            useData=useData,
        )
    )
    return results


async def create_threshold_areas_internal(
        inputLayer: Union[Dict, str],
        idField: str,
        thresholdVariable: str,
        thresholdValues: List[float],
        distanceType: Union[str, Dict],
        thresholdExpression: str = None,
        distanceUnits: str = None,
        maxIterations: int = None,
        travelDirection: str = "FromFacility",
        timeOfDay: int = None,
        timeZoneForTimeOfDay: str = "GeoLocal",
        polygonDetail: str = "Standard",
        minimumStep: float = None,
        targetPercentDifference: float = 5.0,
        outputName: str = None,
        context: Dict = None,
        useData: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/CreateThresholdAreas"  # [cite: 189]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,  #
            "idField": idField,  #
            "thresholdVariable": thresholdVariable,  #
            "thresholdValues": json.dumps(thresholdValues),  #
            "distanceType": json.dumps(distanceType) if isinstance(distanceType, dict) else distanceType,  #
            "travelDirection": travelDirection,  #
            "polygonDetail": polygonDetail,  #
            "targetPercentDifference": targetPercentDifference  #
        }

        if thresholdExpression:
            task_params["thresholdExpression"] = thresholdExpression  #
        if distanceUnits:
            task_params["distanceUnits"] = distanceUnits  #
        if maxIterations is not None:
            task_params["maxIterations"] = maxIterations  #
        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay  #
        if timeZoneForTimeOfDay:
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay  #
        if minimumStep is not None:
            task_params["minimumStep"] = minimumStep  #
        if useData:
            task_params["useData"] = json.dumps(useData)  #

        if outputName:
            if isinstance(outputName, str):
                task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})  #
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):  #
                task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context)  #

        print(f"Submitting Create Threshold Areas job to: {task_url}")
        print(
            f"With parameters: {json.dumps({k: v for k, v in task_params.items() if k != 'distanceType' or isinstance(v, str)}, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token)
        job_id = job_submission_info["jobId"]  #
        job_status_url = f"{task_url}/jobs/{job_id}"  # [cite: 227]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token)
        print("Job completed successfully. Fetching results...")

        results_data = {}
        # Main output: thresholdLayer. The PDF also mentions "createThresholdLayer" as an output [cite: 232]
        # but the main parameter is "thresholdLayer" for the areas. [cite: 229]
        # Let's assume "thresholdLayer" is the primary one.
        # The doc uses "thresholdLayer" and "createThresholdLayer" in example URLs [cite: 229, 232] but "thresholdArea" in the access path [cite: 228]
        # The result param seems to be `thresholdLayer` (for areas) and potentially `createThresholdLayer` (for lines)

        if "thresholdLayer" in final_job_status.get("results", {}):
            results_data["thresholdLayer"] = await get_arcgis_job_results(final_job_status, job_status_url,
                                                                          "thresholdLayer", token)
        if "createThresholdLayer" in final_job_status.get("results", {}):  # This might be the connecting lines
            results_data["connectingLinesLayer"] = await get_arcgis_job_results(final_job_status, job_status_url,
                                                                                "createThresholdLayer", token)

        return {"status": "success", "jobId": job_id, "results": results_data}

    except Exception as e:
        print(f"Error in create_threshold_areas tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def enrich_layer(
    inputLayer: Union[Dict, str],  # Required [cite: 17]
    dataCollections: List[str] = None, # Optional, e.g., ["KeyGlobalFacts", "KeyUSFacts"] [cite: 17]
    analysisVariables: List[str] = None, # Optional, e.g., ["KeyGlobalFacts.AVGHHSIZE"] [cite: 17]
    country: str = None, # Optional, e.g., "FR" [cite: 20]
    bufferType: Union[str, Dict] = None, # Required if inputLayer contains point or line features [cite: 20]
    distance: float = None, # Required if inputLayer contains point or line features [cite: 26]
    units: str = None, # Required if distance parameter is used [cite: 26]
    returnBoundaries: bool = False, # Default False [cite: 26]
    outputName: str = None, # [cite: 29]
    context: Dict = None, # [cite: 32]
) -> Dict:
    """
    Enriches your data by getting facts about the people, places, and businesses
    that surround your data locations. [cite: 4]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        enrich_layer_internal(
            inputLayer=inputLayer,
            dataCollections=dataCollections,
            analysisVariables=analysisVariables,
            country=country,
            bufferType=bufferType,
            distance=distance,
            units=units,
            returnBoundaries=returnBoundaries,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def enrich_layer_internal(
    inputLayer: Union[Dict, str],
    dataCollections: List[str] = None,
    analysisVariables: List[str] = None,
    country: str = None,
    bufferType: Union[str, Dict] = None,
    distance: float = None,
    units: str = None,
    returnBoundaries: bool = False,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass) # [cite: 679, 993, 1206]
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token) # [cite: 681, 995, 1208]
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/EnrichLayer" # [cite: 12]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer, # [cite: 17]
            "returnBoundaries": str(returnBoundaries).lower(), # [cite: 26]
        }

        if dataCollections:
            task_params["dataCollections"] = json.dumps(dataCollections) # [cite: 17]
        if analysisVariables:
            task_params["analysisVariables"] = json.dumps(analysisVariables) # [cite: 17]
        if country:
            task_params["country"] = country # [cite: 20]

        # bufferType, distance, and units are required if inputLayer contains point or line features.
        # This logic might need to be enhanced if we can determine feature type beforehand.
        # For now, we'll pass them if provided.
        if bufferType:
            task_params["bufferType"] = json.dumps(bufferType) if isinstance(bufferType, dict) else bufferType # [cite: 20]
        if distance is not None:
            task_params["distance"] = distance # [cite: 26]
        if units: # units is required if distance is used [cite: 26]
            task_params["units"] = units

        if outputName: # [cite: 29]
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) # [cite: 32]

        print(f"Submitting Enrich Layer job to: {task_url}")
        print(f"With parameters: {json.dumps({k: v for k,v in task_params.items() if k != 'bufferType' or isinstance(v,str)}, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token) # [cite: 682, 996, 1209]
        job_id = job_submission_info["jobId"] # [cite: 37]
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 39]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token) # [cite: 683, 997, 1210]
        print("Job completed successfully. Fetching results...")

        results = await get_arcgis_job_results(final_job_status, job_status_url, "enrichedLayer", token) # [cite: 42, 684, 998, 1211]

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in enrich_layer tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def extract_data(
    inputLayers: List[Union[Dict, str]],  # Required [cite: 57]
    extent: Union[Dict, str] = None, # Optional if context.extent is used [cite: 57]
    clip: bool = False, # Default false [cite: 60]
    dataFormat: str = "CSV",  # CSV | KML | FILEGEODATABASE | SHAPEFILE [cite: 60]
    outputName: Dict = None, # Dict with itemProperties like title, tags, snippet [cite: 60]
    context: Dict = None, # [cite: 63]
) -> Dict:
    """
    Extracts data from one or more layers within a given extent.
    Output can be CSV, KML, File Geodatabase, or Shapefile. [cite: 53, 54]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        extract_data_internal(
            inputLayers=inputLayers,
            extent=extent,
            clip=clip,
            dataFormat=dataFormat,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def extract_data_internal(
    inputLayers: List[Union[Dict, str]],
    extent: Union[Dict, str] = None,
    clip: bool = False,
    dataFormat: str = "CSV",
    outputName: Dict = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass) # [cite: 679, 993, 1206]
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token) # [cite: 681, 995, 1208]
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/ExtractData"

        processed_input_layers = []
        for layer in inputLayers:
            if isinstance(layer, dict):
                processed_input_layers.append(json.dumps(layer))
            else:
                processed_input_layers.append(layer)

        task_params = {
            "inputLayers": json.dumps(processed_input_layers), # [cite: 57]
            "clip": str(clip).lower(), # [cite: 60]
            "dataFormat": dataFormat, # [cite: 60]
        }

        if extent:
             task_params["extent"] = json.dumps(extent) if isinstance(extent, dict) else extent # [cite: 57]
        if outputName: # outputName in ExtractData is for itemProperties [cite: 60]
            task_params["outputName"] = json.dumps(outputName)
        if context:
            task_params["context"] = json.dumps(context) # [cite: 63]

        print(f"Submitting Extract Data job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token) # [cite: 682, 996, 1209]
        job_id = job_submission_info["jobId"] # [cite: 66]
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 68]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token) # [cite: 683, 997, 1210]
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Extract Data is "contentID" which contains item ID and URL [cite: 70]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "contentID", token) # [cite: 684, 998, 1211]

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in extract_data tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def field_calculator(
    inputLayer: Union[Dict, str],  # Required [cite: 82]
    expressions: List[Dict],  # Required, list of field descriptions and expressions [cite: 86]
    outputName: str = None, # [cite: 97]
    context: Dict = None, # [cite: 100]
) -> Dict:
    """
    Updates values in one or more fields based on an expression you provide.
    NOTE: This task is no longer supported in ArcGIS Online (as of March 2021)
    or ArcGIS Enterprise 10.9+. [cite: 77]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        field_calculator_internal(
            inputLayer=inputLayer,
            expressions=expressions,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def field_calculator_internal(
    inputLayer: Union[Dict, str],
    expressions: List[Dict], # Each dict: {"field": {"name": "...", "alias": "...", "type": "...", "length": ...}, "expression": "..."} [cite: 85, 88]
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        # Note: Check for service availability might be needed if running against newer ArcGIS versions.
        # For now, proceeding as per the documented API.

        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass) # [cite: 679, 993, 1206]
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token) # [cite: 681, 995, 1208]
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/FieldCalculator" # [cite: 81]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer, # [cite: 82]
            "expressions": json.dumps(expressions), # [cite: 86]
        }

        if outputName: # [cite: 97]
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) # [cite: 100]

        print(f"Submitting Field Calculator job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token) # [cite: 682, 996, 1209]
        job_id = job_submission_info["jobId"] # [cite: 105]
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 108]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token) # [cite: 683, 997, 1210]
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Field Calculator is "resultLayer" [cite: 109]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "resultLayer", token) # [cite: 684, 998, 1211]

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in field_calculator tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def find_centroids(
    inputLayer: Union[Dict, str],  # Required [cite: 128]
    pointLocation: bool = False,  # true for inside/on feature, false for geometric center (default) [cite: 128]
    outputName: str = None, # [cite: 131]
    context: Dict = None, # [cite: 136]
) -> Dict:
    """
    Finds and generates points from the representative center (centroid)
    of each input multipoint, line, or area feature. [cite: 123]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        find_centroids_internal(
            inputLayer=inputLayer,
            pointLocation=pointLocation,
            outputName=outputName,
            context=context,
        )
    )
    return results

async def find_centroids_internal(
    inputLayer: Union[Dict, str],
    pointLocation: bool = False,
    outputName: str = None,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass) # [cite: 679, 993, 1206]
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token) # [cite: 681, 995, 1208]
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/FindCentroids" # [cite: 126]

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer, # [cite: 128]
            "pointLocation": str(pointLocation).lower(),  # Boolean as string "true" or "false" [cite: 128]
        }

        if outputName: # [cite: 131]
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) # [cite: 136]

        print(f"Submitting Find Centroids job to: {task_url}")
        print(f"With parameters: {json.dumps(task_params, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token) # [cite: 682, 996, 1209]
        job_id = job_submission_info["jobId"] # [cite: 141]
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 143]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token) # [cite: 683, 997, 1210]
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Find Centroids is "outputLayer" [cite: 145]
        results = await get_arcgis_job_results(final_job_status, job_status_url, "outputLayer", token) # [cite: 684, 998, 1211]

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in find_centroids tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@mcp.tool()
async def find_existing_locations(
    inputLayers: List[Union[Dict, str]],  # Required, list of layer definitions or URLs [cite: 163]
    expressions: List[Dict],  # Required, list of attribute and/or spatial expressions [cite: 166]
    outputName: str = None, # [cite: 211]
    clipFeatures: bool = False, # Default False. Available in ArcGIS Online or ArcGIS Enterprise 11.2+ [cite: 217]
    context: Dict = None, # [cite: 217]
) -> Dict:
    """
    Selects features in the input layer that meet a specified query.
    A query is composed of one or more expressions (attribute or spatial). [cite: 157, 158]
    """
    loop = asyncio.get_event_loop()
    results = await loop.create_task(
        find_existing_locations_internal(
            inputLayers=inputLayers,
            expressions=expressions,
            outputName=outputName,
            clipFeatures=clipFeatures,
            context=context,
        )
    )
    return results

async def find_existing_locations_internal(
    inputLayers: List[Union[Dict, str]],
    expressions: List[Dict],
    outputName: str = None,
    clipFeatures: bool = False,
    context: Dict = None,
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass) # [cite: 679, 993, 1206]
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token) # [cite: 681, 995, 1208]
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        if dynamic_analysis_url_base.endswith('/'):
            dynamic_analysis_url_base = dynamic_analysis_url_base[:-1]

        task_url = f"{dynamic_analysis_url_base}/FindExistingLocations" # [cite: 161]

        processed_input_layers = []
        for layer in inputLayers: # [cite: 163]
            if isinstance(layer, dict):
                processed_input_layers.append(json.dumps(layer))
            else:
                processed_input_layers.append(layer)

        task_params = {
            "inputLayers": json.dumps(processed_input_layers),
            "expressions": json.dumps(expressions), # [cite: 166]
            "clipFeatures": str(clipFeatures).lower() # [cite: 217]
        }

        if outputName: # [cite: 211]
            if isinstance(outputName, str):
                 task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
            elif isinstance(outputName, dict) and ("itemId" in outputName and "overwrite" in outputName):
                 task_params["outputName"] = json.dumps(outputName)
            else:
                return {"error": "outputName format is invalid."}
        if context:
            task_params["context"] = json.dumps(context) # [cite: 217]

        print(f"Submitting Find Existing Locations job to: {task_url}")
        print(f"With parameters (expressions omitted for brevity if large): {json.dumps({k: v for k,v in task_params.items() if k != 'expressions'}, indent=2)}")

        job_submission_info = await submit_arcgis_job(task_url, task_params, token) # [cite: 682, 996, 1209]
        job_id = job_submission_info["jobId"] # [cite: 222]
        job_status_url = f"{task_url}/jobs/{job_id}" # [cite: 224]

        print(f"Job submitted. ID: {job_id}. Monitoring URL: {job_status_url}")
        final_job_status = await check_arcgis_job_status(job_status_url, token) # [cite: 683, 997, 1210]
        print("Job completed successfully. Fetching results...")

        # Result parameter name for Find Existing Locations is "resultLayer" [cite: 227]
        # The PDF for FindExistingLocations shows paramName as "unknown" in the example response, [cite: 230]
        # but the description says "resultLayer". Assuming "resultLayer" is correct.
        results = await get_arcgis_job_results(final_job_status, job_status_url, "resultLayer", token) # [cite: 684, 998, 1211]

        return {"status": "success", "jobId": job_id, "results": results}

    except Exception as e:
        print(f"Error in find_existing_locations tool: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Find Nearest Tool ---
@mcp.tool()
async def find_nearest(
        analysisLayer: Union[Dict, str],
        nearLayer: Union[Dict, str],
        measurementType: Union[str, Dict],
        maxCount: Optional[int] = None,
        searchCutoff: Optional[float] = None,
        searchCutoffUnits: Optional[str] = None,
        timeOfDay: Optional[int] = None,  # Unix time in milliseconds
        timeZoneForTimeOfDay: Optional[str] = None,  # GeoLocal | UTC
        includeRouteLayers: Optional[bool] = None,
        pointBarrierLayer: Optional[Union[Dict, str]] = None,
        lineBarrierLayer: Optional[Union[Dict, str]] = None,
        polygonBarrierLayer: Optional[Union[Dict, str]] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Measures distance/time from features in the analysis layer to features in the near layer. [cite: 3]
    """
    results = await find_nearest_internal(
        analysisLayer=analysisLayer,
        nearLayer=nearLayer,
        measurementType=measurementType,
        maxCount=maxCount,
        searchCutoff=searchCutoff,
        searchCutoffUnits=searchCutoffUnits,
        timeOfDay=timeOfDay,
        timeZoneForTimeOfDay=timeZoneForTimeOfDay,
        includeRouteLayers=includeRouteLayers,
        pointBarrierLayer=pointBarrierLayer,
        lineBarrierLayer=lineBarrierLayer,
        polygonBarrierLayer=polygonBarrierLayer,
        outputName=outputName,
        context=context
    )
    return results


async def find_nearest_internal(
        analysisLayer: Union[Dict, str],
        nearLayer: Union[Dict, str],
        measurementType: Union[str, Dict],
        maxCount: Optional[int] = None,
        searchCutoff: Optional[float] = None,
        searchCutoffUnits: Optional[str] = None,
        timeOfDay: Optional[int] = None,
        timeZoneForTimeOfDay: Optional[str] = None,
        includeRouteLayers: Optional[bool] = None,
        pointBarrierLayer: Optional[Union[Dict, str]] = None,
        lineBarrierLayer: Optional[Union[Dict, str]] = None,
        polygonBarrierLayer: Optional[Union[Dict, str]] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        find_nearest_task_url = f"{dynamic_analysis_url_base}/FindNearest"

        task_params = {
            "analysisLayer": json.dumps(analysisLayer) if isinstance(analysisLayer, dict) else analysisLayer,
            "nearLayer": json.dumps(nearLayer) if isinstance(nearLayer, dict) else nearLayer,
            "measurementType": json.dumps(measurementType) if isinstance(measurementType, dict) else measurementType,
        }

        if maxCount is not None:
            task_params["maxCount"] = maxCount
        if searchCutoff is not None:
            task_params["searchCutoff"] = searchCutoff
        if searchCutoffUnits:
            task_params["searchCutoffUnits"] = searchCutoffUnits
        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay
        if timeZoneForTimeOfDay:
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay
        if includeRouteLayers is not None:
            task_params["includeRouteLayers"] = json.dumps(
                includeRouteLayers)  # Boolean needs to be string "true" or "false" for some services, or actual bool
        if pointBarrierLayer:
            task_params["pointBarrierLayer"] = json.dumps(pointBarrierLayer) if isinstance(pointBarrierLayer,
                                                                                           dict) else pointBarrierLayer
        if lineBarrierLayer:
            task_params["lineBarrierLayer"] = json.dumps(lineBarrierLayer) if isinstance(lineBarrierLayer,
                                                                                         dict) else lineBarrierLayer
        if polygonBarrierLayer:
            task_params["polygonBarrierLayer"] = json.dumps(polygonBarrierLayer) if isinstance(polygonBarrierLayer,
                                                                                               dict) else polygonBarrierLayer
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(find_nearest_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{find_nearest_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        nearest_layer_results = await get_arcgis_job_results(
            final_job_status, job_status_url, "nearestLayer", token
        )
        connecting_lines_results = await get_arcgis_job_results(
            final_job_status, job_status_url, "connectingLinesLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "nearestLayer": nearest_layer_results,
                "connectingLinesLayer": connecting_lines_results,
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Find Outliers Tool ---
@mcp.tool()
async def find_outliers(
        analysisLayer: Union[Dict, str],
        analysisField: Optional[str] = None,
        dividedByField: Optional[str] = None,
        boundingPolygonLayer: Optional[Union[Dict, str]] = None,
        aggregationPolygonLayer: Optional[Union[Dict, str]] = None,
        permutations: Optional[str] = None,  # Speed | Balance | Precision
        shapeType: Optional[str] = None,  # Fishnet | Hexagon
        cellSize: Optional[float] = None,
        cellSizeUnits: Optional[str] = None,  # Miles | Feet | Kilometers | Meters
        distanceBand: Optional[float] = None,
        distanceBandUnits: Optional[str] = None,  # Miles | Feet | Kilometers | Meters
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Analyzes point data or field values to find statistically significant spatial clusters and outliers. [cite: 81]
    """
    results = await find_outliers_internal(
        analysisLayer=analysisLayer,
        analysisField=analysisField,
        dividedByField=dividedByField,
        boundingPolygonLayer=boundingPolygonLayer,
        aggregationPolygonLayer=aggregationPolygonLayer,
        permutations=permutations,
        shapeType=shapeType,
        cellSize=cellSize,
        cellSizeUnits=cellSizeUnits,
        distanceBand=distanceBand,
        distanceBandUnits=distanceBandUnits,
        outputName=outputName,
        context=context
    )
    return results


async def find_outliers_internal(
        analysisLayer: Union[Dict, str],
        analysisField: Optional[str] = None,
        dividedByField: Optional[str] = None,
        boundingPolygonLayer: Optional[Union[Dict, str]] = None,
        aggregationPolygonLayer: Optional[Union[Dict, str]] = None,
        permutations: Optional[str] = None,
        shapeType: Optional[str] = None,
        cellSize: Optional[float] = None,
        cellSizeUnits: Optional[str] = None,
        distanceBand: Optional[float] = None,
        distanceBandUnits: Optional[str] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        find_outliers_task_url = f"{dynamic_analysis_url_base}/FindOutliers"

        task_params = {
            "analysisLayer": json.dumps(analysisLayer) if isinstance(analysisLayer, dict) else analysisLayer,
        }

        if analysisField:
            task_params["analysisField"] = analysisField
        if dividedByField:
            task_params["dividedByField"] = dividedByField
        if boundingPolygonLayer:
            task_params["boundingPolygonLayer"] = json.dumps(boundingPolygonLayer) if isinstance(boundingPolygonLayer,
                                                                                                 dict) else boundingPolygonLayer
        if aggregationPolygonLayer:
            task_params["aggregationPolygonLayer"] = json.dumps(aggregationPolygonLayer) if isinstance(
                aggregationPolygonLayer, dict) else aggregationPolygonLayer
        if permutations:
            task_params["permutations"] = permutations
        if shapeType:
            task_params["shapeType"] = shapeType
        if cellSize is not None:
            task_params["cellSize"] = cellSize
        if cellSizeUnits:
            task_params["cellSizeUnits"] = cellSizeUnits
        if distanceBand is not None:
            task_params["distanceBand"] = distanceBand
        if distanceBandUnits:
            task_params["distanceBandUnits"] = distanceBandUnits
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(find_outliers_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{find_outliers_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        outlier_result_layer = await get_arcgis_job_results(
            final_job_status, job_status_url, "outlierResultLayer", token
        )

        process_info_result = None
        # processInfo might be directly in the results or require a separate fetch if it has a paramUrl.
        # Assuming get_arcgis_job_results can fetch it if it's structured like other results.
        if "processInfo" in final_job_status.get("results", {}):
            process_info_result = await get_arcgis_job_results(
                final_job_status, job_status_url, "processInfo", token
            )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "outlierResultLayer": outlier_result_layer,
                "processInfo": process_info_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Find Point Clusters Tool ---
@mcp.tool()
async def find_point_clusters(
        analysisLayer: Union[Dict, str],
        minFeaturesCluster: int,
        method: Optional[str] = None,  # DBSCAN | HDBSCAN | OPTICS
        searchDistance: Optional[float] = None,
        searchDistanceUnit: Optional[str] = None,  # Feet | Miles | Meters | Kilometers
        sensitivity: Optional[int] = None,  # 0-100
        timeField: Optional[str] = None,
        searchTimeInterval: Optional[float] = None,
        searchTimeUnit: Optional[str] = None,  # Seconds | Minutes | Hours | Days | Weeks | Months | Years
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Finds clusters of point features based on their spatial and temporal distribution. [cite: 163]
    """
    results = await find_point_clusters_internal(
        analysisLayer=analysisLayer,
        minFeaturesCluster=minFeaturesCluster,
        method=method,
        searchDistance=searchDistance,
        searchDistanceUnit=searchDistanceUnit,
        sensitivity=sensitivity,
        timeField=timeField,
        searchTimeInterval=searchTimeInterval,
        searchTimeUnit=searchTimeUnit,
        outputName=outputName,
        context=context
    )
    return results


async def find_point_clusters_internal(
        analysisLayer: Union[Dict, str],
        minFeaturesCluster: int,
        method: Optional[str] = None,
        searchDistance: Optional[float] = None,
        searchDistanceUnit: Optional[str] = None,
        sensitivity: Optional[int] = None,
        timeField: Optional[str] = None,
        searchTimeInterval: Optional[float] = None,
        searchTimeUnit: Optional[str] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        find_point_clusters_task_url = f"{dynamic_analysis_url_base}/FindPointClusters"

        task_params = {
            "analysisLayer": json.dumps(analysisLayer) if isinstance(analysisLayer, dict) else analysisLayer,
            "minFeaturesCluster": minFeaturesCluster,
        }

        if method:
            task_params["method"] = method
        if searchDistance is not None:
            task_params["searchDistance"] = searchDistance
        if searchDistanceUnit:
            task_params["searchDistanceUnit"] = searchDistanceUnit
        if sensitivity is not None:  # ArcGIS docs say double, but example shows 22, usually int 0-100
            task_params["sensitivity"] = sensitivity
        if timeField:  # Only available in ArcGIS Online [cite: 180]
            task_params["timeField"] = timeField
        if searchTimeInterval is not None:  # Only available in ArcGIS Online [cite: 183]
            task_params["searchTimeInterval"] = searchTimeInterval
        if searchTimeUnit:  # Only available in ArcGIS Online [cite: 183]
            task_params["searchTimeUnit"] = searchTimeUnit
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(find_point_clusters_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{find_point_clusters_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        point_clusters_result_layer = await get_arcgis_job_results(
            final_job_status, job_status_url, "pointClustersResultLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "pointClustersResultLayer": point_clusters_result_layer
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Find Similar Locations Tool ---
@mcp.tool()
async def find_similar_locations(
        inputLayer: Union[Dict, str],
        searchLayer: Union[Dict, str],
        analysisFields: Optional[List[str]] = None,
        criteriaFields: Optional[List[Dict[str, str]]] = None,  # [{"referenceField":"", "candidateField":""}]
        inputQuery: Optional[str] = None,
        numberOfResults: Optional[int] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Measures the similarity of candidate locations to one or more reference locations based on specified criteria. [cite: 209]
    """
    if not analysisFields and not criteriaFields:
        return {"error": "Either 'analysisFields' or 'criteriaFields' must be provided."}

    results = await find_similar_locations_internal(
        inputLayer=inputLayer,
        searchLayer=searchLayer,
        analysisFields=analysisFields,
        criteriaFields=criteriaFields,
        inputQuery=inputQuery,
        numberOfResults=numberOfResults,
        outputName=outputName,
        context=context
    )
    return results


async def find_similar_locations_internal(
        inputLayer: Union[Dict, str],
        searchLayer: Union[Dict, str],
        analysisFields: Optional[List[str]] = None,
        criteriaFields: Optional[List[Dict[str, str]]] = None,
        inputQuery: Optional[str] = None,
        numberOfResults: Optional[int] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        find_similar_locations_task_url = f"{dynamic_analysis_url_base}/FindSimilarLocations"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "searchLayer": json.dumps(searchLayer) if isinstance(searchLayer, dict) else searchLayer,
        }

        if analysisFields:
            task_params["analysisFields"] = json.dumps(analysisFields)
        if criteriaFields:  # Available in ArcGIS Enterprise 11.2 or higher [cite: 229]
            task_params["criteriaFields"] = json.dumps(criteriaFields)
        if inputQuery:
            task_params["inputQuery"] = inputQuery
        if numberOfResults is not None:
            task_params["numberOfResults"] = numberOfResults
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(find_similar_locations_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{find_similar_locations_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        similar_result_layer = await get_arcgis_job_results(
            final_job_status, job_status_url, "similarResultLayer", token
        )

        process_info_result = None
        if "processInfo" in final_job_status.get("results", {}):  # [cite: 245]
            process_info_result = await get_arcgis_job_results(
                final_job_status, job_status_url, "processInfo", token
            )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "similarResultLayer": similar_result_layer,
                "processInfo": process_info_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Generate Tessellations Tool ---
@mcp.tool()
async def generate_tessellations(
        binType: str,  # Square | Hexagon | Transverse Hexagon | Triangle | Diamond | H3_Hexgon [cite: 259]
        binSize: Optional[float] = None,  # Required if binType is not H3_Hexgon [cite: 259]
        binSizeUnit: Optional[str] = None,  # Required if binType is not H3_Hexgon [cite: 266]
        extentLayer: Optional[Union[Dict, str]] = None,
        intersectStudyArea: Optional[bool] = None,  # Default is false [cite: 269]
        binResolution: Optional[int] = None,  # Required if binType is H3_Hexgon (0-15) [cite: 272]
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Creates tessellations or bins determined by a specified extent, shape, and size. [cite: 254]
    """
    # Basic validation for conditional parameters based on binType
    if binType != "H3_Hexgon" and (binSize is None or binSizeUnit is None):
        return {"error": "For binType other than H3_Hexgon, binSize and binSizeUnit are required."}
    if binType == "H3_Hexgon" and binResolution is None:
        return {"error": "For binType H3_Hexgon, binResolution is required."}

    results = await generate_tessellations_internal(
        binType=binType,
        binSize=binSize,
        binSizeUnit=binSizeUnit,
        extentLayer=extentLayer,
        intersectStudyArea=intersectStudyArea,
        binResolution=binResolution,
        outputName=outputName,
        context=context
    )
    return results


async def generate_tessellations_internal(
        binType: str,
        binSize: Optional[float] = None,
        binSizeUnit: Optional[str] = None,
        extentLayer: Optional[Union[Dict, str]] = None,
        intersectStudyArea: Optional[bool] = None,
        binResolution: Optional[int] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        generate_tessellations_task_url = f"{dynamic_analysis_url_base}/GenerateTessellations"

        task_params = {
            "binType": binType,
        }

        if binSize is not None:
            task_params["binSize"] = binSize
        if binSizeUnit:
            task_params["binSizeUnit"] = binSizeUnit
        if extentLayer:
            task_params["extentLayer"] = json.dumps(extentLayer) if isinstance(extentLayer, dict) else extentLayer
        if intersectStudyArea is not None:
            # ArcGIS API expects boolean as string "true" or "false" for some parameters.
            # However, json.dumps(True) is "true". So direct assignment might work if service expects actual boolean.
            # Checking PDF: "Values: true false" [cite: 272] (for intersectStudyArea) implies it might take actual booleans or stringified "true"/"false".
            # The template uses json.dumps for outputName, which is a complex object.
            # For simple booleans in `task_params`, direct assignment is fine if the service consumes JSON true/false.
            # If it expects strings "true"/"false", then `str(intersectStudyArea).lower()` is needed.
            # Given example `json.dumps(includeRouteLayers)` for Find Nearest, using `json.dumps` for boolean is safer or `str(bool_val).lower()`.
            # The `submit_arcgis_job` in template uses `data=params` for `httpx.post`, which typically form-encodes.
            # If `Content-Type` is `application/x-www-form-urlencoded`, then "true" or "false" as strings are common.
            # Let's assume the ArcGIS API handles JSON booleans correctly when parameters are dumped.
            # The template `submit_arcgis_job` stringifies all params later using `data=params` which forms a query string if not files.
            # So, `str(intersectStudyArea).lower()` is robust for form encoding.
            task_params["intersectStudyArea"] = str(intersectStudyArea).lower()
        if binResolution is not None:  # Available in ArcGIS Online and ArcGIS Enterprise 11.2 or later [cite: 272]
            task_params["binResolution"] = binResolution
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(generate_tessellations_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{generate_tessellations_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        tessellation_layer = await get_arcgis_job_results(
            final_job_status, job_status_url, "tessellationLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "tessellationLayer": tessellation_layer
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Interpolate Points Tool ---
@mcp.tool()
async def interpolate_points(
        inputLayer: Union[Dict, str],
        field: str,
        interpolateOption: Optional[int] = 5,  # 1 (fastest) to 9 (most accurate), default 5 [cite: 18]
        outputPredictionError: Optional[bool] = False,  # Default based on common practice, PDF doesn't state default
        classificationType: Optional[str] = "GeometricInterval",
        # EqualArea | EqualInterval | GeometricInterval | Manual [cite: 21]
        numClasses: Optional[int] = 10,  # Default 10, max 32 [cite: 21]
        classBreaks: Optional[List[float]] = None,  # Required if classificationType is Manual [cite: 21]
        boundingPolygonLayer: Optional[Union[Dict, str]] = None,
        predictAtPointLayer: Optional[Union[Dict, str]] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Predicts values at new locations based on measurements from a collection of points. [cite: 4]
    """
    if classificationType == "Manual" and not classBreaks:
        return {"error": "classBreaks must be provided when classificationType is 'Manual'."}

    results = await interpolate_points_internal(
        inputLayer=inputLayer,
        field=field,
        interpolateOption=interpolateOption,
        outputPredictionError=outputPredictionError,
        classificationType=classificationType,
        numClasses=numClasses,
        classBreaks=classBreaks,
        boundingPolygonLayer=boundingPolygonLayer,
        predictAtPointLayer=predictAtPointLayer,
        outputName=outputName,
        context=context
    )
    return results


async def interpolate_points_internal(
        inputLayer: Union[Dict, str],
        field: str,
        interpolateOption: Optional[int],
        outputPredictionError: Optional[bool],
        classificationType: Optional[str],
        numClasses: Optional[int],
        classBreaks: Optional[List[float]],
        boundingPolygonLayer: Optional[Union[Dict, str]],
        predictAtPointLayer: Optional[Union[Dict, str]],
        outputName: Optional[str],
        context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        interpolate_points_task_url = f"{dynamic_analysis_url_base}/InterpolatePoints"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "field": field,
        }

        if interpolateOption is not None:
            task_params["interpolateOption"] = interpolateOption
        if outputPredictionError is not None:
            task_params["outputPredictionError"] = str(
                outputPredictionError).lower()  # Boolean to string "true"/"false"
        if classificationType:
            task_params["classificationType"] = classificationType
        if numClasses is not None:
            task_params["numClasses"] = numClasses
        if classBreaks:
            task_params["classBreaks"] = json.dumps(classBreaks)
        if boundingPolygonLayer:
            task_params["boundingPolygonLayer"] = json.dumps(boundingPolygonLayer) if isinstance(boundingPolygonLayer,
                                                                                                 dict) else boundingPolygonLayer
        if predictAtPointLayer:
            task_params["predictAtPointLayer"] = json.dumps(predictAtPointLayer) if isinstance(predictAtPointLayer,
                                                                                               dict) else predictAtPointLayer
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(interpolate_points_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{interpolate_points_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        results_dict = {}
        results_dict["resultLayer"] = await get_arcgis_job_results(
            final_job_status, job_status_url, "resultLayer", token
        )

        if outputPredictionError:
            # predictionError is returned if outputPredictionError is true [cite: 18]
            if "predictionError" in final_job_status.get("results", {}):
                results_dict["predictionError"] = await get_arcgis_job_results(
                    final_job_status, job_status_url, "predictionError", token
                )
            else:
                results_dict[
                    "predictionError"] = "Not generated (outputPredictionError was true, but result not found)."

        if predictAtPointLayer:
            # predictedPointLayer is returned if predictAtPointLayer was supplied [cite: 24]
            if "predictedPointLayer" in final_job_status.get("results", {}):
                results_dict["predictedPointLayer"] = await get_arcgis_job_results(
                    final_job_status, job_status_url, "predictedPointLayer", token
                )
            else:
                results_dict[
                    "predictedPointLayer"] = "Not generated (predictAtPointLayer was provided, but result not found)."

        return {
            "status": "success",
            "jobId": job_id,
            "results": results_dict
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Join Features Tool ---
@mcp.tool()
async def join_features(
        targetLayer: Union[Dict, str],
        joinLayer: Union[Dict, str],
        joinOperation: str,  # JoinOneToOne | JoinOneToMany [cite: 64]
        spatialRelationship: Optional[str] = None,  # IdenticalTo | Intersects | CompletelyContains | etc. [cite: 58]
        spatialRelationshipDistance: Optional[float] = None,
        spatialRelationshipDistanceUnits: Optional[str] = None,  # Miles | Yards | Feet | etc. [cite: 61]
        attributeRelationship: Optional[List[Dict[str, str]]] = None,
        # [{"targetField":"", "operator":"equal", "joinField":""}] [cite: 61]
        summaryFields: Optional[List[Dict[str, str]]] = None,
        # [{"statisticType":"", "onStatisticField":""}] [cite: 67]
        recordsToMatch: Optional[Dict[str, Any]] = None,
        # {"groupByFields":"", "orderByFields":"", "topCount":1} [cite: 70]
        joinType: Optional[str] = "Inner",  # Inner | Left [cite: 79]
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Joins attributes from one layer to another based on spatial and attribute relationships. [cite: 55]
    """
    if spatialRelationship == "WithinDistance" and (
            spatialRelationshipDistance is None or spatialRelationshipDistanceUnits is None):
        return {
            "error": "spatialRelationshipDistance and spatialRelationshipDistanceUnits are required when spatialRelationship is 'WithinDistance'."}

    results = await join_features_internal(
        targetLayer=targetLayer,
        joinLayer=joinLayer,
        joinOperation=joinOperation,
        spatialRelationship=spatialRelationship,
        spatialRelationshipDistance=spatialRelationshipDistance,
        spatialRelationshipDistanceUnits=spatialRelationshipDistanceUnits,
        attributeRelationship=attributeRelationship,
        summaryFields=summaryFields,
        recordsToMatch=recordsToMatch,
        joinType=joinType,
        outputName=outputName,
        context=context
    )
    return results


async def join_features_internal(
        targetLayer: Union[Dict, str],
        joinLayer: Union[Dict, str],
        joinOperation: str,
        spatialRelationship: Optional[str],
        spatialRelationshipDistance: Optional[float],
        spatialRelationshipDistanceUnits: Optional[str],
        attributeRelationship: Optional[List[Dict[str, str]]],
        summaryFields: Optional[List[Dict[str, str]]],
        recordsToMatch: Optional[Dict[str, Any]],
        joinType: Optional[str],
        outputName: Optional[str],
        context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        join_features_task_url = f"{dynamic_analysis_url_base}/JoinFeatures"

        task_params = {
            "targetLayer": json.dumps(targetLayer) if isinstance(targetLayer, dict) else targetLayer,
            "joinLayer": json.dumps(joinLayer) if isinstance(joinLayer, dict) else joinLayer,
            "joinOperation": joinOperation,
        }

        if spatialRelationship:
            task_params["spatialRelationship"] = spatialRelationship
        if spatialRelationshipDistance is not None:  # Required if spatialRelationship is WithinDistance [cite: 61]
            task_params["spatialRelationshipDistance"] = spatialRelationshipDistance
        if spatialRelationshipDistanceUnits:  # Required if spatialRelationship is WithinDistance [cite: 61]
            task_params["spatialRelationshipDistanceUnits"] = spatialRelationshipDistanceUnits
        if attributeRelationship:
            task_params["attributeRelationship"] = json.dumps(attributeRelationship)
        if summaryFields:
            task_params["summaryFields"] = json.dumps(summaryFields)
        if recordsToMatch:
            task_params["recordsToMatch"] = json.dumps(recordsToMatch)
        if joinType:
            task_params["joinType"] = joinType
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(join_features_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{join_features_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        # The PDF indicates the result param is "outputLayer" [cite: 85] or "output" in an example[cite: 88].
        # Using "outputLayer" for consistency.
        output_layer_result = await get_arcgis_job_results(
            final_job_status, job_status_url, "outputLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "outputLayer": output_layer_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Merge Layers Tool ---
@mcp.tool()
async def merge_layers(
        inputLayer: Union[Dict, str],
        mergeLayer: Union[Dict, str],
        mergingAttributes: Optional[List[str]] = None,
        # e.g., ['Temp Remove', 'RiskRate Rename RiskRateJan'] [cite: 109]
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Copies features from two layers into a new layer. [cite: 98]
    """
    results = await merge_layers_internal(
        inputLayer=inputLayer,
        mergeLayer=mergeLayer,
        mergingAttributes=mergingAttributes,
        outputName=outputName,
        context=context
    )
    return results


async def merge_layers_internal(
        inputLayer: Union[Dict, str],
        mergeLayer: Union[Dict, str],
        mergingAttributes: Optional[List[str]],
        outputName: Optional[str],
        context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        merge_layers_task_url = f"{dynamic_analysis_url_base}/MergeLayers"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "mergeLayer": json.dumps(mergeLayer) if isinstance(mergeLayer, dict) else mergeLayer,
        }

        if mergingAttributes:
            task_params["mergingAttributes"] = json.dumps(mergingAttributes)  # This should be a JSON array of strings
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(merge_layers_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{merge_layers_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        merged_layer_result = await get_arcgis_job_results(
            final_job_status, job_status_url, "mergedLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "mergedLayer": merged_layer_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Overlay Layers Tool ---
@mcp.tool()
async def overlay_layers(
        inputLayer: Union[Dict, str],
        overlayLayer: Union[Dict, str],
        overlayType: Optional[str] = "Intersect",  # Intersect | Union | Erase [cite: 148]
        outputType: Optional[str] = "Input",  # Input | Line | Point (valid if overlayType is Intersect) [cite: 151]
        snapToInput: Optional[bool] = False,  # Default false
        tolerance: Optional[float] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Combines two or more layers into a single layer. [cite: 136]
    """
    if overlayType != "Intersect" and outputType != "Input":  # outputType is only valid when overlayType is Intersect [cite: 151]
        # Allowing default "Input" for outputType if overlayType is not Intersect,
        # as the service might ignore it or handle it gracefully.
        # Or, one could raise an error or set outputType to None.
        # For now, let it pass, relying on service validation.
        pass

    results = await overlay_layers_internal(
        inputLayer=inputLayer,
        overlayLayer=overlayLayer,
        overlayType=overlayType,
        outputType=outputType,
        snapToInput=snapToInput,
        tolerance=tolerance,
        outputName=outputName,
        context=context
    )
    return results


async def overlay_layers_internal(
        inputLayer: Union[Dict, str],
        overlayLayer: Union[Dict, str],
        overlayType: Optional[str],
        outputType: Optional[str],
        snapToInput: Optional[bool],
        tolerance: Optional[float],
        outputName: Optional[str],
        context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        overlay_layers_task_url = f"{dynamic_analysis_url_base}/OverlayLayers"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
            "overlayLayer": json.dumps(overlayLayer) if isinstance(overlayLayer, dict) else overlayLayer,
        }

        if overlayType:
            task_params["overlayType"] = overlayType
        if outputType and overlayType == "Intersect":  # outputType is only valid when overlayType is Intersect [cite: 151]
            task_params["outputType"] = outputType
        elif outputType and overlayType != "Intersect":
            # As per documentation, outputType is only valid with Intersect.
            # Do not send if overlayType is not Intersect, unless it's the default value 'Input'.
            if outputType != "Input":
                pass  # Or log a warning, service will likely ignore it.
            elif outputType == "Input":  # Default, can be sent or omitted.
                task_params["outputType"] = outputType

        if snapToInput is not None:  # default is false [cite: 154]
            task_params["snapToInput"] = str(snapToInput).lower()
        if tolerance is not None:
            task_params["tolerance"] = tolerance
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(overlay_layers_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{overlay_layers_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        output_layer_result = await get_arcgis_job_results(
            final_job_status, job_status_url, "outputLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "outputLayer": output_layer_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Plan Routes Tool ---
@mcp.tool()
async def plan_routes(
        stopsLayer: Union[Dict, str],
        routeCount: int,
        maxStopsPerRoute: int,
        routeStartTime: int,  # Unix time in milliseconds [cite: 201]
        startLayer: Union[Dict, str],
        travelMode: Union[Dict, str],  # JSON object for travel mode settings [cite: 207]
        startLayerRouteIDField: Optional[str] = None,
        returnToStart: Optional[bool] = True,  # Default true [cite: 204]
        endLayer: Optional[Union[Dict, str]] = None,
        endLayerRouteIDField: Optional[str] = None,
        stopServiceTime: Optional[int] = None,  # minutes [cite: 213]
        maxRouteTime: Optional[int] = None,  # minutes, default 525600 [cite: 213]
        includeRouteLayers: Optional[bool] = None,
        pointBarrierLayer: Optional[Union[Dict, str]] = None,
        lineBarrierLayer: Optional[Union[Dict, str]] = None,
        polygonBarrierLayer: Optional[Union[Dict, str]] = None,
        outputName: Optional[str] = None,
        context: Optional[Dict] = None
) -> Dict:
    """
    Determines how to efficiently divide tasks among a mobile workforce. [cite: 176]
    """
    if startLayer and isinstance(startLayer, dict) and startLayer.get("featureSet", {}).get("features") and len(
            startLayer["featureSet"]["features"]) > 1 and not startLayerRouteIDField:
        return {"error": "startLayerRouteIDField is required when startLayer has more than one point."} # [cite: 251]
    if returnToStart is False and not endLayer:
        return {"error": "endLayer is required when returnToStart is false."} # [cite: 254]
    if endLayer and isinstance(endLayer, dict) and endLayer.get("featureSet", {}).get("features") and len(
            endLayer["featureSet"]["features"]) > 1 and not endLayerRouteIDField and returnToStart is False:
        return {
                   "error": "endLayerRouteIDField is required when endLayer has more than one point and returnToStart is false."}
        #[ cite: 255]

    results = await plan_routes_internal(
        stopsLayer=stopsLayer,
        routeCount=routeCount,
        maxStopsPerRoute=maxStopsPerRoute,
        routeStartTime=routeStartTime,
        startLayer=startLayer,
        travelMode=travelMode,
        startLayerRouteIDField=startLayerRouteIDField,
        returnToStart=returnToStart,
        endLayer=endLayer,
        endLayerRouteIDField=endLayerRouteIDField,
        stopServiceTime=stopServiceTime,
        maxRouteTime=maxRouteTime,
        includeRouteLayers=includeRouteLayers,
        pointBarrierLayer=pointBarrierLayer,
        lineBarrierLayer=lineBarrierLayer,
        polygonBarrierLayer=polygonBarrierLayer,
        outputName=outputName,
        context=context
    )
    return results


async def plan_routes_internal(
        stopsLayer: Union[Dict, str],
        routeCount: int,
        maxStopsPerRoute: int,
        routeStartTime: int,
        startLayer: Union[Dict, str],
        travelMode: Union[Dict, str],
        startLayerRouteIDField: Optional[str],
        returnToStart: Optional[bool],
        endLayer: Optional[Union[Dict, str]],
        endLayerRouteIDField: Optional[str],
        stopServiceTime: Optional[int],
        maxRouteTime: Optional[int],
        includeRouteLayers: Optional[bool],
        pointBarrierLayer: Optional[Union[Dict, str]],
        lineBarrierLayer: Optional[Union[Dict, str]],
        polygonBarrierLayer: Optional[Union[Dict, str]],
        outputName: Optional[str],
        context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        plan_routes_task_url = f"{dynamic_analysis_url_base}/PlanRoutes"

        task_params = {
            "stopsLayer": json.dumps(stopsLayer) if isinstance(stopsLayer, dict) else stopsLayer,
            "routeCount": routeCount,
            "maxStopsPerRoute": maxStopsPerRoute,
            "routeStartTime": routeStartTime,
            "startLayer": json.dumps(startLayer) if isinstance(startLayer, dict) else startLayer,
            "travelMode": json.dumps(travelMode) if isinstance(travelMode, dict) else travelMode,
        }

        if startLayerRouteIDField:
            task_params["startLayerRouteIDField"] = startLayerRouteIDField
        if returnToStart is not None:
            task_params["returnToStart"] = str(returnToStart).lower()
        if endLayer:
            task_params["endLayer"] = json.dumps(endLayer) if isinstance(endLayer, dict) else endLayer
        if endLayerRouteIDField:
            task_params["endLayerRouteIDField"] = endLayerRouteIDField
        if stopServiceTime is not None:
            task_params["stopServiceTime"] = stopServiceTime
        if maxRouteTime is not None:
            task_params["maxRouteTime"] = maxRouteTime
        if includeRouteLayers is not None:
            task_params["includeRouteLayers"] = str(includeRouteLayers).lower()
        if pointBarrierLayer:
            task_params["pointBarrierLayer"] = json.dumps(pointBarrierLayer) if isinstance(pointBarrierLayer,
                                                                                           dict) else pointBarrierLayer
        if lineBarrierLayer:
            task_params["lineBarrierLayer"] = json.dumps(lineBarrierLayer) if isinstance(lineBarrierLayer,
                                                                                         dict) else lineBarrierLayer
        if polygonBarrierLayer:
            task_params["polygonBarrierLayer"] = json.dumps(polygonBarrierLayer) if isinstance(polygonBarrierLayer,
                                                                                               dict) else polygonBarrierLayer
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(plan_routes_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{plan_routes_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        results_dict = {}
        # These are the main output layers for Plan Routes [cite: 180, 233, 239, 242]
        job_results_payload = final_job_status.get("results", {})

        if "routesLayer" in job_results_payload:
            results_dict["routesLayer"] = await get_arcgis_job_results(
                final_job_status, job_status_url, "routesLayer", token
            )
        if "assignedStopsLayer" in job_results_payload:
            results_dict["assignedStopsLayer"] = await get_arcgis_job_results(
                final_job_status, job_status_url, "assignedStopsLayer", token
            )
        if "unassignedStopsLayer" in job_results_payload:
            results_dict["unassignedStopsLayer"] = await get_arcgis_job_results(
                final_job_status, job_status_url, "unassignedStopsLayer", token
            )

        # If includeRouteLayers was true and output was a service, individual route layers might also be referenced.
        # However, the helper function get_arcgis_job_results is geared towards main output parameters.
        # The primary results (routesLayer, assignedStopsLayer, unassignedStopsLayer) are the focus here.

        return {
            "status": "success",
            "jobId": job_id,
            "results": results_dict
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# --- Summarize Nearby Tool ---
@mcp.tool()
async def summarize_nearby(
    sumNearbyLayer: Union[Dict, str],
    summaryLayer: Union[Dict, str],
    distances: List[float],
    nearType: Union[str, Dict] = "StraightLine", # "StraightLine" or travel mode JSON
    units: Optional[str] = None, # Meters, Kilometers, Feet, Yards, Miles, Seconds, Minutes, Hours
    timeOfDay: Optional[int] = None, # Milliseconds since Unix epoch
    timeZoneForTimeOfDay: Optional[str] = None, # GeoLocal | UTC
    returnBoundaries: Optional[bool] = True,
    sumShape: Optional[bool] = True,
    shapeUnits: Optional[str] = None, # Required if sumShape is true
    summaryFields: Optional[List[str]] = None, # Required if sumShape is false, e.g., ["fieldName summaryType"]
    groupByField: Optional[str] = None,
    minorityMajority: Optional[bool] = False,
    percentShape: Optional[bool] = False, # PDF doesn't state default, assuming false
    outputName: Optional[str] = None,
    context: Optional[Dict] = None
) -> Dict:
    """
    Finds features within a specified distance of features in the input layer and calculates statistics. [cite: 131, 133]
    Distance can be straight-line or by a travel mode. [cite: 132]
    """
    if sumShape is True and not shapeUnits:
        return {"error": "shapeUnits is required when sumShape is true."} # [cite: 163]
    if sumShape is False and not summaryFields:
        return {"error": "summaryFields is required when sumShape is false."} # [cite: 163]
    if not units and isinstance(nearType, str) and nearType.lower() == "straightline":
        return {"error": "units are required when nearType is 'StraightLine'."}
    if not units and isinstance(nearType, dict) and "impedanceAttributeName" in nearType and "timeAttributeName" not in nearType.get("impedanceAttributeName", "").lower(): # crude check for distance-based travel mode
        pass # Units would be distance based
    if not units and isinstance(nearType, dict) and "timeAttributeName" in nearType.get("impedanceAttributeName", "").lower():
        pass # Units would be time based

    results = await summarize_nearby_internal(
        sumNearbyLayer=sumNearbyLayer,
        summaryLayer=summaryLayer,
        distances=distances,
        nearType=nearType,
        units=units,
        timeOfDay=timeOfDay,
        timeZoneForTimeOfDay=timeZoneForTimeOfDay,
        returnBoundaries=returnBoundaries,
        sumShape=sumShape,
        shapeUnits=shapeUnits,
        summaryFields=summaryFields,
        groupByField=groupByField,
        minorityMajority=minorityMajority,
        percentShape=percentShape,
        outputName=outputName,
        context=context
    )
    return results

async def summarize_nearby_internal(
    sumNearbyLayer: Union[Dict, str],
    summaryLayer: Union[Dict, str],
    distances: List[float],
    nearType: Union[str, Dict],
    units: Optional[str],
    timeOfDay: Optional[int],
    timeZoneForTimeOfDay: Optional[str],
    returnBoundaries: Optional[bool],
    sumShape: Optional[bool],
    shapeUnits: Optional[str],
    summaryFields: Optional[List[str]],
    groupByField: Optional[str],
    minorityMajority: Optional[bool],
    percentShape: Optional[bool],
    outputName: Optional[str],
    context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        summarize_nearby_task_url = f"{dynamic_analysis_url_base}/SummarizeNearby"

        task_params = {
            "sumNearbyLayer": json.dumps(sumNearbyLayer) if isinstance(sumNearbyLayer, dict) else sumNearbyLayer,
            "summaryLayer": json.dumps(summaryLayer) if isinstance(summaryLayer, dict) else summaryLayer,
            "distances": json.dumps(distances),
            "nearType": json.dumps(nearType) if isinstance(nearType, dict) else nearType,
        }

        if units: # Default is Meters [cite: 151]
            task_params["units"] = units
        if timeOfDay is not None:
            task_params["timeOfDay"] = timeOfDay
        if timeZoneForTimeOfDay:
            task_params["timeZoneForTimeOfDay"] = timeZoneForTimeOfDay
        if returnBoundaries is not None: # Default true [cite: 160]
            task_params["returnBoundaries"] = str(returnBoundaries).lower()
        if sumShape is not None: # Default true [cite: 160]
            task_params["sumShape"] = str(sumShape).lower()
        if shapeUnits:
            task_params["shapeUnits"] = shapeUnits
        if summaryFields:
            task_params["summaryFields"] = json.dumps(summaryFields)
        if groupByField:
            task_params["groupByField"] = groupByField
        if minorityMajority is not None: # Default false [cite: 166]
            task_params["minorityMajority"] = str(minorityMajority).lower()
        if percentShape is not None: # Default false (assumed from similar params) [cite: 169]
            task_params["percentShape"] = str(percentShape).lower()
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(summarize_nearby_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{summarize_nearby_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        results_dict = {}
        results_dict["resultLayer"] = await get_arcgis_job_results(
            final_job_status, job_status_url, "resultLayer", token
        )

        if groupByField:
            if "groupBySummary" in final_job_status.get("results", {}):
                results_dict["groupBySummary"] = await get_arcgis_job_results(
                    final_job_status, job_status_url, "groupBySummary", token
                )
            else: # Handle case where it might not be present if no results or error in its generation
                results_dict["groupBySummary"] = "Not generated (groupByField was provided, but result not found)."


        return {
            "status": "success",
            "jobId": job_id,
            "results": results_dict
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

# --- Summarize Center and Dispersion Tool ---
@mcp.tool()
async def summarize_center_and_dispersion(
    analysisLayer: Union[Dict, str],
    summarizeType: str, # CentralFeature | MeanCenter | MedianCenter | Ellipse
    ellipseSize: Optional[str] = "1 standard deviation", # "1 standard deviation", "2 standard deviations", "3 standard deviations"
    weightField: Optional[str] = None,
    groupField: Optional[str] = None,
    outputName: Optional[str] = None,
    context: Optional[Dict] = None
) -> Dict:
    """
    Finds central features and directional distributions. [cite: 99]
    """
    if summarizeType not in ["CentralFeature", "MeanCenter", "MedianCenter", "Ellipse"]:
        return {"error": "Invalid summarizeType. Must be one of: CentralFeature, MeanCenter, MedianCenter, Ellipse."}
    if summarizeType == "Ellipse" and ellipseSize not in ["1 standard deviation", "2 standard deviations", "3 standard deviations"]:
        return {"error": "Invalid ellipseSize for Ellipse summarizeType."}

    results = await summarize_center_and_dispersion_internal(
        analysisLayer=analysisLayer,
        summarizeType=summarizeType,
        ellipseSize=ellipseSize,
        weightField=weightField,
        groupField=groupField,
        outputName=outputName,
        context=context
    )
    return results

async def summarize_center_and_dispersion_internal(
    analysisLayer: Union[Dict, str],
    summarizeType: str,
    ellipseSize: Optional[str],
    weightField: Optional[str],
    groupField: Optional[str],
    outputName: Optional[str],
    context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        summarize_task_url = f"{dynamic_analysis_url_base}/SummarizeCenterAndDispersion"

        task_params = {
            "analysisLayer": json.dumps(analysisLayer) if isinstance(analysisLayer, dict) else analysisLayer,
            "summarizeType": summarizeType,
        }

        if summarizeType == "Ellipse":
            task_params["ellipseSize"] = ellipseSize # Default "1 standard deviation" [cite: 107]
        if weightField:
            task_params["weightField"] = weightField
        if groupField:
            task_params["groupField"] = groupField
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(summarize_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{summarize_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        result_param_name_map = {
            "CentralFeature": "centralFeatureResultLayer",
            "MeanCenter": "meanCenterResultLayer",
            "MedianCenter": "medianCenterResultLayer",
            "Ellipse": "ellipseResultLayer"
        }
        result_param_name = result_param_name_map.get(summarizeType)
        if not result_param_name:
            return {"error": f"Could not determine result parameter name for summarizeType: {summarizeType}"}


        result_layer = await get_arcgis_job_results(
            final_job_status, job_status_url, result_param_name, token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                result_param_name: result_layer
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

# --- Summarize Within Tool ---
@mcp.tool()
async def summarize_within(
    summaryLayer: Union[Dict, str],
    sumWithinLayer: Optional[Union[Dict, str]] = None, # Polygon features
    binType: Optional[str] = None, # Square | Hexagon
    binSize: Optional[float] = None, # Required if binType is used
    binSizeUnit: Optional[str] = None, # Required if binType is used
    sumShape: Optional[bool] = True,
    shapeUnits: Optional[str] = None, # E.g. Acres, SquareKilometers, Miles. Required if sumShape is true.
    summaryFields: Optional[List[str]] = None, # Required if sumShape is false e.g. ["fieldName summaryType"]
    groupByField: Optional[str] = None,
    minorityMajority: Optional[bool] = False,
    percentShape: Optional[bool] = False,
    outputName: Optional[str] = None,
    context: Optional[Dict] = None
) -> Dict:
    """
    Summarizes features (points, lines, or polygons) that fall within the boundaries of polygons in another layer or within generated bins. [cite: 45]
    """
    if not sumWithinLayer and not binType:
        return {"error": "Either sumWithinLayer or binType (with binSize and binSizeUnit) must be provided."} # [cite: 74]
    if binType and (binSize is None or binSizeUnit is None):
        return {"error": "binSize and binSizeUnit are required when binType is specified."} # [cite: 74]
    if sumWithinLayer and binType:
        return {"error": "Provide either sumWithinLayer or binType parameters, not both."}

    if sumShape is True and not shapeUnits:
        # This is only strictly required if summaryLayer is line/polygon. For points, sumShape=true gives count.
        # The PDF example for shapeUnits only lists polygon and line units[cite: 62].
        # Assuming it's required generally for clarity if sumShape is true.
        return {"error": "shapeUnits is required when sumShape is true."} # [cite: 60, 62]
    if sumShape is False and not summaryFields:
        return {"error": "summaryFields is required when sumShape is false."} # [cite: 62]


    results = await summarize_within_internal(
        summaryLayer=summaryLayer,
        sumWithinLayer=sumWithinLayer,
        binType=binType,
        binSize=binSize,
        binSizeUnit=binSizeUnit,
        sumShape=sumShape,
        shapeUnits=shapeUnits,
        summaryFields=summaryFields,
        groupByField=groupByField,
        minorityMajority=minorityMajority,
        percentShape=percentShape,
        outputName=outputName,
        context=context
    )
    return results

async def summarize_within_internal(
    summaryLayer: Union[Dict, str],
    sumWithinLayer: Optional[Union[Dict, str]],
    binType: Optional[str],
    binSize: Optional[float],
    binSizeUnit: Optional[str],
    sumShape: Optional[bool],
    shapeUnits: Optional[str],
    summaryFields: Optional[List[str]],
    groupByField: Optional[str],
    minorityMajority: Optional[bool],
    percentShape: Optional[bool],
    outputName: Optional[str],
    context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        summarize_within_task_url = f"{dynamic_analysis_url_base}/SummarizeWithin"

        task_params = {
            "summaryLayer": json.dumps(summaryLayer) if isinstance(summaryLayer, dict) else summaryLayer,
        }

        if sumWithinLayer:
            task_params["sumWithinLayer"] = json.dumps(sumWithinLayer) if isinstance(sumWithinLayer, dict) else sumWithinLayer
        elif binType:
            task_params["binType"] = binType # Default Square [cite: 74]
            task_params["binSize"] = binSize
            task_params["binSizeUnit"] = binSizeUnit # Default Meters [cite: 77]

        if sumShape is not None: # Default true [cite: 59]
            task_params["sumShape"] = str(sumShape).lower()
        if shapeUnits:
            task_params["shapeUnits"] = shapeUnits
        if summaryFields:
            task_params["summaryFields"] = json.dumps(summaryFields)
        if groupByField:
            task_params["groupByField"] = groupByField
        if minorityMajority is not None: # Default false [cite: 65]
            task_params["minorityMajority"] = str(minorityMajority).lower()
        if percentShape is not None: # Default false (assumed) [cite: 68]
            task_params["percentShape"] = str(percentShape).lower()
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(summarize_within_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{summarize_within_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        results_dict = {}
        results_dict["resultLayer"] = await get_arcgis_job_results(
            final_job_status, job_status_url, "resultLayer", token
        )

        if groupByField:
            if "groupBySummary" in final_job_status.get("results", {}):
                results_dict["groupBySummary"] = await get_arcgis_job_results(
                    final_job_status, job_status_url, "groupBySummary", token
                )
            else:
                results_dict["groupBySummary"] = "Not generated (groupByField was provided, but result not found)."


        return {
            "status": "success",
            "jobId": job_id,
            "results": results_dict
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

# --- Trace Downstream Tool ---
@mcp.tool()
async def trace_downstream(
    inputLayer: Union[Dict, str], # Point features [cite: 11]
    splitDistance: Optional[float] = None,
    splitUnits: Optional[str] = None, # Meters | Kilometers | Feet | Yards | Miles. Default: Kilometers [cite: 14]
    maxDistance: Optional[float] = None,
    maxDistanceUnits: Optional[str] = None, # Meters | Kilometers | Feet | Yards | Miles. Default: Kilometers [cite: 14]
    boundingPolygonLayer: Optional[Union[Dict, str]] = None,
    sourceDatabase: Optional[str] = "Finest", # Finest | 30m | 90m [cite: 17]
    generalize: Optional[bool] = True, # Default True [cite: 17]
    outputName: Optional[str] = None,
    context: Optional[Dict] = None
) -> Dict:
    """
    Determines the trace, or flow path, in a downstream direction from input points. [cite: 3]
    """
    if splitDistance is not None and splitUnits is None:
        splitUnits = "Kilometers" # Default if splitDistance is provided [cite: 14]
    if maxDistance is not None and maxDistanceUnits is None:
        maxDistanceUnits = "Kilometers" # Default if maxDistance is provided [cite: 14]


    results = await trace_downstream_internal(
        inputLayer=inputLayer,
        splitDistance=splitDistance,
        splitUnits=splitUnits,
        maxDistance=maxDistance,
        maxDistanceUnits=maxDistanceUnits,
        boundingPolygonLayer=boundingPolygonLayer,
        sourceDatabase=sourceDatabase,
        generalize=generalize,
        outputName=outputName,
        context=context
    )
    return results

async def trace_downstream_internal(
    inputLayer: Union[Dict, str],
    splitDistance: Optional[float],
    splitUnits: Optional[str],
    maxDistance: Optional[float],
    maxDistanceUnits: Optional[str],
    boundingPolygonLayer: Optional[Union[Dict, str]],
    sourceDatabase: Optional[str],
    generalize: Optional[bool],
    outputName: Optional[str],
    context: Optional[Dict]
) -> Dict:
    try:
        arcgis_user = os.environ.get("ARCGIS_USER")
        arcgis_pass = os.environ.get("ARCGIS_PASSWORD")
        if not arcgis_user or not arcgis_pass:
            return {"error": "ArcGIS username or password not found in environment variables."}

        token = await get_arcgis_token(arcgis_user, arcgis_pass)
        if not token:
            return {"error": "Failed to obtain ArcGIS token."}

        dynamic_analysis_url_base = await get_analysis_service_url(token)
        if not dynamic_analysis_url_base:
            return {"error": "Failed to obtain ArcGIS Analysis Service URL."}

        trace_downstream_task_url = f"{dynamic_analysis_url_base}/TraceDownstream"

        task_params = {
            "inputLayer": json.dumps(inputLayer) if isinstance(inputLayer, dict) else inputLayer,
        }

        if splitDistance is not None:
            task_params["splitDistance"] = splitDistance
        if splitUnits: # Default Kilometers [cite: 14]
            task_params["splitUnits"] = splitUnits
        if maxDistance is not None:
            task_params["maxDistance"] = maxDistance
        if maxDistanceUnits: # Default Kilometers [cite: 14]
            task_params["maxDistanceUnits"] = maxDistanceUnits
        if boundingPolygonLayer:
            task_params["boundingPolygonLayer"] = json.dumps(boundingPolygonLayer) if isinstance(boundingPolygonLayer, dict) else boundingPolygonLayer
        if sourceDatabase: # Default Finest [cite: 17]
            task_params["sourceDatabase"] = sourceDatabase
        if generalize is not None: # Default True [cite: 17]
            task_params["generalize"] = str(generalize).lower()
        if outputName:
            task_params["outputName"] = json.dumps({"serviceProperties": {"name": outputName}})
        if context:
            task_params["context"] = json.dumps(context)

        job_submission_info = await submit_arcgis_job(trace_downstream_task_url, task_params, token)
        job_id = job_submission_info["jobId"]
        job_status_url = f"{trace_downstream_task_url}/jobs/{job_id}"

        final_job_status = await check_arcgis_job_status(job_status_url, token)

        trace_layer_result = await get_arcgis_job_results(
            final_job_status, job_status_url, "traceLayer", token
        )

        return {
            "status": "success",
            "jobId": job_id,
            "results": {
                "traceLayer": trace_layer_result
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


if __name__ == "__main__":
    print("To run the server, use the command:")
    print("uvicorn arcgis_mcp_server_spatial_analysis:app --reload --port 8000")
    print("Ensure ARCGIS_USER and ARCGIS_PASSWORD environment variables are set.")

