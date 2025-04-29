# server.py

import os
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.routing import Mount, Route
from mcp.server import Server
import uvicorn
from typing import List
import json
import datetime as dt
import click
import logging

from arcgis.gis import GIS
from arcgis.geocoding import geocode, batch_geocode
from arcgis.geocoding import reverse_geocode
# from arcgis.geocoding import get_places_api
import arcgis.network as network
from arcgis.features import Feature, FeatureSet
from arcgis.geometry import Point

## From https://github.com/modelcontextprotocol/python-sdk/issues/423
from mcp.server.session import ServerSession
####################################################################################
# Temporary monkeypatch which avoids crashing when a POST message is received
# before a connection has been initialized, e.g: after a deployment.
# pylint: disable-next=protected-access
old__received_request = ServerSession._received_request

async def _received_request(self, *args, **kwargs):
    try:
        return await old__received_request(self, *args, **kwargs)
    except RuntimeError:
        pass


# pylint: disable-next=protected-access
ServerSession._received_request = _received_request
####################################################################################

# Create an MCP server
mcp = FastMCP("Network Analysis MCP Server")
_DEBUG = False

# ArcGIS Online or ArcGIS Location Platform API Key
try:
    api_key = os.environ['ArcGIS_MAPS_API_KEY']
    logging.info(f"Got ArcGIS_MAPS_API_KEY")
except KeyError:
    api_key = ""
    logging.info("ArcGIS_MAPS_API_KEY environment variable not set")

try:
    referer_domain = os.environ['REFERER_DOMAIN']
    logging.info(f"Got Referer domain: {referer_domain}")
except KeyError:
    referer_domain = "localhost:8090"
    logging.info(f"REFERER_DOMAIN environment variable not set. Default to {referer_domain}")

gis = GIS(api_key=api_key, referer=referer_domain)

## create get places API
## You must be signed into the GIS to use the Places API
# find_places = get_places_api(gis)


# Add all the tools
@mcp.tool(name='geocode')
def mcp_geocode(address: str) -> dict[str, str]:
    """Convert an address into geographic coordinates"""
    try:
        single_line_address = address
        results = geocode(single_line_address)
        print(results)
        result = results[0]
        address = result["address"]
        location = result["location"]
        content = {"latitude": location["y"], "longitude": location["x"], "address": address}
        return {"content": json.dumps(content), "isError": False}
    except KeyError:
        return {"content": "Couldn't convert the given address to a coordinate!", "isError": True}

@mcp.tool(name='reverse_geocode')
def mcp_reverse_geocode(latitude: float, longitude: float) -> dict[str, str]:
    """Convert geographic coordinates into an address"""
    try:
        results = reverse_geocode([longitude, latitude])
        address = results["address"]["LongLabel"]
        location = results["location"]
        content = {"latitude": location["y"], "longitude": location["x"], "address": address}
        return {"content": json.dumps(content), "isError": False}
    except KeyError:
        return {"content": "Couldn't convert the given geographic coordinate to an address!", "isError": True}

@mcp.tool(name='find_route')
def mcp_find_routes(stops: List[str], include_route_geometry: bool=False) -> dict[str, str]:
    """Find a route for the given list of stops"""
    try:
        # use batch geocode to get the given stops' address and location/geometry
        stop_features = convert_poi_list_into_features(stops)
        if stop_features is None:
            return {"content": "Couldn't convert the given stops into valid addresses!", "isError": True}
        logging.info("Convert stops to addresses successfully!")
        logging.info(stop_features)

        logging.info("Calling 'find route' function ...")
        # use current time. TODO: Time_Zone?
        start_time = int(dt.datetime.now().timestamp() * 1000)
        # call find_route function
        results = network.analysis.find_routes(
            stop_features,
            time_of_day=start_time,
            time_zone_for_time_of_day="UTC",
            preserve_terminal_stops="Preserve None",
            reorder_stops_to_find_optimal_routes=True,
            save_output_na_layer=False
        )
        logging.info("'find route' call finished.")
        if not results.solve_succeeded:
            return {"content": "Couldn't find a route for the given stops!", "isError": True}

        # get route directions
        direction_features = results.output_directions.features
        fields_to_drop = ["Shape_Length", "SubItemType", "Type"]
        updated_direction_features = drop_fields_from_features(direction_features, fields_to_drop, False)
        route_directions = json.dumps(updated_direction_features.to_dict())

        # get route summary
        route_summary = '{}'
        if len(results.output_routes.features) > 0:
            fields_to_drop = ["Shape_Length"]
            updated_output_routes = drop_fields_from_features(results.output_routes, fields_to_drop, include_route_geometry)
            if include_route_geometry:
                route_summary = updated_output_routes.to_geojson
            else:
                route_summary = json.dumps(updated_output_routes.to_dict())

        content = {"route_summary": json.loads(route_summary), "route_directions": json.loads(route_directions), "output_stops": json.loads(results.output_stops.to_geojson)}
        logging.info(content)

        return {"content": json.dumps(content), "isError": False}

    except KeyError:
        return {"content": "Error occurred during the process of finding a route for the given stops!", "isError": True}

@mcp.tool(name='service_areas')
def mcp_generate_service_areas(facilities: List[str], time_breaks: List[float] | None=None, include_geometry: bool=False) -> dict[str, str]:
    """Generate a service area for the given list of facilities within the given time breaks such as 5 and/or 10 min travel distance."""
    if time_breaks is None:
        time_breaks = [10]
    try:
        # use batch geocode to get the given stops' address and location/geometry
        facility_features = convert_poi_list_into_features(facilities)
        if facility_features is None:
            return {"content": "Couldn't convert the given facilities into valid addresses!", "isError": True}

        # use current time. TODO: Time_Zone?
        current_time = int(dt.datetime.now().timestamp() * 1000)
        string_of_breaks = ""
        list_of_breaks = [10]  # default to 10 min
        if isinstance(time_breaks, list):
            string_of_breaks = ' '.join(map(str, list_of_breaks))
        break_units = "Minutes"

        # call generate_service_areas function
        results = network.analysis.generate_service_areas(
            facilities=facility_features,
            break_values=string_of_breaks,
            break_units=break_units,
            time_of_day=current_time
        )
        if not results.solve_succeeded:
            return {"content": "Couldn't generate service areas for the given facilities!", "isError": True}

        # get service areas
        service_areas_features = results.service_areas.features
        fields_to_drop = ["Shape_Length", "Shape_Area", "SHAPE"]
        updated_area_features = drop_fields_from_features(service_areas_features, fields_to_drop, include_geometry)
        if include_geometry:
            service_areas_content = updated_area_features.to_geojson
        else:
            service_areas_content = json.dumps(updated_area_features.to_dict())

        # add output_facilities features to content
        content = {"service_areas": json.loads(service_areas_content),
                   "output_facilities": json.loads(results.output_facilities.to_geojson)
                   }

        return {"content": json.dumps(content), "isError": False}

    except KeyError:
        return {"content": "Couldn't generate service areas for the given facilities!", "isError": True}

@mcp.tool(name='find_closest_facilities')
def mcp_find_closest_facilities(incidents: List[str],
                                facilities: List[str],
                                include_geometry: bool=False,
                                cutoff_time_in_min: float = 10) -> dict[str, str]:
    """Find the closest facilities for each incident in the list."""
    try:
        # get incident features
        incident_features = convert_poi_list_into_features(incidents)
        if incident_features is None:
            return {"content": "Couldn't convert the given incident into valid addresses!", "isError": True}

        # get facility features
        facility_features = convert_poi_list_into_features(facilities)
        if facility_features is None:
            return {"content": "Couldn't convert the given facilities into valid addresses!", "isError": True}

        # use current time. TODO: Time_Zone?
        current_time = int(dt.datetime.now().timestamp() * 1000)
        # default cutoff time in minutes
        cutoff = cutoff_time_in_min
        # default facilities to find
        max_facilities_to_find = 4
        if len(facilities) < 4:
            max_facilities_to_find = len(facilities)

        # call find_closest_facilities function
        results = network.analysis.find_closest_facilities(
            incidents=incident_features,
            facilities=facility_features,
            cutoff=cutoff,
            time_of_day=current_time,
            number_of_facilities_to_find=max_facilities_to_find,
            save_output_network_analysis_layer=False
        )
        if not results.solve_succeeded:
            return {"content": "Couldn't generate service areas for the given facilities!", "isError": True}

        # get output routes
        output_routes_features = results.output_routes.features
        fields_to_drop = ["Shape_Length", "Shape_Area", "SHAPE"]
        updated_output_routes_features = drop_fields_from_features(output_routes_features, fields_to_drop, include_geometry)
        if include_geometry:
            output_routes = updated_output_routes_features.to_geojson
        else:
            output_routes = json.dumps(updated_output_routes_features.to_dict())

        content = {"output_routes": json.loads(output_routes),
                   "output_closest_facilities": json.loads(results.output_closest_facilities.to_geojson),
                   "output_incidents": json.loads(results.output_incidents.to_geojson)
                   }

        return {"content": json.dumps(content), "isError": False}

    except KeyError:
        return {"content": "Couldn't generate service areas for the given facilities!", "isError": True}

@mcp.tool(name='create_cost_matrix')
def mcp_create_origin_destination_cost_matrix(origins: List[str],
                                              destinations: List[str],
                                              include_geometry: bool=False,
                                              cutoff_time_in_min: float=10) -> dict[str, str]:
    """Generate a cost matrix for the given origin and destination locations."""
    try:
        # get incident features
        origins_features = convert_poi_list_into_features(origins)
        if origins_features is None:
            return {"content": "Couldn't convert the given origins into valid addresses!", "isError": True}

        # get facility features
        destinations_features = convert_poi_list_into_features(destinations)
        if destinations_features is None:
            return {"content": "Couldn't convert the given destinations into valid addresses!", "isError": True}

        current_time = dt.datetime.now()
        # default cutoff time in minutes
        cutoff = cutoff_time_in_min
        # default destinations to find
        max_destinations_to_find = 4
        if len(destinations) < 4:
            max_destinations_to_find = len(destinations)

        results = network.analysis.generate_origin_destination_cost_matrix(
            origins=origins_features,
            destinations=destinations_features,
            cutoff=cutoff,
            time_of_day=current_time,
            number_of_destinations_to_find=max_destinations_to_find,
            origin_destination_line_shape='Straight Line'
        )
        # get output origin destination lines
        output_lines_features = results.output_origin_destination_lines.features
        fields_to_drop = ["Shape_Length"]
        updated_output_lines_features = drop_fields_from_features(output_lines_features, fields_to_drop, include_geometry)
        if include_geometry:
            output_lines_features = updated_output_lines_features.to_geojson
        else:
            output_lines_features = json.dumps(updated_output_lines_features.to_dict())

        content = {"output_origin_destination_lines": json.loads(output_lines_features),
                   "output_origins": json.loads(results.output_origins.to_geojson),
                   "output_destinations": json.loads(results.output_destinations.to_geojson)
                   }
        return {"content": json.dumps(content), "isError": False}

    except KeyError:
        return {"content": "Couldn't generate service areas for the given facilities!", "isError": True}


# create features from the batch geocoded results
def convert_poi_list_into_features(poi_list: List[str]) -> FeatureSet | None:
    # use batch geocode to get the given stops' address and location/geometry
    addresses = batch_geocode(poi_list)
    if len(addresses) != len(poi_list):
        return None

    poi_features = []
    for address in addresses:
        geometry = Point(address["location"])
        attributes = {"address": address["address"]}
        feature = Feature(geometry, attributes)
        poi_features.append(feature)

    return FeatureSet(features=poi_features)

def drop_fields_from_features(current_features: List[Feature], fields_to_drop: List[str], include_geometry: bool) -> FeatureSet:
    updated_features = []
    for feature in current_features:
        attrs = dict(feature.attributes)
        for field_to_drop in fields_to_drop:
            if field_to_drop in attrs:
                del attrs[field_to_drop]
        if include_geometry:
            updated_features.append(Feature(attributes=attrs, geometry=feature.geometry))
        else:
            updated_features.append(Feature(attributes=attrs))
    return FeatureSet(features=updated_features)

def create_starlette_app(fast_mcp: FastMCP, *, debug: bool = False) -> Starlette:
    """Create a Starlette application that can server the provided mcp server with SSE."""
    sse = SseServerTransport("/messages/")
    current_mcp_server: Server = fast_mcp._mcp_server  # noqa: WPS437

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,  # noqa: SLF001
        ) as (read_stream, write_stream):
            await current_mcp_server.run(
                read_stream,
                write_stream,
                current_mcp_server.create_initialization_options(),
            )

    # Create Starlette routes for SSE and message handling
    routes = [
        Route("/sse", endpoint=handle_sse),
        Mount("/messages", app=sse.handle_post_message),
    ]
    return Starlette(
        debug=debug,
        routes=routes,
    )

@click.command()
@click.option("--host", default="0.0.0.0", help="host")
@click.option("--port", default=8090, help="Port to listen on for SSE")
@click.option("--debug", default=False, help="debug")
def main(host: str, port: int, debug: bool=False) -> int:
    # os.environ['REFERER_DOMAIN'] = f'{args.host}:{args.port}'
    # print(os.environ['REFERER_DOMAIN'])
    global _DEBUG
    _DEBUG = debug
    # Bind SSE request handling to MCP server
    starlette_app = create_starlette_app(mcp, debug=_DEBUG)

    uvicorn.run(starlette_app, host=host, port=port)
    return 0


if __name__ == "__main__":
    # mcp_server = mcp._mcp_server  # noqa: WPS437

    import argparse

    parser = argparse.ArgumentParser(description='Run MCP SSE-based server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8090, help='Port to listen on')
    parser.add_argument('--debug', type=bool, default=False, help='debug')
    args = parser.parse_args()

    main(args.host, args.port)



# @mcp.tool(name='find_place_details')
# def mcp_find_place_details(placeName: str) -> dict[str, str]:
#     """Find detail information for the given place name"""
#     try:
#         results = network.analysis(placeName)
#         address = results["address"]["LongLabel"]
#         location = results["location"]
#         content = {"latitude": location["y"], "longitude": location["x"], "address": address}
#         return {"content": json.dumps(content), "isError": False}
#     except KeyError:
#         return {"content": "Couldn't convert the given geographic coordinate to an address!", "isError": True}

