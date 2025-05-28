from mcp.server.fastmcp import FastMCP, Context
from contextlib import asynccontextmanager
from dataclasses import dataclass
from loguru import logger
import os
import httpx
import asyncio
from dotenv import load_dotenv
from typing import AsyncIterator, List, Dict, Any
from utils import NiFiClient, get_nifi_client

load_dotenv()

@dataclass
class AppContext:
    nifi_client: NiFiClient

@asynccontextmanager
async def nifi_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    nifi = await get_nifi_client(
        base_url=os.getenv("NIFI_BASE_URL"),
        username=os.getenv("NIFI_USERNAME"),
        password=os.getenv("NIFI_PASSWORD"),
        tls_verify=os.getenv("NIFI_TLS_VERIFY", "false").lower() == "true"
    )
    try:
        yield AppContext(nifi_client=nifi)
    finally:
        if hasattr(nifi, '_client') and nifi._client:
            await nifi._client.aclose()

# --- MCP Server ---
mcp = FastMCP(
    "mcp-nifi-2.4.0-test",
    description="MCP server for Apache NiFi 2.4.0 with pipeline resource and advanced tools",
    lifespan=nifi_lifespan,
    host=os.getenv("HOST", "127.0.0.1"),
    port=int(os.getenv("PORT", "8051")),
)

# --- Tool: List Pipelines ---
@mcp.tool()
async def list_pipelines(ctx: Context) -> List[Dict[str, Any]]:
    """List all pipelines (root and child process groups) in NiFi."""
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        # Get root process group flow
        resp = await client.get("/flow/process-groups/root")
        resp.raise_for_status()
        data = resp.json()
        
        pipelines = []
        
        def collect_groups(pg_data, parent_name="root"):
            # Handle the root process group
            if "processGroupFlow" in pg_data:
                flow = pg_data["processGroupFlow"]
                pg_id = flow.get("id", "root")
                pg_name = flow.get("breadcrumb", {}).get("breadcrumb", {}).get("name", "root")
                pipelines.append({"id": pg_id, "name": pg_name, "parent": parent_name})
                
                # Collect child process groups
                for child_pg in flow.get("flow", {}).get("processGroups", []):
                    child_id = child_pg.get("id")
                    child_name = child_pg.get("component", {}).get("name", f"ProcessGroup-{child_id}")
                    pipelines.append({"id": child_id, "name": child_name, "parent": pg_name})
            
        collect_groups(data)
        return pipelines
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise

# --- Tool: Explore Canvas ---
@mcp.tool()
async def explore_canvas(ctx: Context, process_group_id: str = "root") -> Dict[str, Any]:
    """Explore the NiFi canvas: list processors, connections, and child groups for a process group."""
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        # For NiFi 2.4.0, use the flow endpoint
        resp = await client.get(f"/flow/process-groups/{process_group_id}")
        resp.raise_for_status()
        data = resp.json()
        
        flow_data = data.get("processGroupFlow", {}).get("flow", {})
        
        return {
            "process_group_id": process_group_id,
            "processors": flow_data.get("processors", []),
            "connections": flow_data.get("connections", []),
            "child_groups": flow_data.get("processGroups", []),
            "input_ports": flow_data.get("inputPorts", []),
            "output_ports": flow_data.get("outputPorts", []),
            "funnels": flow_data.get("funnels", []),
            "labels": flow_data.get("labels", [])
        }
        
    except Exception as e:
        logger.error(f"Error exploring canvas for process group {process_group_id}: {e}")
        raise

# --- Tool: Get Root Process Group ID ---
@mcp.tool()
async def get_root_pg_id(ctx: Context) -> Dict[str, Any]:
    """Get the root process group ID and basic information."""
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        resp = await client.get("/flow/process-groups/root")
        resp.raise_for_status()
        data = resp.json()
        
        flow = data.get("processGroupFlow", {})
        return {
            "root_id": flow.get("id", "root"),
            "name": flow.get("breadcrumb", {}).get("breadcrumb", {}).get("name", "NiFi Flow"),
            "uri": flow.get("uri"),
            "parent_group_id": flow.get("parentGroupId"),
            "parameter_context": flow.get("parameterContext")
        }
        
    except Exception as e:
        logger.error(f"Error getting root process group ID: {e}")
        raise

# --- Tool: Pipeline Optimization ---
@mcp.tool()
async def optimize_pipeline(ctx: Context, process_group_id: str) -> Dict[str, Any]:
    """Suggest optimizations for a pipeline (process group) based on processor types and connections."""
    try:
        exploration = await explore_canvas(ctx, process_group_id)
        suggestions = []
        
        processors = exploration["processors"]
        connections = exploration["connections"]
        
        if not processors:
            suggestions.append("No processors found. Add processors to start building your pipeline.")
            return {"suggestions": suggestions}
        
        # Check for disconnected processors
        connected_ids = set()
        for conn in connections:
            source = conn.get("component", {}).get("source", {})
            destination = conn.get("component", {}).get("destination", {})
            if source.get("id"):
                connected_ids.add(source["id"])
            if destination.get("id"):
                connected_ids.add(destination["id"])
        
        for proc in processors:
            proc_id = proc.get("id")
            proc_name = proc.get("component", {}).get("name", "Unknown")
            if proc_id not in connected_ids:
                suggestions.append(f"Processor '{proc_name}' (ID: {proc_id}) is not connected to any other component.")
        
        # Check for stopped processors
        for proc in processors:
            status = proc.get("status", {})
            run_status = status.get("runStatus", "UNKNOWN")
            proc_name = proc.get("component", {}).get("name", "Unknown")
            if run_status == "STOPPED":
                suggestions.append(f"Processor '{proc_name}' is stopped. Consider starting it if it should be running.")
        
        # Check for processors with validation errors
        for proc in processors:
            status = proc.get("status", {})
            validation_errors = status.get("validationErrors", [])
            proc_name = proc.get("component", {}).get("name", "Unknown")
            if validation_errors:
                suggestions.append(f"Processor '{proc_name}' has validation errors: {', '.join(validation_errors)}")
        
        return {"suggestions": suggestions, "processor_count": len(processors), "connection_count": len(connections)}
        
    except Exception as e:
        logger.error(f"Error optimizing pipeline {process_group_id}: {e}")
        raise

# --- Tool: Full Pipeline Creation ---
@mcp.tool()
async def create_full_pipeline(
    ctx: Context, 
    parent_pg_id: str, 
    pipeline_name: str, 
    processors: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a new pipeline (process group) and optionally add processors to it.
    
    Args:
        parent_pg_id: The ID of the parent process group where the new pipeline will be created
        pipeline_name: The name for the new pipeline (process group)
        processors: Optional list of processor definitions. Each processor should have:
                   - type: The processor class name (e.g., 'org.apache.nifi.processors.standard.GenerateFlowFile')
                   - name: Display name for the processor
                   - position: Optional dict with x,y coordinates
                   - config: Optional dict with processor configuration
    
    Returns:
        Dict containing the created process group and any processors that were added
    """
    if processors is None:
        processors = []
        
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        # Create process group
        pg_request = {
            "revision": {"clientId": nifi_client._client_id, "version": 0},
            "component": {
                "name": pipeline_name,
                "position": {"x": 100.0, "y": 100.0}
            }
        }
        
        logger.info(f"Creating process group '{pipeline_name}' in parent {parent_pg_id}")
        resp = await client.post(f"/process-groups/{parent_pg_id}/process-groups", json=pg_request)
        resp.raise_for_status()
        pg = resp.json()
        pg_id = pg["id"]
        
        created_processors = []
        
        # Add processors to the new process group if any were specified
        if processors:
            logger.info(f"Adding {len(processors)} processors to process group {pg_id}")
            for i, proc in enumerate(processors):
                # Validate required processor fields
                if "type" not in proc:
                    raise ValueError(f"Processor {i} missing required 'type' field")
                if "name" not in proc:
                    raise ValueError(f"Processor {i} missing required 'name' field")
                
                proc_request = {
                    "revision": {"clientId": nifi_client._client_id, "version": 0},
                    "component": {
                        "type": proc["type"],
                        "name": proc["name"],
                        "position": proc.get("position", {"x": 200.0 + (i * 200), "y": 200.0}),
                        "config": proc.get("config", {})
                    }
                }
                
                logger.info(f"Creating processor '{proc['name']}' of type '{proc['type']}'")
                proc_resp = await client.post(f"/process-groups/{pg_id}/processors", json=proc_request)
                proc_resp.raise_for_status()
                created_processors.append(proc_resp.json())
        
        return {
            "process_group": pg,
            "processors": created_processors,
            "success": True,
            "message": f"Successfully created pipeline '{pipeline_name}' with {len(created_processors)} processors"
        }
        
    except Exception as e:
        logger.error(f"Error creating pipeline '{pipeline_name}': {e}")
        raise

# --- Tool: Get Available Processor Types ---
@mcp.tool()
async def get_processor_types(ctx: Context) -> List[Dict[str, Any]]:
    """Get all available processor types in the NiFi instance."""
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        resp = await client.get("/flow/processor-types")
        resp.raise_for_status()
        data = resp.json()
        
        processor_types = data.get("processorTypes", [])
        
        # Return simplified processor type information
        simplified_types = []
        for proc_type in processor_types:
            simplified_types.append({
                "type": proc_type.get("type"),
                "bundle": proc_type.get("bundle", {}),
                "description": proc_type.get("description", ""),
                "tags": proc_type.get("tags", [])
            })
        
        return simplified_types
        
    except Exception as e:
        logger.error(f"Error getting processor types: {e}")
        raise

# --- Prompts: Common Usage ---
@mcp.prompt("pipeline_overview")
def pipeline_overview_prompt():
    return """You are an expert in Apache NiFi 2.4.0. Given a list of pipelines (process groups), explain their purpose and suggest improvements. Focus on:
1. Data flow patterns
2. Performance optimization
3. Error handling
4. Monitoring and alerting
5. Security considerations"""

@mcp.prompt("canvas_exploration")
def canvas_exploration_prompt():
    return """Explore the NiFi 2.4.0 canvas and describe the main processors, connections, and their roles in the data flow. Analyze:
1. Data ingestion points
2. Transformation steps
3. Routing logic
4. Output destinations
5. Error handling paths"""

@mcp.prompt("pipeline_optimization")
def pipeline_optimization_prompt():
    return """Analyze the given NiFi 2.4.0 pipeline and suggest optimizations for:
1. Performance (throughput, latency)
2. Reliability (error handling, retries)
3. Maintainability (naming, documentation)
4. Resource utilization (memory, CPU)
5. Monitoring and observability"""

@mcp.prompt("full_pipeline_creation")
def full_pipeline_creation_prompt():
    return """Given a set of requirements, create a new NiFi 2.4.0 pipeline with the necessary processors and connections. Consider:
1. Data source and format
2. Required transformations
3. Destination systems
4. Error handling strategy
5. Performance requirements"""

# --- Tool: Create Process Group ---
@mcp.tool()
async def create_process_group(
    ctx: Context, 
    parent_pg_id: str, 
    name: str, 
    position: Dict[str, float] = None
) -> Dict[str, Any]:
    """Create a new empty process group.
    
    Args:
        parent_pg_id: The ID of the parent process group
        name: The name for the new process group
        position: Optional position dict with x,y coordinates
    
    Returns:
        Dict containing the created process group information
    """
    if position is None:
        position = {"x": 100.0, "y": 100.0}
        
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        pg_request = {
            "revision": {"clientId": nifi_client._client_id, "version": 0},
            "component": {
                "name": name,
                "position": position
            }
        }
        
        logger.info(f"Creating process group '{name}' in parent {parent_pg_id}")
        resp = await client.post(f"/process-groups/{parent_pg_id}/process-groups", json=pg_request)
        resp.raise_for_status()
        pg = resp.json()
        
        return {
            "process_group": pg,
            "success": True,
            "message": f"Successfully created process group '{name}'"
        }
        
    except Exception as e:
        logger.error(f"Error creating process group '{name}': {e}")
        raise

# --- Tool: Create Connection ---
@mcp.tool()
async def create_connection(
    ctx: Context,
    process_group_id: str,
    source_id: str,
    destination_id: str,
    relationships: List[str],
    source_type: str = "PROCESSOR",
    destination_type: str = "PROCESSOR",
    name: str = None
) -> Dict[str, Any]:
    """Create a connection between two components in a process group.
    
    Args:
        process_group_id: The ID of the process group containing the components
        source_id: The ID of the source component
        destination_id: The ID of the destination component  
        relationships: List of relationship names to route (e.g., ['success', 'failure'])
        source_type: Type of source component (PROCESSOR, INPUT_PORT, OUTPUT_PORT, FUNNEL)
        destination_type: Type of destination component (PROCESSOR, INPUT_PORT, OUTPUT_PORT, FUNNEL)
        name: Optional name for the connection
    
    Returns:
        Dict containing the created connection information
    """
    nifi_client = ctx.request_context.lifespan_context.nifi_client
    client = await nifi_client._get_client()
    
    try:
        connection_request = {
            "revision": {"clientId": nifi_client._client_id, "version": 0},
            "component": {
                "name": name or f"Connection from {source_id} to {destination_id}",
                "source": {
                    "id": source_id,
                    "groupId": process_group_id,
                    "type": source_type
                },
                "destination": {
                    "id": destination_id,
                    "groupId": process_group_id,
                    "type": destination_type
                },
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
                "flowFileExpiration": "0 sec"
            }
        }
        
        logger.info(f"Creating connection from {source_id} to {destination_id} with relationships {relationships}")
        resp = await client.post(f"/process-groups/{process_group_id}/connections", json=connection_request)
        resp.raise_for_status()
        connection = resp.json()
        
        return {
            "connection": connection,
            "success": True,
            "message": f"Successfully created connection from {source_id} to {destination_id}"
        }
        
    except Exception as e:
        logger.error(f"Error creating connection from {source_id} to {destination_id}: {e}")
        raise

async def main():
    transport = os.getenv("TRANSPORT", "sse")
    if transport == 'sse':
        # Run the MCP server with sse transport
        await mcp.run_sse_async()
    else:
        # Run the MCP server with stdio transport
        await mcp.run_stdio_async()

if __name__ == "__main__":
    asyncio.run(main())