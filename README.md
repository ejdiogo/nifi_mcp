# MCP NiFi Server

A template implementation of the [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server integrated with [Apache NiFi](https://nifi.apache.org/) for providing AI agents with data pipeline engineering capabilities.

## Overview

This project demonstrates how to build an MCP server that enables AI agents to interact with Apache NiFi instances through the NiFi REST API. It serves as a practical template for creating your own MCP servers, providing comprehensive NiFi management capabilities for AI-driven data pipeline automation.

The implementation follows the best practices laid out by Anthropic for building MCP servers, allowing seamless integration with any MCP-compatible client.

## Features

The server provides essential NiFi management tools:

- **Process Group Management**: Get root process group ID, list and create process groups
- **Processor Operations**: List, create, delete, and configure processors
- **Connection Management**: Create and delete connections between processors
- **Flow Discovery**: Get processor types and details
- **Pipeline Automation**: Enable AI agents to build and manage NiFi data flows

## Prerequisites

- Python 3.12+
- Apache NiFi instance (accessible via REST API)
- Docker if running the MCP server as a container (recommended)

## Installation

### Using uv

1. Install uv if you don't have it:
   ```bash
   pip install uv
   ```

2. Install dependencies:
   ```bash
   uv pip install -e .
   ```

3. Create a `.env` file based on `.env.example`:
   ```bash
   cp .env.example .env
   ```

4. Configure your environment variables in the `.env` file (see Configuration section)

### Using Docker (Recommended)

1. Build the Docker image:
   ```bash
   docker build -t mcp/nifi_nk --build-arg PORT=8050 .
   ```

2. Create a `.env` file based on `.env.example` and configure your environment variables

## Configuration

The following environment variables can be configured in your `.env` file:

| Variable | Description | Example |
|----------|-------------|----------|
| `TRANSPORT` | Transport protocol (sse or stdio) | `sse` |
| `HOST` | Host to bind to when using SSE transport | `0.0.0.0` |
| `PORT` | Port to listen on when using SSE transport | `8050` |
| `NIFI_BASE_URL` | Base URL for the NiFi API | `https://localhost:8443/nifi-api` |
| `NIFI_USERNAME` | Username for the NiFi API | `your-nifi-username` |
| `NIFI_PASSWORD` | Password for the NiFi API | `your-nifi-password` |
| `NIFI_TLS_VERIFY` | Verify TLS certificates | `True` / `False` |

## Running the Server

### Using uv

#### SSE Transport

```bash
# Set TRANSPORT=sse in .env then:
uv run src/main_nifi1.py
```

The MCP server will be run as an API endpoint that you can then connect to with the configuration shown below.

#### Stdio Transport

With stdio, the MCP client itself can spin up the MCP server, so nothing to run at this point.

### Using Docker

#### Build

```bash
docker build --tag 'mcp/nifi_nk' .
```

#### SSE Transport

```bash
docker run --env-file .env -p 8050:8050 mcp/nifi_nk
```

The MCP server will be run as an API endpoint within the container that you can then connect to with the configuration shown below.

#### Stdio Transport

With stdio, the MCP client itself can spin up the MCP server container, so nothing to run at this point.

## Integration with MCP Clients

### SSE Configuration

Once you have the server running with SSE transport, you can connect to it using this configuration:

```json
{
  "mcpServers": {
    "nifi_nk": {
      "transport": "sse",
      "url": "http://localhost:8050/sse"
    }
  }
}
```

> **Note for Windsurf users**: Use `serverUrl` instead of `url` in your configuration:
> ```json
> {
>   "mcpServers": {
>     "nifi_nk": {
>       "transport": "sse",
>       "serverUrl": "http://localhost:8050/sse"
>     }
>   }
> }
> ```

> **Note for n8n users**: Use `host.docker.internal` instead of `localhost` since n8n has to reach outside of its own container to the host machine:
> 
> So the full URL in the MCP node would be: `http://host.docker.internal:8050/sse`

Make sure to update the port if you are using a value other than the default 8050.

### Python with Stdio Configuration

Add this server to your MCP configuration for Claude Desktop, Windsurf, or any other MCP client:

```json
{
  "mcpServers": {
    "nifi_nk": {
      "command": "your/path/to/nifi_nk/.venv/Scripts/python.exe",
      "args": ["your/path/to/nifi_nk/src/main_nifi1.py"],
      "env": {
        "TRANSPORT": "stdio",
        "NIFI_BASE_URL": "https://localhost:8443/nifi-api",
        "NIFI_USERNAME": "your-nifi-username",
        "NIFI_PASSWORD": "your-nifi-password",
        "NIFI_TLS_VERIFY": "False"
      }
    }
  }
}
```

### Docker with Stdio Configuration

```json
{
  "mcpServers": {
    "nifi_nk": {
      "command": "docker",
      "args": ["run", "--rm", "-i", 
               "-e", "TRANSPORT", 
               "-e", "NIFI_BASE_URL", 
               "-e", "NIFI_USERNAME", 
               "-e", "NIFI_PASSWORD", 
               "-e", "NIFI_TLS_VERIFY", 
               "mcp/nifi_nk"],
      "env": {
        "TRANSPORT": "stdio",
        "NIFI_BASE_URL": "https://localhost:8443/nifi-api",
        "NIFI_USERNAME": "your-nifi-username",
        "NIFI_PASSWORD": "your-nifi-password",
        "NIFI_TLS_VERIFY": "False"
      }
    }
  }
}
```

## Available Tools

The MCP server provides the following tools for NiFi management:

- `get_root_process_group_id`: Get the root process group ID
- `list_processors`: List all processors in a process group
- `create_processor`: Create a new processor
- `delete_processor`: Delete an existing processor
- `get_processor_details`: Get detailed information about a processor
- `create_connection`: Create connections between processors
- `delete_connection`: Delete existing connections
- `get_process_groups`: List process groups
- `create_process_group`: Create new process groups
- `get_processor_types`: Get available processor types

## Building Your Own Server

This template provides a foundation for building more complex MCP servers. To build your own:

1. Add your own tools by creating methods with the `@mcp.tool()` decorator
2. Create your own lifespan function to add your own dependencies (clients, database connections, etc.)
3. Modify the `utils.py` file for any helper functions you need for your MCP server
4. Feel free to add prompts and resources as well with `@mcp.resource()` and `@mcp.prompt()`

## License

This project is licensed under the terms specified in the LICENSE file.
