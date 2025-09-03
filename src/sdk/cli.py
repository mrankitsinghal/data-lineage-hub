"""Command-line interface for Data Lineage Hub SDK."""

import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime

import click
import httpx
import structlog

from .client import LineageHubClient
from .config import configure, get_config


logger = structlog.get_logger(__name__)


@click.group()
@click.option("--endpoint", envvar="LINEAGE_HUB_ENDPOINT", help="Hub endpoint URL")
@click.option(
    "--api-key", envvar="LINEAGE_HUB_API_KEY", help="API key for authentication"
)
@click.option("--namespace", envvar="LINEAGE_HUB_NAMESPACE", help="Default namespace")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx, endpoint, api_key, namespace, debug):
    """Data Lineage Hub SDK CLI."""
    # Configure SDK
    config_kwargs = {}
    if endpoint:
        config_kwargs["hub_endpoint"] = endpoint
    if api_key:
        config_kwargs["api_key"] = api_key
    if namespace:
        config_kwargs["namespace"] = namespace
    if debug:
        config_kwargs["debug"] = debug

    if config_kwargs:
        configure(**config_kwargs)

    # Store config in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["config"] = get_config()


@cli.command()
@click.pass_context
def health(_ctx):
    """Check hub service health."""

    async def check_health():
        async with LineageHubClient() as client:
            try:
                health_status = await client.health_check()
            except httpx.HTTPError as e:
                click.echo(f"Health check failed: {e}", err=True)
                return False
            else:
                click.echo(f"Status: {health_status.status}")
                click.echo(f"Service: {health_status.service}")
                click.echo(f"Version: {health_status.version}")
                click.echo(f"Timestamp: {health_status.timestamp}")
                click.echo("Dependencies:")
                for dep, status in health_status.dependencies.items():
                    click.echo(f"  {dep}: {status}")
                return True

    success = asyncio.run(check_health())
    sys.exit(0 if success else 1)


@cli.command()
@click.option("--file", "-f", type=click.File("r"), help="JSON file containing events")
@click.option("--namespace", "-n", help="Target namespace")
@click.argument("events", nargs=-1)
@click.pass_context
def send_events(_ctx, file, namespace, events):
    """Send lineage events to the hub."""
    if file:
        try:
            event_data = json.load(file)
            event_list = [event_data] if isinstance(event_data, dict) else event_data
        except json.JSONDecodeError as e:
            click.echo(f"Invalid JSON in file: {e}", err=True)
            sys.exit(1)
    elif events:
        try:
            event_list = [json.loads(event) for event in events]
        except json.JSONDecodeError as e:
            click.echo(f"Invalid JSON in events: {e}", err=True)
            sys.exit(1)
    else:
        click.echo(
            "No events provided. Use --file or provide JSON events as arguments.",
            err=True,
        )
        sys.exit(1)

    async def send():
        async with LineageHubClient() as client:
            try:
                response = await client.send_lineage_events(
                    event_list, namespace=namespace
                )
            except httpx.HTTPError as e:
                click.echo(f"Failed to send events: {e}", err=True)
                return False
            else:
                click.echo(f"Sent {response.accepted} events successfully")
                if response.rejected > 0:
                    click.echo(f"Rejected: {response.rejected}")
                    for error in response.errors:
                        click.echo(f"  Error: {error}")
                return response.rejected == 0

    success = asyncio.run(send())
    sys.exit(0 if success else 1)


@cli.command()
@click.pass_context
def list_namespaces(_ctx):
    """List accessible namespaces."""

    async def list_ns():
        async with LineageHubClient() as client:
            try:
                namespaces = await client.list_namespaces()
            except httpx.HTTPError as e:
                click.echo(f"Failed to list namespaces: {e}", err=True)
                return False
            else:
                if not namespaces:
                    click.echo("No accessible namespaces found.")
                    return True

                click.echo(f"Found {len(namespaces)} namespace(s):")
                for ns in namespaces:
                    click.echo(f"  {ns.name} ({ns.display_name})")
                    if ns.description:
                        click.echo(f"    Description: {ns.description}")
                    click.echo(f"    Owners: {', '.join(ns.owners)}")
                    click.echo(f"    Created: {ns.created_at}")
                return True

    success = asyncio.run(list_ns())
    sys.exit(0 if success else 1)


@cli.command()
@click.argument("namespace_name")
@click.pass_context
def get_namespace(_ctx, namespace_name):
    """Get detailed namespace information."""

    async def get_ns():
        async with LineageHubClient() as client:
            try:
                ns = await client.get_namespace(namespace_name)
            except httpx.HTTPError as e:
                click.echo(f"Failed to get namespace: {e}", err=True)
                return False
            else:
                click.echo(f"Namespace: {ns.name}")
                click.echo(f"Display Name: {ns.display_name}")
                click.echo(f"Description: {ns.description or 'N/A'}")
                click.echo(f"Owners: {', '.join(ns.owners)}")
                click.echo(f"Viewers: {', '.join(ns.viewers)}")
                click.echo(f"Daily Event Quota: {ns.daily_event_quota}")
                click.echo(f"Retention Days: {ns.storage_retention_days}")
                click.echo(f"Created: {ns.created_at}")
                click.echo(f"Updated: {ns.updated_at}")
                if ns.tags:
                    click.echo("Tags:")
                    for key, value in ns.tags.items():
                        click.echo(f"  {key}: {value}")
                return True

    success = asyncio.run(get_ns())
    sys.exit(0 if success else 1)


@cli.command()
@click.pass_context
def config(ctx):
    """Show current configuration."""
    config = ctx.obj["config"]

    click.echo("Current SDK Configuration:")
    click.echo(f"  Hub Endpoint: {config.hub_endpoint}")
    click.echo(f"  API Key: {'***' if config.api_key else 'Not set'}")
    click.echo(f"  Namespace: {config.namespace}")
    click.echo(f"  Timeout: {config.timeout}s")
    click.echo(f"  Retry Attempts: {config.retry_attempts}")
    click.echo(f"  Enable Lineage: {config.enable_lineage}")
    click.echo(f"  Enable Telemetry: {config.enable_telemetry}")
    click.echo(f"  Debug Mode: {config.debug}")
    click.echo(f"  Dry Run: {config.dry_run}")


@cli.command()
@click.option("--job-name", required=True, help="Name of the job")
@click.option("--namespace", help="Job namespace")
@click.option("--run-id", help="Custom run ID (auto-generated if not provided)")
@click.option("--input", "-i", multiple=True, help="Input dataset paths")
@click.option("--output", "-o", multiple=True, help="Output dataset paths")
@click.option("--description", help="Job description")
@click.option("--tag", multiple=True, help="Tags in key=value format")
@click.pass_context
def create_job_events(
    ctx, job_name, namespace, run_id, inputs, output, description, tag
):
    """Create START and COMPLETE events for a job."""


    actual_run_id = run_id or str(uuid.uuid4())
    actual_namespace = namespace or ctx.obj["config"].namespace

    # Parse tags
    tags = {}
    for tag_str in tag:
        if "=" not in tag_str:
            click.echo(
                f"Invalid tag format: {tag_str}. Use key=value format.", err=True
            )
            sys.exit(1)
        key, value = tag_str.split("=", 1)
        tags[key] = value

    # Create START event
    start_event = {
        "eventType": "START",
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {"runId": actual_run_id},
        "job": {"namespace": actual_namespace, "name": job_name},
        "producer": "data-lineage-hub-sdk-cli",
    }

    if description:
        start_event["job"]["description"] = description

    if inputs:
        start_event["inputs"] = [{"namespace": "file", "name": path} for path in inputs]

    if tags:
        start_event["run"]["facets"] = {
            "tags": {
                "_producer": "data-lineage-hub-sdk-cli",
                "_schemaURL": "custom://tags",
                "tags": tags,
            }
        }

    # Create COMPLETE event
    complete_event = start_event.copy()
    complete_event["eventType"] = "COMPLETE"
    complete_event["eventTime"] = datetime.now(UTC).isoformat()

    if output:
        complete_event["outputs"] = [
            {"namespace": "file", "name": path} for path in output
        ]

    events = [start_event, complete_event]

    async def send():
        async with LineageHubClient() as client:
            try:
                response = await client.send_lineage_events(
                    events, namespace=actual_namespace
                )
            except httpx.HTTPError as e:
                click.echo(f"Failed to send events: {e}", err=True)
                return False
            else:
                click.echo(f"Created job events for run ID: {actual_run_id}")
                click.echo(f"Sent {response.accepted} events successfully")
                if response.rejected > 0:
                    click.echo(f"Rejected: {response.rejected}")
                    for error in response.errors:
                        click.echo(f"  Error: {error}")
                return response.rejected == 0

    success = asyncio.run(send())
    sys.exit(0 if success else 1)


def main():
    """Main entry point for CLI."""
    cli()
