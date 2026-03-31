#!/usr/bin/env python3
"""OCI Compute Availability Report Generator.

Generates self-contained HTML availability reports for OCI Compute VM instances
using CpuUtilization and instance_status metrics.
"""

import argparse
import base64
import html
import logging
import math
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime, timezone, timedelta

try:
    import oci
except ImportError:
    oci = None

VERSION = "1.0"

log = logging.getLogger("availability-report")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate OCI Compute availability reports"
    )

    # Required
    parser.add_argument("--compartment-id", required=True, help="Target compartment OCID")

    # Authentication
    parser.add_argument("--auth", choices=["instance_principal", "config"],
                        default="instance_principal", help="Auth method (default: instance_principal)")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile (default: DEFAULT)")

    # Reporting
    parser.add_argument("--days", type=int, choices=[7, 14, 30, 60, 90],
                        default=7, help="Reporting period in days (default: 7)")
    parser.add_argument("--sla-target", type=float, default=99.95,
                        help="SLA target %% (default: 99.95)")
    parser.add_argument("--running-only", action="store_true",
                        help="Only include RUNNING instances (default: all non-TERMINATED)")
    parser.add_argument("--region", help="OCI region override")
    parser.add_argument("--compartment-name", help="Compartment display name override")

    # Branding
    parser.add_argument("--title", help="Custom report title (top-right header)")
    parser.add_argument("--logo", help="Path to logo image (embedded as base64)")

    # Exclusions
    parser.add_argument("--exclude", nargs="*", default=[],
                        help="Instance names or OCIDs to exclude from the report")
    parser.add_argument("--exclude-file", help="Path to file with instance names/OCIDs to exclude (one per line)")

    # Output
    parser.add_argument("--output", help="Output HTML file path")
    parser.add_argument("--upload", action="store_true", help="Upload to Object Storage")
    parser.add_argument("--bucket", default="availability-reports", help="Bucket name")
    parser.add_argument("--os-namespace", help="Object Storage namespace")
    parser.add_argument("--par-expiry-days", type=int, default=30,
                        help="PAR link expiry in days (default: 30)")

    return parser.parse_args(argv)


def setup_auth(args):
    """Create OCI config and signer based on auth method.

    Returns:
        (config, signer) tuple. For config auth, signer is None.
        For instance_principal, config is minimal and signer is set.
    """
    if oci is None:
        raise RuntimeError("The 'oci' package is required. Install it with: pip install oci")
    if args.auth == "config":
        config = oci.config.from_file(profile_name=args.profile)
        if args.region:
            config["region"] = args.region
        oci.config.validate_config(config)
        return config, None
    else:
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        except Exception as e:
            if "169.254.169.254" in str(e) or "ConnectTimeout" in str(e) or "timed out" in str(e):
                log.error("Instance Principals auth failed — this only works on OCI compute instances.")
                log.error("If running locally, use: --auth config --profile <PROFILE_NAME>")
                sys.exit(1)
            raise
        config = {"region": args.region} if args.region else {"region": signer.region}
        return config, signer


def make_client(client_class, config, signer):
    """Create an OCI SDK client with appropriate auth.

    For signer-based auth, pass the config dict (which contains region)
    so --region override is respected.
    """
    if signer:
        return client_class(config=config, signer=signer)
    return client_class(config)


def classify_hour(has_cpu, instance_status, query_failed=False):
    """Classify an hourly bucket as up, down, stopped, or nodata.

    Args:
        has_cpu: True if CpuUtilization data exists for this hour
        instance_status: 0 (healthy), 1 (unhealthy), or None (no data)
        query_failed: True if the Monitoring API call failed for this scope

    Returns:
        "up", "down", "stopped", or "nodata"
    """
    if query_failed:
        return "nodata"
    if has_cpu:
        if instance_status == 1:
            return "down"
        return "up"
    if instance_status == 0:
        return "up"
    if instance_status == 1:
        return "down"
    return "stopped"


def build_availability_matrix(instances, hourly_buckets, cpu_metrics, status_metrics,
                               failed_instance_ids=None):
    """Build availability matrix from metric data.

    Args:
        instances: list of instance dicts (need id) or list of instance ID strings
        hourly_buckets: list of hour keys (ISO format strings)
        cpu_metrics: {instance_id: {hour_key: value}} from CpuUtilization
        status_metrics: {instance_id: {hour_key: value}} from instance_status
        failed_instance_ids: set of instance OCIDs where metric queries failed

    Returns:
        {instance_id: {hour_key: "up"|"down"|"stopped"|"nodata"}}
    """
    failed_instance_ids = failed_instance_ids or set()
    matrix = {}
    for inst in instances:
        inst_id = inst["id"] if isinstance(inst, dict) else inst
        query_failed = inst_id in failed_instance_ids

        inst_cpu = cpu_metrics.get(inst_id, {})
        inst_status = status_metrics.get(inst_id, {})
        hourly = {}
        for hour in hourly_buckets:
            has_cpu = hour in inst_cpu
            status_val = inst_status.get(hour)
            if status_val is not None:
                status_val = int(status_val)
            hourly[hour] = classify_hour(has_cpu, status_val, query_failed=query_failed)
        matrix[inst_id] = hourly
    return matrix


def compute_instance_stats(hourly_statuses):
    """Compute availability stats from hourly classification dict.

    Args:
        hourly_statuses: dict of {hour_key: "up"|"down"|"stopped"|"nodata"}

    Returns:
        dict with up_hours, down_hours, stopped_hours, nodata_hours,
        monitored_hours, total_hours, availability_pct (float or None),
        downtime_minutes, data_complete
    """
    up = sum(1 for v in hourly_statuses.values() if v == "up")
    down = sum(1 for v in hourly_statuses.values() if v == "down")
    stopped = sum(1 for v in hourly_statuses.values() if v == "stopped")
    nodata = sum(1 for v in hourly_statuses.values() if v == "nodata")
    monitored = up + down
    total = len(hourly_statuses)
    data_complete = nodata == 0

    if nodata > 0:
        # Any nodata hours -> availability is unreliable, show N/A
        availability_pct = None
    elif monitored == 0:
        availability_pct = None
    else:
        availability_pct = round(up / monitored * 100, 2)

    return {
        "up_hours": up,
        "down_hours": down,
        "stopped_hours": stopped,
        "nodata_hours": nodata,
        "monitored_hours": monitored,
        "total_hours": total,
        "availability_pct": availability_pct,
        "downtime_minutes": down * 60,
        "data_complete": data_complete,
    }


def compute_compartment_stats(instances_in_compartment, sla_target):
    """Compute availability stats for a single compartment.

    If ANY instance in the compartment has data_complete=False (nodata hours),
    compartment_availability_pct and at_target_count both become None (fail closed).

    Args:
        instances_in_compartment: list of instance dicts with stats
        sla_target: SLA target percentage

    Returns:
        dict with instance_count, compartment_availability_pct, at_target_count, data_complete
    """
    total_up = sum(s["up_hours"] for s in instances_in_compartment)
    total_monitored = sum(s["monitored_hours"] for s in instances_in_compartment)
    all_complete = all(s.get("data_complete", True) for s in instances_in_compartment)

    if not all_complete or total_monitored == 0:
        return {
            "instance_count": len(instances_in_compartment),
            "compartment_availability_pct": None,
            "at_target_count": None,
            "data_complete": all_complete,
        }

    pct = round(total_up / total_monitored * 100, 2)
    at_target = sum(
        1 for s in instances_in_compartment
        if s["availability_pct"] is not None and s["availability_pct"] >= sla_target
    )

    return {
        "instance_count": len(instances_in_compartment),
        "compartment_availability_pct": pct,
        "at_target_count": at_target,
        "data_complete": all_complete,
    }


def compute_fleet_stats(instance_stats_list, sla_target, discovery_warnings=None):
    """Compute fleet-level availability from per-instance stats.

    Fail-closed rules:
    - Any instance with data_complete=False -> data_complete=False
    - Any discovery_warnings -> discovery_complete=False
    - report_complete = data_complete AND discovery_complete
    - If report_complete=False: fleet_availability_pct, at_target_count,
      total_up_hours, total_monitored_hours all become None
    - discovered_instance_count always stays numeric for diagnostics

    Args:
        instance_stats_list: list of dicts from compute_instance_stats
        sla_target: SLA target percentage (e.g. 99.95)
        discovery_warnings: list of warning strings from discovery phase

    Returns:
        dict with discovered_instance_count, fleet_availability_pct,
        at_target_count, total_up_hours, total_monitored_hours,
        data_complete, discovery_complete, report_complete
    """
    discovery_warnings = discovery_warnings or []
    total_up = sum(s["up_hours"] for s in instance_stats_list)
    total_monitored = sum(s["monitored_hours"] for s in instance_stats_list)
    data_complete = all(s.get("data_complete", True) for s in instance_stats_list)
    discovery_complete = len(discovery_warnings) == 0
    report_complete = data_complete and discovery_complete

    # Only count instances with computable availability toward SLA target
    # Stopped instances (availability_pct=None, monitored_hours=0) are excluded
    monitorable = [s for s in instance_stats_list if s["availability_pct"] is not None]
    monitorable_count = len(monitorable)

    if not report_complete or total_monitored == 0:
        return {
            "discovered_instance_count": len(instance_stats_list),
            "monitorable_count": monitorable_count,
            "fleet_availability_pct": None,
            "at_target_count": None,
            "total_up_hours": None if not report_complete else total_up,
            "total_monitored_hours": None if not report_complete else total_monitored,
            "data_complete": data_complete,
            "discovery_complete": discovery_complete,
            "report_complete": report_complete,
        }

    fleet_pct = round(total_up / total_monitored * 100, 2)
    at_target = sum(
        1 for s in monitorable
        if s["availability_pct"] >= sla_target
    )

    return {
        "discovered_instance_count": len(instance_stats_list),
        "monitorable_count": monitorable_count,
        "fleet_availability_pct": fleet_pct,
        "at_target_count": at_target,
        "total_up_hours": total_up,
        "total_monitored_hours": total_monitored,
        "data_complete": data_complete,
        "discovery_complete": discovery_complete,
        "report_complete": report_complete,
    }


def get_heatmap_resolution(days):
    """Return (block_hours, label) for adaptive heatmap resolution."""
    if days <= 7:
        return 1, "1 hour"
    elif days <= 14:
        return 4, "4 hours"
    elif days <= 30:
        return 6, "6 hours"
    else:
        return 24, "1 day"


def _build_ancestor_path(compartment_map, comp_id, max_depth=10):
    """Build full ancestor path for a compartment: grandparent/parent/name."""
    parts = []
    current = comp_id
    for _ in range(max_depth):
        info = compartment_map.get(current)
        if not info:
            break
        parts.append(info["name"])
        if not info.get("parent_id") or info["parent_id"] not in compartment_map:
            break
        current = info["parent_id"]
    parts.reverse()
    return "/".join(parts)


def build_compartment_labels(compartment_map):
    """Add a 'label' key to each compartment entry.

    Algorithm:
    1. Start with label = name for all compartments
    2. Find groups of compartments that share the same label
    3. For each collision group, prepend one more ancestor to each label
    4. Repeat until all labels are unique, or fall back to full path

    This handles arbitrarily deep duplicates:
    - orgA/team/prod vs orgB/team/prod (not just parent/prod)
    """
    # Initialize labels with just the name
    for comp_id, info in compartment_map.items():
        info["_path_parts"] = [info["name"]]
        info["_current_id"] = comp_id
        info["label"] = info["name"]

    # Iteratively disambiguate until all labels are unique (max 10 levels)
    for _ in range(10):
        # Find label collisions
        label_groups = {}
        for comp_id, info in compartment_map.items():
            label_groups.setdefault(info["label"], []).append(comp_id)

        collisions = {label: ids for label, ids in label_groups.items() if len(ids) > 1}
        if not collisions:
            break

        # For each collision group, prepend one more ancestor
        for label, comp_ids in collisions.items():
            for comp_id in comp_ids:
                info = compartment_map[comp_id]
                # Walk up to next ancestor not yet in the path
                current = comp_id
                for _ in range(len(info["_path_parts"])):
                    parent_id = compartment_map.get(current, {}).get("parent_id")
                    if not parent_id or parent_id not in compartment_map:
                        break
                    current = parent_id
                parent_info = compartment_map.get(current)
                if parent_info and parent_info["name"] not in info["_path_parts"]:
                    info["_path_parts"].insert(0, parent_info["name"])
                else:
                    # Fall back to full path if we can't disambiguate further
                    info["_path_parts"] = _build_ancestor_path(
                        compartment_map, comp_id
                    ).split("/")
                info["label"] = "/".join(info["_path_parts"])

    # Clean up temporary keys
    for info in compartment_map.values():
        info.pop("_path_parts", None)
        info.pop("_current_id", None)


def discover_compartments(identity_client, compartment_id):
    """Get compartment name and list all sub-compartments.

    Returns:
        (root_compartment_name, compartment_map, discovery_warnings) where:
        - compartment_map: {compartment_ocid: {"name": str, "parent_id": str|None, "label": str}}
        - discovery_warnings: list of warning strings (empty = fully successful)
    """
    discovery_warnings = []

    # Get root compartment name
    root = identity_client.get_compartment(compartment_id).data
    root_name = root.name

    # compartment_map stores {ocid: {"name": str, "parent_id": str}}
    compartment_map = {compartment_id: {"name": root_name, "parent_id": None}}

    # List sub-compartments recursively
    # Note: compartment_id_in_subtree=True only works when compartment_id is a
    # tenancy root OCID. For non-root compartments, list direct children and
    # recurse manually.
    try:
        if is_tenancy_ocid(compartment_id):
            sub_compartments = oci.pagination.list_call_get_all_results(
                identity_client.list_compartments,
                compartment_id,
                compartment_id_in_subtree=True,
                access_level="ACCESSIBLE",
                lifecycle_state="ACTIVE",
            ).data
        else:
            # For non-root: list direct children only (no subtree flag)
            sub_compartments = oci.pagination.list_call_get_all_results(
                identity_client.list_compartments,
                compartment_id,
                access_level="ACCESSIBLE",
                lifecycle_state="ACTIVE",
            ).data
        for c in sub_compartments:
            compartment_map[c.id] = {"name": c.name, "parent_id": c.compartment_id}
    except oci.exceptions.ServiceError as e:
        msg = f"Could not list sub-compartments: {e.message}"
        log.warning(msg)
        discovery_warnings.append(msg)

    # Build display labels, disambiguating duplicate names
    build_compartment_labels(compartment_map)

    return root_name, compartment_map, discovery_warnings


def discover_instances(compute_client, compartment_map, running_only=False, exclude_list=None):
    """Discover VM instances across compartment tree.

    Note: Compute.ListInstances does NOT support compartment_id_in_subtree.
    We must iterate each compartment individually.

    Args:
        compute_client: OCI ComputeClient
        compartment_map: {compartment_ocid: {"name": str, "parent_id": str, "label": str}}
        running_only: if True, only include RUNNING instances

    Returns:
        (instances, discovery_warnings) tuple:
        - instances: list of instance dicts with metadata
        - discovery_warnings: list of warning strings for failed compartments
    """
    instances = []
    discovery_warnings = []

    for comp_id, comp_info in compartment_map.items():
        comp_name = comp_info["name"]
        comp_label = comp_info.get("label", comp_name)
        try:
            comp_instances = oci.pagination.list_call_get_all_results(
                compute_client.list_instances,
                comp_id,
            ).data
        except oci.exceptions.ServiceError as e:
            msg = f"Could not list instances in {comp_label} ({comp_id}): {e.message}"
            log.warning(msg)
            discovery_warnings.append(msg)
            continue

        for inst in comp_instances:
            # Skip terminated always
            if inst.lifecycle_state == "TERMINATED":
                continue
            # Skip non-running if --running-only
            if running_only and inst.lifecycle_state != "RUNNING":
                continue
            # Skip excluded instances (by name or OCID)
            if exclude_list and (inst.display_name in exclude_list or inst.id in exclude_list):
                log.info(f"Excluding instance: {inst.display_name}")
                continue

            instances.append({
                "id": inst.id,
                "name": inst.display_name,
                "state": inst.lifecycle_state,
                "shape": inst.shape,
                "ad": inst.availability_domain,
                "fd": inst.fault_domain,
                "region": inst.region,
                "compartment_id": inst.compartment_id,
                "compartment_name": comp_name,
                "compartment_label": comp_label,
            })

    log.info(f"Discovered {len(instances)} instances across {len(compartment_map)} compartments")
    return instances, discovery_warnings


def group_instances_by_compartment(instances):
    """Group instances by compartment OCID, sorted worst-availability-first within each group.

    Groups by compartment_id (OCID) to avoid merging distinct compartments
    that share the same display name. Uses compartment_label for display.

    Returns:
        OrderedDict of {compartment_id: {
            "name": compartment_display_name,
            "instances": [instances sorted by availability asc]
        }}
    """
    groups = {}
    for inst in instances:
        comp_id = inst.get("compartment_id", inst.get("compartment_name"))
        comp_label = inst.get("compartment_label", inst.get("compartment_name", comp_id))
        if comp_id not in groups:
            groups[comp_id] = {"name": comp_label, "instances": []}
        groups[comp_id]["instances"].append(inst)

    # Sort instances within each group: worst availability first
    # None (N/A) sorts before numbers (worst)
    for comp_id in groups:
        groups[comp_id]["instances"].sort(key=lambda i: (
            i.get("availability_pct") is not None,
            i.get("availability_pct", 0),
        ))

    return OrderedDict(sorted(groups.items(), key=lambda x: x[1]["name"]))


DATAPOINT_LIMIT = 80_000  # safety margin below 100K API limit


def build_hourly_buckets(start_time, end_time):
    """Build list of hourly bucket keys (ISO format, UTC) for the reporting period."""
    buckets = []
    current = start_time
    while current < end_time:
        buckets.append(current.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current += timedelta(hours=1)
    return buckets


def calculate_batch_groups(instance_ids, hours):
    """Split instance IDs into batches to stay under API data point limit.

    Args:
        instance_ids: list of instance OCIDs
        hours: number of hourly buckets in the reporting period

    Returns:
        list of lists, each sub-list is a batch of instance IDs
    """
    if not instance_ids:
        return []

    max_per_batch = max(1, DATAPOINT_LIMIT // hours)
    batches = []
    for i in range(0, len(instance_ids), max_per_batch):
        batches.append(instance_ids[i:i + max_per_batch])
    return batches


def is_tenancy_ocid(ocid):
    """Check if an OCID is a tenancy root OCID."""
    return ocid.startswith("ocid1.tenancy.")


def collect_metrics(monitoring_client, compartment_id, namespace, metric_name,
                    start_time, end_time, use_subtree=False, instance_ids=None):
    """Query SummarizeMetricsData for a metric across instances.

    Args:
        monitoring_client: OCI MonitoringClient
        compartment_id: compartment OCID for the query
        namespace: metric namespace (e.g. "oci_computeagent")
        metric_name: metric name (e.g. "CpuUtilization")
        start_time: datetime, start of reporting window
        end_time: datetime, end of reporting window
        use_subtree: only set True when compartment_id is tenancy root OCID
        instance_ids: optional list of instance OCIDs to filter by

    Returns:
        (metrics_dict, failed) tuple:
        - metrics_dict: {instance_ocid: {hour_key: value}}
        - failed: bool, True if the API call failed
    """
    if instance_ids:
        resource_filter = " || ".join(
            f'resourceId = "{rid}"' for rid in instance_ids
        )
        query = f"{metric_name}[1h]{{{resource_filter}}}.max()"
    else:
        query = f"{metric_name}[1h].max()"

    try:
        result = monitoring_client.summarize_metrics_data(
            compartment_id,
            oci.monitoring.models.SummarizeMetricsDataDetails(
                namespace=namespace,
                query=query,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                resolution="1h",
            ),
            compartment_id_in_subtree=use_subtree,
        ).data
    except Exception as e:
        log.warning(f"Metric query failed for {namespace}/{metric_name} "
                    f"in {compartment_id}: {e}")
        return {}, True  # Return failure flag

    # Parse results: group data points by resourceId
    metrics_by_instance = {}
    for metric_data in result:
        resource_id = metric_data.dimensions.get("resourceId") if metric_data.dimensions else None
        if not resource_id:
            continue

        if resource_id not in metrics_by_instance:
            metrics_by_instance[resource_id] = {}

        for dp in metric_data.aggregated_datapoints:
            hour_key = dp.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            metrics_by_instance[resource_id][hour_key] = dp.value

    return metrics_by_instance, False


def collect_all_metrics(monitoring_client, root_compartment_id, compartment_map,
                        instances, start_time, end_time):
    """Collect CpuUtilization and instance_status for all instances.

    Query strategy:
    - If root_compartment_id is tenancy OCID: single query with subtree=True
    - Otherwise: per-compartment queries with subtree=False

    Returns:
        (cpu_metrics, status_metrics, failed_instance_ids) where:
        - cpu_metrics: {instance_ocid: {hour_key: value}}
        - status_metrics: {instance_ocid: {hour_key: value}}
        - failed_instance_ids: set of instance OCIDs where metric queries failed
    """
    hours = int((end_time - start_time).total_seconds() / 3600)
    cpu_metrics = {}
    status_metrics = {}
    failed_instance_ids = set()

    use_subtree = is_tenancy_ocid(root_compartment_id)

    if use_subtree:
        # Tenancy-wide: single query with subtree
        scopes = [(root_compartment_id, True)]
    else:
        # Non-root: per-compartment queries
        scopes = [(comp_id, False) for comp_id in compartment_map.keys()]

    for comp_id, subtree in scopes:
        # Determine instances in this scope for batching
        if subtree:
            scope_instance_ids = [inst["id"] for inst in instances]
        else:
            scope_instance_ids = [inst["id"] for inst in instances
                                  if inst["compartment_id"] == comp_id]
        if not scope_instance_ids:
            continue

        batches = calculate_batch_groups(scope_instance_ids, hours)

        for batch in batches:
            log.info(f"Querying metrics for batch of {len(batch)} instances "
                     f"in {comp_id[:30]}...")

            # CpuUtilization
            batch_cpu, cpu_failed = collect_metrics(
                monitoring_client, comp_id,
                "oci_computeagent", "CpuUtilization",
                start_time, end_time,
                use_subtree=subtree,
                instance_ids=batch if len(batches) > 1 else None,
            )
            cpu_metrics.update(batch_cpu)

            # instance_status
            batch_status, status_failed = collect_metrics(
                monitoring_client, comp_id,
                "oci_compute_infrastructure_health", "instance_status",
                start_time, end_time,
                use_subtree=subtree,
                instance_ids=batch if len(batches) > 1 else None,
            )
            status_metrics.update(batch_status)

            if cpu_failed or status_failed:
                # Mark specific instances in this batch as failed,
                # not the whole compartment -- preserves successful batches
                failed_instance_ids.update(batch)

    return cpu_metrics, status_metrics, failed_instance_ids


# Load Chart.js from bundled file
_chart_js_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chart.min.js")
try:
    with open(_chart_js_path, "r") as _f:
        CHART_JS = _f.read()
except FileNotFoundError:
    CHART_JS = "/* Chart.js not found */"
    logging.getLogger("availability-report").warning(
        "chart.min.js not found alongside script; donut chart will not render in reports"
    )


def _aggregate_heatmap_block(statuses):
    """Aggregate a list of hourly statuses into a single block status.

    Rules:
    - if ANY hour is 'nodata' -> nodata
    - if ANY hour is 'down' -> down
    - if ALL hours are 'stopped' -> stopped
    - if mix of up + stopped -> up (instance was available when running)
    - else -> up
    """
    if not statuses:
        return "nodata"
    if "nodata" in statuses:
        return "nodata"
    if "down" in statuses:
        return "down"
    if all(s == "stopped" for s in statuses):
        return "stopped"
    return "up"


def _format_number(n):
    """Format a number with comma separators."""
    if n is None:
        return "N/A"
    return f"{n:,}"


def generate_html_report(instances, fleet, heatmap_data, all_hours,
                         compartment_name, region, days, sla_target,
                         start_date, end_date, title=None, logo_data=None,
                         discovery_warnings=None):
    """Generate self-contained HTML availability report.

    Args:
        instances: list of instance dicts (with stats merged in)
        fleet: fleet stats dict from compute_fleet_stats
        heatmap_data: {instance_id: [status_per_bucket]} for heatmap blocks
        all_hours: list of hourly bucket keys (ISO format)
        compartment_name: root compartment display name
        region: OCI region string
        days: reporting period in days
        sla_target: SLA target percentage
        start_date: formatted start date string
        end_date: formatted end date string
        title: optional custom branding title (top-right)
        logo_data: optional base64-encoded logo data URI
        discovery_warnings: optional list of warning strings from discovery phase

    Returns:
        Complete HTML string
    """
    discovery_warnings = discovery_warnings or []
    report_complete = fleet.get("report_complete", True)
    data_complete = fleet.get("data_complete", True)
    discovery_complete = fleet.get("discovery_complete", True)
    show_warning = not report_complete or len(discovery_warnings) > 0

    # Fleet values
    fleet_pct = fleet.get("fleet_availability_pct")
    at_target = fleet.get("at_target_count")
    total_up = fleet.get("total_up_hours")
    total_mon = fleet.get("total_monitored_hours")
    inst_count = fleet.get("discovered_instance_count", len(instances))

    # Format fleet availability for display
    if fleet_pct is not None:
        fleet_pct_str = f"{fleet_pct:.2f}%"
        if fleet_pct >= sla_target:
            fleet_color = "#0f6e56"
        elif fleet_pct >= 99.0:
            fleet_color = "#633806"
        else:
            fleet_color = "#a32d2d"
    else:
        fleet_pct_str = "N/A"
        fleet_color = "#888780"

    # Instances card value
    if not discovery_complete:
        inst_value = f"{inst_count} (partial scope)"
    else:
        inst_value = str(inst_count)

    # Meeting SLA card — denominator is monitorable instances only
    # (excludes stopped/N/A instances that have no computable availability)
    monitorable_count = fleet.get("monitorable_count", inst_count)
    if at_target is not None:
        sla_value = f"{at_target} / {monitorable_count}"
    else:
        sla_value = "N/A"

    # Total uptime card
    if total_up is not None and total_mon is not None:
        uptime_value = f"{_format_number(total_up)} / {_format_number(total_mon)}"
    else:
        uptime_value = "N/A"

    # Group instances by compartment
    grouped = group_instances_by_compartment(instances)

    # Heatmap resolution
    block_hours, resolution_label = get_heatmap_resolution(days)

    # Generation timestamp
    gen_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # --- Build HTML ---
    parts = []

    # Section A: DOCTYPE + HEAD
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Compute Availability Report &mdash; {compartment_name}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f8f7f4; color: #1a1a1a; font-size: 14px; line-height: 1.5; }}
.container {{ max-width: 960px; margin: 0 auto; padding: 32px 24px; }}
.header {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 28px; }}
.header h1 {{ font-size: 22px; font-weight: 600; margin-bottom: 4px; }}
.header-meta {{ font-size: 13px; color: #6b6b6b; display: flex; gap: 16px; flex-wrap: wrap; }}
.header-brand {{ text-align: right; font-size: 11px; color: #b4b2a9; }}
.header-brand .brand-title {{ font-weight: 500; font-size: 12px; color: #888780; }}
.metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 28px; }}
.metric-card {{ background: #fff; border-radius: 10px; padding: 16px 20px; border: 1px solid #e8e6df; }}
.metric-label {{ font-size: 12px; color: #888780; margin-bottom: 4px; }}
.metric-value {{ font-size: 24px; font-weight: 600; }}
.section-title {{ font-size: 16px; font-weight: 600; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #e8e6df; color: #1a1a1a; }}
.summary-row {{ display: grid; grid-template-columns: 180px 1fr; gap: 24px; margin-bottom: 32px; align-items: start; }}
.donut-wrap {{ position: relative; width: 160px; height: 160px; margin: 0 auto; }}
.donut-center {{ position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; }}
.donut-center .big {{ font-size: 24px; font-weight: 600; color: #1a1a1a; }}
.donut-center .sub {{ font-size: 12px; color: #888780; }}
.tbl-wrap {{ background: #fff; border-radius: 10px; border: 1px solid #e8e6df; overflow: hidden; }}
.comp-section {{ border: none; }}
.comp-section + .comp-section {{ border-top: 2px solid #e8e6df; }}
.comp-header {{ background: #faf9f6; padding: 10px 16px; font-weight: 600; font-size: 12px; color: #1a1a1a; border-bottom: 1px solid #e8e6df; cursor: pointer; list-style: none; }}
.comp-header::-webkit-details-marker {{ display: none; }}
.comp-header::before {{ content: ""; display: inline-block; width: 0; height: 0; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #888780; margin-right: 8px; vertical-align: middle; transition: transform 0.2s; }}
details:not([open]) > .comp-header::before {{ transform: rotate(-90deg); }}
.comp-header .comp-count {{ color: #888780; font-weight: 400; }}
.comp-header .comp-pct {{ color: #0f6e56; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ text-align: left; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #888780; padding: 10px 16px; background: #faf9f6; border-bottom: 1px solid #e8e6df; }}
th.center {{ text-align: center; }}
td {{ padding: 12px 16px; border-bottom: 1px solid #f0efe9; }}
td.center {{ text-align: center; }}
.dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }}
.dot-green {{ background: #1d9e75; }}
.dot-red {{ background: #e24b4a; }}
.dot-amber {{ background: #ef9f27; }}
.badge {{ font-size: 11px; padding: 2px 10px; border-radius: 10px; font-weight: 500; }}
.badge-ok {{ background: #e1f5ee; color: #085041; }}
.badge-warn {{ background: #faeeda; color: #633806; }}
.badge-bad {{ background: #fcebeb; color: #791f1f; }}
.avail-bar {{ display: flex; height: 6px; border-radius: 3px; overflow: hidden; width: 120px; }}
.bar-up {{ background: #1d9e75; }}
.bar-down {{ background: #e24b4a; }}
.bar-stopped {{ background: #e8e6df; }}
.uptime-cell {{ display: flex; flex-direction: column; align-items: center; gap: 3px; }}
.uptime-hours {{ font-size: 12px; color: #888780; }}
.heatmap-section {{ margin-bottom: 32px; }}
.heatmap-dates {{ font-size: 11px; color: #b4b2a9; margin-bottom: 8px; display: flex; margin-left: 252px; }}
.heatmap-dates span {{ flex: 1; }}
.heatmap-dates span:last-child {{ text-align: right; }}
.heatmap-comp {{ font-size: 10px; font-weight: 600; color: #888780; text-transform: uppercase; letter-spacing: 0.5px; margin: 10px 0 4px; padding-left: 4px; }}
.heatmap-comp:first-child {{ margin-top: 0; }}
.heatmap-row {{ display: flex; align-items: center; margin-bottom: 4px; }}
.heatmap-label {{ width: 200px; flex-shrink: 0; font-size: 13px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.heatmap-pct {{ width: 52px; flex-shrink: 0; font-size: 12px; text-align: right; padding-right: 10px; font-weight: 500; }}
.heatmap-blocks {{ display: flex; gap: 1px; flex: 1; }}
.hblk {{ height: 24px; flex: 1; border-radius: 1.5px; cursor: pointer; }}
.hblk-up {{ background: #1d9e75; }}
.hblk-down {{ background: #e24b4a; }}
.hblk-nodata {{ background: #ef9f27; }}
.hblk-stopped {{ background: #e8e6df; }}
.legend {{ display: flex; gap: 16px; align-items: center; font-size: 12px; color: #888780; margin: 12px 0 0; }}
.legend-block {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; vertical-align: middle; margin-right: 4px; }}
.tooltip {{ position: fixed; background: #2c2c2a; color: #fff; font-size: 11px; padding: 4px 8px; border-radius: 4px; display: none; pointer-events: none; z-index: 1000; white-space: nowrap; }}
.hidden {{ display: none !important; }}
.show-all-btn {{ background: #fff; border: 1px solid #e8e6df; border-radius: 6px; padding: 6px 14px; font-size: 12px; color: #888780; cursor: pointer; margin-top: 8px; }}
.show-all-btn:hover {{ background: #faf9f6; }}
.footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #e8e6df; font-size: 12px; color: #b4b2a9; display: flex; justify-content: space-between; }}
@media print {{
  body {{ background: #fff; }}
  .container {{ max-width: 100%; padding: 16px; }}
  .tooltip {{ display: none !important; }}
  .show-all-btn {{ display: none !important; }}
  .heatmap-hidden {{ display: flex !important; }}
  .metric-card {{ border: 1px solid #ccc; }}
  .tbl-wrap {{ border: 1px solid #ccc; }}
}}
</style>
</head>
<body>
<div class="container">
""")

    # Section B: HEADER + DATA QUALITY BANNER
    branding_html = ""
    if title or logo_data:
        brand_parts = []
        if logo_data:
            brand_parts.append(f'<img src="{logo_data}" alt="Logo" style="max-height:32px;margin-bottom:4px;">')
        if title:
            brand_parts.append(f'<div class="brand-title">{title}</div>')
        branding_html = f'<div class="header-brand">{"".join(brand_parts)}</div>'

    parts.append(f"""<div class="header">
<div>
<h1>Compute Availability Report</h1>
<div class="header-meta">
<span>Compartment: <strong>{compartment_name}</strong></span>
<span>Region: <strong>{region}</strong></span>
<span>Period: <strong>{start_date} &mdash; {end_date} ({days} days)</strong></span>
<span>SLA target: <strong>{sla_target}%</strong></span>
</div>
</div>
{branding_html}
</div>
""")

    # Warning banner
    if show_warning:
        parts.append("""<div data-warning="true" style="background:#faeeda;border-left:4px solid #ef9f27;padding:12px 16px;border-radius:0 6px 6px 0;margin-bottom:20px;font-size:13px;color:#633806;display:flex;align-items:center;gap:8px;">
<svg width="16" height="16" viewBox="0 0 16 16" fill="none" style="flex-shrink:0;"><path d="M8 1L1 14h14L8 1z" stroke="#ef9f27" stroke-width="1.5" fill="#faeeda"/><text x="8" y="12" text-anchor="middle" font-size="10" font-weight="700" fill="#633806">!</text></svg>
<span>Incomplete data: some metrics or compartments could not be queried. Affected availability values are shown as N/A.</span>
</div>
""")

    # Section C: METRIC CARDS
    parts.append(f"""<div class="metrics">
<div class="metric-card">
<div class="metric-label">Fleet availability</div>
<div class="metric-value" style="color:{fleet_color};">{fleet_pct_str}</div>
</div>
<div class="metric-card">
<div class="metric-label">Instances monitored</div>
<div class="metric-value">{inst_value}</div>
</div>
<div class="metric-card">
<div class="metric-label">Meeting SLA target</div>
<div class="metric-value">{sla_value}</div>
</div>
<div class="metric-card">
<div class="metric-label">Total uptime hours</div>
<div class="metric-value">{uptime_value}</div>
</div>
</div>
""")

    # Section D: EXECUTIVE SUMMARY (donut + table)
    # Donut values
    donut_pct = fleet_pct if fleet_pct is not None else 0
    donut_remainder = 100 - donut_pct if fleet_pct is not None else 100
    donut_unavail_color = "#e24b4a" if donut_remainder > 0 else "#e8e6df"
    donut_center_text = fleet_pct_str

    parts.append(f"""<div class="section-title">Executive summary</div>
<div class="summary-row">
<div class="donut-wrap">
<canvas id="donut" width="160" height="160"></canvas>
<div class="donut-center">
<div class="big">{donut_center_text}</div>
<div class="sub">fleet uptime</div>
</div>
</div>
<div class="tbl-wrap">
""")

    # Table header row (only first compartment gets the header)
    first_comp = True
    for comp_id, group in grouped.items():
        comp_name = group["name"]
        comp_instances = group["instances"]

        # Compute compartment stats
        comp_stats = compute_compartment_stats(comp_instances, sla_target)
        comp_pct = comp_stats["compartment_availability_pct"]
        comp_pct_str = f"{comp_pct:.2f}%" if comp_pct is not None else "N/A"
        comp_pct_color = "#0f6e56" if comp_pct is not None and comp_pct >= sla_target else "#a32d2d"

        # Compartment header — collapsible via <details>/<summary>
        border_style = ' style="border-top:2px solid #e8e6df;"' if not first_comp else ''
        parts.append(f'<details class="comp-section" open{border_style}>')
        parts.append(f'<summary class="comp-header">{html.escape(comp_name)} <span class="comp-count">({len(comp_instances)} instances)</span> &mdash; <span class="comp-pct" style="color:{comp_pct_color};">{comp_pct_str}</span></summary>')

        # Table
        parts.append('<table>')
        if first_comp:
            parts.append("""<thead><tr>
<th style="width:28%;">Instance</th>
<th class="center" style="width:14%;">Status</th>
<th style="width:14%;">Availability</th>
<th class="center" style="width:30%;">Uptime</th>
<th style="width:14%;">Downtime</th>
</tr></thead>""")
            first_comp = False

        parts.append('<tbody>')
        for inst in comp_instances:
            name = inst["name"]
            state = inst.get("state", "UNKNOWN")
            avail = inst.get("availability_pct")
            up_h = inst.get("up_hours", 0)
            down_h = inst.get("down_hours", 0)
            stopped_h = inst.get("stopped_hours", 0)
            monitored_h = inst.get("monitored_hours", 0)
            downtime_min = inst.get("downtime_minutes", 0)

            # Dot color
            if state == "RUNNING":
                dot_cls = "dot-green"
            elif state == "STOPPED":
                dot_cls = "dot-red"
            else:
                dot_cls = "dot-amber"

            # Badge
            if state == "RUNNING":
                badge_cls = "badge-ok"
            elif state == "STOPPED":
                badge_cls = "badge-bad"
            else:
                badge_cls = "badge-warn"

            # Availability color
            if avail is not None:
                avail_str = f"{avail:.2f}%" if avail < 100 else "100%"
                if avail >= sla_target:
                    avail_color = "#0f6e56"
                elif avail >= 99.0:
                    avail_color = "#633806"
                else:
                    avail_color = "#a32d2d"
            else:
                avail_str = "N/A"
                avail_color = "#888780"

            # Uptime bar proportions
            total_h = up_h + down_h + stopped_h
            if total_h > 0:
                up_pct = up_h / total_h * 100
                down_pct = down_h / total_h * 100
                stopped_pct = stopped_h / total_h * 100
            else:
                up_pct = 100
                down_pct = 0
                stopped_pct = 0

            # Bar segments
            bar_segments = []
            if up_pct > 0:
                bar_segments.append(f'<div class="bar-up" style="width:{up_pct:.1f}%;"></div>')
            if down_pct > 0:
                bar_segments.append(f'<div class="bar-down" style="width:{down_pct:.1f}%;"></div>')
            if stopped_pct > 0:
                bar_segments.append(f'<div class="bar-stopped" style="width:{stopped_pct:.1f}%;"></div>')

            # Downtime color
            dt_color = "#a32d2d" if downtime_min > 0 else "inherit"

            parts.append(f"""<tr>
<td><span class="dot {dot_cls}"></span>{html.escape(name)}</td>
<td class="center"><span class="badge {badge_cls}">{html.escape(state)}</span></td>
<td style="font-weight:600;color:{avail_color};">{avail_str}</td>
<td class="center"><div class="uptime-cell"><span class="uptime-hours">{up_h}h</span><div class="avail-bar">{"".join(bar_segments)}</div></div></td>
<td style="color:{dt_color};">{downtime_min} min</td>
</tr>""")

        parts.append('</tbody></table>')
        parts.append('</details>')

    parts.append('</div></div>')  # close tbl-wrap and summary-row

    # Section E: HEATMAP
    parts.append('<div class="heatmap-section">')
    parts.append('<div class="section-title">Hourly availability heatmap</div>')

    # Date markers
    if all_hours:
        from datetime import datetime as _dt
        first_hour = _dt.strptime(all_hours[0], "%Y-%m-%dT%H:%M:%SZ")
        last_hour = _dt.strptime(all_hours[-1], "%Y-%m-%dT%H:%M:%SZ")
        total_span = (last_hour - first_hour).days
        date_labels = []
        if total_span <= 7:
            step = 1
        elif total_span <= 14:
            step = 2
        elif total_span <= 30:
            step = 5
        else:
            step = 7
        d = first_hour
        while d <= last_hour:
            date_labels.append(f"{d.strftime('%b')} {d.day}")
            d += timedelta(days=step)
        last_label = f"{last_hour.strftime('%b')} {last_hour.day}"
        if date_labels and last_label != date_labels[-1]:
            date_labels.append(last_label)

        parts.append('<div class="heatmap-dates">')
        for dl in date_labels:
            parts.append(f'<span>{dl}</span>')
        parts.append('</div>')

    # Determine if we need the toggle (>50 instances)
    total_instances = len(instances)
    need_toggle = total_instances > 50

    # Build heatmap rows grouped by compartment
    heatmap_row_idx = 0
    for comp_id, group in grouped.items():
        comp_name = group["name"]
        comp_instances = group["instances"]

        parts.append(f'<div class="heatmap-comp">{html.escape(comp_name)}</div>')

        for inst in comp_instances:
            inst_id = inst["id"]
            inst_name = inst["name"]
            avail = inst.get("availability_pct")
            statuses = heatmap_data.get(inst_id, [])

            # Availability color for heatmap
            if avail is not None:
                pct_str = f"{avail:.1f}%" if avail < 100 else "100%"
                if avail >= sla_target:
                    pct_color = "#0f6e56"
                elif avail >= 99.0:
                    pct_color = "#633806"
                else:
                    pct_color = "#a32d2d"
            else:
                pct_str = "N/A"
                pct_color = "#888780"

            # Determine if this row should be hidden (>50 instances, above SLA)
            row_hidden = ""
            if need_toggle:
                if avail is not None and avail >= sla_target:
                    row_hidden = " heatmap-hidden hidden"

            # Aggregate blocks
            blocks = []
            num_buckets = len(statuses)
            for b_start in range(0, num_buckets, block_hours):
                chunk = statuses[b_start:b_start + block_hours]
                agg = _aggregate_heatmap_block(chunk)
                blk_cls = f"hblk hblk-{agg}"
                # Data attributes for tooltip
                if b_start < len(all_hours):
                    hour_key = all_hours[b_start]
                else:
                    hour_key = ""
                blocks.append(f'<div class="{blk_cls}" data-name="{html.escape(inst_name, quote=True)}" data-hour="{hour_key}" data-status="{agg}"></div>')

            parts.append(f'<div class="heatmap-row{row_hidden}">')
            parts.append(f'<div class="heatmap-label">{html.escape(inst_name)}</div>')
            parts.append(f'<div class="heatmap-pct" style="color:{pct_color};">{pct_str}</div>')
            parts.append(f'<div class="heatmap-blocks">{"".join(blocks)}</div>')
            parts.append('</div>')
            heatmap_row_idx += 1

    # Toggle button
    if need_toggle:
        parts.append('<button class="show-all-btn" id="show-all-toggle">Show all</button>')

    # Legend
    parts.append(f"""<div class="legend">
<span><span class="legend-block" style="background:#1d9e75;"></span> Available</span>
<span><span class="legend-block" style="background:#e24b4a;"></span> Unavailable</span>
<span><span class="legend-block" style="background:#e8e6df;"></span> Stopped</span>
<span><span class="legend-block" style="background:#ef9f27;"></span> No data (incomplete)</span>
<span style="margin-left:auto;font-size:11px;">Each block = {resolution_label}</span>
</div>
""")

    parts.append('</div>')  # close heatmap-section

    # Section F: TOOLTIP + FOOTER
    parts.append('<div class="tooltip" id="tooltip"></div>')
    parts.append(f"""<div class="footer">
<span>Generated: {gen_time}</span>
<span>OCI Compute Availability Report v{VERSION}</span>
</div>
""")

    parts.append('</div>')  # close container

    # Section G: JAVASCRIPT
    # Embed Chart.js inline
    parts.append(f'<script>{CHART_JS}</script>')

    # Donut chart initialization
    parts.append(f"""<script>
(function() {{
  var ctx = document.getElementById('donut');
  if (ctx) {{
    new Chart(ctx, {{
      type: 'doughnut',
      data: {{
        datasets: [{{
          data: [{donut_pct}, {donut_remainder}],
          backgroundColor: ['#1d9e75', '{donut_unavail_color}'],
          borderWidth: 0
        }}]
      }},
      options: {{
        responsive: false,
        cutout: '74%',
        plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
        animation: {{ animateRotate: true, duration: 600 }}
      }}
    }});
  }}
}})();
</script>
""")

    # Heatmap tooltip JS
    parts.append("""<script>
(function() {
  var tip = document.getElementById('tooltip');
  if (!tip) return;
  var statusLabels = {'up': 'Available', 'down': 'Unavailable', 'nodata': 'No data', 'stopped': 'Stopped'};
  document.querySelectorAll('.hblk').forEach(function(el) {
    el.addEventListener('mouseenter', function(e) {
      var name = el.getAttribute('data-name') || '';
      var hour = el.getAttribute('data-hour') || '';
      var status = el.getAttribute('data-status') || '';
      var label = statusLabels[status] || status;
      var dateStr = '';
      if (hour) {
        var d = new Date(hour);
        dateStr = d.toISOString().split('T')[0] + ' ' + d.getUTCHours() + ':00 UTC';
      }
      tip.textContent = name + ' \\u2014 ' + dateStr + ' \\u2014 ' + label;
      tip.style.display = 'block';
    });
    el.addEventListener('mousemove', function(e) {
      tip.style.left = (e.clientX + 12) + 'px';
      tip.style.top = (e.clientY - 20) + 'px';
    });
    el.addEventListener('mouseleave', function() {
      tip.style.display = 'none';
    });
  });
})();
</script>
""")

    # HEAT-9 toggle JS
    if need_toggle:
        parts.append("""<script>
(function() {
  var btn = document.getElementById('show-all-toggle');
  if (!btn) return;
  btn.addEventListener('click', function() {
    document.querySelectorAll('.heatmap-hidden').forEach(function(el) {
      el.classList.toggle('hidden');
    });
    this.textContent = this.textContent === 'Show all' ? 'Show below SLA only' : 'Show all';
  });
})();
</script>
""")

    parts.append('</body>\n</html>')

    return "".join(parts)


def upload_report(config, signer, compartment_id, html_content, object_name,
                  bucket_name, namespace=None, par_expiry_days=30):
    """Upload HTML report to Object Storage and create a PAR link.

    Args:
        config: OCI config dict
        signer: OCI signer (or None for config auth)
        compartment_id: compartment OCID for the bucket
        html_content: HTML string to upload
        object_name: object name in the bucket
        bucket_name: bucket name
        namespace: Object Storage namespace (auto-detected if None)
        par_expiry_days: PAR expiry in days

    Returns:
        PAR URL string, or None on failure
    """
    os_client = make_client(oci.object_storage.ObjectStorageClient, config, signer)

    # Auto-detect namespace
    if not namespace:
        namespace = os_client.get_namespace().data

    # Create bucket if it doesn't exist
    try:
        os_client.get_bucket(namespace, bucket_name)
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            log.info(f"Creating bucket '{bucket_name}'...")
            os_client.create_bucket(
                namespace,
                oci.object_storage.models.CreateBucketDetails(
                    name=bucket_name,
                    compartment_id=compartment_id,
                    public_access_type="NoPublicAccess",
                ),
            )
        else:
            log.error(f"Bucket check failed: {e.message}")
            return None

    # Upload
    os_client.put_object(
        namespace, bucket_name, object_name,
        html_content.encode("utf-8"),
        content_type="text/html",
    )
    log.info(f"Uploaded to {namespace}/{bucket_name}/{object_name}")

    # Create PAR
    expiry = datetime.now(timezone.utc) + timedelta(days=par_expiry_days)
    par = os_client.create_preauthenticated_request(
        namespace, bucket_name,
        oci.object_storage.models.CreatePreauthenticatedRequestDetails(
            name=f"availability-report-{object_name}",
            access_type="ObjectRead",
            time_expires=expiry,
            object_name=object_name,
            bucket_listing_action="Deny",
        ),
    ).data

    par_url = f"https://objectstorage.{config.get('region', 'unknown')}.oraclecloud.com{par.access_uri}"
    return par_url


def sanitize_filename(name):
    """Convert compartment name to safe filename component."""
    return re.sub(r'[^\w\-]', '_', name).lower()


def embed_logo(logo_path):
    """Read logo file and return base64-encoded data URI."""
    if not logo_path or not os.path.isfile(logo_path):
        return None
    ext = os.path.splitext(logo_path)[1].lower()
    mime_types = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                  ".svg": "image/svg+xml", ".gif": "image/gif"}
    mime = mime_types.get(ext, "image/png")
    with open(logo_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def main():
    args = parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("OCI Compute Availability Report v%s", VERSION)

    # Phase 1: Auth
    log.info("Authenticating (%s)...", args.auth)
    config, signer = setup_auth(args)

    # Phase 2: Discover
    log.info("Discovering instances...")
    identity_client = make_client(oci.identity.IdentityClient, config, signer)
    compute_client = make_client(oci.core.ComputeClient, config, signer)

    compartment_name = args.compartment_name
    if not compartment_name:
        compartment_name, compartment_map, disc_warnings = discover_compartments(identity_client, args.compartment_id)
        if is_tenancy_ocid(args.compartment_id):
            compartment_name = f"{compartment_name} (tenancy)"
    else:
        _, compartment_map, disc_warnings = discover_compartments(identity_client, args.compartment_id)

    # Build exclusion list from --exclude and --exclude-file
    exclude_list = list(args.exclude) if args.exclude else []
    if args.exclude_file:
        try:
            with open(args.exclude_file, "r") as f:
                for line in f:
                    entry = line.strip()
                    if entry and not entry.startswith("#"):
                        exclude_list.append(entry)
            log.info("Loaded %d exclusions from %s", len(exclude_list) - len(args.exclude or []), args.exclude_file)
        except FileNotFoundError:
            log.warning("Exclude file not found: %s", args.exclude_file)

    instances, inst_disc_warnings = discover_instances(
        compute_client, compartment_map, args.running_only,
        exclude_list=exclude_list if exclude_list else None,
    )
    disc_warnings.extend(inst_disc_warnings)
    if not instances:
        log.error("No instances found. Exiting.")
        sys.exit(1)

    # Phase 3: Collect metrics
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=args.days)
    hourly_buckets = build_hourly_buckets(start_time, end_time)

    log.info("Collecting metrics for %d instances over %d days (%d hours)...",
             len(instances), args.days, len(hourly_buckets))
    monitoring_client = make_client(oci.monitoring.MonitoringClient, config, signer)
    cpu_metrics, status_metrics, failed_instance_ids = collect_all_metrics(
        monitoring_client, args.compartment_id, compartment_map,
        instances, start_time, end_time,
    )

    if failed_instance_ids:
        log.warning(f"Metric queries failed for {len(failed_instance_ids)} instance(s). "
                    "Affected instances will show N/A availability.")

    # Phase 4: Compute availability
    log.info("Computing availability...")
    matrix = build_availability_matrix(
        instances, hourly_buckets, cpu_metrics, status_metrics, failed_instance_ids
    )

    # Merge stats into instance dicts
    for inst in instances:
        stats = compute_instance_stats(matrix[inst["id"]])
        inst.update(stats)

    fleet = compute_fleet_stats(instances, args.sla_target, discovery_warnings=disc_warnings)

    # Build heatmap data (list of statuses per instance)
    heatmap_data = {}
    for inst in instances:
        heatmap_data[inst["id"]] = [matrix[inst["id"]][h] for h in hourly_buckets]

    # Phase 5: Render
    log.info("Generating report...")
    region = args.region or config.get("region", "unknown")
    logo_data = embed_logo(args.logo) if args.logo else None

    html = generate_html_report(
        instances=instances,
        fleet=fleet,
        heatmap_data=heatmap_data,
        all_hours=hourly_buckets,
        compartment_name=compartment_name,
        region=region,
        days=args.days,
        sla_target=args.sla_target,
        start_date=start_time.strftime("%b %d, %Y"),
        end_date=(end_time - timedelta(hours=1)).strftime("%b %d, %Y"),
        title=args.title,
        logo_data=logo_data,
        discovery_warnings=disc_warnings if disc_warnings else None,
    )

    # Write to file
    if args.output:
        output_path = args.output
    else:
        safe_name = sanitize_filename(compartment_name)
        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"availability_report_{safe_name}_{date_str}.html"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info("Report written to %s", output_path)

    # Phase 6: Upload (optional)
    if args.upload:
        log.info("Uploading to Object Storage...")
        object_name = os.path.basename(output_path)
        par_url = upload_report(
            config, signer, args.compartment_id, html, object_name,
            args.bucket, args.os_namespace, args.par_expiry_days,
        )
        if par_url:
            log.info("PAR URL (expires in %d days):", args.par_expiry_days)
            print(par_url)

    log.info("Done. Fleet availability: %s",
             f"{fleet['fleet_availability_pct']}%" if fleet['fleet_availability_pct'] is not None else "N/A")


if __name__ == "__main__":
    main()
