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

VERSION = "1.2"

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
    parser.add_argument("--days", type=int, default=7,
                        help="Reporting period in days, 1-90 (default: 7)")
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

    args = parser.parse_args(argv)
    if args.days < 1 or args.days > 90:
        parser.error(
            "--days must be between 1 and 90. "
            "OCI Monitoring retains metric data for a maximum of 90 days "
            "(at hourly resolution)."
        )
    return args


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


def classify_minute(has_cpu, instance_status, query_failed=False):
    """Classify a per-minute bucket as up, down, stopped, or nodata.

    Args:
        has_cpu: True if CpuUtilization data exists for this minute
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


def build_availability_matrix(instances, time_buckets, cpu_metrics, status_metrics,
                               failed_instance_ids=None):
    """Build availability matrix from metric data at minute granularity.

    Args:
        instances: list of instance dicts (need id) or list of instance ID strings
        time_buckets: list of minute bucket keys (ISO format strings)
        cpu_metrics: {instance_id: {minute_key: value}} from CpuUtilization
        status_metrics: {instance_id: {minute_key: value}} from instance_status
        failed_instance_ids: set of instance OCIDs where metric queries failed

    Returns:
        {instance_id: {minute_key: "up"|"down"|"stopped"|"nodata"}}
    """
    failed_instance_ids = failed_instance_ids or set()
    matrix = {}
    for inst in instances:
        inst_id = inst["id"] if isinstance(inst, dict) else inst
        query_failed = inst_id in failed_instance_ids

        inst_cpu = cpu_metrics.get(inst_id, {})
        inst_status = status_metrics.get(inst_id, {})
        minutely = {}
        for bucket in time_buckets:
            has_cpu = bucket in inst_cpu
            status_val = inst_status.get(bucket)
            if status_val is not None:
                status_val = int(status_val)
            minutely[bucket] = classify_minute(has_cpu, status_val, query_failed=query_failed)
        matrix[inst_id] = minutely
    return matrix


def compute_instance_stats(minute_statuses):
    """Compute availability stats from per-minute classification dict.

    Args:
        minute_statuses: dict of {minute_key: "up"|"down"|"stopped"|"nodata"}

    Returns:
        dict with up_minutes, down_minutes, stopped_minutes, nodata_minutes,
        monitored_minutes, total_minutes, availability_pct (float or None),
        downtime_minutes, data_complete
    """
    up = sum(1 for v in minute_statuses.values() if v == "up")
    down = sum(1 for v in minute_statuses.values() if v == "down")
    stopped = sum(1 for v in minute_statuses.values() if v == "stopped")
    nodata = sum(1 for v in minute_statuses.values() if v == "nodata")
    monitored = up + down
    total = len(minute_statuses)
    data_complete = nodata == 0

    if nodata > 0:
        availability_pct = None
    elif monitored == 0:
        availability_pct = None
    else:
        availability_pct = round(up / monitored * 100, 4)

    return {
        "up_minutes": up,
        "down_minutes": down,
        "stopped_minutes": stopped,
        "nodata_minutes": nodata,
        "monitored_minutes": monitored,
        "total_minutes": total,
        "availability_pct": availability_pct,
        "downtime_minutes": down,   # exact — no * 60 approximation
        "data_complete": data_complete,
    }


def compute_compartment_stats(instances_in_compartment, sla_target):
    """Compute availability stats for a single compartment.

    If ANY instance in the compartment has data_complete=False (nodata minutes),
    compartment_availability_pct and at_target_count both become None (fail closed).

    Args:
        instances_in_compartment: list of instance dicts with stats
        sla_target: SLA target percentage

    Returns:
        dict with instance_count, compartment_availability_pct, at_target_count, data_complete
    """
    total_up = sum(s["up_minutes"] for s in instances_in_compartment)
    total_monitored = sum(s["monitored_minutes"] for s in instances_in_compartment)
    all_complete = all(s.get("data_complete", True) for s in instances_in_compartment)

    if not all_complete or total_monitored == 0:
        return {
            "instance_count": len(instances_in_compartment),
            "compartment_availability_pct": None,
            "at_target_count": None,
            "data_complete": all_complete,
        }

    pct = round(total_up / total_monitored * 100, 4)
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
      total_up_minutes, total_monitored_minutes all become None
    - discovered_instance_count always stays numeric for diagnostics

    Args:
        instance_stats_list: list of dicts from compute_instance_stats
        sla_target: SLA target percentage (e.g. 99.95)
        discovery_warnings: list of warning strings from discovery phase

    Returns:
        dict with discovered_instance_count, fleet_availability_pct,
        at_target_count, total_up_minutes, total_monitored_minutes,
        data_complete, discovery_complete, report_complete
    """
    discovery_warnings = discovery_warnings or []
    total_up = sum(s["up_minutes"] for s in instance_stats_list)
    total_monitored = sum(s["monitored_minutes"] for s in instance_stats_list)
    data_complete = all(s.get("data_complete", True) for s in instance_stats_list)
    discovery_complete = len(discovery_warnings) == 0
    report_complete = data_complete and discovery_complete

    monitorable = [s for s in instance_stats_list if s["availability_pct"] is not None]
    monitorable_count = len(monitorable)

    if not report_complete or total_monitored == 0:
        return {
            "discovered_instance_count": len(instance_stats_list),
            "monitorable_count": monitorable_count,
            "fleet_availability_pct": None,
            "at_target_count": None,
            "total_up_minutes": None if not report_complete else total_up,
            "total_monitored_minutes": None if not report_complete else total_monitored,
            "data_complete": data_complete,
            "discovery_complete": discovery_complete,
            "report_complete": report_complete,
        }

    fleet_pct = round(total_up / total_monitored * 100, 4)
    at_target = sum(
        1 for s in monitorable
        if s["availability_pct"] >= sla_target
    )

    return {
        "discovered_instance_count": len(instance_stats_list),
        "monitorable_count": monitorable_count,
        "fleet_availability_pct": fleet_pct,
        "at_target_count": at_target,
        "total_up_minutes": total_up,
        "total_monitored_minutes": total_monitored,
        "data_complete": data_complete,
        "discovery_complete": discovery_complete,
        "report_complete": report_complete,
    }


def get_heatmap_resolution(days):
    """Return (block_minutes, label) for adaptive heatmap display resolution.

    The underlying data is always at 1-minute granularity.
    This controls how many minutes each visual block represents.

    Days range -> block size:
      1         -> 15 min
      2         -> 30 min
      3–4       -> 15 min
      5–7       -> 30 min
      8–14      ->  1 hour  (60 min)
      15–21     ->  2 hours (120 min)
      22–45     ->  6 hours (360 min)
      46–60     -> 12 hours (720 min)
      61–90     ->  1 day   (1440 min)
    """
    if days <= 1:
        return 15,   "15 minutes"
    elif days <= 2:
        return 30,   "30 minutes"
    elif days <= 4:
        return 15,   "15 minutes"
    elif days <= 7:
        return 30,   "30 minutes"
    elif days <= 14:
        return 60,   "1 hour"
    elif days <= 21:
        return 120,  "2 hours"
    elif days <= 45:
        return 360,  "6 hours"
    elif days <= 60:
        return 720,  "12 hours"
    else:
        return 1440, "1 day"


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



# Retry configuration for transient API errors (429 TooManyRequests, 5xx).
# 400 errors are not retried — they indicate a query bug, not a transient fault.
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds; doubles each attempt (2s, 4s, 8s)

# Minimum pause between consecutive instance_status calls to avoid sustained 429s.
# At 1100 instances: 1100 × 0.05 s = 55 s added, but retry storms (2–8 s each) eliminated.
STATUS_CALL_THROTTLE_SECS = 0.05


def build_time_buckets(start_time, end_time, resolution="1m"):
    """Build list of time bucket keys (ISO format, UTC) for the reporting period.

    Args:
        start_time: datetime, start of reporting window
        end_time: datetime, end of reporting window
        resolution: "1m" for per-minute buckets, "1h" for per-hour buckets

    Returns:
        list of ISO format timestamp strings
    """
    buckets = []
    current = start_time
    delta = timedelta(minutes=1) if resolution == "1m" else timedelta(hours=1)
    while current < end_time:
        buckets.append(current.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current += delta
    return buckets


def get_metric_resolution():
    """Always use 1m resolution for maximum precision.

    OCI constrains each query window to 7 days at 1m resolution.
    collect_all_metrics handles chunking automatically.

    Returns:
        "1m"
    """
    return "1m"


def is_tenancy_ocid(ocid):
    """Check if an OCID is a tenancy root OCID."""
    return ocid.startswith("ocid1.tenancy.")


def collect_metrics(monitoring_client, compartment_id, namespace, metric_name,
                    start_time, end_time, use_subtree=False, instance_ids=None,
                    resolution="1m"):
    """Query SummarizeMetricsData for a metric across instances.

    Args:
        monitoring_client: OCI MonitoringClient
        compartment_id: compartment OCID for the query
        namespace: metric namespace (e.g. "oci_computeagent")
        metric_name: metric name (e.g. "CpuUtilization")
        start_time: datetime, start of query window (max 7-day span for 1m resolution)
        end_time: datetime, end of query window
        use_subtree: only set True when compartment_id is tenancy root OCID
        instance_ids: optional list of instance OCIDs to filter by
        resolution: OCI resolution string e.g. "1m" or "1h"

    Returns:
        (metrics_dict, failed) tuple:
        - metrics_dict: {instance_ocid: {minute_key: value}}
        - failed: bool, True if the API call failed
    """
    interval = resolution  # interval == resolution for our use case
    if instance_ids:
        resource_filter = " || ".join(
            f'resourceId = "{rid}"' for rid in instance_ids
        )
        query = f"{metric_name}[{interval}]{{{resource_filter}}}.max()"
    else:
        query = f"{metric_name}[{interval}].max()"

    import time as _time
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = monitoring_client.summarize_metrics_data(
                compartment_id,
                oci.monitoring.models.SummarizeMetricsDataDetails(
                    namespace=namespace,
                    query=query,
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    resolution=resolution,
                ),
                compartment_id_in_subtree=use_subtree,
            ).data
            last_exc = None
            break  # success — exit retry loop
        except oci.exceptions.ServiceError as e:
            last_exc = e
            if e.status == 400:
                # Query error (bad filter, unsupported pattern) — retrying won't help
                log.warning(f"Metric query failed (400, not retrying) for "
                            f"{namespace}/{metric_name} in {compartment_id[:30]}...: {e.message}")
                return {}, True
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            log.warning(f"Metric query attempt {attempt}/{MAX_RETRIES} failed ({e.status}) "
                        f"for {namespace}/{metric_name} — retrying in {wait}s")
            _time.sleep(wait)
        except Exception as e:
            last_exc = e
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            log.warning(f"Metric query attempt {attempt}/{MAX_RETRIES} failed "
                        f"for {namespace}/{metric_name}: {e} — retrying in {wait}s")
            _time.sleep(wait)

    if last_exc is not None:
        log.warning(f"Metric query failed after {MAX_RETRIES} attempts for "
                    f"{namespace}/{metric_name} in {compartment_id}: {last_exc}")
        return {}, True

    # Parse results: group data points by resourceId
    metrics_by_instance = {}
    for metric_data in result:
        resource_id = metric_data.dimensions.get("resourceId") if metric_data.dimensions else None
        if not resource_id:
            continue

        if resource_id not in metrics_by_instance:
            metrics_by_instance[resource_id] = {}

        for dp in metric_data.aggregated_datapoints:
            key = dp.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            metrics_by_instance[resource_id][key] = dp.value

    return metrics_by_instance, False


def collect_all_metrics(monitoring_client, root_compartment_id, compartment_map,
                        instances, start_time, end_time):
    """Collect CpuUtilization and instance_status for all instances at 1m resolution.

    Chunking strategy:
    - OCI limits a single 1m-resolution query to a 7-day window span.
    - We split the full date range into ≤7-day chunks and query each chunk
      separately, then merge. This allows minute-level precision for any
      --days value up to 90.
    - Within each chunk, CPU is batched by datapoint limit; instance_status
      is queried one instance at a time (OCI rejects || filters for it).

    Returns:
        (cpu_metrics, status_metrics, failed_instance_ids) where:
        - cpu_metrics: {instance_ocid: {minute_key: value}}
        - status_metrics: {instance_ocid: {minute_key: value}}
        - failed_instance_ids: set of instance OCIDs where metric queries failed
    """
    import time as _time

    RESOLUTION = "1m"
    MAX_CHUNK_DAYS = 7  # OCI 1m resolution window limit

    cpu_metrics = {}
    status_metrics = {}
    failed_instance_ids = set()

    # Build ≤7-day chunks covering the full reporting window
    chunks = []
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + timedelta(days=MAX_CHUNK_DAYS), end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    log.info(f"Querying metrics in {len(chunks)} chunk(s) of up to {MAX_CHUNK_DAYS} days "
             f"at {RESOLUTION} resolution.")

    use_subtree = is_tenancy_ocid(root_compartment_id)

    for chunk_idx, (c_start, c_end) in enumerate(chunks):
        chunk_minutes = int((c_end - c_start).total_seconds() / 60)
        log.info(f"Chunk {chunk_idx + 1}/{len(chunks)}: "
                 f"{c_start.strftime('%Y-%m-%d %H:%M')} → {c_end.strftime('%Y-%m-%d %H:%M')} "
                 f"({chunk_minutes} minutes)")

        # Determine scope (tenancy-wide subtree vs per-compartment)
        # At 1m resolution OCI rejects || filters, so all queries are
        # one-per-instance. Subtree=True is safe for tenancy-wide scope
        # since no batching filter is needed.
        if use_subtree:
            _use_subtree = True
        else:
            _use_subtree = False

        if _use_subtree:
            scopes = [(root_compartment_id, True)]
        else:
            scopes = [(comp_id, False) for comp_id in compartment_map.keys()]

        for comp_id, subtree in scopes:
            if subtree:
                scope_instance_ids = [inst["id"] for inst in instances]
            else:
                scope_instance_ids = [inst["id"] for inst in instances
                                      if inst["compartment_id"] == comp_id]
            if not scope_instance_ids:
                continue

            # ── CpuUtilization: one call per instance ───────────────────────
            # OCI rejects multi-resourceId || filters at 1m resolution
            # ("Not Supported yet"), so we query one instance at a time,
            # matching the same pattern used for instance_status.
            for inst_id in scope_instance_ids:
                _time.sleep(STATUS_CALL_THROTTLE_SECS)
                log.info(f"  CPU for {inst_id[:30]}...")
                inst_cpu, cpu_failed = collect_metrics(
                    monitoring_client, comp_id,
                    "oci_computeagent", "CpuUtilization",
                    c_start, c_end,
                    use_subtree=subtree,
                    instance_ids=[inst_id],
                    resolution=RESOLUTION,
                )
                for iid, dp_map in inst_cpu.items():
                    cpu_metrics.setdefault(iid, {}).update(dp_map)
                if cpu_failed:
                    failed_instance_ids.add(inst_id)

            # ── instance_status: one call per instance ───────────────────────
            for inst_id in scope_instance_ids:
                _time.sleep(STATUS_CALL_THROTTLE_SECS)
                log.info(f"  instance_status for {inst_id[:30]}...")
                batch_status, status_failed = collect_metrics(
                    monitoring_client, comp_id,
                    "oci_compute_infrastructure_health", "instance_status",
                    c_start, c_end,
                    use_subtree=subtree,
                    instance_ids=[inst_id],
                    resolution=RESOLUTION,
                )
                for iid, dp_map in batch_status.items():
                    status_metrics.setdefault(iid, {}).update(dp_map)
                if status_failed:
                    failed_instance_ids.add(inst_id)

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


def generate_html_report(instances, fleet, heatmap_data, all_buckets,
                         compartment_name, region, days, sla_target,
                         start_date, end_date, title=None, logo_data=None,
                         discovery_warnings=None):
    """Generate self-contained HTML availability report.

    Args:
        instances: list of instance dicts (with stats merged in)
        fleet: fleet stats dict from compute_fleet_stats
        heatmap_data: {instance_id: [status_per_minute_bucket]}
        all_buckets: list of minute bucket keys (ISO format)
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
    total_up = fleet.get("total_up_minutes")
    total_mon = fleet.get("total_monitored_minutes")
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

    # Instances card value — show monitorable (had uptime) vs total discovered
    # Meeting SLA card — denominator is monitorable instances only
    # (excludes stopped/N/A instances that have no computable availability)
    monitorable_count = fleet.get("monitorable_count", inst_count)
    if not discovery_complete:
        inst_value = f"{monitorable_count} active / {inst_count} total (partial scope)"
    else:
        inst_value = f"{monitorable_count} active / {inst_count} total"
    if at_target is not None:
        sla_value = f"{at_target} / {monitorable_count}"
    else:
        sla_value = "N/A"

    # Total uptime card — show as hours for readability
    def fmt_minutes(m):
        if m is None:
            return "N/A"
        if m < 60:
            return f"{m}m"
        h, rem = divmod(m, 60)
        return f"{_format_number(h)}h {rem}m" if rem else f"{_format_number(h)}h"

    if total_up is not None and total_mon is not None:
        uptime_value = f"{fmt_minutes(total_up)} / {fmt_minutes(total_mon)}"
    else:
        uptime_value = "N/A"

    # Group instances by compartment
    grouped = group_instances_by_compartment(instances)

    # Heatmap resolution
    block_minutes, resolution_label = get_heatmap_resolution(days)

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
.header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 28px; }}
.header h1 {{ font-size: 22px; font-weight: 600; margin-bottom: 4px; }}
.header-left {{ flex: 1; min-width: 0; }}
.header-meta {{ font-size: 13px; color: #6b6b6b; display: flex; gap: 16px; flex-wrap: wrap; }}
.header-brand {{ text-align: right; flex-shrink: 0; margin-left: 24px; }}
.header-brand .brand-title {{ font-weight: 600; font-size: 14px; color: #1a1a1a; }}
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
table {{ width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 13px; }}
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
.hblk {{ height: 28px; flex: 1; border-radius: 2px; cursor: default; transition: opacity 0.1s, transform 0.1s; position: relative; }}
.hblk:hover {{ opacity: 0.85; transform: scaleY(1.15); z-index: 2; }}
.hblk-up {{ background: #1d9e75; }}
.hblk-down {{ background: #e24b4a; }}
.hblk-nodata {{ background: #ef9f27; }}
.hblk-stopped {{ background: #d1d0ca; }}
.legend {{ display: flex; gap: 16px; align-items: center; font-size: 12px; color: #888780; margin: 12px 0 0; }}
.legend-block {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; vertical-align: middle; margin-right: 4px; }}
.tooltip {{
  position: fixed;
  background: #1c1c1a;
  color: #e8e5dc;
  font-size: 12px;
  padding: 12px 16px;
  border-radius: 8px;
  display: none;
  pointer-events: none;
  z-index: 9999;
  white-space: normal;
  max-width: 300px;
  min-width: 200px;
  line-height: 1.55;
  box-shadow: 0 8px 32px rgba(0,0,0,0.55), 0 2px 8px rgba(0,0,0,0.3);
  border: 1px solid rgba(255,255,255,0.08);
}}
.tooltip-arrow {{
  position: absolute;
  left: 50%;
  transform: translateX(-50%);
  width: 0; height: 0;
}}
.tooltip-arrow-down {{
  bottom: -7px;
  border-left: 7px solid transparent;
  border-right: 7px solid transparent;
  border-top: 7px solid #1c1c1a;
}}
.tooltip-arrow-up {{
  top: -7px;
  border-left: 7px solid transparent;
  border-right: 7px solid transparent;
  border-bottom: 7px solid #1c1c1a;
}}
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
            brand_parts.append(f'<div class="brand-title">{html.escape(title)}</div>')
        branding_html = f'<div class="header-brand">{"".join(brand_parts)}</div>'

    parts.append(f"""<div class="header">
<div class="header-left">
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
<div class="metric-value" style="font-size:18px;">{inst_value}</div>
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

    # Unmeasured instances info banner
    unmeasured = inst_count - monitorable_count
    if unmeasured > 0:
        parts.append(f"""<div style="background:#f0f4ff;border-left:4px solid #4a7cc7;padding:12px 16px;border-radius:0 6px 6px 0;margin-bottom:20px;font-size:13px;color:#1e3a6e;display:flex;align-items:center;gap:8px;">
<svg width="16" height="16" viewBox="0 0 16 16" fill="none" style="flex-shrink:0;"><circle cx="8" cy="8" r="7" stroke="#4a7cc7" stroke-width="1.5" fill="#f0f4ff"/><text x="8" y="12" text-anchor="middle" font-size="10" font-weight="700" fill="#1e3a6e">i</text></svg>
<span><strong>{unmeasured} of {inst_count} instances</strong> had no uptime during this period (always stopped) and are excluded from fleet availability calculations &mdash; shown as <strong>N/A</strong>. Fleet availability reflects the <strong>{monitorable_count} active instance(s)</strong> only.</span>
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

    for comp_idx, (comp_id, group) in enumerate(grouped.items()):
        comp_name = group["name"]
        comp_instances = group["instances"]

        # Compute compartment stats
        comp_stats = compute_compartment_stats(comp_instances, sla_target)
        comp_pct = comp_stats["compartment_availability_pct"]
        comp_pct_str = f"{comp_pct:.2f}%" if comp_pct is not None else "N/A"
        comp_pct_color = "#0f6e56" if comp_pct is not None and comp_pct >= sla_target else "#a32d2d"

        # Compartment header — collapsible via <details>/<summary>
        border_style = ' style="border-top:2px solid #e8e6df;"' if comp_idx > 0 else ''
        parts.append(f'<details class="comp-section" open{border_style}>')
        parts.append(f'<summary class="comp-header">{html.escape(comp_name)} <span class="comp-count">({len(comp_instances)} instances)</span> &mdash; <span class="comp-pct" style="color:{comp_pct_color};">{comp_pct_str}</span></summary>')

        # Table
        parts.append("""<table>
<colgroup>
<col style="width:28%;">
<col style="width:14%;">
<col style="width:14%;">
<col style="width:30%;">
<col style="width:14%;">
</colgroup>""")
        parts.append("""<thead><tr>
<th>Instance</th>
<th class="center">Status</th>
<th>Availability</th>
<th class="center">Uptime</th>
<th>Downtime</th>
</tr></thead>""")

        parts.append('<tbody>')
        for inst in comp_instances:
            name = inst["name"]
            inst_id = inst["id"]
            state = inst.get("state", "UNKNOWN")
            avail = inst.get("availability_pct")
            up_m = inst.get("up_minutes", 0)
            down_m = inst.get("down_minutes", 0)
            stopped_m = inst.get("stopped_minutes", 0)
            monitored_m = inst.get("monitored_minutes", 0)
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
                avail_str = f"{avail:.4f}%" if avail < 100 else "100%"
                if state == "STOPPED":
                    # Grey out stopped instances — green 100% on a stopped instance is misleading
                    avail_color = "#888780"
                elif avail >= sla_target:
                    avail_color = "#0f6e56"
                elif avail >= 99.0:
                    avail_color = "#633806"
                else:
                    avail_color = "#a32d2d"
            else:
                avail_str = "N/A"
                avail_color = "#888780"

            # Uptime bar proportions
            total_m = up_m + down_m + stopped_m
            if total_m > 0:
                up_pct = up_m / total_m * 100
                down_pct = down_m / total_m * 100
                stopped_pct = stopped_m / total_m * 100
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

            # Downtime display
            dt_color = "#a32d2d" if downtime_min > 0 else "inherit"
            if downtime_min >= 60:
                h2, rem2 = divmod(downtime_min, 60)
                dt_str = f"{h2}h {rem2}m" if rem2 else f"{h2}h"
            else:
                dt_str = f"{downtime_min} min"

            # Uptime display
            up_h_display = f"{up_m // 60}h {up_m % 60}m" if up_m >= 60 else f"{up_m}m"

            parts.append(f"""<tr>
<td><span class="dot {dot_cls}"></span><span class="inst-name" data-ocid="{html.escape(inst_id, quote=True)}" style="cursor:default;border-bottom:1px dotted #b4b2a9;">{html.escape(name)}</span></td>
<td class="center"><span class="badge {badge_cls}">{html.escape(state)}</span></td>
<td style="font-weight:600;color:{avail_color};">{avail_str}</td>
<td class="center"><div class="uptime-cell"><span class="uptime-hours">{up_h_display}</span><div class="avail-bar">{"".join(bar_segments)}</div></div></td>
<td style="color:{dt_color};">{dt_str}</td>
</tr>""")

        parts.append('</tbody></table>')
        parts.append('</details>')

    parts.append('</div></div>')  # close tbl-wrap and summary-row

    # Section E: HEATMAP
    parts.append('<div class="heatmap-section">')
    parts.append('<div class="section-title">Availability heatmap</div>')

    # Date markers
    if all_buckets:
        from datetime import datetime as _dt
        first_bucket = _dt.strptime(all_buckets[0], "%Y-%m-%dT%H:%M:%SZ")
        last_bucket = _dt.strptime(all_buckets[-1], "%Y-%m-%dT%H:%M:%SZ")
        total_span = (last_bucket - first_bucket).days
        date_labels = []
        if total_span <= 7:
            step = 1
        elif total_span <= 14:
            step = 2
        elif total_span <= 30:
            step = 5
        else:
            step = 7
        d = first_bucket
        while d <= last_bucket:
            date_labels.append(f"{d.strftime('%b')} {d.day}")
            d += timedelta(days=step)
        last_label = f"{last_bucket.strftime('%b')} {last_bucket.day}"
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
            inst_state = inst.get("state", "")
            if avail is not None:
                pct_str = f"{avail:.2f}%" if avail < 100 else "100%"
                if inst_state == "STOPPED":
                    pct_color = "#888780"
                elif avail >= sla_target:
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

            # Aggregate blocks — each block covers block_minutes minutes
            blocks = []
            num_buckets = len(statuses)
            for b_start in range(0, num_buckets, block_minutes):
                chunk = statuses[b_start:b_start + block_minutes]
                agg = _aggregate_heatmap_block(chunk)
                blk_cls = f"hblk hblk-{agg}"

                # Timestamps for tooltip
                if b_start < len(all_buckets):
                    blk_start_key = all_buckets[b_start]
                else:
                    blk_start_key = ""
                b_end_idx = min(b_start + block_minutes - 1, len(all_buckets) - 1)
                blk_end_key = all_buckets[b_end_idx] if b_end_idx >= 0 else ""

                # Per-block minute counts for tooltip
                blk_up = sum(1 for s in chunk if s == "up")
                blk_down = sum(1 for s in chunk if s == "down")
                blk_total = len(chunk)

                blocks.append(
                    f'<div class="{blk_cls}"'
                    f' data-name="{html.escape(inst_name, quote=True)}"'
                    f' data-start="{blk_start_key}"'
                    f' data-end="{blk_end_key}"'
                    f' data-status="{agg}"'
                    f' data-up="{blk_up}"'
                    f' data-down="{blk_down}"'
                    f' data-total="{blk_total}"'
                    f'></div>'
                )

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

    # Heatmap block tooltip + instance name OCID tooltip JS
    parts.append("""<script>
(function() {
  var tip = document.getElementById('tooltip');
  if (!tip) return;

  // Build arrow element inside tooltip
  var arrow = document.createElement('div');
  arrow.className = 'tooltip-arrow';
  tip.appendChild(arrow);

  var statusLabels = {
    'up':      'Operational',
    'down':    'Outage',
    'nodata':  'No data',
    'stopped': 'Stopped'
  };
  var statusColors = {
    'up':      '#1d9e75',
    'down':    '#e24b4a',
    'nodata':  '#ef9f27',
    'stopped': '#9e9b94'
  };
  var statusBg = {
    'up':      'rgba(29,158,117,0.15)',
    'down':    'rgba(226,75,74,0.15)',
    'nodata':  'rgba(239,159,39,0.15)',
    'stopped': 'rgba(158,155,148,0.12)'
  };

  function fmtUtc(isoStr) {
    if (!isoStr) return '';
    try {
      var s = isoStr.replace(' ', 'T');
      if (!s.endsWith('Z')) s += 'Z';
      var d = new Date(s);
      if (isNaN(d.getTime())) return isoStr;
      var mo = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getUTCMonth()];
      var day = d.getUTCDate();
      var h = String(d.getUTCHours()).padStart(2,'0');
      var m = String(d.getUTCMinutes()).padStart(2,'0');
      return mo + ' ' + day + ', ' + h + ':' + m + ' UTC';
    } catch(e) { return isoStr; }
  }

  function fmtDur(mins) {
    mins = parseInt(mins, 10);
    if (isNaN(mins) || mins < 0) return '\u2014';
    if (mins === 0) return '0 min';
    if (mins < 60) return mins + ' min';
    var h = Math.floor(mins / 60), r = mins % 60;
    return r ? h + 'h ' + r + 'm' : h + 'h';
  }

  function positionTip(anchorEl) {
    tip.style.display = 'block';
    var rect = anchorEl.getBoundingClientRect();
    var tw = tip.offsetWidth;
    var th = tip.offsetHeight;
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var MARGIN = 10;
    var ARROW_H = 8;

    // Horizontal: center over element, clamp to viewport
    var left = rect.left + rect.width / 2 - tw / 2;
    left = Math.max(MARGIN, Math.min(left, vw - tw - MARGIN));

    // Vertical: prefer above, fall back to below
    var topAbove = rect.top - th - ARROW_H - 4;
    var topBelow = rect.bottom + ARROW_H + 4;
    var above = topAbove >= MARGIN;

    var top = above ? topAbove : topBelow;

    tip.style.left = left + 'px';
    tip.style.top  = top  + 'px';

    // Arrow: point toward the element
    arrow.className = 'tooltip-arrow ' + (above ? 'tooltip-arrow-down' : 'tooltip-arrow-up');
    // Horizontal arrow position relative to tooltip
    var arrowLeft = rect.left + rect.width / 2 - left;
    arrowLeft = Math.max(12, Math.min(arrowLeft, tw - 12));
    arrow.style.left = arrowLeft + 'px';
    arrow.style.transform = 'none';
  }

  // ── Heatmap block tooltips ──────────────────────────────────────────────
  document.querySelectorAll('.hblk').forEach(function(el) {
    el.addEventListener('mouseenter', function() {
      var name   = el.getAttribute('data-name')   || '';
      var start  = el.getAttribute('data-start')  || '';
      var end    = el.getAttribute('data-end')    || '';
      var status = el.getAttribute('data-status') || '';
      var upM    = el.getAttribute('data-up')     || '0';
      var downM  = el.getAttribute('data-down')   || '0';
      var totalM = el.getAttribute('data-total')  || '0';
      var label  = statusLabels[status] || status;
      var color  = statusColors[status] || '#888780';
      var bg     = statusBg[status]    || 'rgba(136,135,128,0.1)';
      var startFmt = fmtUtc(start);
      var endFmt   = (end && end !== start) ? fmtUtc(end) : '';
      var timeStr  = endFmt ? (startFmt + ' \u2013 ' + endFmt) : startFmt;

      var upPct = parseInt(totalM,10) > 0
        ? Math.round(parseInt(upM,10) / parseInt(totalM,10) * 100)
        : (status === 'up' ? 100 : 0);

      // Build content (arrow will be appended after setting innerHTML)
      tip.innerHTML =
        '<div style="font-weight:700;font-size:13px;margin-bottom:2px;color:#f5f2eb;">' + name + '</div>' +
        '<div style="color:#7c7a73;font-size:10px;margin-bottom:8px;">' + timeStr + '</div>' +
        '<div style="display:flex;align-items:center;gap:8px;background:' + bg + ';border-radius:6px;padding:7px 10px;margin-bottom:6px;">' +
          '<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:' + color + ';flex-shrink:0;"></span>' +
          '<span style="font-weight:600;font-size:12px;color:#f5f2eb;">' + label + '</span>' +
          (status === 'up' || status === 'down' ? '<span style="margin-left:auto;font-size:11px;color:' + color + ';font-weight:600;">' + upPct + '% up</span>' : '') +
        '</div>' +
        '<div style="font-size:11px;line-height:1.9;color:#9e9b94;border-top:1px solid rgba(255,255,255,0.07);padding-top:6px;">' +
          '<div style="display:flex;justify-content:space-between;"><span>Uptime</span><span style="color:#e8e5dc;font-variant-numeric:tabular-nums;">' + fmtDur(upM) + ' / ' + fmtDur(totalM) + '</span></div>' +
          (parseInt(downM,10) > 0 ? '<div style="display:flex;justify-content:space-between;"><span>Downtime</span><span style="color:#e24b4a;font-variant-numeric:tabular-nums;">' + fmtDur(downM) + '</span></div>' : '') +
        '</div>';
      tip.appendChild(arrow);
      positionTip(el);
    });
    el.addEventListener('mouseleave', function() { tip.style.display = 'none'; });
  });

  // ── Instance name OCID tooltips ─────────────────────────────────────────
  document.querySelectorAll('.inst-name').forEach(function(el) {
    el.addEventListener('mouseenter', function() {
      var ocid = el.getAttribute('data-ocid') || '';
      tip.innerHTML =
        '<div style="color:#7c7a73;font-size:10px;font-weight:600;letter-spacing:0.8px;margin-bottom:5px;text-transform:uppercase;">Instance OCID</div>' +
        '<div style="font-family:\\'Courier New\\',\\'SF Mono\\',monospace;font-size:10px;word-break:break-all;color:#a8c8f0;line-height:1.6;">' + ocid + '</div>' +
        '<div style="font-size:10px;color:#7c7a73;margin-top:5px;border-top:1px solid rgba(255,255,255,0.07);padding-top:5px;">Click to copy \u2192 not yet enabled</div>';
      tip.appendChild(arrow);
      positionTip(el);
    });
    el.addEventListener('mouseleave', function() { tip.style.display = 'none'; });
  });

  // Hide tip on scroll
  window.addEventListener('scroll', function() { tip.style.display = 'none'; }, true);

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
    if not logo_path:
        return None
    if not os.path.isfile(logo_path):
        log.warning("Logo file not found: %s — skipping logo.", logo_path)
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
    time_buckets = build_time_buckets(start_time, end_time, resolution="1m")

    log.info("Collecting metrics for %d instances over %d days (%d minute buckets)...",
             len(instances), args.days, len(time_buckets))
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
        instances, time_buckets, cpu_metrics, status_metrics, failed_instance_ids
    )

    # Merge stats into instance dicts
    for inst in instances:
        stats = compute_instance_stats(matrix[inst["id"]])
        inst.update(stats)

    fleet = compute_fleet_stats(instances, args.sla_target, discovery_warnings=disc_warnings)

    # Build heatmap data (list of statuses per instance, one per minute bucket)
    heatmap_data = {}
    for inst in instances:
        heatmap_data[inst["id"]] = [matrix[inst["id"]][b] for b in time_buckets]

    # Phase 5: Render
    log.info("Generating report...")
    region = args.region or config.get("region", "unknown")
    logo_data = embed_logo(args.logo) if args.logo else None

    html = generate_html_report(
        instances=instances,
        fleet=fleet,
        heatmap_data=heatmap_data,
        all_buckets=time_buckets,
        compartment_name=compartment_name,
        region=region,
        days=args.days,
        sla_target=args.sla_target,
        start_date=start_time.strftime("%b %d, %Y %H:%M UTC"),
        end_date=(end_time - timedelta(minutes=1)).strftime("%b %d, %Y %H:%M UTC"),
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
             f"{fleet['fleet_availability_pct']:.4f}%" if fleet['fleet_availability_pct'] is not None else "N/A")


if __name__ == "__main__":
    main()
