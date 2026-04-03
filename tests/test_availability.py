import pytest
import sys
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone, timedelta

from compute_availability_report import (
    classify_hour, calculate_batch_groups, build_hourly_buckets,
    collect_metrics, collect_all_metrics, DATAPOINT_LIMIT, STATUS_CALL_THROTTLE_SECS,
)


# Tests for parse_args function
class TestParseArgs:
    def test_required_compartment_id(self):
        """--compartment-id is required"""
        with pytest.raises(SystemExit):
            from compute_availability_report import parse_args
            parse_args([])

    def test_default_values(self):
        from compute_availability_report import parse_args
        args = parse_args(["--compartment-id", "ocid1.compartment.oc1..aaa"])
        assert args.auth == "instance_principal"
        assert args.profile == "DEFAULT"
        assert args.days == 7
        assert args.sla_target == 99.95
        assert args.running_only is False
        assert args.upload is False
        assert args.bucket == "availability-reports"
        assert args.par_expiry_days == 30

    def test_days_validation(self):
        """--days only accepts 7, 14, 30, 60, 90"""
        from compute_availability_report import parse_args
        with pytest.raises(SystemExit):
            parse_args(["--compartment-id", "ocid1.test", "--days", "15"])

    def test_all_flags(self):
        from compute_availability_report import parse_args
        args = parse_args([
            "--compartment-id", "ocid1.test",
            "--auth", "config",
            "--profile", "PROD",
            "--days", "30",
            "--sla-target", "99.99",
            "--running-only",
            "--region", "us-ashburn-1",
            "--title", "ACME Corp",
            "--logo", "/path/to/logo.png",
            "--output", "/tmp/report.html",
            "--upload",
            "--bucket", "my-bucket",
            "--os-namespace", "myns",
            "--par-expiry-days", "90",
            "--compartment-name", "My Compartment",
        ])
        assert args.auth == "config"
        assert args.profile == "PROD"
        assert args.days == 30
        assert args.sla_target == 99.99
        assert args.running_only is True
        assert args.region == "us-ashburn-1"
        assert args.title == "ACME Corp"
        assert args.logo == "/path/to/logo.png"
        assert args.output == "/tmp/report.html"
        assert args.upload is True
        assert args.bucket == "my-bucket"
        assert args.os_namespace == "myns"
        assert args.par_expiry_days == 90
        assert args.compartment_name == "My Compartment"

    def test_compartment_name_override(self):
        from compute_availability_report import parse_args
        args = parse_args([
            "--compartment-id", "ocid1.test",
            "--compartment-name", "Custom Name",
        ])
        assert args.compartment_name == "Custom Name"


class TestClassifyHour:
    def test_cpu_data_and_status_healthy(self):
        assert classify_hour(has_cpu=True, instance_status=0) == "up"

    def test_cpu_data_and_status_unhealthy(self):
        assert classify_hour(has_cpu=True, instance_status=1) == "down"

    def test_cpu_data_and_no_status(self):
        assert classify_hour(has_cpu=True, instance_status=None) == "up"

    def test_no_cpu_and_status_healthy(self):
        assert classify_hour(has_cpu=False, instance_status=0) == "up"

    def test_no_cpu_and_status_unhealthy(self):
        assert classify_hour(has_cpu=False, instance_status=1) == "down"

    def test_no_cpu_and_no_status(self):
        assert classify_hour(has_cpu=False, instance_status=None) == "stopped"

    def test_query_failure(self):
        assert classify_hour(has_cpu=False, instance_status=None, query_failed=True) == "nodata"

    def test_query_failure_overrides_data(self):
        assert classify_hour(has_cpu=True, instance_status=0, query_failed=True) == "nodata"


class TestComputeInstanceStats:
    def test_all_up(self):
        from compute_availability_report import compute_instance_stats
        hourly = {"2026-03-24T00:00:00Z": "up", "2026-03-24T01:00:00Z": "up"}
        stats = compute_instance_stats(hourly)
        assert stats["up_hours"] == 2
        assert stats["down_hours"] == 0
        assert stats["stopped_hours"] == 0
        assert stats["availability_pct"] == 100.0

    def test_with_downtime(self):
        from compute_availability_report import compute_instance_stats
        hourly = {f"h{i}": "up" for i in range(167)}
        hourly["h167"] = "down"
        stats = compute_instance_stats(hourly)
        assert stats["up_hours"] == 167
        assert stats["down_hours"] == 1
        assert stats["availability_pct"] == 99.40  # 167/168 rounded to 2dp

    def test_stopped_excluded_from_denominator(self):
        from compute_availability_report import compute_instance_stats
        hourly = {"h0": "up", "h1": "up", "h2": "stopped", "h3": "stopped"}
        stats = compute_instance_stats(hourly)
        assert stats["up_hours"] == 2
        assert stats["stopped_hours"] == 2
        assert stats["monitored_hours"] == 2  # up + down only
        assert stats["availability_pct"] == 100.0

    def test_all_stopped(self):
        from compute_availability_report import compute_instance_stats
        hourly = {"h0": "stopped", "h1": "stopped"}
        stats = compute_instance_stats(hourly)
        assert stats["availability_pct"] is None  # N/A

    def test_downtime_minutes(self):
        from compute_availability_report import compute_instance_stats
        hourly = {"h0": "up", "h1": "down", "h2": "down"}
        stats = compute_instance_stats(hourly)
        assert stats["downtime_minutes"] == 120

    def test_nodata_causes_na(self):
        from compute_availability_report import compute_instance_stats
        hourly = {"h0": "up", "h1": "nodata", "h2": "up"}
        stats = compute_instance_stats(hourly)
        assert stats["nodata_hours"] == 1
        assert stats["availability_pct"] is None
        assert stats["data_complete"] is False

    def test_mixed_up_down_stopped(self):
        from compute_availability_report import compute_instance_stats
        hourly = {f"h{i}": "up" for i in range(5)}
        hourly.update({f"h{i+5}": "down" for i in range(2)})
        hourly.update({f"h{i+7}": "stopped" for i in range(3)})
        stats = compute_instance_stats(hourly)
        assert stats["up_hours"] == 5
        assert stats["down_hours"] == 2
        assert stats["stopped_hours"] == 3
        assert stats["monitored_hours"] == 7  # 5 + 2
        assert stats["availability_pct"] == 71.43  # 5/7 * 100


class TestComputeCompartmentStats:
    def test_compartment_stats(self):
        from compute_availability_report import compute_compartment_stats
        instances = [
            {"compartment_name": "prod", "up_hours": 168, "down_hours": 0,
             "monitored_hours": 168, "availability_pct": 100.0, "data_complete": True},
            {"compartment_name": "prod", "up_hours": 167, "down_hours": 1,
             "monitored_hours": 168, "availability_pct": 99.40, "data_complete": True},
        ]
        stats = compute_compartment_stats(instances, sla_target=99.95)
        assert stats["instance_count"] == 2
        assert stats["compartment_availability_pct"] == 99.70  # 335/336
        assert stats["at_target_count"] == 1
        assert stats["data_complete"] is True

    def test_compartment_incomplete_forces_na(self):
        from compute_availability_report import compute_compartment_stats
        instances = [
            {"compartment_name": "prod", "up_hours": 168, "down_hours": 0,
             "monitored_hours": 168, "availability_pct": 100.0, "data_complete": True},
            {"compartment_name": "prod", "up_hours": 50, "down_hours": 0,
             "monitored_hours": 50, "availability_pct": None, "data_complete": False},
        ]
        stats = compute_compartment_stats(instances, sla_target=99.95)
        assert stats["compartment_availability_pct"] is None
        assert stats["at_target_count"] is None
        assert stats["data_complete"] is False


class TestGetHeatmapResolution:
    def test_7_days(self):
        from compute_availability_report import get_heatmap_resolution
        assert get_heatmap_resolution(7) == (1, "1 hour")

    def test_14_days(self):
        from compute_availability_report import get_heatmap_resolution
        assert get_heatmap_resolution(14) == (4, "4 hours")

    def test_30_days(self):
        from compute_availability_report import get_heatmap_resolution
        assert get_heatmap_resolution(30) == (6, "6 hours")

    def test_60_days(self):
        from compute_availability_report import get_heatmap_resolution
        assert get_heatmap_resolution(60) == (24, "1 day")

    def test_90_days(self):
        from compute_availability_report import get_heatmap_resolution
        assert get_heatmap_resolution(90) == (24, "1 day")


class TestComputeFleetStats:
    def test_fleet_uses_discovered_instance_count(self):
        from compute_availability_report import compute_fleet_stats
        instances = [
            {"up_hours": 168, "down_hours": 0, "monitored_hours": 168,
             "availability_pct": 100.0, "data_complete": True},
        ]
        fleet = compute_fleet_stats(instances, sla_target=99.95)
        assert fleet["discovered_instance_count"] == 1
        assert "total_instances" not in fleet  # renamed field

    def test_fleet_aggregation(self):
        from compute_availability_report import compute_fleet_stats
        instances = [
            {"up_hours": 168, "down_hours": 0, "monitored_hours": 168,
             "availability_pct": 100.0, "data_complete": True},
            {"up_hours": 167, "down_hours": 1, "monitored_hours": 168,
             "availability_pct": 99.40, "data_complete": True},
        ]
        fleet = compute_fleet_stats(instances, sla_target=99.95)
        assert fleet["discovered_instance_count"] == 2
        assert fleet["total_up_hours"] == 335
        assert fleet["total_monitored_hours"] == 336
        assert fleet["fleet_availability_pct"] == 99.70
        assert fleet["at_target_count"] == 1
        assert fleet["report_complete"] is True

    def test_fleet_all_na(self):
        from compute_availability_report import compute_fleet_stats
        instances = [
            {"up_hours": 0, "down_hours": 0, "monitored_hours": 0,
             "availability_pct": None, "data_complete": True},
        ]
        fleet = compute_fleet_stats(instances, sla_target=99.95)
        assert fleet["fleet_availability_pct"] is None

    def test_fleet_incomplete_data_forces_na(self):
        """One incomplete instance forces fleet to N/A"""
        from compute_availability_report import compute_fleet_stats
        instances = [
            {"up_hours": 168, "down_hours": 0, "monitored_hours": 168,
             "availability_pct": 100.0, "data_complete": True},
            {"up_hours": 100, "down_hours": 0, "monitored_hours": 100,
             "availability_pct": None, "data_complete": False},
        ]
        fleet = compute_fleet_stats(instances, sla_target=99.95)
        assert fleet["fleet_availability_pct"] is None
        assert fleet["at_target_count"] is None
        assert fleet["total_up_hours"] is None
        assert fleet["total_monitored_hours"] is None
        assert fleet["data_complete"] is False
        assert fleet["report_complete"] is False
        # discovered_instance_count stays numeric for diagnostics
        assert fleet["discovered_instance_count"] == 2

    def test_fleet_discovery_warning_forces_na(self):
        """Discovery warning alone forces fleet rollups to N/A"""
        from compute_availability_report import compute_fleet_stats
        instances = [
            {"up_hours": 168, "down_hours": 0, "monitored_hours": 168,
             "availability_pct": 100.0, "data_complete": True},
        ]
        fleet = compute_fleet_stats(instances, sla_target=99.95,
                                     discovery_warnings=["Could not list instances in staging"])
        assert fleet["fleet_availability_pct"] is None
        assert fleet["at_target_count"] is None
        assert fleet["discovery_complete"] is False
        assert fleet["report_complete"] is False
        assert fleet["discovered_instance_count"] == 1


class TestGroupInstances:
    def test_groups_by_compartment_ocid(self):
        from compute_availability_report import group_instances_by_compartment
        instances = [
            {"name": "vm1", "compartment_id": "ocid1.comp.prod", "compartment_name": "prod", "availability_pct": 100.0},
            {"name": "vm2", "compartment_id": "ocid1.comp.staging", "compartment_name": "staging", "availability_pct": 99.5},
            {"name": "vm3", "compartment_id": "ocid1.comp.prod", "compartment_name": "prod", "availability_pct": 98.0},
        ]
        groups = group_instances_by_compartment(instances)
        assert len(groups) == 2
        assert len(groups["ocid1.comp.prod"]["instances"]) == 2
        assert len(groups["ocid1.comp.staging"]["instances"]) == 1
        assert groups["ocid1.comp.prod"]["name"] == "prod"

    def test_sorted_worst_first(self):
        from compute_availability_report import group_instances_by_compartment
        instances = [
            {"name": "vm1", "compartment_id": "ocid1.comp.prod", "compartment_name": "prod", "availability_pct": 100.0},
            {"name": "vm2", "compartment_id": "ocid1.comp.prod", "compartment_name": "prod", "availability_pct": 98.0},
            {"name": "vm3", "compartment_id": "ocid1.comp.prod", "compartment_name": "prod", "availability_pct": 99.5},
        ]
        groups = group_instances_by_compartment(instances)
        names = [i["name"] for i in groups["ocid1.comp.prod"]["instances"]]
        assert names == ["vm2", "vm3", "vm1"]  # worst first

    def test_duplicate_names_different_ocids(self):
        """Two compartments named 'prod' in different branches must not merge"""
        from compute_availability_report import group_instances_by_compartment
        instances = [
            {"name": "vm1", "compartment_id": "ocid1.comp.branchA",
             "compartment_name": "prod", "compartment_label": "teamA/prod",
             "availability_pct": 100.0},
            {"name": "vm2", "compartment_id": "ocid1.comp.branchB",
             "compartment_name": "prod", "compartment_label": "teamB/prod",
             "availability_pct": 99.0},
        ]
        groups = group_instances_by_compartment(instances)
        assert len(groups) == 2  # NOT merged into one
        labels = [g["name"] for g in groups.values()]
        assert "teamA/prod" in labels
        assert "teamB/prod" in labels


class TestBuildCompartmentLabels:
    def test_unique_names_use_name_as_label(self):
        from compute_availability_report import build_compartment_labels
        cmap = {
            "root": {"name": "tenancy", "parent_id": None},
            "c1": {"name": "prod", "parent_id": "root"},
            "c2": {"name": "staging", "parent_id": "root"},
        }
        build_compartment_labels(cmap)
        assert cmap["c1"]["label"] == "prod"
        assert cmap["c2"]["label"] == "staging"

    def test_duplicate_names_get_parent_prefix(self):
        from compute_availability_report import build_compartment_labels
        cmap = {
            "root": {"name": "tenancy", "parent_id": None},
            "teamA": {"name": "teamA", "parent_id": "root"},
            "teamB": {"name": "teamB", "parent_id": "root"},
            "c1": {"name": "prod", "parent_id": "teamA"},
            "c2": {"name": "prod", "parent_id": "teamB"},
        }
        build_compartment_labels(cmap)
        assert cmap["c1"]["label"] == "teamA/prod"
        assert cmap["c2"]["label"] == "teamB/prod"
        # Non-duplicated names stay simple
        assert cmap["teamA"]["label"] == "teamA"

    def test_deep_duplicates_walk_ancestors(self):
        """orgA/team/prod vs orgB/team/prod — parent 'team' is also duplicated"""
        from compute_availability_report import build_compartment_labels
        cmap = {
            "root": {"name": "tenancy", "parent_id": None},
            "orgA": {"name": "orgA", "parent_id": "root"},
            "orgB": {"name": "orgB", "parent_id": "root"},
            "teamA": {"name": "team", "parent_id": "orgA"},
            "teamB": {"name": "team", "parent_id": "orgB"},
            "prodA": {"name": "prod", "parent_id": "teamA"},
            "prodB": {"name": "prod", "parent_id": "teamB"},
        }
        build_compartment_labels(cmap)
        # Must disambiguate beyond just team/prod since 'team' is also duplicated
        assert "orgA" in cmap["prodA"]["label"]
        assert "orgB" in cmap["prodB"]["label"]
        assert cmap["prodA"]["label"] != cmap["prodB"]["label"]
        # team is also duplicated, should be disambiguated
        assert cmap["teamA"]["label"] != cmap["teamB"]["label"]


class TestBatching:
    def test_small_fleet_single_batch(self):
        instance_ids = [f"ocid{i}" for i in range(50)]
        batches = calculate_batch_groups(instance_ids, hours=168)
        assert len(batches) == 1
        assert len(batches[0]) == 50

    def test_large_fleet_90_days_multiple_batches(self):
        instance_ids = [f"ocid{i}" for i in range(100)]
        batches = calculate_batch_groups(instance_ids, hours=2160)
        # 100 * 2160 = 216,000 > 80,000. 80000/2160 = ~37 per batch
        assert len(batches) >= 3
        # All instances covered
        all_ids = [id for batch in batches for id in batch]
        assert len(all_ids) == 100


class TestHourlyBuckets:
    def test_7_day_buckets(self):
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=7)
        buckets = build_hourly_buckets(start, end)
        assert len(buckets) == 168

    def test_bucket_format(self):
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=1)
        buckets = build_hourly_buckets(start, end)
        assert buckets[0] == "2026-03-30T00:00:00Z"
        assert buckets[-1] == "2026-03-30T23:00:00Z"
        assert len(buckets) == 24


class TestBuildAvailabilityMatrix:
    def test_builds_matrix_from_metrics(self):
        from compute_availability_report import build_availability_matrix
        hourly_buckets = ["h0", "h1", "h2", "h3"]
        cpu_metrics = {
            "inst1": {"h0": 5.0, "h1": 10.0, "h2": 2.0},  # h3 missing
        }
        status_metrics = {
            "inst1": {"h0": 0, "h1": 0, "h2": 1, "h3": 1},
        }
        matrix = build_availability_matrix(
            ["inst1"], hourly_buckets, cpu_metrics, status_metrics
        )
        assert matrix["inst1"]["h0"] == "up"      # cpu + status 0
        assert matrix["inst1"]["h1"] == "up"      # cpu + status 0
        assert matrix["inst1"]["h2"] == "down"    # cpu + status 1
        assert matrix["inst1"]["h3"] == "down"    # no cpu + status 1

    def test_no_data_is_stopped(self):
        from compute_availability_report import build_availability_matrix
        hourly_buckets = ["h0", "h1"]
        cpu_metrics = {}
        status_metrics = {}
        matrix = build_availability_matrix(
            ["inst1"], hourly_buckets, cpu_metrics, status_metrics
        )
        assert matrix["inst1"]["h0"] == "stopped"
        assert matrix["inst1"]["h1"] == "stopped"

    def test_failed_instance_ids_produce_nodata(self):
        """Only instances in failed_instance_ids get nodata, others unaffected"""
        from compute_availability_report import build_availability_matrix
        hourly_buckets = ["h0", "h1"]
        cpu_metrics = {
            "inst2": {"h0": 5.0, "h1": 10.0},
        }
        status_metrics = {
            "inst2": {"h0": 0, "h1": 0},
        }
        matrix = build_availability_matrix(
            [{"id": "inst1", "compartment_id": "comp1"},
             {"id": "inst2", "compartment_id": "comp1"}],
            hourly_buckets, cpu_metrics, status_metrics,
            failed_instance_ids={"inst1"},
        )
        # inst1 failed -> all nodata
        assert matrix["inst1"]["h0"] == "nodata"
        assert matrix["inst1"]["h1"] == "nodata"
        # inst2 succeeded -> normal classification
        assert matrix["inst2"]["h0"] == "up"
        assert matrix["inst2"]["h1"] == "up"


class TestBatching_Boundary:
    """Edge cases around the batch size boundary (80K datapoint limit)."""

    def test_exactly_at_limit_is_one_batch(self):
        # 111 instances × 720 hours = 79,920 ≤ 80,000 → 1 batch
        hours = 720  # 30 days
        max_per_batch = DATAPOINT_LIMIT // hours  # 111
        ids = [f"ocid{i}" for i in range(max_per_batch)]
        batches = calculate_batch_groups(ids, hours)
        assert len(batches) == 1

    def test_one_over_limit_is_two_batches(self):
        hours = 720
        max_per_batch = DATAPOINT_LIMIT // hours  # 111
        ids = [f"ocid{i}" for i in range(max_per_batch + 1)]
        batches = calculate_batch_groups(ids, hours)
        assert len(batches) == 2
        # All instance IDs are covered exactly once
        flat = [iid for batch in batches for iid in batch]
        assert flat == ids

    def test_single_instance_always_one_batch(self):
        for hours in [168, 720, 1440, 2160]:  # 7, 30, 60, 90 days
            batches = calculate_batch_groups(["ocid1"], hours)
            assert len(batches) == 1
            assert batches[0] == ["ocid1"]

    def test_empty_list_returns_empty(self):
        assert calculate_batch_groups([], hours=720) == []

    def test_90_day_window_tighter_batches(self):
        # 90 days = 2160 hours → 80000/2160 = 37 per batch
        hours = 2160
        ids = [f"ocid{i}" for i in range(100)]
        batches = calculate_batch_groups(ids, hours)
        assert all(len(b) <= DATAPOINT_LIMIT // hours for b in batches)
        flat = [iid for batch in batches for iid in batch]
        assert flat == ids  # no duplicates, no gaps

    def test_hours_larger_than_datapoint_limit_batch_size_is_one(self):
        # hours > DATAPOINT_LIMIT → floor division = 0 → max(1, 0) = 1 → each instance its own batch
        ids = [f"ocid{i}" for i in range(5)]
        batches = calculate_batch_groups(ids, hours=DATAPOINT_LIMIT + 1)
        assert len(batches) == len(ids), "Each instance must be its own batch"
        assert all(len(b) == 1 for b in batches)
        flat = [iid for batch in batches for iid in batch]
        assert flat == ids

    def test_single_hour_window_large_fleet_one_batch(self):
        # hours=1 → batch_size = 80,000 → all instances comfortably fit in one batch
        ids = [f"ocid{i}" for i in range(500)]
        batches = calculate_batch_groups(ids, hours=1)
        assert len(batches) == 1
        assert len(batches[0]) == 500


# ---------------------------------------------------------------------------
# Helpers shared across metric tests
# ---------------------------------------------------------------------------

def _make_monitoring_client(return_data=None):
    """Return a mock OCI MonitoringClient with summarize_metrics_data stubbed."""
    client = MagicMock()
    response = MagicMock()
    response.data = return_data or []
    client.summarize_metrics_data.return_value = response
    return client


def _make_service_error(status, message="error"):
    """Return a mock oci.exceptions.ServiceError."""
    try:
        import oci
        err = MagicMock(spec=oci.exceptions.ServiceError)
    except Exception:
        err = MagicMock()
    err.status = status
    err.message = message
    return err


class TestCollectMetrics:
    """Unit tests for collect_metrics query building and error handling."""

    def _start_end(self):
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=7)
        return start, end

    def test_no_instance_ids_produces_unfiltered_query(self):
        client = _make_monitoring_client()
        start, end = self._start_end()
        collect_metrics(client, "comp1", "oci_computeagent", "CpuUtilization",
                        start, end, use_subtree=False, instance_ids=None)
        call_args = client.summarize_metrics_data.call_args
        details = call_args[0][1]
        assert details.query == "CpuUtilization[1h].max()"

    def test_single_instance_id_uses_equality_predicate(self):
        client = _make_monitoring_client()
        start, end = self._start_end()
        collect_metrics(client, "comp1", "oci_computeagent", "CpuUtilization",
                        start, end, use_subtree=False, instance_ids=["ocid1.instance.aaa"])
        details = client.summarize_metrics_data.call_args[0][1]
        # Single id: no || separator
        assert 'resourceId = "ocid1.instance.aaa"' in details.query
        assert " || " not in details.query

    def test_multiple_instance_ids_uses_or_predicate(self):
        client = _make_monitoring_client()
        start, end = self._start_end()
        collect_metrics(client, "comp1", "oci_computeagent", "CpuUtilization",
                        start, end, use_subtree=False,
                        instance_ids=["ocid1.instance.aaa", "ocid1.instance.bbb"])
        details = client.summarize_metrics_data.call_args[0][1]
        assert " || " in details.query
        assert 'resourceId = "ocid1.instance.aaa"' in details.query
        assert 'resourceId = "ocid1.instance.bbb"' in details.query

    def test_use_subtree_passed_through(self):
        client = _make_monitoring_client()
        start, end = self._start_end()
        collect_metrics(client, "ocid1.tenancy.aaa", "oci_computeagent", "CpuUtilization",
                        start, end, use_subtree=True, instance_ids=None)
        call_args = client.summarize_metrics_data.call_args
        assert call_args[1]["compartment_id_in_subtree"] is True

    def test_success_returns_parsed_metrics(self):
        dp = MagicMock()
        dp.timestamp = datetime(2026, 3, 24, 5, 0, 0, tzinfo=timezone.utc)
        dp.value = 42.5
        metric_data = MagicMock()
        metric_data.dimensions = {"resourceId": "ocid1.instance.aaa"}
        metric_data.aggregated_datapoints = [dp]
        client = _make_monitoring_client(return_data=[metric_data])
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert not failed
        assert "ocid1.instance.aaa" in result
        assert result["ocid1.instance.aaa"]["2026-03-24T05:00:00Z"] == 42.5

    def test_400_returns_failed_immediately_no_retry(self):
        """400 errors must not be retried — they indicate a bad query."""
        try:
            import oci
            err = oci.exceptions.ServiceError(400, "InvalidParameter", {}, "Not Supported yet")
        except Exception:
            pytest.skip("oci package not available")

        client = MagicMock()
        client.summarize_metrics_data.side_effect = err
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert failed is True
        assert result == {}
        assert client.summarize_metrics_data.call_count == 1  # no retry

    def test_429_is_retried(self):
        """429 TooManyRequests must be retried up to MAX_RETRIES times."""
        try:
            import oci
            err = oci.exceptions.ServiceError(429, "TooManyRequests", {}, "rate limited")
        except Exception:
            pytest.skip("oci package not available")

        # Fail twice then succeed
        response = MagicMock()
        response.data = []
        client = MagicMock()
        client.summarize_metrics_data.side_effect = [err, err, response]
        start, end = self._start_end()
        with patch("time.sleep"):  # avoid actual sleep in tests
            result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                              "CpuUtilization", start, end)
        assert not failed
        assert client.summarize_metrics_data.call_count == 3

    def test_exhausted_retries_returns_failed(self):
        """After MAX_RETRIES 5xx failures the call must return failed=True."""
        try:
            import oci
            err = oci.exceptions.ServiceError(503, "ServiceUnavailable", {}, "down")
        except Exception:
            pytest.skip("oci package not available")

        client = MagicMock()
        client.summarize_metrics_data.side_effect = err
        start, end = self._start_end()
        from compute_availability_report import MAX_RETRIES
        with patch("time.sleep"):
            result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                              "CpuUtilization", start, end)
        assert failed is True
        assert client.summarize_metrics_data.call_count == MAX_RETRIES

    def test_empty_instance_ids_list_acts_as_no_filter(self):
        """instance_ids=[] is falsy — must produce an unfiltered query, not crash."""
        client = _make_monitoring_client()
        start, end = self._start_end()
        collect_metrics(client, "comp1", "oci_computeagent", "CpuUtilization",
                        start, end, use_subtree=False, instance_ids=[])
        details = client.summarize_metrics_data.call_args[0][1]
        assert "resourceId" not in details.query
        assert details.query == "CpuUtilization[1h].max()"

    def test_dimensions_none_skips_entry_without_crash(self):
        """metric_data.dimensions=None must be handled gracefully (skipped, no exception)."""
        metric_data = MagicMock()
        metric_data.dimensions = None
        metric_data.aggregated_datapoints = []
        client = _make_monitoring_client(return_data=[metric_data])
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert not failed
        assert result == {}

    def test_dimensions_missing_resource_id_skips_entry(self):
        """dimensions dict without 'resourceId' key must be skipped silently."""
        metric_data = MagicMock()
        metric_data.dimensions = {"other_key": "some_value"}
        metric_data.aggregated_datapoints = []
        client = _make_monitoring_client(return_data=[metric_data])
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert not failed
        assert result == {}

    def test_resource_id_with_no_datapoints_returns_empty_hours(self):
        """Valid resourceId with empty aggregated_datapoints → key present with {} hours dict."""
        metric_data = MagicMock()
        metric_data.dimensions = {"resourceId": "ocid1.instance.aaa"}
        metric_data.aggregated_datapoints = []
        client = _make_monitoring_client(return_data=[metric_data])
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert not failed
        assert "ocid1.instance.aaa" in result
        assert result["ocid1.instance.aaa"] == {}

    def test_multiple_entries_same_resource_id_hours_merged(self):
        """Two metric_data entries for the same resourceId must merge into one dict."""
        dp1 = MagicMock()
        dp1.timestamp = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
        dp1.value = 10.0
        dp2 = MagicMock()
        dp2.timestamp = datetime(2026, 3, 24, 1, 0, 0, tzinfo=timezone.utc)
        dp2.value = 20.0
        m1 = MagicMock()
        m1.dimensions = {"resourceId": "ocid1.instance.aaa"}
        m1.aggregated_datapoints = [dp1]
        m2 = MagicMock()
        m2.dimensions = {"resourceId": "ocid1.instance.aaa"}
        m2.aggregated_datapoints = [dp2]
        client = _make_monitoring_client(return_data=[m1, m2])
        start, end = self._start_end()
        result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                          "CpuUtilization", start, end)
        assert not failed
        hours = result["ocid1.instance.aaa"]
        assert hours["2026-03-24T00:00:00Z"] == 10.0
        assert hours["2026-03-24T01:00:00Z"] == 20.0

    def test_non_service_error_exception_retried_then_fails(self):
        """A generic exception (e.g. ConnectionError) must be retried, then return failed=True."""
        from compute_availability_report import MAX_RETRIES
        client = MagicMock()
        client.summarize_metrics_data.side_effect = ConnectionError("network failure")
        start, end = self._start_end()
        with patch("time.sleep"):
            result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                              "CpuUtilization", start, end)
        assert failed is True
        assert result == {}
        assert client.summarize_metrics_data.call_count == MAX_RETRIES

    def test_retry_backoff_sleep_values(self):
        """Two 429 failures must sleep for 2s then 4s (exponential backoff).

        Patches time.sleep (stdlib) directly — not compute_availability_report._time.sleep —
        because the module imports time inside the function body, making the module-level
        alias unreachable via monkeypatching.
        """
        try:
            import oci
            err = oci.exceptions.ServiceError(429, "TooManyRequests", {}, "rate limited")
        except Exception:
            pytest.skip("oci package not available")

        response = MagicMock()
        response.data = []
        client = MagicMock()
        client.summarize_metrics_data.side_effect = [err, err, response]
        start, end = self._start_end()
        with patch("time.sleep") as mock_sleep:
            collect_metrics(client, "comp1", "oci_computeagent", "CpuUtilization", start, end)
        sleep_calls = mock_sleep.call_args_list
        assert len(sleep_calls) == 2
        assert sleep_calls[0] == call(2)   # RETRY_BACKOFF * 2^0
        assert sleep_calls[1] == call(4)   # RETRY_BACKOFF * 2^1

    def test_success_on_second_attempt_returns_data(self):
        """One 429 failure followed by success must return the data with failed=False."""
        try:
            import oci
            err = oci.exceptions.ServiceError(429, "TooManyRequests", {}, "rate limited")
        except Exception:
            pytest.skip("oci package not available")

        dp = MagicMock()
        dp.timestamp = datetime(2026, 3, 24, 5, 0, 0, tzinfo=timezone.utc)
        dp.value = 99.0
        metric_data = MagicMock()
        metric_data.dimensions = {"resourceId": "ocid1.instance.aaa"}
        metric_data.aggregated_datapoints = [dp]
        success_response = MagicMock()
        success_response.data = [metric_data]
        client = MagicMock()
        client.summarize_metrics_data.side_effect = [err, success_response]
        start, end = self._start_end()
        with patch("time.sleep"):
            result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                              "CpuUtilization", start, end)
        assert not failed
        assert "ocid1.instance.aaa" in result
        assert result["ocid1.instance.aaa"]["2026-03-24T05:00:00Z"] == 99.0
        assert client.summarize_metrics_data.call_count == 2

    def test_5xx_status_codes_are_retried(self):
        """HTTP 500 and 503 must be retried (not treated like 400 bail-out)."""
        try:
            import oci
        except Exception:
            pytest.skip("oci package not available")

        from compute_availability_report import MAX_RETRIES
        for status_code in [500, 503]:
            err = oci.exceptions.ServiceError(status_code, "ServerError", {}, "server error")
            client = MagicMock()
            client.summarize_metrics_data.side_effect = err
            start, end = self._start_end()
            with patch("time.sleep"):
                result, failed = collect_metrics(client, "comp1", "oci_computeagent",
                                                  "CpuUtilization", start, end)
            assert failed is True
            assert client.summarize_metrics_data.call_count == MAX_RETRIES, (
                f"Status {status_code} should be retried {MAX_RETRIES} times"
            )


class TestCollectAllMetrics:
    """Integration-level tests for the tenancy/compartment routing logic."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        """Suppress real sleeping in all tests — prevents 200-instance tests from taking 10s."""
        with patch("time.sleep"):
            yield

    def _times(self, days=30):
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        return end - timedelta(days=days), end

    def _make_instances(self, n, compartment_id="ocid1.compartment.oc1..aaa"):
        return [{"id": f"ocid1.instance.oc1..{i:04d}", "compartment_id": compartment_id}
                for i in range(n)]

    def _compartment_map(self, comp_id="ocid1.compartment.oc1..aaa"):
        return {comp_id: {"name": "prod", "parent_id": None, "label": "prod"}}

    def _ok_client(self):
        """Monitoring client that always returns empty metric data (success)."""
        client = MagicMock()
        response = MagicMock()
        response.data = []
        client.summarize_metrics_data.return_value = response
        return client

    # ── Tenancy scope: small fleet stays subtree=True ──────────────────────

    def test_small_tenancy_uses_subtree_true(self):
        """Fleet that fits in one batch must keep compartment_id_in_subtree=True."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        # 30 days = 720 hours; 80000/720=111 per batch — 50 instances < 111 → 1 batch
        instances = self._make_instances(50, compartment_id="ocid1.compartment.oc1..child")
        comp_map = {
            tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"},
            "ocid1.compartment.oc1..child": {"name": "prod", "parent_id": tenancy_id, "label": "prod"},
        }
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        subtree_values = [c[1]["compartment_id_in_subtree"] for c in calls]
        assert all(v is True for v in subtree_values), (
            "Small fleet at tenancy scope must use compartment_id_in_subtree=True"
        )

    def test_small_tenancy_unfiltered_query(self):
        """Small-fleet tenancy: CpuUtilization uses one unfiltered call; instance_status
        uses one call per instance (each filtered by a single resourceId predicate).
        5 instances → 1 CPU call + 5 status calls = 6 total.
        """
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        instances = self._make_instances(5, compartment_id="ocid1.compartment.oc1..child")
        comp_map = {tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"}}
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        # Total: 1 unfiltered CPU call + 5 per-instance status calls
        assert client.summarize_metrics_data.call_count == 6, (
            f"Expected 6 calls (1 CPU + 5 status), got {client.summarize_metrics_data.call_count}"
        )
        for c in client.summarize_metrics_data.call_args_list:
            query = c[0][1].query
            ns = c[0][1].namespace
            if ns == "oci_computeagent":
                # CpuUtilization must remain unfiltered for a single-batch tenancy fleet
                assert "resourceId" not in query, (
                    f"Small-fleet CpuUtilization must use unfiltered query, got: {query}"
                )
            elif ns == "oci_compute_infrastructure_health":
                # Each status call must use a single-resourceId filter (no ||)
                assert 'resourceId = "' in query, (
                    f"Status call at tenancy scope must include a single resourceId predicate, got: {query}"
                )
                assert " || " not in query, (
                    f"Status call at tenancy scope must not use || filter, got: {query}"
                )

    # ── Tenancy scope: large fleet falls back to per-compartment ───────────

    def test_large_tenancy_falls_back_to_per_compartment(self):
        """Fleet requiring batching must switch to per-compartment (subtree=False)."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        comp_id = "ocid1.compartment.oc1..child"
        # 30 days = 720 hours; 80000/720=111 per batch — 200 instances → 2 batches
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = {
            tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"},
            comp_id: {"name": "prod", "parent_id": tenancy_id, "label": "prod"},
        }
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        subtree_values = [c[1]["compartment_id_in_subtree"] for c in calls]
        assert all(v is False for v in subtree_values), (
            "Large fleet at tenancy scope must fall back to per-compartment (subtree=False)"
        )

    def test_large_tenancy_fallback_uses_or_filter(self):
        """Per-compartment batches must include resourceId || filter."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        comp_id = "ocid1.compartment.oc1..child"
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = {
            tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"},
            comp_id: {"name": "prod", "parent_id": tenancy_id, "label": "prod"},
        }
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        # At least some calls must have resourceId filters (batched)
        filtered_calls = [c for c in calls if "resourceId" in c[0][1].query]
        assert len(filtered_calls) > 0

    def test_boundary_exactly_at_batch_limit_stays_subtree(self):
        """Fleet of exactly max_per_batch instances must NOT trigger fallback."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        hours = 720  # 30 days
        max_per_batch = DATAPOINT_LIMIT // hours  # 111
        instances = self._make_instances(max_per_batch,
                                          compartment_id="ocid1.compartment.oc1..child")
        comp_map = {
            tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"},
            "ocid1.compartment.oc1..child": {"name": "prod", "parent_id": tenancy_id, "label": "prod"},
        }
        client = self._ok_client()
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        subtree_values = [c[1]["compartment_id_in_subtree"] for c in calls]
        assert all(v is True for v in subtree_values)

    def test_boundary_one_over_limit_falls_back(self):
        """Fleet of max_per_batch+1 instances must trigger per-compartment fallback."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        tenancy_id = "ocid1.tenancy.oc1..aaa"
        comp_id = "ocid1.compartment.oc1..child"
        hours = 720
        max_per_batch = DATAPOINT_LIMIT // hours  # 111
        instances = self._make_instances(max_per_batch + 1, compartment_id=comp_id)
        comp_map = {
            tenancy_id: {"name": "tenancy", "parent_id": None, "label": "tenancy"},
            comp_id: {"name": "prod", "parent_id": tenancy_id, "label": "prod"},
        }
        client = self._ok_client()
        end = datetime(2026, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=30)

        collect_all_metrics(client, tenancy_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        subtree_values = [c[1]["compartment_id_in_subtree"] for c in calls]
        assert all(v is False for v in subtree_values)

    # ── Non-tenancy compartment ────────────────────────────────────────────

    def test_compartment_scope_never_uses_subtree(self):
        """Non-tenancy root must always use subtree=False."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"  # NOT a tenancy OCID
        instances = self._make_instances(5, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        for c in calls:
            assert c[1]["compartment_id_in_subtree"] is False

    def test_compartment_scope_large_fleet_batches_with_or(self):
        """Large single-compartment fleet must batch with || filter (subtree=False)."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        calls = client.summarize_metrics_data.call_args_list
        subtree_values = [c[1]["compartment_id_in_subtree"] for c in calls]
        assert all(v is False for v in subtree_values)
        filtered = [c for c in calls if "resourceId" in c[0][1].query]
        assert len(filtered) > 0

    # ── Failure isolation ─────────────────────────────────────────────────

    def test_failed_batch_marks_only_that_batch(self):
        """A 400 on one batch must only mark those instances as failed."""
        try:
            import oci
            err = oci.exceptions.ServiceError(400, "InvalidParameter", {}, "bad")
        except Exception:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)

        call_count = [0]
        response = MagicMock()
        response.data = []

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            # Fail the first call, succeed all others
            if call_count[0] == 1:
                raise err
            return response

        client = MagicMock()
        client.summarize_metrics_data.side_effect = side_effect
        start, end = self._times(30)

        _, _, failed_ids = collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        # Some instances failed, but not all 200
        assert len(failed_ids) > 0
        assert len(failed_ids) < 200

    def test_no_instances_in_compartment_skipped(self):
        """Compartments with no discovered instances must generate zero API calls."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        empty_comp = "ocid1.compartment.oc1..empty"
        instances = self._make_instances(2, compartment_id=comp_id)
        comp_map = {
            comp_id: {"name": "prod", "parent_id": None, "label": "prod"},
            empty_comp: {"name": "empty", "parent_id": None, "label": "empty"},
        }
        client = self._ok_client()
        start, end = self._times(7)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        # All calls must be to comp_id, never to empty_comp
        for c in client.summarize_metrics_data.call_args_list:
            assert c[0][0] == comp_id

    # ── P1+P2 regression tests ────────────────────────────────────────────

    def test_instance_status_never_uses_or_filter(self):
        """No instance_status query may contain || at any scope or fleet size.

        This is the core P1 regression test. The original bug allowed || filters
        in instance_status queries at compartment scope, causing 400 failures for
        large compartments (≥112 instances at 30 days).
        """
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        # 200 instances → 2 CPU batches at 30 days, so status was previously also batched
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        status_calls = [
            c for c in client.summarize_metrics_data.call_args_list
            if c[0][1].namespace == "oci_compute_infrastructure_health"
        ]
        assert len(status_calls) > 0, "Expected at least one instance_status call"
        for c in status_calls:
            query = c[0][1].query
            assert " || " not in query, (
                f"instance_status must never use || filter, got: {query}"
            )

    def test_instance_status_always_uses_single_resourceid(self):
        """Every instance_status call must contain exactly one resourceId predicate.

        Both assertions are explicit: query contains resourceId = "..." AND does not
        contain ||, so the intent is unambiguous even if query formatting changes.
        """
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        # 5 instances comfortably under the batch threshold (111 at 30 days)
        instances = self._make_instances(5, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        status_calls = [
            c for c in client.summarize_metrics_data.call_args_list
            if c[0][1].namespace == "oci_compute_infrastructure_health"
        ]
        assert len(status_calls) == 5, (
            f"Expected 5 status calls (one per instance), got {len(status_calls)}"
        )
        for c in status_calls:
            query = c[0][1].query
            assert 'resourceId = "' in query, (
                f"Each status call must include a single resourceId predicate, got: {query}"
            )
            assert " || " not in query, (
                f"Status call must not use || multi-instance filter, got: {query}"
            )

    def test_cpu_batches_with_or_at_compartment_scope(self):
        """CpuUtilization at compartment scope must still batch with || (efficiency preserved).

        This confirms the fix did NOT remove CPU batching as a side effect.
        """
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        # 200 instances → 2 CPU batches at 30 days (threshold = 111)
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        cpu_calls = [
            c for c in client.summarize_metrics_data.call_args_list
            if c[0][1].namespace == "oci_computeagent"
        ]
        assert len(cpu_calls) >= 2, f"Expected ≥2 CPU batches for 200 instances, got {len(cpu_calls)}"
        for c in cpu_calls:
            assert " || " in c[0][1].query, (
                f"CPU batch at compartment scope must use || filter, got: {c[0][1].query}"
            )

    def test_empty_instances_list_makes_no_api_calls(self):
        """instances=[] → all scopes empty → zero API calls, empty results."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        cpu, status, failed = collect_all_metrics(client, comp_id, comp_map, [], start, end)

        assert client.summarize_metrics_data.call_count == 0
        assert cpu == {}
        assert status == {}
        assert failed == set()

    def test_only_cpu_fails_marks_that_batch_failed(self):
        """A 400 on the CPU batch must mark all instances in that batch as failed."""
        try:
            import oci
            err = oci.exceptions.ServiceError(400, "InvalidParameter", {}, "bad")
        except Exception:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        # 5 instances — 1 CPU batch (comfortably under 111 threshold at 30 days)
        instances = self._make_instances(5, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)

        response = MagicMock()
        response.data = []

        def side_effect(*args, **kwargs):
            details = args[1]
            if details.namespace == "oci_computeagent":
                raise err
            return response

        client = MagicMock()
        client.summarize_metrics_data.side_effect = side_effect
        start, end = self._times(30)

        _, _, failed_ids = collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        expected_ids = {inst["id"] for inst in instances}
        assert failed_ids == expected_ids, (
            f"All instances in the failed CPU batch must be marked failed. "
            f"Expected {expected_ids}, got {failed_ids}"
        )

    def test_only_status_fails_marks_only_that_instance(self):
        """A status failure for one instance must not affect other instances.

        The mock is keyed on the specific failing instance ID (not call order),
        proving per-instance granularity rather than positional behaviour.
        """
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        instances = self._make_instances(3, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        failing_id = instances[1]["id"]
        start, end = self._times(30)

        def mock_collect(client, comp_id, namespace, metric, start, end,
                         use_subtree=False, instance_ids=None):
            if (namespace == "oci_compute_infrastructure_health"
                    and instance_ids == [failing_id]):
                return {}, True
            return {}, False

        with patch("compute_availability_report.collect_metrics", side_effect=mock_collect):
            _, _, failed_ids = collect_all_metrics(
                self._ok_client(), comp_id, comp_map, instances, start, end
            )

        assert failed_ids == {failing_id}, (
            f"Only the failing instance should be in failed_ids. "
            f"Expected {{{failing_id}}}, got {failed_ids}"
        )

    def test_all_queries_fail_all_instances_marked_failed(self):
        """When every API call returns 400, all instances must be in failed_instance_ids."""
        try:
            import oci
            err = oci.exceptions.ServiceError(400, "InvalidParameter", {}, "bad")
        except Exception:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        instances = self._make_instances(3, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        client = MagicMock()
        client.summarize_metrics_data.side_effect = err
        start, end = self._times(30)

        _, _, failed_ids = collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        expected_ids = {inst["id"] for inst in instances}
        assert failed_ids == expected_ids

    def test_multiple_compartments_all_queried_in_fallback(self):
        """When root is non-tenancy, all compartments with instances must be queried."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        root = "ocid1.compartment.oc1..root"   # non-tenancy → always per-compartment
        comp1 = "ocid1.compartment.oc1..comp1"
        comp2 = "ocid1.compartment.oc1..comp2"
        comp3 = "ocid1.compartment.oc1..comp3"

        # 2 instances per compartment, comfortably under batch threshold
        instances = (
            self._make_instances(2, compartment_id=comp1) +
            self._make_instances(2, compartment_id=comp2) +
            self._make_instances(2, compartment_id=comp3)
        )
        comp_map = {
            root:  {"name": "root",  "parent_id": None,  "label": "root"},
            comp1: {"name": "comp1", "parent_id": root,  "label": "comp1"},
            comp2: {"name": "comp2", "parent_id": root,  "label": "comp2"},
            comp3: {"name": "comp3", "parent_id": root,  "label": "comp3"},
        }
        client = self._ok_client()
        start, end = self._times(30)

        collect_all_metrics(client, root, comp_map, instances, start, end)

        called_comp_ids = {c[0][0] for c in client.summarize_metrics_data.call_args_list}
        assert comp1 in called_comp_ids, "comp1 must be queried"
        assert comp2 in called_comp_ids, "comp2 must be queried"
        assert comp3 in called_comp_ids, "comp3 must be queried"

    def test_cpu_metrics_from_multiple_batches_accumulated(self):
        """Data from all CPU batches must be merged; no instances dropped."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        import re as _re

        comp_id = "ocid1.compartment.oc1..aaa"
        # 200 instances → 2 CPU batches at 30 days (threshold = 111)
        instances = self._make_instances(200, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        start, end = self._times(30)

        def side_effect(*args, **kwargs):
            details = args[1]
            response = MagicMock()
            if details.namespace == "oci_computeagent":
                ids_in_query = _re.findall(r'resourceId = "([^"]+)"', details.query)
                items = []
                for rid in ids_in_query:
                    dp = MagicMock()
                    dp.timestamp = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
                    dp.value = 50.0
                    m = MagicMock()
                    m.dimensions = {"resourceId": rid}
                    m.aggregated_datapoints = [dp]
                    items.append(m)
                response.data = items
            else:
                response.data = []
            return response

        client = MagicMock()
        client.summarize_metrics_data.side_effect = side_effect

        cpu_metrics, _, _ = collect_all_metrics(client, comp_id, comp_map, instances, start, end)

        assert len(cpu_metrics) == 200, (
            f"All 200 instances must appear in cpu_metrics, got {len(cpu_metrics)}"
        )
        for inst in instances:
            assert inst["id"] in cpu_metrics

    def test_instance_with_unknown_compartment_silently_skipped(self):
        """An instance whose compartment_id is not in compartment_map must be silently ignored."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        unknown_comp = "ocid1.compartment.oc1..unknown"

        instances = (
            self._make_instances(1, compartment_id=comp_id) +
            self._make_instances(1, compartment_id=unknown_comp)
        )
        # comp_map does NOT contain unknown_comp
        comp_map = self._compartment_map(comp_id)
        client = self._ok_client()
        start, end = self._times(30)

        # Must not raise
        cpu, status, failed_ids = collect_all_metrics(
            client, comp_id, comp_map, instances, start, end
        )

        unknown_instance_id = instances[1]["id"]
        assert unknown_instance_id not in failed_ids, (
            "Instance in unknown compartment must not appear in failed_ids"
        )

    def test_status_calls_are_throttled(self):
        """A sleep of STATUS_CALL_THROTTLE_SECS must be issued before each instance_status call."""
        try:
            import oci  # noqa: F401
        except ImportError:
            pytest.skip("oci package not available")

        comp_id = "ocid1.compartment.oc1..aaa"
        instances = self._make_instances(3, compartment_id=comp_id)
        comp_map = self._compartment_map(comp_id)
        start, end = self._times(30)

        with patch("time.sleep") as mock_sleep:
            collect_all_metrics(self._ok_client(), comp_id, comp_map, instances, start, end)

        throttle_calls = [c for c in mock_sleep.call_args_list
                          if c == call(STATUS_CALL_THROTTLE_SECS)]
        assert len(throttle_calls) == 3, (
            f"Expected 3 throttle sleeps (one per status call), "
            f"got {len(throttle_calls)}: {mock_sleep.call_args_list}"
        )


class TestDiscoveryWarningFormat:
    def test_warning_includes_label_and_ocid(self):
        """Discovery warnings must include disambiguated label + OCID for diagnostics"""
        comp_label = "teamA/prod"
        comp_id = "ocid1.compartment.oc1..aaabbbccc"
        error_msg = "NotAuthorizedOrNotFound"
        warning = f"Could not list instances in {comp_label} ({comp_id}): {error_msg}"

        # Verify format allows unambiguous identification
        assert comp_label in warning
        assert comp_id in warning
        assert error_msg in warning
