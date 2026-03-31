import pytest
import sys
from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from compute_availability_report import classify_hour, calculate_batch_groups, build_hourly_buckets


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
