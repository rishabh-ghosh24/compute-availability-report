import logging
import pytest
import re
from compute_availability_report import generate_html_report, embed_logo


@pytest.fixture
def sample_report_data():
    instances = [
        {"id": "ocid1.instance.oc1.aaa1", "name": "web-server-1", "state": "RUNNING",
         "shape": "VM.Standard.E4.Flex", "compartment_name": "production",
         "compartment_id": "ocid1.comp.prod", "compartment_label": "production",
         "availability_pct": 100.0, "up_hours": 168, "down_hours": 0,
         "stopped_hours": 0, "nodata_hours": 0, "monitored_hours": 168,
         "downtime_minutes": 0, "data_complete": True},
        {"id": "ocid1.instance.oc1.aaa2", "name": "api-server-1", "state": "RUNNING",
         "shape": "VM.Standard.E4.Flex", "compartment_name": "production",
         "compartment_id": "ocid1.comp.prod", "compartment_label": "production",
         "availability_pct": 99.40, "up_hours": 167, "down_hours": 1,
         "stopped_hours": 0, "nodata_hours": 0, "monitored_hours": 168,
         "downtime_minutes": 60, "data_complete": True},
    ]
    fleet = {
        "discovered_instance_count": 2, "fleet_availability_pct": 99.70,
        "at_target_count": 1, "total_up_hours": 335, "total_monitored_hours": 336,
        "data_complete": True, "discovery_complete": True, "report_complete": True,
    }
    heatmap_data = {
        "ocid1.instance.oc1.aaa1": ["up"] * 168,
        "ocid1.instance.oc1.aaa2": ["up"] * 75 + ["down"] + ["up"] * 92,
    }
    all_hours = []
    from datetime import datetime, timezone, timedelta
    start = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
    for h in range(168):
        all_hours.append((start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    return {
        "instances": instances, "fleet": fleet, "heatmap_data": heatmap_data,
        "all_hours": all_hours, "compartment_name": "acme-corp",
        "region": "us-ashburn-1", "days": 7, "sla_target": 99.95,
        "start_date": "Mar 24, 2026", "end_date": "Mar 30, 2026",
    }


class TestHTMLReport:
    def test_generates_valid_html(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_contains_report_title(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "Compute Availability Report" in html

    def test_contains_fleet_availability(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "99.70%" in html or "99.7%" in html

    def test_contains_instance_names(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "web-server-1" in html
        assert "api-server-1" in html

    def test_contains_compartment_grouping(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "production" in html

    def test_contains_chart_js(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "cdn.jsdelivr.net" not in html
        assert "Chart" in html

    def test_contains_heatmap(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "hblk" in html

    def test_contains_print_styles(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "@media print" in html

    def test_self_contained(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert '<link rel="stylesheet"' not in html
        assert 'src="http' not in html

    def test_branding_title(self, sample_report_data):
        html = generate_html_report(**sample_report_data, title="ACME Corp")
        assert "ACME Corp" in html

    def test_branding_title_html_escaped(self, sample_report_data):
        html = generate_html_report(**sample_report_data, title="<ACME Corp>")
        assert "&lt;ACME Corp&gt;" in html
        assert "<ACME Corp>" not in html

    def test_branding_logo(self, sample_report_data):
        logo_data = "data:image/png;base64,iVBORw0KGgo="
        html = generate_html_report(**sample_report_data, logo_data=logo_data)
        assert logo_data in html

    def test_embed_logo_warns_when_file_missing(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = embed_logo("/nonexistent/logo.png")
        assert result is None
        assert "/nonexistent/logo.png" in caplog.text

    def test_footer_version(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "OCI Compute Availability Report v1.0" in html

    def test_no_warning_when_complete(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "data-warning" not in html

    def test_warning_banner_when_data_incomplete(self, sample_report_data):
        fleet = {**sample_report_data["fleet"], "data_complete": False, "report_complete": False}
        html = generate_html_report(**{**sample_report_data, "fleet": fleet})
        assert "data-warning" in html
        assert "Incomplete data" in html

    def test_warning_banner_when_discovery_incomplete(self, sample_report_data):
        html = generate_html_report(**sample_report_data,
            discovery_warnings=["Could not list instances in staging"])
        assert "data-warning" in html

    def test_cards_show_na_when_report_incomplete(self, sample_report_data):
        fleet = {**sample_report_data["fleet"],
            "fleet_availability_pct": None, "at_target_count": None,
            "total_up_hours": None, "total_monitored_hours": None,
            "report_complete": False, "data_complete": False, "discovery_complete": True}
        html = generate_html_report(**{**sample_report_data, "fleet": fleet})
        card_values = re.findall(r'class="metric-value"[^>]*>([^<]+)<', html)
        na_count = sum(1 for v in card_values if 'N/A' in v)
        assert na_count >= 3, f"Expected 3+ N/A card values, found {na_count}: {card_values}"
        assert any(str(fleet["discovered_instance_count"]) in v for v in card_values)

    def test_instances_card_shows_partial_scope(self, sample_report_data):
        fleet = {**sample_report_data["fleet"],
            "fleet_availability_pct": None, "at_target_count": None,
            "total_up_hours": None, "total_monitored_hours": None,
            "report_complete": False, "data_complete": True, "discovery_complete": False}
        html = generate_html_report(**{**sample_report_data, "fleet": fleet})
        instances_card = re.search(
            r'Instances monitored.*?</div>\s*<div[^>]*class="metric-value"[^>]*>(.*?)</div>',
            html, re.DOTALL)
        assert instances_card, "Instances monitored card not found"
        assert "partial scope" in instances_card.group(1).lower()

    def test_all_tables_have_consistent_column_widths(self, sample_report_data):
        """Every compartment table must have a <colgroup> with correct column widths."""
        instances = list(sample_report_data["instances"])
        instances.append({
            "id": "ocid1.instance.oc1.bbb1", "name": "db-server-1", "state": "RUNNING",
            "shape": "VM.Standard.E4.Flex", "compartment_name": "staging",
            "compartment_id": "ocid1.comp.staging", "compartment_label": "staging",
            "availability_pct": 99.80, "up_hours": 167, "down_hours": 1,
            "stopped_hours": 0, "nodata_hours": 0, "monitored_hours": 168,
            "downtime_minutes": 60, "data_complete": True,
        })
        data = {**sample_report_data, "instances": instances}
        data["heatmap_data"] = {
            **sample_report_data["heatmap_data"],
            "ocid1.instance.oc1.bbb1": ["up"] * 168,
        }
        html = generate_html_report(**data)
        table_count = html.count('<table>')
        colgroup_count = html.count('<colgroup>')
        assert table_count >= 2, f"Expected at least 2 tables (2 compartments), got {table_count}"
        assert colgroup_count == table_count, (
            f"Expected {table_count} <colgroup> tags (one per table), got {colgroup_count}"
        )
        expected_widths = ["28%", "14%", "14%", "30%", "14%"]
        colgroups = re.findall(r'<colgroup>(.*?)</colgroup>', html, re.DOTALL)
        for i, cg in enumerate(colgroups):
            widths = re.findall(r'width:\s*(\d+%)', cg)
            assert widths == expected_widths, (
                f"Table {i+1} colgroup has widths {widths}, expected {expected_widths}"
            )

    def test_heatmap_toggle_hidden_under_50(self, sample_report_data):
        html = generate_html_report(**sample_report_data)
        assert "show-all-toggle" not in html

    def test_heatmap_toggle_shown_over_50(self, sample_report_data):
        instances = []
        heatmap_data = {}
        for i in range(55):
            inst = {"id": f"ocid{i}", "name": f"vm-{i}", "state": "RUNNING",
                "shape": "VM.Standard.E4.Flex", "compartment_name": "prod",
                "compartment_id": "ocid1.comp.prod", "compartment_label": "prod",
                "availability_pct": 100.0, "up_hours": 168, "down_hours": 0,
                "stopped_hours": 0, "nodata_hours": 0, "monitored_hours": 168,
                "downtime_minutes": 0, "data_complete": True}
            instances.append(inst)
            heatmap_data[f"ocid{i}"] = ["up"] * 168
        data = {**sample_report_data, "instances": instances, "heatmap_data": heatmap_data}
        html = generate_html_report(**data)
        assert "show-all-toggle" in html
