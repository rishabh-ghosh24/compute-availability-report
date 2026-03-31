#!/bin/bash
# ──────────────────────────────────────────────────────────────
# OCI Compute Availability Report — OEL 9 Setup Script
# ──────────────────────────────────────────────────────────────
# Installs all dependencies on a fresh Oracle Enterprise Linux 9
# VM and clones the repository. Idempotent — safe to run again.
#
# Usage:
#   curl -sL <raw-url> | bash
#   # or
#   bash setup_oel9.sh
# ──────────────────────────────────────────────────────────────

set -euo pipefail

REPO_URL="https://github.com/rishabh-ghosh24/compute-availability-report.git"
BRANCH="sla-report"
INSTALL_DIR="$HOME/compute-availability-report"

echo "════════════════════════════════════════════════════"
echo "  OCI Compute Availability Report — Setup (OEL 9)"
echo "════════════════════════════════════════════════════"

# ── 1. System packages ────────────────────────────────────────
echo ""
echo "▶ Installing system packages..."
sudo dnf install -y git python3 python3-pip 2>/dev/null || {
    echo "  dnf failed, trying yum..."
    sudo yum install -y git python3 python3-pip
}

echo "  git:    $(git --version)"
echo "  python: $(python3 --version)"
echo "  pip:    $(pip3 --version)"

# ── 2. OCI Python SDK ────────────────────────────────────────
echo ""
echo "▶ Installing OCI Python SDK..."
pip3 install --user --quiet oci
echo "  oci SDK: $(pip3 show oci 2>/dev/null | grep Version)"

# ── 3. Clone / update repository ─────────────────────────────
echo ""
if [ -d "$INSTALL_DIR/.git" ]; then
    echo "▶ Repository already exists at $INSTALL_DIR, updating..."
    cd "$INSTALL_DIR"
    git fetch origin
    git checkout "$BRANCH"
    git pull origin "$BRANCH"
else
    echo "▶ Cloning repository..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    git checkout "$BRANCH"
fi

cd "$INSTALL_DIR"
echo "  Working directory: $(pwd)"

# ── 4. Verify OCI CLI / config ────────────────────────────────
echo ""
echo "▶ Checking OCI configuration..."
if [ -f "$HOME/.oci/config" ]; then
    echo "  OCI config found at ~/.oci/config"
    PROFILES=$(grep '^\[' "$HOME/.oci/config" | tr -d '[]')
    echo "  Profiles: $PROFILES"
else
    echo "  ⚠ No OCI config found at ~/.oci/config"
    echo "  If using --auth config, you need to set up OCI CLI first:"
    echo "    oci setup config"
    echo ""
    echo "  If using Instance Principals (default), ensure:"
    echo "    1. VM is in a dynamic group"
    echo "    2. IAM policies grant read access to instances, metrics, compartments"
    echo "    See: sla-report/iam/ for Terraform examples"
fi

# ── 5. Verify chart.min.js is present ─────────────────────────
echo ""
if [ -f "chart.min.js" ]; then
    echo "  Chart.js found ($(wc -c < chart.min.js) bytes)"
else
    echo "▶ Downloading Chart.js..."
    curl -sL "https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js" -o chart.min.js
    echo "  Downloaded chart.min.js ($(wc -c < chart.min.js) bytes)"
fi

# ── 6. Run tests ──────────────────────────────────────────────
echo ""
echo "▶ Running unit tests..."
if python3 -m pytest tests/ -v 2>/dev/null; then
    echo "  ✓ All tests passed"
else
    echo "  ⚠ pytest not installed, installing..."
    pip3 install --user --quiet pytest
    python3 -m pytest tests/ -v
fi

# ── 7. Print usage ────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
echo "  Setup complete!"
echo "════════════════════════════════════════════════════"
echo ""
echo "Generate your first report:"
echo ""
echo "  cd $INSTALL_DIR"
echo ""
echo "  # Using Instance Principals (on OCI VM with dynamic group):"
echo "  python3 compute_availability_report.py \\"
echo "    --compartment-id <YOUR_COMPARTMENT_OCID>"
echo ""
echo "  # Using OCI config file:"
echo "  python3 compute_availability_report.py \\"
echo "    --auth config --profile DEFAULT \\"
echo "    --compartment-id <YOUR_COMPARTMENT_OCID>"
echo ""
echo "  # 30-day tenancy-wide report with upload:"
echo "  python3 compute_availability_report.py \\"
echo "    --compartment-id <YOUR_TENANCY_OCID> \\"
echo "    --days 30 --upload"
echo ""
echo "See README.md for full CLI reference."
