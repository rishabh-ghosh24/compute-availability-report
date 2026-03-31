terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = ">= 5.0"
    }
  }
}

# Configure the OCI provider.
# Authentication defaults to ~/.oci/config with the DEFAULT profile.
# Override with environment variables or provider arguments as needed.
# See: https://registry.terraform.io/providers/oracle/oci/latest/docs
provider "oci" {
  tenancy_ocid = var.tenancy_ocid
}
