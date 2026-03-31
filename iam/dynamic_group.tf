variable "tenancy_ocid" {}
variable "monitoring_vm_ocid" {}

resource "oci_identity_dynamic_group" "availability_reporter" {
  name           = "availability-reporter"
  description    = "VM that generates availability reports"
  compartment_id = var.tenancy_ocid
  matching_rule  = "instance.id = '${var.monitoring_vm_ocid}'"
}
