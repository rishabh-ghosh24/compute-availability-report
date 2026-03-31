resource "oci_identity_policy" "availability_reporter_policy" {
  name           = "availability-reporter-policy"
  description    = "Allow availability report generation"
  compartment_id = var.tenancy_ocid

  statements = [
    "Allow dynamic-group availability-reporter to read instances in compartment ${var.compartment_name}",
    "Allow dynamic-group availability-reporter to read metrics in compartment ${var.compartment_name}",
    "Allow dynamic-group availability-reporter to read compartments in tenancy",
    "Allow dynamic-group availability-reporter to manage objects in compartment ${var.compartment_name} where target.bucket.name='${var.bucket_name}'",
    "Allow dynamic-group availability-reporter to manage buckets in compartment ${var.compartment_name} where target.bucket.name='${var.bucket_name}'",
    "Allow dynamic-group availability-reporter to manage preauthenticated-requests in compartment ${var.compartment_name} where target.bucket.name='${var.bucket_name}'",
  ]
}
