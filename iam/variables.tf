variable "tenancy_ocid" {
  description = "OCID of the OCI tenancy"
  type        = string
}

variable "monitoring_vm_ocid" {
  description = "OCID of the VM that will run the availability report generator"
  type        = string
}

variable "compartment_name" {
  description = "Name of the compartment to grant access to"
  type        = string
}

variable "bucket_name" {
  description = "Object Storage bucket name for report uploads"
  type        = string
  default     = "availability-reports"
}
