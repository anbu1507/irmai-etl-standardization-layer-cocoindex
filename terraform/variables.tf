variable "resource_group_name" {
  description = "The name of the existing resource group"
  default     = "rg-irmai-uat-us-1"
}

variable "location" {
  description = "The Azure region"
  default     = "Central US"
}

variable "storage_account_name" {
  description = "The name of the existing storage account"
  default     = "irmaiuatstorage"
}

variable "function_app_name" {
  description = "The name of the function app engine"
  default     = "irmai-standardization-engine-v1"
}