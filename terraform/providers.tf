terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-irmai-uat-us-1"
    storage_account_name = "irmaiuatstorage"
    container_name       = "tfstate"
    key                  = "irmai-standardization.tfstate"
  }
}

provider "azurerm" {
  features {}
}