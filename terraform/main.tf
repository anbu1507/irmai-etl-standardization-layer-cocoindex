provider "azurerm" {
  features {}
  subscription_id = "36448a90-905c-4f48-b1b3-deb171f7c247"
}

data "azurerm_storage_account" "storage" {
  name                = "irmaiuatstorage"
  resource_group_name = "rg-irmai-uat-us-1"
}

data "azurerm_service_plan" "standardization_plan" {
  name                = "standardization-service-plan"
  resource_group_name = "rg-irmai-uat-us-1"
}

resource "azurerm_linux_function_app" "standardization_func" {
  # WE ARE KEEPING THE NAME VIGNESH APPROVED
  name                = "standardization-engine-anbu-final" 
  resource_group_name = "rg-irmai-uat-us-1"
  location            = "centralus"
  
  storage_account_name       = data.azurerm_storage_account.storage.name
  storage_account_access_key = data.azurerm_storage_account.storage.primary_access_key
  service_plan_id            = data.azurerm_service_plan.standardization_plan.id

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  app_settings = {
    "ENABLE_ORYX_BUILD"              = "true"
    "SCM_DO_BUILD_DURING_DEPLOYMENT" = "true"
    "AzureWebJobsStorage"            = data.azurerm_storage_account.storage.primary_connection_string
    
    # Matches the connection name in your Python code
    "MyStorageConn"                  = data.azurerm_storage_account.storage.primary_connection_string
    
    # --- BYPASSES 403 FORBIDDEN ---
    # This prevents Azure from trying to create a new File Share
    "WEBSITE_RUN_FROM_PACKAGE"       = "1"
  }
}