# --- DATA SOURCES ---
# 1. Reference the existing Resource Group
data "azurerm_resource_group" "existing" {
  name = var.resource_group_name
}

# 2. Reference the existing Storage Account
data "azurerm_storage_account" "existing" {
  name                = var.storage_account_name
  resource_group_name = data.azurerm_resource_group.existing.name
}

# --- RESOURCES ---

# 3. Create the Service Plan (Renamed to v2 to avoid 'Already Exists' error)
resource "azurerm_service_plan" "plan" {
  name                = "sp-irmai-standardization-v2" 
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location
  os_type             = "Linux"
  sku_name            = "B1" # Basic SKU to bypass regional dynamic limits
}

# 4. Create the Function App (The Engine)
resource "azurerm_linux_function_app" "standardization_engine" {
  name                = var.function_app_name
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location

  storage_account_name       = data.azurerm_storage_account.existing.name
  storage_account_access_key = data.azurerm_storage_account.existing.primary_access_key
  service_plan_id            = azurerm_service_plan.plan.id

  # Enable Managed Identity for professional security
  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.9"
    }
  }

  app_settings = {
    # --- 403 FORBIDDEN FIXES (Bypasses Azure File Share creation) ---
    "WEBSITE_RUN_FROM_PACKAGE"                  = "1"
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = "" 
    "WEBSITE_CONTENTSHARE"                      = ""
    
    # --- APP CONFIG ---
    "FUNCTIONS_WORKER_RUNTIME" = "python"
    "MyStorageConn"            = data.azurerm_storage_account.existing.primary_connection_string
    "AzureWebJobsStorage"      = data.azurerm_storage_account.existing.primary_connection_string
  }
}

# 5. Output the Managed Identity ID (Principal ID)
output "function_app_identity_principal_id" {
  value = azurerm_linux_function_app.standardization_engine.identity[0].principal_id
}