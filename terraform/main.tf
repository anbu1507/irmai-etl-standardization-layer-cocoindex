# 1. Reference the existing Resource Group
data "azurerm_resource_group" "existing" {
  name = var.resource_group_name
}

# 2. Reference the existing Storage Account
data "azurerm_storage_account" "existing" {
  name                = var.storage_account_name
  resource_group_name = data.azurerm_resource_group.existing.name
}

# 3. Create Linux B1 Service Plan (B1 Linux is allowed in this resource group)
resource "azurerm_service_plan" "plan" {
  name                = "sp-irmai-standardization-linux"
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location
  os_type             = "Linux"
  sku_name            = "B1"
}

# 4. Create Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "workspace" {
  name                = "log-irmai-standardization"
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# 5. Create Application Insights
resource "azurerm_application_insights" "insights" {
  name                = "appi-irmai-standardization"
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location
  workspace_id        = azurerm_log_analytics_workspace.workspace.id
  application_type    = "web"
}

# 6. Create the Function App
resource "azurerm_linux_function_app" "standardization_engine" {
  name                = var.function_app_name
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location

  storage_account_name       = data.azurerm_storage_account.existing.name
  storage_account_access_key = data.azurerm_storage_account.existing.primary_access_key
  service_plan_id            = azurerm_service_plan.plan.id

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
    application_insights_key               = azurerm_application_insights.insights.instrumentation_key
    application_insights_connection_string = azurerm_application_insights.insights.connection_string
  }

  app_settings = {
    "WEBSITE_RUN_FROM_PACKAGE"              = "1"
    "FUNCTIONS_WORKER_RUNTIME"              = "python"
    "AzureWebJobsStorage"                   = data.azurerm_storage_account.existing.primary_connection_string
    "MyStorageConn"                         = data.azurerm_storage_account.existing.primary_connection_string
    "APPINSIGHTS_INSTRUMENTATIONKEY"        = azurerm_application_insights.insights.instrumentation_key
    "APPLICATIONINSIGHTS_CONNECTION_STRING" = azurerm_application_insights.insights.connection_string
  }
}

# 7. Output the Managed Identity principal ID
output "function_app_identity_principal_id" {
  value = azurerm_linux_function_app.standardization_engine.identity[0].principal_id
}
