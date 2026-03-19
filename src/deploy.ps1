$subscriptionId = "36448a90-905c-4f48-b1b3-deb171f7c247"
$resourceGroup  = "rg-irmai-uat-us-1"
$functionApp    = "irmai-standardization-engine-v1"
$storageAccount = "irmaiuatstorage"
$container      = "function-releases"
$blobName       = "$functionApp.zip"

# Get storage connection string
$connStr = (az storage account show-connection-string --name $storageAccount --resource-group $resourceGroup --query connectionString -o tsv)
Write-Host "Got storage connection string."

# Get App Insights keys
$aiKey = (az monitor app-insights component show --app appi-irmai-standardization --resource-group $resourceGroup --query instrumentationKey -o tsv)
$aiConn = (az monitor app-insights component show --app appi-irmai-standardization --resource-group $resourceGroup --query connectionString -o tsv)
Write-Host "Got App Insights keys."

# Generate SAS URL for package
$expiry = (Get-Date).AddYears(1).ToString("yyyy-MM-ddTHH:mmZ")
$key = (az storage account keys list --account-name $storageAccount --resource-group $resourceGroup --query '[0].value' -o tsv)
$sas = (az storage blob generate-sas --account-name $storageAccount --container-name $container --name $blobName --permissions r --expiry $expiry --account-key $key -o tsv)
$packageUrl = "https://$storageAccount.blob.core.windows.net/$container/$blobName`?$sas"
Write-Host "Package URL generated."

# Get bearer token
$token = (az account get-access-token --query accessToken -o tsv)
$headers = @{
    'Authorization' = "Bearer $token"
    'Content-Type'  = 'application/json'
}

# Build full app settings object
$settings = [PSCustomObject]@{
    FUNCTIONS_WORKER_RUNTIME                  = "python"
    FUNCTIONS_EXTENSION_VERSION               = "~4"
    WEBSITE_RUN_FROM_PACKAGE                  = $packageUrl
    AzureWebJobsStorage                       = $connStr
    MyStorageConn                             = $connStr
    APPINSIGHTS_INSTRUMENTATIONKEY            = $aiKey
    APPLICATIONINSIGHTS_CONNECTION_STRING     = $aiConn
    WEBSITE_CONTENTAZUREFILECONNECTIONSTRING  = $connStr
    WEBSITE_CONTENTSHARE                      = "irmai-standardization-engine-v1"
}

# PUT settings via REST API
$body = (@{ properties = $settings } | ConvertTo-Json -Depth 10)
$putUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Web/sites/$functionApp/config/appsettings?api-version=2022-03-01"
Invoke-RestMethod -Uri $putUri -Method PUT -Headers $headers -Body $body | Out-Null
Write-Host "All app settings restored."

# Restart
az functionapp restart --name $functionApp --resource-group $resourceGroup
Write-Host "Function app restarted. Done."
