$subscriptionId = "36448a90-905c-4f48-b1b3-deb171f7c247"
$resourceGroup  = "rg-irmai-uat-us-1"
$functionApp    = "irmai-standardization-engine-v1"

$token = (az account get-access-token --query accessToken -o tsv)
$headers = @{ 'Authorization' = "Bearer $token"; 'Content-Type' = 'application/json' }

# Get current settings
$getUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Web/sites/$functionApp/config/appsettings/list?api-version=2022-03-01"
$current = Invoke-RestMethod -Uri $getUri -Method POST -Headers $headers

# Remove only the 3 conflicting keys
$props = $current.properties
$props.PSObject.Properties.Remove('WEBSITE_RUN_FROM_PACKAGE')
$props.PSObject.Properties.Remove('WEBSITE_CONTENTAZUREFILECONNECTIONSTRING')
$props.PSObject.Properties.Remove('WEBSITE_CONTENTSHARE')

# PUT back all other settings intact
$putUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Web/sites/$functionApp/config/appsettings?api-version=2022-03-01"
$body = (@{ properties = $props } | ConvertTo-Json -Depth 10)
Invoke-RestMethod -Uri $putUri -Method PUT -Headers $headers -Body $body | Out-Null
Write-Host "Done. Removed WEBSITE_RUN_FROM_PACKAGE, WEBSITE_CONTENTAZUREFILECONNECTIONSTRING, WEBSITE_CONTENTSHARE."
