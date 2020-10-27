# Connect to Azure
Connect-AzAccount
# List Azure Subscriptions
Get-AzSubscription

# Define Variables
$subscriptionId = "azure-prod-bsc"
$storageAccountRG = "bsc-tpcds-test"
$storageAccountName = "bsctpcds"
$storageContainerName = "tpcds-datasets"

# Select right Azure Subscription
Select-AzSubscription -SubscriptionId $SubscriptionId

# Get Storage Account Key
$storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $storageAccountRG -AccountName $storageAccountName).Value[0]

# Set AzStorageContext
$destinationContext = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey

# Generate SAS URI
# Use -Permission rwdl for full permissions
$containerSASURI = New-AzStorageContainerSASToken -Context $destinationContext -ExpiryTime(get-date).AddSeconds(3600) -FullUri -Name $storageContainerName -Permission rwdl

Write-Output $containerSASURI


