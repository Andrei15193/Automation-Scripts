param(
    [Parameter(Mandatory = $true)][string]$resourceGroupName,
    [timespan]$expiration = [timespan]::FromHours(1)
)

(az storage account list,
    --resource-group $resourceGroupName,
    --query "[].{accountName: name, storageEndpoints: primaryEndpoints}" | ConvertFrom-Json) |
ForEach-Object {
    $accountKey = az storage account keys list,
        --account-name $PSItem.accountName,
        --resource-group $resourceGroupName,
        --query "[0:1].value" | ConvertFrom-Json

    $start = [DateTime]::UtcNow
    $expiry = $start.Add($expiration)
    $dateFormat = "yyyy-MM-ddTHH:mmK"
    $sasToken = [Uri]::UnescapeDataString((
        az storage account generate-sas,
            --start ($start.ToString($dateFormat)),
            --expiry ($expiry.ToString($dateFormat)),
            --permissions acdlpruw,
            --services bfqt,
            --resource-types sco,
            --account-key $accountKey,
            --account-name $PSItem.accountName,
            --https-only |
        ConvertFrom-Json
    ))

    $endpoints = $PSItem.storageEndpoints |
        Get-Member -MemberType Properties |
        ForEach-Object -Begin { $collection = $PSItem.storageEndpoints } -Process {
            @{
                name = $PSItem.name
                value = $collection.($PSItem.name)
            }
        } |
        Where-Object {
            $PSItem.value -ne $null
        } |
        Foreach-Object -Process {
            [char]::ToUpper($PSItem.name[0]) + $PSItem.name.Substring(1) + "Endpoint=" + $PSItem.value.TrimEnd('/')
        }

    ($endpoints + ("SharedAccessSignature=" + $sasToken)) -join ";"
}
