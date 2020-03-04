# TODO:
# * Add backup to blob storage (similar to saving to local file system, instead each file is uploaded as a blob)
# * Remove default value for connection string (it's there just to make testing easier)
# * Check if SAS can be used with a connection string

param (
    [string]$connectionString = "UseDevelopmentStorage=true",
    [string]$backupsConnectionStrings = $null,
    [string]$container = "backups",
    [string]$blobsPrefix = ""
)

function Save-TablesToBlobStorage (
    [Parameter(Mandatory=$true)][string]$sourceConnectionString,
    [Parameter(Mandatory=$true)][string]$destinationConnectionString,
    [Parameter(Mandatory=$true)][string]$container,
    [Parameter(Mandatory=$true)][string]$blobsPrefix)
{

}

function ContinueOnSuccess([Parameter(Mandatory=$true)][scriptblock]$scriptBlock)
{
    $output = (& $scriptBlock 2>&1)
    if ($LASTEXITCODE -ne 0)
    {
        Write-Error "$output.Exception.Message" -ErrorAction Stop
    }
    $result = $output | Out-String | ConvertFrom-Json
    if ($result -is [array])
    {
        return $result.GetEnumerator() | ForEach-Object { $_ }
    }
    return $result;
}

function Save-Table(
    [Parameter(Mandatory=$true)][string]$connectionString,
    [Parameter(Mandatory=$true)][string]$table,
    [Parameter(Mandatory=$true)][string]$file)
{
    $nextPartitionKey = $null
    $nextRowKey = $null
    $items = @()

    New-Item -ItemType Directory -Force -Path (Split-Path $file -Parent) | Out-Null
    do
    {
        $result = ContinueOnSuccess {
            az storage entity query `
                --accept full `
                --connection-string $connectionString `
                --table-name $table `
                --marker "nextpartitionkey='$nextPartitionKey'" "nextrowkey='$nextRowKey'"
            }
        $nextPartitionKey = $result.marker.nextPartitionKey
        $nextRowKey = $result.marker.nextRowKey
        $items += $result.items | ForEach-Object { $_.PSObject.Properties.Remove('etag'); $_.PSObject.Properties.Remove('Timestamp'); $_ }
    } while($null -ne $nextPartitionKey -and $null -ne $nextRowKey)

    ConvertTo-Json $items | Out-File $file -Force
}

function Save-Tables (
    [Parameter(Mandatory=$true)][string]$connectionString,
    [Parameter(Mandatory=$true)][string]$folder,
    [switch]$writeStatus = $false)
{
    $count = 1
    $tables = ContinueOnSuccess { az storage table list --connection-string $connectionString --query "[].name" }
    foreach ($table in $tables)
    {
        $destinationPath = (Join-Path $folder "$table.json")
        if ($writeStatus)
        {
            Write-Output "Saving table $table ($count of $($tables.Count)) to $destinationPath"
            $count++
        }
        Save-Table $connectionString $table $destinationPath
    }
}

function Clear-Table(
    [Parameter(Mandatory=$true)][string]$connectionString,
    [Parameter(Mandatory=$true)][string]$table)
{
    if ((ContinueOnSuccess {az storage table exists --connection-string $connectionString --name $table }).Exists)
    {
        do
        {
            $result = ContinueOnSuccess {
                az storage entity query `
                    --connection-string $connectionString `
                    --table-name $table `
                    --query "items[].{ PartitionKey: PartitionKey, RowKey: RowKey }" `
                    --select "PartitionKey" "RowKey"
                }
            $result |
                ForEach-Object {
                    ContinueOnSuccess {
                        az storage entity delete `
                            --connection-string $connectionString `
                            --table-name $table `
                            --partition-key $_.PartitionKey `
                            --row-key $_.RowKey
                        } | Out-Null
                }
        } while($result.Count -gt 0)
    }
}

function Restore-Table(
    [Parameter(Mandatory=$true)][string]$connectionString,
    [Parameter(Mandatory=$true)][string]$table,
    [Parameter(Mandatory=$true)][string]$file,
    [switch]$writeStatus = $false)
{
    if ($writeStatus)
    {
        Write-Output "Clearing data from $table"
    }
    Clear-Table $connectionString $table

    if ($writeStatus)
    {
        Write-Output "Restoring data into $table"
    }
    ContinueOnSuccess { az storage table create --connection-string $connectionString --name $table } | Out-Null
    $items = (Get-Content $file | ConvertFrom-Json)
    foreach ($item in $items)
    {
        $entity = @()
        foreach ($property in $item.PSObject.Properties)
        {
            $name = $property.Name
            $propertyValue = $item.($name)

            switch ($propertyValue.type) {
                {$_ -ieq "Edm.Binary"}
                {
                    $binaryValue = [Convert]::ToBase64String(($propertyValue.value.GetEnumerator() | Foreach-Object { [byte]$_ }))
                    $entity += "$name=$binaryValue"
                    $entity += "$name@odata.type=$($propertyValue.type)"
                    break
                }
                {$_ -ne $null}
                {
                    $entity += "$name=$($propertyValue.value)"
                    $entity += "$name@odata.type=$($propertyValue.type)"
                    break
                }
                Default
                {
                    $entity += "$name=$propertyValue"
                    break
                }
            }
        }

        ContinueOnSuccess { az storage entity insert --connection-string $connectionString --table-name $table --entity $entity --if-exists fail } | Out-Null
    }
}

function Restore-Tables (
        [Parameter(Mandatory=$true)][string]$connectionString,
        [Parameter(Mandatory=$true)][string]$folder,
        [string]$filter = "*.json",
        [switch]$writeStatus = $false)
{
    $count = 1
    $files = Get-ChildItem $folder $filter
    foreach ($file in $files)
    {
        $tableName = $file.Name.Substring(0, ($file.Name.Length - $file.Extension.Length));
        if ($writeStatus)
        {
            Write-Output "Restoring table $tableName ($count of $($files.Count)) from $($file.Name)"
            $count++
        }
        Restore-Table $connectionString $tableName $file.FullName -writeStatus:$writeStatus
    }
}

if ($null -eq $backupsConnectionStrings)
{
    $backupsConnectionStrings = $connectionString
}

#BackupTablesToBlobStorage $connectionString $backupsConnectionStrings $container $blobsPrefix

#Save-Tables $connectionString "test" -writeStatus

Restore-Table $connectionString "testTable" ".\test\testTable.json" -writeStatus

#Restore-Tables $connectionString "test" -writeStatus