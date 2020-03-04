# TODO:
# * Extract script into functions (similar to Backup-Tables.ps1)
# * Remove default value for connection string (it's there just to make testing easier)
# * Check if SAS can be used with a connection string

param (
    [Parameter(Mandatory=$true)][string]$folder,
    [string]$connectionString = "UseDevelopmentStorage=true"
)

$env:AZURE_STORAGE_CONNECTION_STRING = $connectionString
$migrationsTableName = "AzureStorageMigrations"
$changeSet = (Get-Item $folder).Name.ToLowerInvariant()

$output = az storage table create --name $migrationsTableName 2>&1
if ($LastExitCode -ne 0)
{
    Write-Error "$output.Exception.Message" -ErrorAction Stop
}

$nextPartitionKey = $null;
$nextRowKey = $null;
$executedScripts = @()
do
{
    $output = az storage entity query 2>&1 `
        --table-name $migrationsTableName `
        --marker "nextpartitionkey='$nextPartitionKey'" "nextrowkey='$nextRowKey'" `
        --query "{ executedScripts: items[?PartitionKey=='$changeSet'].RowKey, nextMarker: nextMarker }"
    if ($LastExitCode -ne 0)
    {
        Write-Error "$output.Exception.Message" -ErrorAction Stop
    }

    $result = ConvertFrom-Json "$output"
    $nextPartitionKey = $result.nextMarker.nextpartitionkey
    $nextRowKey = $result.nextMarker.nextrowkey
    $executedScripts += $result.executedScripts
}
while($null -ne $nextPartitionKey -and $null -ne $nextRowKey)

Write-Host "Running migration scripts for $changeSet"
Get-ChildItem -Path $folder -Filter "*.ps1" |
    ForEach-Object {
        $scriptName = $_.Name
        $scriptFullName = $_.FullName
        if ($executedScripts -contains $scriptName)
        {
            Write-Output "Skipping $scriptName, already executed"
        }
        else
        {
            Write-Output "Executing $scriptName"
            $startTime = (Get-Date).ToUniversalTime()
            
            $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
            $output = (& $scriptFullName 2>&1)
            $stopwatch.Stop()
            $duration = $stopwatch.Elapsed

            if ($LastExitCode -ne 0)
            {
                Write-Error "$output.Exception.Message" -ErrorAction Stop
            }
            else
            {
                Write-Output $output
                Write-Output "Duration: $duration"
                Write-Output ('-' * 70)
            }

            $output = az storage entity insert 2>&1 `
                --table-name $migrationsTableName `
                --if-exists fail `
                --entity (@{
                    PartitionKey = $changeSet
                    RowKey = $scriptName
                    FullName = $scriptFullName
                    StartTime = $startTime.ToString("o")
                    DurationSeconds = $duration.TotalSeconds
                }.GetEnumerator() | ForEach-Object { "$($_.Name)=$($_.Value)" })
            if ($LastExitCode -ne 0)
            {
                Write-Error "$output.Exception.Message" -ErrorAction Stop
            }
        }
    }