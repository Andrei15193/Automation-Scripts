# Tests whether binary data is retrieved correctly

$connectionString = "UseDevelopmentStorage=true"
$tableName = "BinaryDataTestTable"

$binaryData = 0..[byte]::MaxValue # inserting 0..3 values works

# Ensure test table exists
az storage table create `
    --name $tableName `
    --connection-string $connectionString

# Ensure the test entity exists
# Make sure the property type is specified otherwise it is Edm.String
az storage entity insert `
    --table-name $tableName `
    --entity `
        "PartitionKey=partitionKey" `
        "RowKey=rowKey" `
        "BinaryProperty=$([Convert]::ToBase64String($binaryData))" `
        "BinaryProperty@odata.type=Edm.Binary" `
    --if-exists replace `
    --connection-string $connectionString

# Query the table, should work but it fails for binary data
az storage entity query `
    --table-name $tableName `
    --connection-string $connectionString
