# Tests whether the specified OData type is returned for all properties or at least the necessary ones

$connectionString = "UseDevelopmentStorage=true"
$tableName = "MissingPropertyMetadataTestTable"

# Insert data through REST API, the odata.type is ignored for numeric values
# --------------------------------------------------------------------------

# Ensure test table exists
# az storage table create,
#     --name $tableName,
#     --connection-string $connectionString

# Ensure the test entity exists
# az storage entity insert,
#     --table-name $tableName,
#     --entity,
#         "PartitionKey=partitionKey",
#         "RowKey=rowKey",
#         "BinaryProperty=$([Convert]::ToBase64String(@(1, 2, 3)))",
#         "BinaryProperty@odata.type=Edm.Binary",
#         "Boolean=true",
#         "Boolean@odata.type=Edm.Boolean",
#         "DateTime@odata.type=Edm.DateTime",
#         "DateTime=2018-12-10T20:33:53.4475462Z",
#         "Double@odata.type=Edm.Double",
#         "Double=1.2",
#         "Guid@odata.type=Edm.Guid",
#         "Guid=41ddfac7-8f88-4c6c-bc91-a0aa2f036b05",
#         "Int32@odata.type=Edm.Int32",
#         "Int32=3",
#         "Int64@odata.type=Edm.Int64",
#         "Int64=4",
#         "String=string",
#     --if-exists replace,
#     --connection-string $connectionString

# --------------------------------------------------------------------------

# Query the table, should work but it fails for binary data
az storage entity query,
    --accept minimal, # returns the same information as full option
    --table-name $tableName,
    --connection-string $connectionString