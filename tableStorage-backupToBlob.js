const azure = require('azure-storage');
const { getCommandLineOptions } = require("./common.js");
const { backupTablesStorageAsync } = require('./tableStorage-backup');

const commandLineOptions = getCommandLineOptions(process.argv.slice(2));
validateCommandLineOptions(commandLineOptions);

if (commandLineOptions.named.help)
    writeHelp();
else
    backupTableStorageAsync(commandLineOptions);

function writeHelp() {
    console.log('Backup Azure Table Storage to Azure Blob Storage');
    console.log('Usage: node ./tableStorage-backupToBlob.js -connectionString <conStr> -blobContainer <container name> -blobName <zip name>');
    console.log('  -connectionString:     the Azure Storage Account connection string');
    console.log('  -blobConnectionString: the connection string for the storage account where to store the backup');
    console.log('                         when not specified the -connectionString value is used');
    console.log('  -blobContainer:        the Azure Blob Container to backup to');
    console.log('  -blobName:             the zip file blob name of the backup');
    console.log('  -overwrite:            overwrites the resulting zip blob if exists');
}

function validateCommandLineOptions(commandLineOptions) {
    if (!commandLineOptions.named.connectionString)
        throw 'Expected -connectionString';
    if (!commandLineOptions.named.blobContainer)
        throw 'Expected -blobContainer';
    if (!commandLineOptions.named.blobName)
        throw 'Expected -blobName';
}

function backupTableStorageAsync(commandLineOptions) {
    const blobService = azure.createBlobService(commandLineOptions.named.blobConnectionString || commandLineOptions.named.connectionString);

    ensureContainerExistsAsync(blobService, commandLineOptions.named.blobContainer)
        .then(
            function () {
                return doesBlobExistAsync(blobService, commandLineOptions.named.blobContainer, commandLineOptions.named.blobName)
            }
        )
        .then(
            function (blobExists) {
                if (!commandLineOptions.named.overwrite && blobExists)
                    throw `Blob ${commandLineOptions.named.blobName} already exists in ${commandLineOptions.named.blobContainer}`;
            }
        )
        .then(
            function () {
                return backupTablesStorageAsync(
                    commandLineOptions.named.connectionString,
                    blobService.createWriteStreamToBlockBlob(commandLineOptions.named.blobContainer, commandLineOptions.named.blobName)
                )
                    .catch(
                        function (error) {
                            console.error(error);
                            return deleteBlobAsync(blobService, commandLineOptions.named.blobContainer, commandLineOptions.named.blobName);
                        }
                    );
            }
        )
        .catch(console.error);
}

function ensureContainerExistsAsync(blobService, containerName) {
    return new Promise(
        (resolve, reject) => {
            blobService
                .createContainerIfNotExists(
                    containerName,
                    function (error) {
                        if (error)
                            reject(error);
                        else
                            resolve();
                    }
                );
        }
    );
}

function doesBlobExistAsync(blobService, containerName, blobName) {
    return new Promise(
        function (resolve, reject) {
            blobService.doesBlobExist(
                containerName,
                blobName,
                function (error, result) {
                    if (error)
                        reject(error);
                    else
                        resolve(result.exists);
                }
            );
        }
    );
}

function deleteBlobAsync(blobService, containerName, blobName) {
    return new Promise(
        function (resolve, reject) {
            blobService.deleteBlob(
                containerName,
                blobName,
                function (error) {
                    if (error)
                        reject(error);
                    else
                        resolve();
                }
            );
        }
    )
}