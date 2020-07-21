const azure = require('azure-storage');
const { getCommandLineOptions } = require("./common.js");
const { restoreTableStorageAsync } = require('./tableStorage-restore');

const commandLineOptions = getCommandLineOptions(process.argv.slice(2));
validateCommandLineOptions(commandLineOptions);

if (commandLineOptions.named.help)
    writeHelp();
else {
    const blobService = azure.createBlobService(commandLineOptions.named.blobConnectionString || commandLineOptions.named.connectionString);

    restoreTableStorageAsync(commandLineOptions.named.connectionString, blobService.createReadStream(commandLineOptions.named.blobContainer, commandLineOptions.named.blobName))
        .catch((error) => console.error(error));
}

function writeHelp() {
    console.log('Restore Azure Table Storage from Azure Blob Storage');
    console.log('Usage: node ./tableStorage-restoreFromBlob.js -connectionString <conStr> -blobContainer <container name> -blobName <gz blob name>');
    console.log('  -connectionString:     the Azure Storage Account connection string');
    console.log('  -blobConnectionString: the connection string for the storage account where the backup is stored');
    console.log('                         when not specified the -connectionString value is used');
    console.log('  -blobContainer:        the Azure Blob Container to restore from');
    console.log('  -blobName:             the gz archive blob name to restore from');
}

function validateCommandLineOptions(commandLineOptions) {
    if (!commandLineOptions.named.connectionString)
        throw 'Expected -connectionString';
    if (!commandLineOptions.named.blobContainer)
        throw 'Expected -blobContainer';
    if (!commandLineOptions.named.blobName)
        throw 'Expected -blobName';
}