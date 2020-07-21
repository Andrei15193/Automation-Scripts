const fs = require('fs');
const { getCommandLineOptions } = require('./common');
const { restoreTableStorageAsync } = require('./tableStorage-restore');

const commandLineOptions = getCommandLineOptions(process.argv.slice(2));
validateCommandLineOptions(commandLineOptions);

if (commandLineOptions.named.help)
    writeHelp();
else
    restoreTableStorageAsync(commandLineOptions.named.connectionString, fs.createReadStream(commandLineOptions.named.filePath))
        .catch((error) => console.error(error));

function writeHelp() {
    console.log('Restore Azure Table Storage to Azure Blob Storage');
    console.log('Usage: node ./tableStorage-restoreFromFile.js -connectionString <conStr> -filePath <gz file path>');
    console.log('  -connectionString:     the Azure Storage Account connection string');
    console.log('  -filePath:             the gz archive file path where the backup is stored');
}

function validateCommandLineOptions(commandLineOptions) {
    if (!commandLineOptions.named.connectionString)
        throw 'Expected -connectionString';
    if (!commandLineOptions.named.filePath)
        throw 'Expected -filePath';
}