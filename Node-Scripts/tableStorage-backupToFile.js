const fs = require('fs');
const { getCommandLineOptions } = require('./common');
const { backupTableStorageAsync } = require('./tableStorage-backup');

const commandLineOptions = getCommandLineOptions(process.argv.slice(2));
validateCommandLineOptions(commandLineOptions);

if (commandLineOptions.named.help)
    writeHelp();
else {
    backupTableStorageAsync(
        commandLineOptions.named.connectionString,
        fs.createWriteStream(
            commandLineOptions.named.filePath,
            {
                flags: (commandLineOptions.named.overwrite ? 'w' : 'wx')
            }
        )
    )
        .catch(
            function (error) {
                console.error(error);
                return new Promise(function (resolve, reject) {
                    fs.unlink(
                        commandLineOptions.named.filePath,
                        function (error) {
                            if (error)
                                reject(error);
                            else
                                resolve();
                        }
                    );
                });
            }
        )
        .catch(console.error);
}

function writeHelp() {
    console.log('Backup Azure Table Storage to Azure Blob Storage');
    console.log('Usage: node ./tableStorage-backupToFile.js -connectionString <conStr> -filePath <gz file path>');
    console.log('  -connectionString:     the Azure Storage Account connection string');
    console.log('  -filePath:             the gz archive file path to backup the tables to');
    console.log('  -overwrite:            overwrites the output file if exists');
}

function validateCommandLineOptions(commandLineOptions) {
    if (!commandLineOptions.named.connectionString)
        throw 'Expected -connectionString';
    if (!commandLineOptions.named.filePath)
        throw 'Expected -filePath';
}