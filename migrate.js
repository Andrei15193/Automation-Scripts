const path = require("path");
const fs = require("fs");
const azure = require('azure-storage');

const EntityGenerator = azure.TableUtilities.entityGenerator;
const migrationsTableName = "AzureStorageMigrations";

function TableStorage() {
    const sdkTableService = azure.createTableService(connectionString);
    this.getTable = function (tableName) {
        return new Table(sdkTableService, tableName);
    };
}

function Table(sdkTable, tableName) {
    this.insertAsync = function (entity) {
        return new Promise(
            (resolve, reject) => {
                sdkTable.insertEntity(tableName, entity, function (error, result) {
                    if (error)
                        reject(error);
                    else
                        resolve(result);
                });
            }
        );
    };

    this.createIfNotExistsAsync = function () {
        return new Promise(
            (resolve, reject) => {
                sdkTable.createTableIfNotExists(
                    tableName,
                    (error, result) => {
                        if (error)
                            reject(error);
                        else
                            resolve(result.created);
                    }
                );
            }
        );
    }

    this.queryAllAsync = function (query) {
        return new Promise(
            (resolve, reject) => {
                function queryItems(items, nextContinuationToken) {
                    sdkTable.queryEntities(
                        tableName,
                        query,
                        nextContinuationToken,
                        (error, results) => {
                            if (error)
                                reject(error);
                            else if (results.continuationToken)
                                queryItems(items.concat(results.entries), results.continuationToken);
                            else
                                resolve(items.concat(results.entries));
                        }
                    );
                }
                queryItems([], null);
            }
        );
    }
}

function migrate(folder, connectionString) {
    global.connectionString = connectionString || process.env.AZURE_STORAGE_CONNECTION_STRING;

    const updateScripts = fs
        .readdirSync(path.join(__dirname, folder))
        .filter(fileName => path.extname(fileName) == ".js")
        .map(fileName => path.basename(fileName))
        .sort();

    const migrationsTable = new TableStorage().getTable(migrationsTableName);

    const query = new azure.TableQuery().where('PartitionKey eq ?', folder);
    console.log("Running update scripts from", folder);
    migrationsTable
        .createIfNotExistsAsync()
        .then(() => migrationsTable.queryAllAsync(query))
        .then(
            executedScripts => updateScripts.reduce(
                (previousPromise, updateScript) => previousPromise.then(
                    () => {
                        const executedScript = executedScripts.find((executedScript) => executedScript.RowKey._ === updateScript.toLocaleLowerCase());
                        if (executedScript)
                            console.log("SKIP " + updateScript + " (already executed on", executedScript.startTime._, ")");
                        else {
                            console.log("-".repeat(70));
                            const startTime = new Date();
                            console.log("EXECUTE", updateScript, "at", startTime);
                            const startMoment = process.hrtime();
                            return Promise
                                .resolve()
                                .then(() => require(path.join(__dirname, folder, updateScript)))
                                .then(() => {
                                    const durationInSeconds = process.hrtime(startMoment).join(".");
                                    console.log("EXECUTED", updateScript, ", took", durationInSeconds, "seconds");
                                    return migrationsTable.insertAsync({
                                        PartitionKey: EntityGenerator.String(folder),
                                        RowKey: EntityGenerator.String(updateScript.toLocaleLowerCase()),
                                        startTime: EntityGenerator.DateTime(startTime),
                                        durationInSeconds: EntityGenerator.String(durationInSeconds)
                                    });
                                })
                                .catch(error => {
                                    const durationInSeconds = process.hrtime(startMoment).join(".");
                                    console.error("FAILED", updateScript, ", ran for", durationInSeconds, "seconds");
                                    throw error;
                                });
                        }
                    }
                ),
                Promise.resolve()
            )
        )
        .catch(error => console.error(error));
}

module.exports = {
    writeHelp: function () {
        console.log("npm run migrate <folder> <connection_string>");
    },
    execute: function (args) {
        migrate.apply(this, args);
    }
};