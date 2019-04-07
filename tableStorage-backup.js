const { PassThrough } = require('stream');
const azure = require('azure-storage');
const archiver = require('archiver');

module.exports = {
    backupTablesStorageAsync
};

function backupTablesStorageAsync(connectionString, outputStream) {
    const tableService = azure.createTableService(connectionString);

    const archive = archiver(
        'zip',
        {
            zlib:
            {
                level: 9
            }
        }
    );
    archive.on(
        'error',
        function (error) {
            throw error;
        }
    );
    archive.pipe(outputStream);

    return writeTablesAsync(archive, tableService)
        .then(
            function () {
                archive.finalize();
            }
        );
}

function writeTablesAsync(archive, tableService) {
    return new Promise(
        function promiseCallback(resolve, reject, continuationToken) {
            tableService.listTablesSegmented(
                continuationToken,
                function (error, result) {
                    if (error)
                        reject(error);
                    else
                        result
                            .entries
                            .reduce(
                                function (promise, tableName) {
                                    return promise.then(
                                        function () {
                                            const outputStream = new PassThrough();
                                            archive.append(outputStream, { name: `${tableName}.json` });

                                            return writeEntitiesAsync(outputStream, tableService, tableName)
                                                .then(function () {
                                                    outputStream.end();
                                                });
                                        }
                                    );
                                },
                                Promise.resolve()
                            )
                            .then(
                                function () {
                                    if (result.continuationToken)
                                        promiseCallback(resolve, reject, result.continuationToken);
                                    else
                                        resolve();
                                }
                            );
                }
            );
        }
    );
}

function writeEntitiesAsync(outputStream, tableService, tableName) {
    console.log(`Backing up ${tableName} table`);

    const query = new azure.TableQuery();
    let separator = '';
    let totalEntities = 0;

    outputStream.write('[');
    return new Promise(
        function promiseCallback(resolve, reject, continuationToken) {
            tableService.queryEntities(
                tableName,
                query,
                continuationToken,
                function (error, result) {
                    if (error)
                        reject(error);
                    else {
                        totalEntities += result.entries.length;
                        result.entries.forEach(function (entity) {
                            outputStream.write(separator);
                            separator = ',';
                            outputStream.write(JSON.stringify(toBackupEntity(entity)));
                        });

                        if (result.continuationToken)
                            promiseCallback(resolve, reject, result.continuationToken);
                        else {
                            outputStream.write(']');
                            console.log(`Backup complete, ${totalEntities} total entities`);
                            resolve();
                        }
                    }
                }
            )
        }
    );
}

function toBackupEntity(entity) {
    return Object
        .getOwnPropertyNames(entity)
        .filter(
            function (property) {
                return property !== '.metadata' && property !== 'Timestamp'
            }
        )
        .reduce(
            function (result, property) {
                return Object.assign(
                    {
                        [property]: toBackupEntityProperty(entity[property])
                    },
                    result
                );
            },
            {}
        );
}

function toBackupEntityProperty(entityProperty) {
    if (entityProperty.$)
        if (entityProperty.$ === 'Edm.Binary')
            return {
                type: entityProperty.$,
                value: entityProperty._.toString('base64')
            };
        else
            return {
                type: entityProperty.$,
                value: entityProperty._
            };
    else if (typeof (entityProperty._) === 'number')
        return {
            type: 'Edm.Int32',
            value: entityProperty._
        };
    else if (typeof (entityProperty._) === 'boolean')
        return {
            type: 'Edm.Boolean',
            value: entityProperty._
        };
    else
        return {
            type: 'Edm.String',
            value: entityProperty._
        };
}