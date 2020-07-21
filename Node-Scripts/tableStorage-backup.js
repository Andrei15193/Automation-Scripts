const { PassThrough } = require('stream');
const { createGzip } = require('zlib');
const azure = require('azure-storage');

module.exports = {
    backupTableStorageAsync
};

function backupTableStorageAsync(connectionString, outputStream) {
    const _tableService = azure.createTableService(connectionString);
    const _gzip = createGzip({ level: 9 });
    const _writer = new PassThrough();
    let separator = '';

    _gzip.on('error', function (error) {
        throw error;
    });

    _writer
        .pipe(_gzip)
        .pipe(outputStream);

    _writer.write('[');
    return _writeTablesAsync()
        .then(
            () => {
                _writer.write(']');
                _writer.end();
            }
        );

    function _writeTablesAsync() {
        return new Promise(
            function promiseCallback(resolve, reject, continuationToken) {
                _tableService.listTablesSegmented(
                    continuationToken,
                    (error, result) => {
                        if (error)
                            reject(error);
                        else
                            result
                                .entries
                                .reduce(
                                    (promise, tableName) => promise.then(() => _writeEntitiesAsync(tableName)),
                                    Promise.resolve()
                                )
                                .then(
                                    () => {
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

    function _writeEntitiesAsync(tableName) {
        console.log(`Backing up ${tableName} table`);

        const query = new azure.TableQuery();
        let totalEntities = 0;

        return new Promise(
            function promiseCallback(resolve, reject, continuationToken) {
                _tableService.queryEntities(
                    tableName,
                    query,
                    continuationToken,
                    (error, result) => {
                        if (error)
                            reject(error);
                        else {
                            totalEntities += result.entries.length;
                            result.entries.forEach(
                                entity => {
                                    _writer.write(separator);
                                    separator = ',';
                                    _writer.write(
                                        JSON.stringify(
                                            {
                                                table: tableName,
                                                entity: _toBackupEntity(entity)
                                            }
                                        )
                                    );
                                }
                            );

                            if (result.continuationToken)
                                promiseCallback(resolve, reject, result.continuationToken);
                            else {
                                if (totalEntities === 0) {
                                    _writer.write(separator);
                                    separator = ',';
                                    _writer.write(
                                        JSON.stringify(
                                            {
                                                table: tableName
                                            }
                                        )
                                    );
                                }
                                console.log(`Backup complete, ${totalEntities} total entities`);
                                resolve();
                            }
                        }
                    }
                )
            }
        );
    }

    function _toBackupEntity(entity) {
        return Object
            .getOwnPropertyNames(entity)
            .filter(property => property !== '.metadata' && property !== 'Timestamp')
            .reduce(
                (result, property) =>
                    Object.assign(
                        {},
                        result,
                        { [property]: _toBackupEntityProperty(entity[property]) }
                    ),
                {}
            );
    }

    function _toBackupEntityProperty(entityProperty) {
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
}