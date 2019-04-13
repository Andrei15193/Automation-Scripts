const { createGunzip } = require('zlib');
const azure = require('azure-storage');
const { parser: jsonParser } = require('stream-json');
const { streamArray: jsonStreamArray } = require('stream-json/streamers/StreamArray');

module.exports = {
    restoreTableStorageAsync
};

function restoreTableStorageAsync(connectionString, inputStream) {
    return new Promise((resolve, reject) => {
        const _tableService = azure.createTableService(connectionString);
        const _concurrencyLevel = 30;
        const _jsonStream = jsonStreamArray();
        const _entitiesCountPerTable = {};
        let _promiseCount = 0;
        let _streamEnded = false;
        let _errors = [];
        let _deletedTables = 0;

        _jsonStream
            .on('data', _readEntityAsync)
            .on('end', () => {
                _deleteExtraTablesAsync().then(
                    () => {
                        _streamEnded = true;
                        _writeResults();
                    }
                )
            });

        inputStream
            .pipe(createGunzip())
            .pipe(jsonParser())
            .pipe(_jsonStream);

        function _deleteExtraTablesAsync() {
            return new Promise(
                function promiseCallback(resolve, reject, continuationToken) {
                    _tableService.listTablesSegmented(continuationToken, (error, result) => {
                        if (error)
                            reject(error);
                        else
                            Promise
                                .all(result.entries.filter(tableName => !(tableName in _entitiesCountPerTable)).map(_deleteTableAsync))
                                .then(
                                    () => {
                                        if (result.continuationToken)
                                            promiseCallback(resolve, reject, result.continuationToken);
                                        else
                                            resolve();
                                    }
                                )
                                .catch(error => reject(error));
                    });
                }
            );
        }

        function _deleteTableAsync(tableName) {
            return new Promise(
                (resolve, reject) => {
                    console.log('Deleting', tableName);
                    _tableService.deleteTableIfExists(tableName, (error) => {
                        if (error)
                            reject(error);
                        else {
                            _deletedTables++;
                            console.log('Deleted', tableName);
                            resolve();
                        }
                    });
                }
            );
        }

        function _readEntityAsync(data) {
            const { table: tableName, entity } = data.value;

            _promiseCount++;
            const shouldClearTable = !(tableName in _entitiesCountPerTable);
            if (_promiseCount === _concurrencyLevel || shouldClearTable) {
                _jsonStream.pause();
                if (shouldClearTable)
                    _entitiesCountPerTable[tableName] = 0;
            }

            return (shouldClearTable ? _clearTableDataAsync(tableName) : Promise.resolve())
                .then(() => {
                    let insertEntityPromise = Promise.resolve();
                    if (entity) {
                        console.log(`Inserting entity (${entity.PartitionKey.value}, ${entity.RowKey.value}) into ${tableName}`);
                        insertEntityPromise = _insertEntityAsync(tableName, entity);
                    }

                    return insertEntityPromise
                        .catch(error => _errors.push(error))
                        .then(() => {
                            _entitiesCountPerTable[tableName] += 1;

                            if ((_promiseCount === _concurrencyLevel || shouldClearTable) && _errors.length === 0)
                                _jsonStream.resume();
                            _promiseCount--;
                            _writeResults();
                        });
                })
                .catch(error => {
                    _errors.push(error);
                    _promiseCount--;
                    _writeResults();
                });
        }

        function _clearTableDataAsync(tableName) {
            let totalEntities = 0;
            const tableQuery = new azure.TableQuery().select('PartitionKey', 'RowKey');
            console.log('Clearing data from', tableName);
            return new Promise(
                function promiseCallback(resolve, reject, continuationToken) {
                    _tableService.queryEntities(
                        tableName,
                        tableQuery,
                        continuationToken,
                        (error, result) => {
                            if (error)
                                reject(error);
                            else {
                                totalEntities += result.entries.length;
                                _deleteEntitiesAsync(tableName, result.entries)
                                    .then(() => {
                                        if (result.continuationToken)
                                            promiseCallback(resolve, reject, result.continuationToken);
                                        else {
                                            console.log('Cleared', totalEntities, 'entities from', tableName);
                                            resolve();
                                        }
                                    });
                            }
                        }
                    );
                }
            );
        }

        function _deleteEntitiesAsync(tableName, entities) {
            const tableBatchesByPartitionKey = entities.reduce(
                (grouping, entity) => {
                    if (entity.PartitionKey._ in grouping) {
                        const tableBatches = grouping[entity.PartitionKey._];
                        const tableBatch = tableBatches[tableBatches.length - 1];
                        let currentTableBatch = tableBatch;
                        if (tableBatch.operations.length === 100) {
                            currentTableBatch = new azure.TableBatch();
                            tableBatches.push(currentTableBatch);
                        }
                        currentTableBatch.deleteEntity(entity);
                    }
                    else {
                        const tableBatch = new azure.TableBatch();
                        tableBatch.deleteEntity(entity)
                        grouping[entity.PartitionKey._] = [tableBatch];
                    }
                    return grouping;
                },
                {}
            );

            return Promise.all(
                Object
                    .getOwnPropertyNames(tableBatchesByPartitionKey)
                    .reduce((allTableBatches, partitionKey) => allTableBatches.concat(tableBatchesByPartitionKey[partitionKey]), [])
                    .map(
                        tableBatch => new Promise(
                            (resolve, reject) => _tableService.executeBatch(
                                tableName,
                                tableBatch,
                                error => {
                                    if (error)
                                        reject(error);
                                    else
                                        resolve(error);
                                }
                            )
                        )
                    )
            );
        }

        function _insertEntityAsync(tableName, entity) {
            return new Promise(
                (resolve, reject) => _tableService.insertEntity(
                    tableName,
                    _getAzureEntity(entity),
                    error => {
                        if (error)
                            reject(error);
                        else
                            resolve();
                    }
                )
            );
        }

        function _getAzureEntity(entity) {
            return Object
                .getOwnPropertyNames(entity)
                .reduce((result, propertyName) => Object
                    .assign(
                        {},
                        result,
                        { [propertyName]: _getAzureEntityProperty(entity[propertyName]) }),
                    {}
                );
        }

        function _getAzureEntityProperty(property) {
            switch (property.type) {
                case 'Edm.Guid':
                    return azure.TableUtilities.entityGenerator.Guid(property.value);

                case 'Edm.DateTime':
                    return azure.TableUtilities.entityGenerator.DateTime(property.value);

                case 'Edm.Binary':
                    return azure.TableUtilities.entityGenerator.Binary(property.value);

                case 'Edm.Boolean':
                    return azure.TableUtilities.entityGenerator.Boolean(property.value);

                case 'Edm.Double':
                    return azure.TableUtilities.entityGenerator.Double(property.value);

                case 'Edm.Int32':
                    return azure.TableUtilities.entityGenerator.Int32(property.value);

                case 'Edm.Int64':
                    return azure.TableUtilities.entityGenerator.Int64(property.value);

                default:
                    return azure.TableUtilities.entityGenerator.String(property.value);
            }
        }

        function _writeResults() {
            if (!_streamEnded || _promiseCount > 0)
                return;

            if (_errors.length > 0)
                reject(_errors);
            else {
                let totalEntities = 0;
                Object
                    .getOwnPropertyNames(_entitiesCountPerTable)
                    .forEach(tableName => {
                        totalEntities += _entitiesCountPerTable[tableName];
                        console.log('Restored', _entitiesCountPerTable[tableName], 'entities into', tableName);
                    });
                console.log('Restore completed,', totalEntities, 'entities restored,', 'deleted', _deletedTables, 'tables');
                resolve();
            }
        }
    });
}