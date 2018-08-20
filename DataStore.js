const Promise = require('bluebird');
const _ = require('lodash');
const GCloudDataStore = require('@google-cloud/datastore');

const defaultQueryLimit = 500;

class GoogleDataStore{
    constructor(config, namespace, dataStoreName) {
        this._namespace = namespace;
        this._dataStoreName = dataStoreName;
        this._datastore = new GCloudDataStore(config);
    }
    save(entities, options) {
        const encodedEntities = _.map(entities, (entity) => {
            const datastoreKey = buildKey.call(this, entity.key);
            const dataToSave = _.has(options, 'indexes') ? createDataArray.call(this, entity.data, options.indexes) : entity.data;

            return {
                key: datastoreKey,
                method: 'insert',
                data: dataToSave
            };
        });

        return this._datastore.save(encodedEntities).then(() => true);
    }
    query(filter, {limit, dataCallback}) {
        let query = this._datastore.createQuery(this._namespace, this._dataStoreName);

        const parentKey = this._datastore.key({
            namespace: this._namespace,
            path: ['Artifacts', 'Artifact']
        });
        query = query.hasAncestor(parentKey);

        query = query.limit(limit || defaultQueryLimit);

        _.forEach(filter, (valueOrCondition, key) => {
            query = query.filter(key, valueOrCondition);
        });

        return new Promise((resolve, reject) => {
            return query.runStream({ consistency: 'strong' })
                .on('error', reject)
                .on('data', (entity) => {
                    const key = entity[this._datastore.KEY];

                    return dataCallback(key.id || key.name, entity);
                }).on('end', () => resolve());
        })
    }
    remove(keys) {
        const encodedKeys = _.map(keys, _.bind(buildKey, this));

        return this._datastore.delete(encodedKeys).then(() => true);
    }
}

function buildKey(key) {
    return this._datastore.key({
        namespace: this._namespace,
        path: ['Artifacts', 'Artifact', this._dataStoreName, key]
    });
}

function createDataArray(data, indexes) {
    return _.map(data, (value, key) => {
        return {
            name: key,
            value: value,
            excludeFromIndexes: indexes[key] ? false : true
        };
    });
}

module.exports = GoogleDataStore;