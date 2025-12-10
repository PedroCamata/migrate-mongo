const { MongoClient } = require("mongodb");
const _get = require("lodash.get");
const config = require("./config");

module.exports = {
  async connect() {
    const configContent = await config.read();
    const url = _get(configContent, "mongodb.url");
    const databaseName = _get(configContent, "mongodb.databaseName");
    const options = _get(configContent, "mongodb.options");

    if (!url) {
      throw new Error("No `url` defined in config file!");
    }

    const client = await MongoClient.connect(
      url,
      options
    );

    const db = client.db(databaseName);

    // Store the original collection method
    const originalCollection = db.collection.bind(db);

    // Override the collection method to return a wrapped collection
    db.collection = function (name, options) {
      const collection = originalCollection(name, options);

      if (db.isRollback || !db.autoRollbackEnabled) {
        return collection;
      }

      const excludedCollections = [
        configContent.changelogCollectionName,
        configContent.lockCollectionName,
        configContent.autoRollbackCollectionName
      ];

      if (excludedCollections.includes(name)) {
        return collection;
      }

      // Helper function to wrap collection methods
      const wrapMethod = (methodName, tempCollection, originalMethod) => {
        return async function (...args) {
          const autoRollbackCollection = db.collection(configContent.autoRollbackCollectionName);

          const filterArg = args[0];

          const inverseOperation = {
            insertOne: {
              getOperations: async () => {
                return [{ deleteOne: { filter: filterArg} }];
              }
            },
            insertMany: {
              getOperations: async () => {
                return [{ deleteMany: { filter: { $or: filterArg } } }];
              }
            },
            replaceOne: {
              getOperations: async () => {
                let doc = await tempCollection.findOne(filterArg);
                return [{
                  replaceOne: {
                    filter: { _id: doc._id },
                    replacement: doc
                  }
                }];
              }
            },
            updateOne: {
              getOperations: async () => {
                let doc = await tempCollection.findOne(filterArg);
                return [{
                  replaceOne: {
                    filter: { _id: doc._id },
                    replacement: doc
                  }
                }];
              }
            },
            updateMany: {
              getOperations: async () => {
                let docs = await tempCollection.find(filterArg).toArray();
                return docs.map(doc => ({
                  replaceOne: {
                    filter: { _id: doc._id },
                    replacement: doc
                  }
                }));
              }
            },
            deleteOne: {
              getOperations: async () => {
                return [{ insertOne: await tempCollection.findOne(filterArg) }];
              },
            },
            deleteMany: {
              getOperations: async () => {
                let docs = await tempCollection.find(filterArg).toArray();
                return docs.map(doc => ({
                  insertOne: doc
                }));
              },
            }
          };

          // Get rollback configuration for this method
          let operations = await inverseOperation[methodName].getOperations();
          let timestamp = new Date();
          let bulkWriteInsertOperations = operations.map(operation => ({
            insertOne: {
              timestamp: timestamp,
              migrationFile: db.migrationFile,
              orderIndex: db.autoRollbackCounter++,
              collection: tempCollection.collectionName,
              bulkWriteOperation: operation
            }
          }));

          await autoRollbackCollection.bulkWrite(bulkWriteInsertOperations, { ordered: false });
          

          // Call the original method
          return originalMethod(...args);
        };
      };

      // Override collection methods
      [
        'insertOne',
        'insertMany',
        'replaceOne',
        'updateOne',
        'updateMany',
        'deleteOne',
        'deleteMany',
      ].forEach(methodName => {
        const originalMethod = collection[methodName].bind(collection);
        collection[methodName] = wrapMethod(methodName, collection, originalMethod);
      });

      return collection;
    };

    db.autoRollback = async function () {
      if (!db.isRollback) {
        return;
      }

      const autoRollbackCollection = originalCollection(configContent.autoRollbackCollectionName);

      const collectionNames = await autoRollbackCollection.distinct("collection", { migrationFile: db.migrationFile });

      for (const collectionName of collectionNames) {
        const targetCollection = originalCollection(collectionName);
        const rollbackEntries = await autoRollbackCollection
          .find({ migrationFile: db.migrationFile, collection: collectionName })
          .sort({ timestamp: -1, orderIndex: -1 })
          .project({ _id: 0, bulkWriteOperation: 1 })
          .toArray();

        const operations = rollbackEntries.map(e => e.bulkWriteOperation);
        await targetCollection.bulkWrite(operations, { ordered: true });
      }

      // Clean up rollback entries for this migration
      await autoRollbackCollection.deleteMany({ migrationFile: db.migrationFile });
    };

    db.close = client.close;
    return {
      client,
      db,
    };
  }
};