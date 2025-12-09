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
      const wrapMethod = (methodName, originalMethod) => {
        return async function (...args) {

          const autoRollbackCollection = db.collection(configContent.autoRollbackCollectionName);

          // Configuration for rollback operations
          const rollbackConfig = {
            insertOne: {
              operation: "deleteOne",
              getParams: async () => args[0]
            },
            insertMany: {
              operation: "deleteMany",
              getParams: async () => args[0]
            },
            replaceOne: {
              operation: "replaceOne",
              getParams: async () => await collection.findOne(args[0][0])
            },
            updateOne: {
              operation: "updateOne",
              getParams: async () => await collection.findOne(args[0][0])
            },
            updateMany: {
              operation: "updateMany",
              getParams: async () => await collection.find(args[0][0]).toArray(),
            },
            deleteOne: {
              operation: "insertOne",
              getParams: async () => await collection.findOne(args[0])
            },
            deleteMany: {
              operation: "insertMany",
              getParams: async () => await collection.find(args[0]).toArray(),
            }
          };

          // Get rollback configuration for this method
          const config = rollbackConfig[methodName];
          const params = await config.getParams();
          await autoRollbackCollection.insertOne({
            timestamp: new Date(),
            migrationFile: db.migrationFile,
            orderIndex: db.autoRollbackCounter++,
            operation: config.operation,
            collection: collection.collectionName,
            parameters: params,
          });

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
        collection[methodName] = wrapMethod(methodName, originalMethod);
      });

      return collection;
    };

    db.autoRollback = async function () {
      if (!db.isRollback) {
        return;
      }

      const autoRollbackCollection = originalCollection(configContent.autoRollbackCollectionName);

      const rollbackEntries = await autoRollbackCollection
        .find({ migrationFile: db.migrationFile })
        .sort({ timestamp: -1, orderIndex: -1 })
        .project({ _id: 0, operation: 1, collection: 1, parameters: 1 })
        .toArray();


      // Define rollback handlers
      const rollbackHandlers = {
        insertOne: (targetCollection, params) => targetCollection.insertOne(params),
        insertMany: (targetCollection, params) => targetCollection.insertMany(params),
        replaceOne: (targetCollection, params) => {
          const doc = params;
          const filter = { _id: doc._id };
          return targetCollection.replaceOne(filter, doc);
        },
        updateOne: (targetCollection, params) => {
          const doc = params;
          const filter = { _id: doc._id };
          return targetCollection.replaceOne(filter, doc);
        },
        updateMany: async (targetCollection, params) => {
          for (const doc of params) {
            const filter = { _id: doc._id };
            await targetCollection.replaceOne(filter, doc);
          }
        },
        deleteOne: (targetCollection, params) => targetCollection.deleteOne(params),
        deleteMany: (targetCollection, params) => {
          let docs = params;
          return targetCollection.deleteMany({ $or: docs });
        },
      };

      // Execute rollback operations
      for (const entry of rollbackEntries) {
        const targetCollection = originalCollection(entry.collection);
        const handler = rollbackHandlers[entry.operation];

        if (handler) {
          await handler(targetCollection, entry.parameters);
        }
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