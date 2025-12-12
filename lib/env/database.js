const { MongoClient } = require("mongodb");
const _get = require("lodash.get");
const config = require("./config");
const { wrapDbCollection } = require("./wrapDbCollection");

module.exports = {
  async connect() {
    const configContent = await config.read();
    const url = _get(configContent, "mongodb.url");
    const databaseName = _get(configContent, "mongodb.databaseName");
    const options = _get(configContent, "mongodb.options");

    if (!url) {
      throw new Error("No `url` defined in config file!");
    }

    let client;
    try {
      client = await MongoClient.connect(url, options);
    } catch (error) {
      throw new Error(`Failed to connect to MongoDB: ${error.message}`);
    }

    const db = client.db(databaseName);
    const originalCollection = db.collection.bind(db);

    const excludedCollections = [
      configContent.changelogCollectionName,
      configContent.lockCollectionName,
      configContent.autoRollbackCollectionName
    ];

    // Override the collection method to return wrapped collections
    db.collection = function (name, options) {
      const collection = originalCollection(name, options);
      return wrapDbCollection(collection, db, configContent, excludedCollections);
    };

    /**
     * Performs auto-rollback for the current migration
     */
    db.autoRollback = async function () {
      if (!db.isRollback) {
        return;
      }

      try {
        const autoRollbackCollection = originalCollection(configContent.autoRollbackCollectionName);
        const collectionNames = await autoRollbackCollection.distinct(
          "collection",
          { migrationFile: db.migrationFile }
        );

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

        await autoRollbackCollection.deleteMany({ migrationFile: db.migrationFile });
      } catch (error) {
        /* istanbul ignore next */
        throw new Error(`Auto-rollback failed: ${error.message}`);
      }
    };

    db.close = client.close.bind(client);
    
    return {
      client,
      db,
    };
  }
};