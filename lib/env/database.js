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

    const client = await MongoClient.connect(
      url,
      options
    );

    const db = client.db(databaseName);
    const originalCollection = db.collection.bind(db);

    const autoRollbackExcludedCollections = [
      configContent.changelogCollectionName,
      configContent.lockCollectionName,
      configContent.autoRollbackCollectionName
    ];

    // Override the collection method to return wrapped collections
    db.collection = (name, options) => {
      const collection = originalCollection(name, options);

      if (autoRollbackExcludedCollections.includes(collection.collectionName)) {
        return collection;
      }

      // istanbul ignore next
      if (db.isRollback
        || !configContent.autoRollbackCollectionName
        || !db.autoRollbackEnabled) {

        if (db.autoRollbackEnabled) {
          // Auto-rollback is enabled but not properly configured
          throw new Error("Auto-rollback is not enabled in the config file.");
        }
        return collection;
      }

      return wrapDbCollection(collection, db, configContent, autoRollbackExcludedCollections);
    };

    // Performs auto-rollback for the current migration
    db.autoRollback = async () => {
      if (!db.isRollback || configContent.autoRollbackCollectionName === undefined) {
        throw new Error("Auto-rollback is not enabled for this migration.");
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

    db.close = client.close;

    return {
      client,
      db,
    };
  }
};