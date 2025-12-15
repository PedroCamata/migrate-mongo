// Constants
const COLLECTION_INTERCEPTED_METHODS = [
  'insertOne',
  'insertMany',
  'replaceOne',
  'updateOne',
  'updateMany',
  'deleteOne',
  'deleteMany',
];

/**
 * Creates inverse operations for auto-rollback functionality
 */
const INVERSE_OPERATIONS = {
  async insertOne(collection, filterArg, operationResult) {
    if (!operationResult) return [];
    return [{ deleteOne: { filter: { _id: operationResult.insertedId} } }];
  },
  async insertMany(collection, filterArg, operationResult) {
    if (!operationResult) return [];
    return [{ deleteMany: { filter: { _id: { $in: Object.values(operationResult.insertedIds) } } } }];
  },
  async replaceOne(collection, filterArg, operationResult) {
    if (operationResult) return [];
    const doc = await collection.findOne(filterArg);

    /* istanbul ignore next */
    if (!doc) return [];

    return [{
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc
      }
    }];
  },
  async updateOne(collection, filterArg, operationResult) {
    return this.replaceOne(collection, filterArg, operationResult);
  },
  async updateMany(collection, filterArg, operationResult) {
    if (operationResult) return [];

    const docs = await collection.find(filterArg).toArray();
    return docs.map(doc => ({
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc
      }
    }));
  },
  async deleteOne(collection, filterArg, operationResult) {
    if (operationResult) return [];
    const doc = await collection.findOne(filterArg);

    /* istanbul ignore next */
    if (!doc) return [];

    return [{ insertOne: doc }];
  },
  async deleteMany(collection, filterArg, operationResult) {
    if (operationResult) return [];

    const docs = await collection.find(filterArg).toArray();
    return docs.map(doc => ({ insertOne: doc }));
  }
};


// Creates a wrapped collection method that records inverse operations for rollback
function createWrappedMethod(methodName, collection, originalMethod, db, autoRollbackCollection) {
  return async function (...args) {
    try {
      const filterArg = args[0];
      const preOperation = await INVERSE_OPERATIONS[methodName](collection, filterArg, null);
      
      // Original MongoDb operation
      const operationResult = await originalMethod(...args);

      const postOperation = await INVERSE_OPERATIONS[methodName](collection, filterArg, operationResult);

      // Combine PreOperation and postRollback operations
      const rollbackOperations = [...preOperation, ...postOperation];

      const timestamp = new Date();
      const bulkWriteInsertOperations = rollbackOperations.map(operation => ({
        insertOne: {
          timestamp,
          migrationFile: db.migrationFile,
          orderIndex: db.autoRollbackCounter++,
          originalArgs: args,
          collection: collection.collectionName,
          bulkWriteOperation: operation,
        }
      }));

      // Write rollback operations to the auto-rollback collection
      await autoRollbackCollection.bulkWrite(bulkWriteInsertOperations, { ordered: true });

      return operationResult;
    } catch (error) {
      /* istanbul ignore next */
      throw new Error(`Failed to execute ${methodName} with auto-rollback: ${error.message}`);
    }
  };
}

// Wraps a collection to intercept methods for auto-rollback tracking
function wrapDbCollection(collection, db, configContent, excludedCollections) {
  const autoRollbackCollection = db.collection(configContent.autoRollbackCollectionName);

  COLLECTION_INTERCEPTED_METHODS.forEach(methodName => {
    const originalMethod = collection[methodName].bind(collection);
    collection[methodName] = createWrappedMethod(
      methodName,
      collection,
      originalMethod,
      db,
      autoRollbackCollection);
  });

  return collection;
}

export default {
    wrapDbWithAutoRollback(db, configContent, originalCollection) {
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
    }
};