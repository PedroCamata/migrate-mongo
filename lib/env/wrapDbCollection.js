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
  async insertOne(collection, filterArg) {
    return [{ deleteOne: { filter: filterArg } }];
  },
  async insertMany(collection, filterArg) {
    return [{ deleteMany: { filter: { $or: filterArg } } }];
  },
  async replaceOne(collection, filterArg) {
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
  async updateOne(collection, filterArg) {
    return this.replaceOne(collection, filterArg);
  },
  async updateMany(collection, filterArg) {
    const docs = await collection.find(filterArg).toArray();
    return docs.map(doc => ({
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc
      }
    }));
  },
  async deleteOne(collection, filterArg) {
    const doc = await collection.findOne(filterArg);
    /* istanbul ignore next */
    if (!doc) return [];
    return [{ insertOne: doc }];
  },
  async deleteMany(collection, filterArg) {
    const docs = await collection.find(filterArg).toArray();
    return docs.map(doc => ({ insertOne: doc }));
  }
};
/**
 * Creates a wrapped collection method that records inverse operations for rollback
 */
function createWrappedMethod(methodName, collection, originalMethod, db, configContent) {
  return async function (...args) {
    try {
      const autoRollbackCollection = db.collection(configContent.autoRollbackCollectionName);
      const filterArg = args[0];

      const operations = await INVERSE_OPERATIONS[methodName](collection, filterArg);

      const timestamp = new Date();
      const bulkWriteInsertOperations = operations.map(operation => ({
        insertOne: {
          timestamp,
          migrationFile: db.migrationFile,
          orderIndex: db.autoRollbackCounter++,
          collection: collection.collectionName,
          bulkWriteOperation: operation
        }
      }));

      await autoRollbackCollection.bulkWrite(bulkWriteInsertOperations, { ordered: false });

      return await originalMethod(...args);
    } catch (error) {
      /* istanbul ignore next */
      throw new Error(`Failed to execute ${methodName} with auto-rollback: ${error.message}`);
    }
  };
}
/**
 * Wraps a collection to intercept methods for auto-rollback tracking
 */
function wrapDbCollection(collection, db, configContent, excludedCollections) {
  if (db.isRollback || !db.autoRollbackEnabled) {
    return collection;
  }

  if (excludedCollections.includes(collection.collectionName)) {
    return collection;
  }

  COLLECTION_INTERCEPTED_METHODS.forEach(methodName => {
    const originalMethod = collection[methodName].bind(collection);
    collection[methodName] = createWrappedMethod(methodName, collection, originalMethod, db, configContent);
  });

  return collection;
}
exports.wrapDbCollection = wrapDbCollection;
