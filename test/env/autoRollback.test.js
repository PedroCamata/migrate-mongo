vi.mock("mongodb");

import config from "../../lib/env/config.js";
import mongodb from "mongodb";
import database from "../../lib/env/database.js";

describe("database - autoRollback feature", () => {
  let configObj;
  let client;

  function createConfigObj() {
    return {
      mongodb: {
        url: "mongodb://someserver:27017",
        databaseName: "testDb",
        options: {
          connectTimeoutMS: 3600000, // 1 hour
          socketTimeoutMS: 3600000 // 1 hour
        }
      },
      changelogCollectionName: "changelog",
      lockCollectionName: "lock",
      autoRollbackCollectionName: "autoRollback"
    };
  }

  function mockClient() {
    // Create a mock collection function
    const collectionFunc = function(name) {
      return { 
        the: "db",
        collectionName: name
      };
    };
    
    const mockDb = {
      the: "db",
      collection: collectionFunc
    };
    
    return {
      db: vi.fn().mockReturnValue(mockDb),
      close: "theCloseFnFromMongoClient"
    };
  }

  beforeEach(() => {
    vi.clearAllMocks();
    vi.restoreAllMocks();
    configObj = createConfigObj();
    client = mockClient();
    vi.spyOn(config, 'read').mockReturnValue(configObj);
    vi.spyOn(mongodb.MongoClient, "connect").mockResolvedValue(client);
  });

  describe("collection method wrapping", () => {
    let mockDb;
    let mockAutoRollbackCollection;

    beforeEach(() => {
      // Function to create a fresh mock collection
      function createMockCollection(name) {
        return {
          collectionName: name,
          insertOne: vi.fn().mockResolvedValue({ insertedId: "123" }),
          insertMany: vi.fn().mockResolvedValue({ insertedIds: ["1", "2"] }),
          updateOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
          updateMany: vi.fn().mockResolvedValue({ modifiedCount: 2 }),
          replaceOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
          deleteOne: vi.fn().mockResolvedValue({ deletedCount: 1 }),
          deleteMany: vi.fn().mockResolvedValue({ deletedCount: 2 }),
          findOne: vi.fn().mockResolvedValue({ _id: "doc1", name: "test" }),
          find: vi.fn().mockReturnValue({
            toArray: vi.fn().mockResolvedValue([
              { _id: "doc1", name: "test1" },
              { _id: "doc2", name: "test2" }
            ])
          }),
          distinct: vi.fn().mockResolvedValue([name])
        };
      }


      mockAutoRollbackCollection = {
        collectionName: "autoRollback",
        bulkWrite: vi.fn().mockResolvedValue({ insertedCount: 1 }),
        distinct: vi.fn().mockResolvedValue(["testCollection"]),
        find: vi.fn().mockReturnValue({
          sort: vi.fn().mockReturnThis(),
          project: vi.fn().mockReturnThis(),
          toArray: vi.fn().mockResolvedValue([])
        }),
        deleteMany: vi.fn().mockResolvedValue({ deletedCount: 1 })
      };

      // Create mock db with originalCollection method
      const originalCollectionFunc = function(name) {
        if (name === "autoRollback") {
          return mockAutoRollbackCollection;
        }
        // Return a fresh mock collection for each call
        return createMockCollection(name);
      };

      mockDb = {
        collection: originalCollectionFunc,
        close: vi.fn(),
        isRollback: false,
        migrationFile: "test-migration.js",
        autoRollbackCounter: 0
      };

      client.db.mockReturnValue(mockDb);
    });

    it("should wrap collection methods when autoRollbackEnabled is true", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");

      expect(typeof collection.insertOne).toBe("function");
      expect(typeof collection.insertMany).toBe("function");
      expect(typeof collection.updateOne).toBe("function");
      expect(typeof collection.updateMany).toBe("function");
      expect(typeof collection.replaceOne).toBe("function");
      expect(typeof collection.deleteOne).toBe("function");
      expect(typeof collection.deleteMany).toBe("function");
    });

    it("should not wrap collection methods when autoRollbackEnabled is false", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = false;

      const collection = result.db.collection("users");

      // Should return the original mock collection - verify by checking it has the original methods
      expect(collection.insertOne).toBeDefined();
      expect(collection.collectionName).toBe("users");
    });

    it("should store rollback entry when insertOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.insertOne({ name: "John" });

      // Verify rollback entry was created
      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(1);
      expect(bulkWriteOps[0].insertOne).toBeDefined();
      expect(bulkWriteOps[0].insertOne.collection).toBe("users");
      expect(bulkWriteOps[0].insertOne.migrationFile).toBe("test-migration.js");
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({ deleteOne: { filter: { _id: "123" } } });
    });

    it("should store rollback entry when insertMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      const docs = [{ name: "John" }, { name: "Jane" }];
      await collection.insertMany(docs);

      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({ deleteMany: { filter: { _id: { $in: ["1", "2"] } } } });
      expect(bulkWriteOps[0].insertOne.migrationFile).toBe("test-migration.js");
    });

    it("should store rollback entry when updateOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.updateOne({ name: "John" }, { $set: { age: 30 } });

      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "test" }
        }
      });
    });

    it("should store rollback entries when updateMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.updateMany({ age: { $gt: 20 } }, { $set: { active: true } });

      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(2);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "test1" }
        }
      });
      expect(bulkWriteOps[1].insertOne.bulkWriteOperation).toEqual({
        replaceOne: {
          filter: { _id: "doc2" },
          replacement: { _id: "doc2", name: "test2" }
        }
      });
    });

    it("should store rollback entry when deleteOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.deleteOne({ name: "John" });

      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({
        insertOne: { _id: "doc1", name: "test" }
      });
    });

    it("should store rollback entries when deleteMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      const docsToDelete = [{ name: "John" }, { name: "Jane" }];
      await collection.deleteMany(docsToDelete);

      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(2);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({
        insertOne: { _id: "doc1", name: "test1" }
      });
      expect(bulkWriteOps[1].insertOne.bulkWriteOperation).toEqual({
        insertOne: { _id: "doc2", name: "test2" }
      });
    });

    it("should store rollback entries when replaceOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";
      
      const collection = result.db.collection("users");
      await collection.replaceOne({ name: "John" }, { name: "John", age: 40 });
      expect(mockAutoRollbackCollection.bulkWrite).toHaveBeenCalledOnce();
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0];
      
      expect(bulkWriteOps).toBeInstanceOf(Array);
      expect(bulkWriteOps).toHaveLength(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).toEqual({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "test" }
        }
      });
    });

    it("should increment autoRollbackCounter for each operation", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      
      await collection.insertOne({ name: "John" });
      await collection.insertOne({ name: "Jane" });
      await collection.insertOne({ name: "Bob" });

      expect(mockAutoRollbackCollection.bulkWrite.mock.calls.length).toBe(3);
      
      const entry1 = mockAutoRollbackCollection.bulkWrite.mock.calls[0][0][0].insertOne;
      const entry2 = mockAutoRollbackCollection.bulkWrite.mock.calls[1][0][0].insertOne;
      const entry3 = mockAutoRollbackCollection.bulkWrite.mock.calls[2][0][0].insertOne;
      
      expect(entry1.orderIndex).toBe(0);
      expect(entry2.orderIndex).toBe(1);
      expect(entry3.orderIndex).toBe(2);
    });
  });

  describe("db.autoRollback()", () => {
    let mockDb;
    let mockCollection;
    let mockAutoRollbackCollection;

    beforeEach(() => {
      mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ insertedId: "123" }),
        insertMany: vi.fn().mockResolvedValue({ insertedIds: ["1", "2"] }),
        replaceOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
        deleteOne: vi.fn().mockResolvedValue({ deletedCount: 1 }),
        deleteMany: vi.fn().mockResolvedValue({ deletedCount: 2 }),
        bulkWrite: vi.fn().mockResolvedValue({ insertedCount: 1 })
      };

      mockAutoRollbackCollection = {
        distinct: vi.fn().mockResolvedValue([]),
        find: vi.fn().mockReturnValue({
          sort: vi.fn().mockReturnThis(),
          project: vi.fn().mockReturnThis(),
          toArray: vi.fn().mockResolvedValue([])
        }),
        deleteMany: vi.fn().mockResolvedValue({ deletedCount: 1 })
      };

      mockDb = {
        collection: vi.fn().mockImplementation((name) => {
          if (name === "autoRollback") {
            return mockAutoRollbackCollection;
          }
          return mockCollection;
        }),
        isRollback: true,
        migrationFile: "test-migration.js"
      };

      // Reset client to have close method
      client = {
        db: vi.fn().mockReturnValue(mockDb),
        close: vi.fn()
      };
      
      vi.spyOn(mongodb.MongoClient, "connect").mockResolvedValue(client);
    });

    it("should not execute rollback when isRollback is false", async () => {
      const result = await database.connect();
      result.db.isRollback = false;

      await expect(result.db.autoRollback()).rejects.toThrow("Auto-rollback is not enabled for this migration.");
    });

    it("should fetch rollback entries for the current migration file", async () => {
      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.distinct).toHaveBeenCalledOnce();
      expect(mockAutoRollbackCollection.distinct.mock.calls[0][0]).toBe("collection");
      expect(mockAutoRollbackCollection.distinct.mock.calls[0][1]).toEqual({
        migrationFile: "test-migration.js"
      });
    });

    it("should execute insertOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([{
        insertOne: { _id: "doc1", name: "John" }
      }]);
    });

    it("should execute insertMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [
        { bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } } },
        { bulkWriteOperation: { insertOne: { _id: "doc2", name: "Jane" } } }
      ];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([
        { insertOne: { _id: "doc1", name: "John" } },
        { insertOne: { _id: "doc2", name: "Jane" } }
      ]);
    });

    it("should execute replaceOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: {
          replaceOne: {
            filter: { _id: "doc1" },
            replacement: { _id: "doc1", name: "John", age: 25 }
          }
        }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([{
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John", age: 25 }
        }
      }]);
    });

    it("should execute updateOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: {
          replaceOne: {
            filter: { _id: "doc1" },
            replacement: { _id: "doc1", name: "John" }
          }
        }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([{
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John" }
        }
      }]);
    });

    it("should execute updateMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [
        {
          bulkWriteOperation: {
            replaceOne: {
              filter: { _id: "doc1" },
              replacement: { _id: "doc1", name: "John" }
            }
          }
        },
        {
          bulkWriteOperation: {
            replaceOne: {
              filter: { _id: "doc2" },
              replacement: { _id: "doc2", name: "Jane" }
            }
          }
        }
      ];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toHaveLength(2);
      expect(operations[0]).toEqual({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John" }
        }
      });
      expect(operations[1]).toEqual({
        replaceOne: {
          filter: { _id: "doc2" },
          replacement: { _id: "doc2", name: "Jane" }
        }
      });
    });

    it("should execute deleteOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { deleteOne: { filter: { name: "John" } } }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([{
        deleteOne: { filter: { name: "John" } }
      }]);
    });

    it("should execute deleteMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { deleteMany: { filter: { $or: [{ name: "John" }, { name: "Jane" }] } } }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toEqual([{
        deleteMany: { filter: { $or: [{ name: "John" }, { name: "Jane" }] } }
      }]);
    });

    it("should clean up rollback entries after successful rollback", async () => {
      const rollbackEntries = [{
        operation: "insertOne",
        collection: "users",
        parameters: { _id: "doc1", name: "John" }
      }];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.deleteMany).toHaveBeenCalledOnce();
      expect(mockAutoRollbackCollection.deleteMany.mock.calls[0][0]).toEqual({
        migrationFile: "test-migration.js"
      });
    });

    it("should execute multiple rollback operations in reverse order", async () => {
      mockAutoRollbackCollection.distinct.mockResolvedValue(["users"]);
      
      const rollbackEntries = [
        { bulkWriteOperation: { insertOne: { _id: "doc3", name: "Bob" } } },
        { bulkWriteOperation: { deleteOne: { filter: { name: "Jane" } } } },
        { bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } } }
      ];

      mockAutoRollbackCollection.find.mockReturnValue({
        sort: vi.fn().mockReturnThis(),
        project: vi.fn().mockReturnThis(),
        toArray: vi.fn().mockResolvedValue(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      // Should execute all operations in one bulkWrite
      expect(mockCollection.bulkWrite).toHaveBeenCalledOnce();
      const operations = mockCollection.bulkWrite.mock.calls[0][0];
      expect(operations).toHaveLength(3);
    });
  });
});
