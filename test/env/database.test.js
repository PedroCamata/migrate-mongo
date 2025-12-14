const { expect } = require("chai");
const sinon = require("sinon");
const proxyquire = require("proxyquire");

describe("database", () => {
  let configObj;
  let database;
  let config;
  let mongodb;
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
      db: sinon.stub().returns(mockDb),
      close: sinon.stub()
    };
  }

  function mockConfig() {
    return {
      read: sinon.stub().returns(configObj)
    };
  }

  function mockMongodb() {
    return {
      MongoClient: {
        connect: sinon.stub().returns(Promise.resolve(client))
      }
    };
  }

  beforeEach(() => {
    configObj = createConfigObj();
    client = mockClient();
    config = mockConfig();
    mongodb = mockMongodb();

    database = proxyquire("../../lib/env/database", {
      "./config": config,
      mongodb
    });
  });

  describe("connect()", () => {
    it("should connect MongoClient to the configured mongodb url with the configured options", async () => {
      const result = await database.connect();
      expect(mongodb.MongoClient.connect.called).to.equal(true);
      expect(mongodb.MongoClient.connect.getCall(0).args[0]).to.equal(
        "mongodb://someserver:27017"
      );

      expect(mongodb.MongoClient.connect.getCall(0).args[1]).to.deep.equal({
        connectTimeoutMS: 3600000, // 1 hour
        socketTimeoutMS: 3600000 // 1 hour
      });

      expect(client.db.getCall(0).args[0]).to.equal("testDb");
      // db now has additional methods; assert key props instead of deep equality
      expect(result.db).to.be.an("object");
      expect(result.db.the).to.equal("db");
      expect(result.db.close).to.be.a("function");
      expect(result.db.collection).to.be.a("function");
      expect(result.client).to.deep.equal(client);
    });

    it("should yield an error when no url is defined in the config file", async () => {
      delete configObj.mongodb.url;
      try {
        await database.connect();
        expect.fail("Error was not thrown");
      } catch (err) {
        expect(err.message).to.equal("No `url` defined in config file!");
      }
    });

    it("should yield an error when unable to connect", async () => {
      mongodb.MongoClient.connect.returns(
        Promise.reject(new Error("Unable to connect"))
      );
      try {
        await database.connect();
      } catch (err) {
        expect(err.message).to.equal("Failed to connect to MongoDB: Unable to connect");
      }
    });
  });

  describe("autoRollback feature", () => {
    let mockDb;
    let mockAutoRollbackCollection;

    beforeEach(() => {
      // Function to create a fresh mock collection
      function createMockCollection(name) {
        return {
          collectionName: name,
          insertOne: sinon.stub().resolves({ insertedId: "123" }),
          insertMany: sinon.stub().resolves({ insertedIds: ["1", "2"] }),
          updateOne: sinon.stub().resolves({ modifiedCount: 1 }),
          updateMany: sinon.stub().resolves({ modifiedCount: 2 }),
          replaceOne: sinon.stub().resolves({ modifiedCount: 1 }),
          deleteOne: sinon.stub().resolves({ deletedCount: 1 }),
          deleteMany: sinon.stub().resolves({ deletedCount: 2 }),
          findOne: sinon.stub().resolves({ _id: "doc1", name: "test" }),
          find: sinon.stub().returns({
            toArray: sinon.stub().resolves([
              { _id: "doc1", name: "test1" },
              { _id: "doc2", name: "test2" }
            ])
          }),
          distinct: sinon.stub().resolves([name])
        };
      }


      mockAutoRollbackCollection = {
        collectionName: "autoRollback",
        bulkWrite: sinon.stub().resolves({ insertedCount: 1 }),
        distinct: sinon.stub().resolves(["testCollection"]),
        find: sinon.stub().returns({
          sort: sinon.stub().returnsThis(),
          project: sinon.stub().returnsThis(),
          toArray: sinon.stub().resolves([])
        }),
        deleteMany: sinon.stub().resolves({ deletedCount: 1 })
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
        close: sinon.stub(),
        autoRollbackEnabled: true,
        isRollback: false,
        migrationFile: "test-migration.js",
        autoRollbackCounter: 0
      };

      client.db.returns(mockDb);
    });

    it("should wrap collection methods when autoRollbackEnabled is true", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");

      expect(typeof collection.insertOne).to.equal("function");
      expect(typeof collection.insertMany).to.equal("function");
      expect(typeof collection.updateOne).to.equal("function");
      expect(typeof collection.updateMany).to.equal("function");
      expect(typeof collection.replaceOne).to.equal("function");
      expect(typeof collection.deleteOne).to.equal("function");
      expect(typeof collection.deleteMany).to.equal("function");
    });

    it("should not wrap collection methods when autoRollbackEnabled is false", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = false;

      const collection = result.db.collection("users");

      // Should return the original mock collection - verify by checking it has the original methods
      expect(collection.insertOne).to.exist;
      expect(collection.collectionName).to.equal("users");
    });

    it("should store rollback entry when insertOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.insertOne({ name: "John" });

      // Verify rollback entry was created
      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(1);
      expect(bulkWriteOps[0].insertOne).to.exist;
      expect(bulkWriteOps[0].insertOne.collection).to.equal("users");
      expect(bulkWriteOps[0].insertOne.migrationFile).to.equal("test-migration.js");
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({ deleteOne: { filter: { _id: "123" } } });
    });

    it("should store rollback entry when insertMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      const docs = [{ name: "John" }, { name: "Jane" }];
      await collection.insertMany(docs);

      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({ deleteMany: { filter: { _id: { $in: ["1", "2"] } } } });
      expect(bulkWriteOps[0].insertOne.migrationFile).to.equal("test-migration.js");
    });

    it("should store rollback entry when updateOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.updateOne({ name: "John" }, { $set: { age: 30 } });

      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({
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

      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(2);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "test1" }
        }
      });
      expect(bulkWriteOps[1].insertOne.bulkWriteOperation).to.deep.equal({
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

      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({
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

      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(2);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({
        insertOne: { _id: "doc1", name: "test1" }
      });
      expect(bulkWriteOps[1].insertOne.bulkWriteOperation).to.deep.equal({
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
      expect(mockAutoRollbackCollection.bulkWrite.calledOnce).to.equal(true);
      const bulkWriteOps = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0];
      
      expect(bulkWriteOps).to.be.an("array");
      expect(bulkWriteOps).to.have.lengthOf(1);
      expect(bulkWriteOps[0].insertOne.bulkWriteOperation).to.deep.equal({
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

      expect(mockAutoRollbackCollection.bulkWrite.callCount).to.equal(3);
      
      const entry1 = mockAutoRollbackCollection.bulkWrite.getCall(0).args[0][0].insertOne;
      const entry2 = mockAutoRollbackCollection.bulkWrite.getCall(1).args[0][0].insertOne;
      const entry3 = mockAutoRollbackCollection.bulkWrite.getCall(2).args[0][0].insertOne;
      
      expect(entry1.orderIndex).to.equal(0);
      expect(entry2.orderIndex).to.equal(1);
      expect(entry3.orderIndex).to.equal(2);
    });
  });

  describe("db.autoRollback()", () => {
    let mockDb;
    let mockCollection;
    let mockAutoRollbackCollection;

    beforeEach(() => {
      mockCollection = {
        insertOne: sinon.stub().resolves({ insertedId: "123" }),
        insertMany: sinon.stub().resolves({ insertedIds: ["1", "2"] }),
        replaceOne: sinon.stub().resolves({ modifiedCount: 1 }),
        deleteOne: sinon.stub().resolves({ deletedCount: 1 }),
        deleteMany: sinon.stub().resolves({ deletedCount: 2 }),
        bulkWrite: sinon.stub().resolves({ insertedCount: 1 })
      };

      mockAutoRollbackCollection = {
        distinct: sinon.stub().resolves([]),
        find: sinon.stub().returns({
          sort: sinon.stub().returnsThis(),
          project: sinon.stub().returnsThis(),
          toArray: sinon.stub().resolves([])
        }),
        deleteMany: sinon.stub().resolves({ deletedCount: 1 })
      };

      mockDb = {
        collection: sinon.stub().callsFake((name) => {
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
        db: sinon.stub().returns(mockDb),
        close: sinon.stub()
      };
      
      mongodb.MongoClient.connect.returns(Promise.resolve(client));
    });

    it("should not execute rollback when isRollback is false", async () => {
      const result = await database.connect();
      result.db.isRollback = false;

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.distinct.called).to.equal(false);
      expect(mockCollection.bulkWrite.called).to.equal(false);
    });

    it("should fetch rollback entries for the current migration file", async () => {
      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.distinct.calledOnce).to.equal(true);
      expect(mockAutoRollbackCollection.distinct.getCall(0).args[0]).to.equal("collection");
      expect(mockAutoRollbackCollection.distinct.getCall(0).args[1]).to.deep.equal({
        migrationFile: "test-migration.js"
      });
    });

    it("should execute insertOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([{
        insertOne: { _id: "doc1", name: "John" }
      }]);
    });

    it("should execute insertMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [
        { bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } } },
        { bulkWriteOperation: { insertOne: { _id: "doc2", name: "Jane" } } }
      ];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([
        { insertOne: { _id: "doc1", name: "John" } },
        { insertOne: { _id: "doc2", name: "Jane" } }
      ]);
    });

    it("should execute replaceOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: {
          replaceOne: {
            filter: { _id: "doc1" },
            replacement: { _id: "doc1", name: "John", age: 25 }
          }
        }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([{
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John", age: 25 }
        }
      }]);
    });

    it("should execute updateOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: {
          replaceOne: {
            filter: { _id: "doc1" },
            replacement: { _id: "doc1", name: "John" }
          }
        }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([{
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John" }
        }
      }]);
    });

    it("should execute updateMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
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

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.have.lengthOf(2);
      expect(operations[0]).to.deep.equal({
        replaceOne: {
          filter: { _id: "doc1" },
          replacement: { _id: "doc1", name: "John" }
        }
      });
      expect(operations[1]).to.deep.equal({
        replaceOne: {
          filter: { _id: "doc2" },
          replacement: { _id: "doc2", name: "Jane" }
        }
      });
    });

    it("should execute deleteOne rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { deleteOne: { filter: { name: "John" } } }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([{
        deleteOne: { filter: { name: "John" } }
      }]);
    });

    it("should execute deleteMany rollback operation", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [{
        bulkWriteOperation: { deleteMany: { filter: { $or: [{ name: "John" }, { name: "Jane" }] } } }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.deep.equal([{
        deleteMany: { filter: { $or: [{ name: "John" }, { name: "Jane" }] } }
      }]);
    });

    it("should clean up rollback entries after successful rollback", async () => {
      const rollbackEntries = [{
        operation: "insertOne",
        collection: "users",
        parameters: { _id: "doc1", name: "John" }
      }];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.deleteMany.calledOnce).to.equal(true);
      expect(mockAutoRollbackCollection.deleteMany.getCall(0).args[0]).to.deep.equal({
        migrationFile: "test-migration.js"
      });
    });

    it("should execute multiple rollback operations in reverse order", async () => {
      mockAutoRollbackCollection.distinct.resolves(["users"]);
      
      const rollbackEntries = [
        { bulkWriteOperation: { insertOne: { _id: "doc3", name: "Bob" } } },
        { bulkWriteOperation: { deleteOne: { filter: { name: "Jane" } } } },
        { bulkWriteOperation: { insertOne: { _id: "doc1", name: "John" } } }
      ];

      mockAutoRollbackCollection.find.returns({
        sort: sinon.stub().returnsThis(),
        project: sinon.stub().returnsThis(),
        toArray: sinon.stub().resolves(rollbackEntries)
      });

      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      // Should execute all operations in one bulkWrite
      expect(mockCollection.bulkWrite.calledOnce).to.equal(true);
      const operations = mockCollection.bulkWrite.getCall(0).args[0];
      expect(operations).to.have.lengthOf(3);
    });
  });
});

