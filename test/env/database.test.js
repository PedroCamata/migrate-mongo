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
      close: "theCloseFnFromMongoClient"
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
      expect(result.db.close).to.equal("theCloseFnFromMongoClient");
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
        expect(err.message).to.equal("Unable to connect");
      }
    });
  });

  describe("autoRollback feature", () => {
    let mockDb;
    let mockCollection;
    let mockAutoRollbackCollection;

    beforeEach(() => {
      // Create mock collections
      mockCollection = {
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
        collectionName: "testCollection"
      };

      mockAutoRollbackCollection = {
        insertOne: sinon.stub().resolves({ insertedId: "rollback1" }),
        find: sinon.stub().returns({
          sort: sinon.stub().returnsThis(),
          project: sinon.stub().returnsThis(),
          toArray: sinon.stub().resolves([])
        }),
        deleteMany: sinon.stub().resolves({ deletedCount: 1 })
      };

      // Create mock db
      mockDb = {
        collection: sinon.stub().callsFake((name) => {
          if (name === "autoRollback") {
            return mockAutoRollbackCollection;
          }
          return mockCollection;
        }),
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

      // Should return the original mock collection
      expect(collection).to.equal(mockCollection);
    });

    it("should not wrap collection methods when isRollback is true", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = true;

      const collection = result.db.collection("users");

      // Should return the original mock collection
      expect(collection).to.equal(mockCollection);
    });

    it("should not wrap excluded collections (changelog, lock, autoRollback)", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;

      const changelogCollection = result.db.collection("changelog");
      const lockCollection = result.db.collection("lock");
      const autoRollbackCollection = result.db.collection("autoRollback");

      // All should return the original mock collection
      expect(changelogCollection).to.equal(mockCollection);
      expect(lockCollection).to.equal(mockCollection);
      expect(autoRollbackCollection).to.equal(mockAutoRollbackCollection);
    });

    it("should store rollback entry when insertOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.insertOne({ name: "John" });

      // Verify rollback entry was created
      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("deleteOne");
      expect(rollbackEntry.collection).to.equal("testCollection");
      expect(rollbackEntry.migrationFile).to.equal("test-migration.js");
      expect(rollbackEntry.parameters).to.deep.equal({ name: "John" });
    });

    it("should store rollback entry when insertMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      const docs = [{ name: "John" }, { name: "Jane" }];
      await collection.insertMany(docs);

      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("deleteMany");
      expect(rollbackEntry.parameters).to.deep.equal(docs);
      expect(rollbackEntry.migrationFile).to.equal("test-migration.js");
    });

    it("should store rollback entry when updateOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.updateOne({ name: "John" }, { $set: { age: 30 } });

      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("updateOne");
      expect(rollbackEntry.parameters).to.deep.equal({ _id: "doc1", name: "test" });
    });

    it("should store rollback entries when updateMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.updateMany({ age: { $gt: 20 } }, { $set: { active: true } });

      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("updateMany");
      expect(rollbackEntry.parameters).to.deep.equal([
        { _id: "doc1", name: "test1" },
        { _id: "doc2", name: "test2" }
      ]);
    });

    it("should store rollback entry when deleteOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      await collection.deleteOne({ name: "John" });

      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("insertOne");
      expect(rollbackEntry.parameters).to.deep.equal({ _id: "doc1", name: "test" });
    });

    it("should store rollback entries when deleteMany is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";

      const collection = result.db.collection("users");
      const docsToDelete = [{ name: "John" }, { name: "Jane" }];
      await collection.deleteMany(docsToDelete);

      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("insertMany");
      expect(rollbackEntry.parameters).to.deep.equal([
        { _id: "doc1", name: "test1" },
        { _id: "doc2", name: "test2" }
      ]);
    });

    it("should store rollback entries when replaceOne is called", async () => {
      const result = await database.connect();
      result.db.autoRollbackEnabled = true;
      result.db.isRollback = false;
      result.db.migrationFile = "test-migration.js";
      
      const collection = result.db.collection("users");
      await collection.replaceOne({ name: "John" }, { name: "John", age: 40 });
      expect(mockAutoRollbackCollection.insertOne.calledOnce).to.equal(true);
      const rollbackEntry = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      
      expect(rollbackEntry.operation).to.equal("replaceOne");
      expect(rollbackEntry.parameters).to.deep.equal({ _id: "doc1", name: "test" });
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

      expect(mockAutoRollbackCollection.insertOne.callCount).to.equal(3);
      
      const entry1 = mockAutoRollbackCollection.insertOne.getCall(0).args[0];
      const entry2 = mockAutoRollbackCollection.insertOne.getCall(1).args[0];
      const entry3 = mockAutoRollbackCollection.insertOne.getCall(2).args[0];
      
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
        deleteMany: sinon.stub().resolves({ deletedCount: 2 })
      };

      mockAutoRollbackCollection = {
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

      client.db.returns(mockDb);
    });

    it("should not execute rollback when isRollback is false", async () => {
      const result = await database.connect();
      result.db.isRollback = false;

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.find.called).to.equal(false);
      expect(mockCollection.insertOne.called).to.equal(false);
      expect(mockCollection.insertMany.called).to.equal(false);
      expect(mockCollection.replaceOne.called).to.equal(false);
      expect(mockCollection.deleteOne.called).to.equal(false);
      expect(mockCollection.deleteMany.called).to.equal(false);
    });

    it("should fetch rollback entries for the current migration file", async () => {
      const result = await database.connect();
      result.db.isRollback = true;
      result.db.migrationFile = "test-migration.js";

      await result.db.autoRollback();

      expect(mockAutoRollbackCollection.find.calledOnce).to.equal(true);
      expect(mockAutoRollbackCollection.find.getCall(0).args[0]).to.deep.equal({
        migrationFile: "test-migration.js"
      });
    });

    it("should execute insertOne rollback operation", async () => {
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

      expect(mockCollection.insertOne.calledOnce).to.equal(true);
      expect(mockCollection.insertOne.getCall(0).args[0]).to.deep.equal({
        _id: "doc1",
        name: "John"
      });
    });

    it("should execute insertMany rollback operation", async () => {
      const rollbackEntries = [{
        operation: "insertMany",
        collection: "users",
        parameters: [
          { _id: "doc1", name: "John" },
          { _id: "doc2", name: "Jane" }
        ]
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

      expect(mockCollection.insertMany.calledOnce).to.equal(true);
      expect(mockCollection.insertMany.getCall(0).args[0]).to.deep.equal([
        { _id: "doc1", name: "John" },
        { _id: "doc2", name: "Jane" }
      ]);
    });

    it("should execute replaceOne rollback operation", async () => {
      const rollbackEntries = [{
        operation: "replaceOne",
        collection: "users",
        parameters: { _id: "doc1", name: "John", age: 25 }
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

      expect(mockCollection.replaceOne.calledOnce).to.equal(true);
      expect(mockCollection.replaceOne.getCall(0).args[0]).to.deep.equal({ _id: "doc1" });
      expect(mockCollection.replaceOne.getCall(0).args[1]).to.deep.equal({
        _id: "doc1",
        name: "John",
        age: 25
      });
    });

    it("should execute updateOne rollback operation", async () => {
      const rollbackEntries = [{
        operation: "updateOne",
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

      expect(mockCollection.replaceOne.calledOnce).to.equal(true);
      expect(mockCollection.replaceOne.getCall(0).args[0]).to.deep.equal({ _id: "doc1" });
      expect(mockCollection.replaceOne.getCall(0).args[1]).to.deep.equal({ _id: "doc1", name: "John" });
    });

    it("should execute updateMany rollback operation", async () => {
      const rollbackEntries = [{
        operation: "updateMany",
        collection: "users",
        parameters: [
          { _id: "doc1", name: "John" },
          { _id: "doc2", name: "Jane" }
        ]
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

      expect(mockCollection.replaceOne.callCount).to.equal(2);
      expect(mockCollection.replaceOne.getCall(0).args[0]).to.deep.equal({ _id: "doc1" });
      expect(mockCollection.replaceOne.getCall(1).args[0]).to.deep.equal({ _id: "doc2" });
    });

    it("should execute deleteOne rollback operation", async () => {
      const rollbackEntries = [{
        operation: "deleteOne",
        collection: "users",
        parameters: { name: "John" }
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

      expect(mockCollection.deleteOne.calledOnce).to.equal(true);
      expect(mockCollection.deleteOne.getCall(0).args[0]).to.deep.equal({ name: "John" });
    });

    it("should execute deleteMany rollback operation", async () => {
      const rollbackEntries = [{
        operation: "deleteMany",
        collection: "users",
        parameters: [{ name: "John" }, { name: "Jane" }]
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

      expect(mockCollection.deleteMany.calledOnce).to.equal(true);
      expect(mockCollection.deleteMany.getCall(0).args[0]).to.deep.equal(
        { $or: [{ name: "John" }, { name: "Jane" }] }
      );
    });

    it("should not execute invalid operations rollback operation", async () => {
      const rollbackEntries = [{
        operation: "invalidOperation",
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

      expect(mockCollection.insertOne.calledOnce).to.equal(false);
      expect(mockCollection.insertMany.calledOnce).to.equal(false);
      expect(mockCollection.replaceOne.calledOnce).to.equal(false);
      expect(mockCollection.deleteOne.calledOnce).to.equal(false);
      expect(mockCollection.deleteMany.calledOnce).to.equal(false);
      expect(mockCollection.updateOne.calledOnce).to.equal(false);
      expect(mockCollection.updateMany.calledOnce).to.equal(false);
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
      const rollbackEntries = [
        { operation: "insertOne", collection: "users", parameters: { _id: "doc3", name: "Bob" } },
        { operation: "deleteOne", collection: "users", parameters: { name: "Jane" } },
        { operation: "insertOne", collection: "users", parameters: { _id: "doc1", name: "John" } }
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

      // Should execute all operations
      expect(mockCollection.insertOne.callCount).to.equal(2);
      expect(mockCollection.deleteOne.callCount).to.equal(1);
    });
  });
});

