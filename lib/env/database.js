const { MongoClient } = require("mongodb");
const _get = require("lodash.get");
const config = require("./config");
const { wrapDbWithAutoRollback } = require("./autoRollback");

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

    wrapDbWithAutoRollback(db, configContent, originalCollection);

    db.close = client.close;
    return {
      client,
      db,
    };
  }
};