// const { default: autoRollback } = require("../lib/env/autoRollback");

const config = {
  mongodb: {
    url: "mongodb://root:root@localhost:27017",

    databaseName: "mtest", // Short name to make it easier to drop

    options: {}
  },
  migrationsDir: "migrations",
  changelogCollectionName: "changelog",
  lockCollectionName: "changelog_lock",
  lockTtl: 0,
  migrationFileExtension: ".js",
  useFileHash: false,
  moduleSystem: 'commonjs',

  // TODO: This config is not created automatically, it should
  autoRollbackCollectionName: "autoRollback"
};

module.exports = config;
