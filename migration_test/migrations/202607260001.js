module.exports = {
  async up(db) {
    // Enable auto-rollback from this point onward
    db.autoRollbackEnabled = true;
    await db.collection('test_collection').insertOne(
      {
        metaId: 1,
        text: "1111"
      });
  },

  async down(db) {
    // Automatically rolls back all tracked operations in reverse order
    await db.autoRollback();
  },
};