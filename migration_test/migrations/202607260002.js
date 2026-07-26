module.exports = {
  async up(db) {
    // Enable auto-rollback from this point onward
    db.autoRollbackEnabled = true;
    await db.collection('test_collection').updateOne(
      {metaId: 1},
      {$set: {text: "2222"}});

    
  },

  async down(db) {
    // Automatically rolls back all tracked operations in reverse order
    await db.autoRollback();
  },
};