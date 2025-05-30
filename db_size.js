const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");
require('dotenv').config();

const uri = process.env.MONGO_URI || "mongodb://localhost:27017";

async function analyzeTransactions() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(process.env.DB);

    let totalSize = 0
    db.admin().listDatabases().then(async databases => {
        console.log(`total size: ${databases.totalSize}`);
        await client.close();
    });

}

analyzeTransactions();
