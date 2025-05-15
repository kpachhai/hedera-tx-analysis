const { MongoClient, ObjectId } = require("mongodb");
const fs = require("fs");
const readline = require("readline");

const uri = process.env.MONGO_URI || "mongodb://localhost:27017";

async function importTransactions(filename) {
  const client = new MongoClient(uri);
  const dbName = "hedera";
  const collectionName = "transactions";

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const fileStream = fs.createReadStream(filename);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    let count = 0;
    let skipped = 0;

    for await (const line of rl) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      try {
        const doc = JSON.parse(trimmed);
        if (doc._id?.$oid) {
          doc._id = new ObjectId(doc._id.$oid);
        }
        await collection.insertOne(doc);
        count++;
      } catch (err) {
        skipped++;
        console.warn(
          `Skipped invalid JSON line (${skipped}): ${trimmed.slice(0, 100)}...`
        );
      }
    }

    console.log(`Imported ${count} transaction(s) from "${filename}".`);
    if (skipped > 0) {
      console.log(`Skipped ${skipped} invalid line(s).`);
    }
  } catch (err) {
    console.error("Import failed:", err);
  } finally {
    await client.close();
  }
}

const filename = process.argv[2];
if (!filename) {
  console.error("No filename provided.\nUsage: node import.js <filename>");
  process.exit(1);
}

importTransactions(filename);
