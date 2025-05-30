const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");
require('dotenv').config();

//from Thu May 15 2025 06:43:14 GMT+0000
//to Fri May 16 2025 06:58:24 GMT+0000
const uri = process.env.MONGO_URI || "mongodb://localhost:27017";

async function writeCSV(filename, records) {
  const csvWriter = createObjectCsvWriter({
    path: filename,
    header: Object.keys(records[0] || {}).map((key) => ({
      id: key,
      title: key,
    })),
  });

  await csvWriter.writeRecords(records);
  console.log(`Exported to ${filename}`);
}

async function analyzeTransactions() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(process.env.DB);
  const collection = db.collection(process.env.COLLECTION);

  // Transaction Type vs Signature Count
  console.log("\nTransaction Type vs Number of Signatures:");
  const fileName = "output/transaction_type_signatures_count.csv";
  const txTypeStats = await collection
    .aggregate([
      {
        $project: {
          numSignatures: "$sigCount",
          transactionTypes: { $objectToArray: "$body" },
        },
      },
      {
        $project: {
          numSignatures: 1,
          transactionType: {
            $arrayElemAt: [
              {
                $filter: {
                  input: "$transactionTypes",
                  as: "entry",
                  cond: {
                    $and: [
                      { $ne: ["$$entry.k", "transactionID"] },
                      { $ne: ["$$entry.k", "nodeAccountID"] },
                      { $ne: ["$$entry.k", "transactionFee"] },
                      { $ne: ["$$entry.k", "transactionValidDuration"] },
                      { $ne: ["$$entry.k", "memo"] },
                    ],
                  },
                },
              },
              0,
            ],
          },
        },
      },
      {
        $group: {
          _id: {
            type: "$transactionType.k",
            numSignatures: "$numSignatures",
          },
          count: { $sum: 1 },
        },
      },
      { $sort: { "_id.type": 1, "_id.numSignatures": 1 } },
    ])
    .toArray();

  console.table(txTypeStats);
  await writeCSV(
    fileName,
    txTypeStats.map((e) => ({
      transactionType: e._id.type,
      numSignatures: e._id.numSignatures,
      count: e.count,
    }))
  );

  await client.close();
}

analyzeTransactions();
