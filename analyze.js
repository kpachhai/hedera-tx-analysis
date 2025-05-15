const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");

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
  const db = client.db("hedera");
  const collection = db.collection("transactions");

  // 1. Transaction Type vs Signature Count
  console.log("\nTransaction Type vs Number of Signatures:");
  const txTypeStats = await collection
    .aggregate([
      {
        $project: {
          numSignatures: { $size: "$signatures" },
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
    "output/transaction_type_signature_stats.csv",
    txTypeStats.map((e) => ({
      transactionType: e._id.type,
      numSignatures: e._id.numSignatures,
      count: e.count,
    }))
  );

  // 2. Token Creation Compliance Key Stats
  console.log("\nToken Creation Compliance Key Stats (per key per line):");

  const complianceKeys = [
    "supplyKey",
    "kycKey",
    "freezeKey",
    "wipeKey",
    "pauseKey",
    "feeScheduleKey",
    "adminKey",
  ];

  const keyPresenceStats = await collection
    .aggregate([
      { $match: { "body.tokenCreation": { $exists: true } } },
      {
        $project: {
          numSignatures: { $size: "$signatures" },
          tokenKeys: {
            $map: {
              input: { $objectToArray: "$body.tokenCreation" },
              as: "field",
              in: "$$field.k",
            },
          },
        },
      },
      {
        $project: {
          numSignatures: 1,
          presentKeys: {
            $filter: {
              input: complianceKeys,
              as: "key",
              cond: { $in: ["$$key", "$tokenKeys"] },
            },
          },
        },
      },
      { $unwind: "$presentKeys" },
      {
        $group: {
          _id: {
            numSignatures: "$numSignatures",
            key: "$presentKeys",
          },
          count: { $sum: 1 },
        },
      },
      { $sort: { "_id.numSignatures": 1, "_id.key": 1 } },
    ])
    .toArray();

  // Transform for display/export
  const flattenedStats = keyPresenceStats.map((stat) => ({
    numSignatures: stat._id.numSignatures,
    key: stat._id.key,
    count: stat.count,
  }));

  console.table(flattenedStats);

  await writeCSV(
    "output/token_creation_compliance_key_per_key_stats.csv",
    flattenedStats
  );

  // 3. Distinct transaction types
  console.log("\nDetected Transaction Types:");
  const types = await collection
    .aggregate([
      {
        $project: {
          bodyKeys: { $objectToArray: "$body" },
        },
      },
      { $unwind: "$bodyKeys" },
      {
        $match: {
          "bodyKeys.k": {
            $nin: [
              "transactionID",
              "nodeAccountID",
              "transactionFee",
              "transactionValidDuration",
              "memo",
            ],
          },
        },
      },
      {
        $group: {
          _id: "$bodyKeys.k",
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1 } },
    ])
    .toArray();

  console.table(types);
  await writeCSV(
    "output/distinct_transaction_types.csv",
    types.map((e) => ({
      transactionType: e._id,
      count: e.count,
    }))
  );

  await client.close();
}

analyzeTransactions();
