const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");
require("dotenv").config();

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

  const fileName = "output/tx_file_delete_by_accountId_inc_fees.csv";

  const txTypeStats = await collection
    .aggregate([
      {
        $match: {
          "body.fileDelete": { $exists: true },
          sigCount: { $gt: 0 },
          "record.transactionFee": { $exists: true },
          "record.receipt.exchangeRate.currentRate": { $exists: true },
        },
      },
      {
        $project: {
          sigCount: 1,
          payer: "$body.transactionID.accountID.accountNum",
          transactionFee: { $toInt: "$record.transactionFee" },
          exchRate: {
            $divide: [
              "$record.receipt.exchangeRate.currentRate.centEquiv",
              "$record.receipt.exchangeRate.currentRate.hbarEquiv",
            ],
          },
        },
      },
      {
        $project: {
          payer: 1,
          sigCount: 1,
          feeInHbar: {
            $multiply: ["$exchRate", "$transactionFee", 0.0000000001],
          },
        },
      },
      {
        $group: {
          _id: {
            accountId: "$payer",
            sigCount: "$sigCount",
          },
          txCount: { $sum: 1 },
          avgFee: { $avg: "$feeInHbar" },
          maxFee: { $max: "$feeInHbar" },
          minFee: { $min: "$feeInHbar" },
        },
      },
      {
        $sort: {
          "_id.sigCount": 1,
          "_id.accountId": 1,
        },
      },
    ])
    .toArray();

  console.table(txTypeStats);

  await writeCSV(
    fileName,
    txTypeStats.map((e) => ({
      accountId: `0.0.${e._id.accountId}`,
      executedSigVerifications: e._id.sigCount,
      txCount: e.txCount,
      avgFee: e.avgFee,
      maxFee: e.maxFee,
      minFee: e.minFee,
    }))
  );

  await client.close();
}

analyzeTransactions();
