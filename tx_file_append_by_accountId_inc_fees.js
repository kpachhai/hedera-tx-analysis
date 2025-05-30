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

  const fileName = "output/tx_file_append_by_accountId_inc_fees.csv";

  const txTypeStats = await collection
    .aggregate([
      {
        $match: {
          "body.fileAppend": { $exists: true },
          sigCount: { $gt: 0 },
          "record.transactionFee": { $exists: true },
          "record.receipt.exchangeRate.currentRate": { $exists: true },
        },
      },
      {
        $project: {
          sigCount: 1,
          payer: "$body.transactionID.accountID.accountNum",
          fileSize: {
            $cond: {
              if: { $isArray: "$body.fileAppend.contents" },
              then: { $size: "$body.fileAppend.contents" },
              else: {
                $strLenCP: {
                  $ifNull: ["$body.fileAppend.contents", ""],
                },
              },
            },
          },
          numKeys: {
            $cond: {
              if: { $isArray: "$body.fileAppend.keys" },
              then: { $size: "$body.fileAppend.keys" },
              else: 0,
            },
          },
          transactionFee: {
            $toInt: "$record.transactionFee",
          },
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
          sigCount: 1,
          payer: 1,
          fileSize: 1,
          numKeys: 1,
          feeInHbar: {
            $multiply: ["$exchRate", "$transactionFee", 0.0000000001],
          },
        },
      },
      {
        $group: {
          _id: {
            sigCount: "$sigCount",
            accountId: "$payer",
            numKeys: "$numKeys",
          },
          txCount: { $sum: 1 },
          avgFee: { $avg: "$feeInHbar" },
          maxFee: { $max: "$feeInHbar" },
          minFee: { $min: "$feeInHbar" },
          avgFileSize: { $avg: "$fileSize" },
          maxFileSize: { $max: "$fileSize" },
          minFileSize: { $min: "$fileSize" },
        },
      },
      {
        $match: {
          txCount: { $gt: 1 },
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
      numSignatures: e._id.sigCount,
      accountId: `0.0.${e._id.accountId}`,
      numKeys: e._id.numKeys,
      txCount: e.txCount,
      avgFee: e.avgFee,
      maxFee: e.maxFee,
      minFee: e.minFee,
      avgFileSize: e.avgFileSize,
      maxFileSize: e.maxFileSize,
      minFileSize: e.minFileSize,
    }))
  );

  await client.close();
}

analyzeTransactions();
