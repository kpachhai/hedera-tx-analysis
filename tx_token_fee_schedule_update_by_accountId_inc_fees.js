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

async function analyzeTokenFeeScheduleUpdates() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(process.env.DB);
  const collection = db.collection(process.env.COLLECTION);

  const fileName =
    "output/tx_token_fee_schedule_update_by_accountId_inc_fees.csv";

  const results = await collection
    .aggregate([
      {
        $match: {
          "body.tokenFeeScheduleUpdate": { $exists: true },
          "record.transactionFee": { $exists: true },
        },
      },
      {
        $project: {
          sigCount: 1,
          payer: "$body.transactionID.accountID.accountNum",
          transactionFee: { $toInt: "$record.transactionFee" },
          feeCount: {
            $size: "$body.tokenFeeScheduleUpdate.customFees",
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
          feeCount: 1,
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
            feeCount: "$feeCount",
          },
          txCount: { $sum: 1 },
          avgFee: { $avg: "$feeInHbar" },
          maxFee: { $max: "$feeInHbar" },
          minFee: { $min: "$feeInHbar" },
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

  console.table(results);

  await writeCSV(
    fileName,
    results.map((e) => ({
      numSignatures: e._id.sigCount,
      accountId: `0.0.${e._id.accountId}`,
      numCustomFees: e._id.feeCount,
      txCount: e.txCount,
      avgFee: e.avgFee,
      maxFee: e.maxFee,
      minFee: e.minFee,
    }))
  );

  await client.close();
}

analyzeTokenFeeScheduleUpdates();
