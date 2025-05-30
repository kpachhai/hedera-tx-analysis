const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");
require("dotenv").config();

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

  const fileName = "output/tx_crypto_transfer_by_accountId_inc_fees.csv";
  const txTypeStats = await collection
    .aggregate([
      {
        $match:
          /**
           * query: The query in MQL.
           */
          {
            "body.cryptoTransfer.transfers.accountAmounts": {
              $exists: {},
            },
            "body.cryptoTransfer.tokenTransfers": {
              $exists: false,
            },
            sigCount: {
              $gt: 1,
            },
            $expr: {
              $gt: [
                {
                  $size: "$body.cryptoTransfer.transfers.accountAmounts",
                },
                2,
              ],
            },
          },
      },
      {
        $project: {
          sigCount: 1,
          body: 1,
          record: 1,
          accountAmounts: {
            $size: "$body.cryptoTransfer.transfers.accountAmounts",
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
        $group: {
          _id: {
            sigCount: "$sigCount",
            accountAmounts: "$accountAmounts",
            accountId: "$body.transactionID.accountID.accountNum",
          },
          txCount: {
            $sum: 1,
          },
          avgAmount: {
            $avg: {
              $multiply: ["$exchRate", "$transactionFee", 0.0000000001],
            },
          },
          maxAmount: {
            $max: {
              $multiply: ["$exchRate", "$transactionFee", 0.0000000001],
            },
          },
          minAmount: {
            $min: {
              $multiply: ["$exchRate", "$transactionFee", 0.0000000001],
            },
          },
        },
      },
      {
        $match: {
          txCount: {
            $gt: 1,
          },
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
      accountAmounts: e._id.accountAmounts,
      txCount: e.txCount,
      avgFee: e.avgAmount,
      maxFee: e.maxAmount,
      minFee: e.minAmount,
    }))
  );

  await client.close();
}

analyzeTransactions();
