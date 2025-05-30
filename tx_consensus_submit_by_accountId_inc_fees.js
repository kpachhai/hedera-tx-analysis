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

  const fileName = "output/tx_consensus_submit_by_accountId_inc_fees.csv";
  const txTypeStats = await collection
    .aggregate(  [
        {
            $match:
            /**
             * query: The query in MQL.
             */
                {
                    // "body.transactionID.accountID.accountNum": {
                    //   $eq: "1682785"
                    // },
                    "body.consensusSubmitMessage": {
                        $exists: {}
                    }
                    // sigCount: {
                    //   $gt: 1
                    // },
                    // "sigCount": {
                    //   $eq: 6
                    // },
                    // "body.transactionID.transactionValidStart.seconds":
                    //   {
                    //     $eq: "1747312529"
                    //   },
                    // "body.transactionID.transactionValidStart.nanos":
                    //   {
                    //     $eq: 105541049
                    //   }
                }
        },
        {
            $project:
            /**
             * specifications: The fields to
             *   include or exclude.
             */
                {
                    sigCount: 1,
                    body: 1,
                    record: 1,
                    msgLength: {
                        $multiply: [
                            0.75,
                            {
                                $strLenCP:
                                    "$body.consensusSubmitMessage.message"
                            }
                        ]
                    },
                    transactionFee: {
                        $toInt: "$record.transactionFee"
                    },
                    exchRate: {
                        $divide: [
                            "$record.receipt.exchangeRate.currentRate.centEquiv",
                            "$record.receipt.exchangeRate.currentRate.hbarEquiv"
                        ]
                    }
                }
        },
        {
            $group: {
                _id: {
                    sigCount: "$sigCount",
                    accountId:
                        "$body.transactionID.accountID.accountNum"
                },
                txCount: {
                    $sum: 1
                },
                avgAmount: {
                    $avg: {
                        $multiply: [
                            "$exchRate",
                            "$transactionFee",
                            0.0000000001
                        ]
                    }
                },
                maxAmount: {
                    $max: {
                        $multiply: [
                            "$exchRate",
                            "$transactionFee",
                            0.0000000001
                        ]
                    }
                },
                minAmount: {
                    $min: {
                        $multiply: [
                            "$exchRate",
                            "$transactionFee",
                            0.0000000001
                        ]
                    }
                },
                avgLength: {
                    $avg: "$msgLength"
                },
                maxLength: {
                    $max: "$msgLength"
                },
                minLength: {
                    $min: "$msgLength"
                }
            }
        }
        // {
        //   $match: {
        //     txCount: {
        //       $gt: 1
        //     }
        //   }
        // }
    ])
    .toArray();

  console.table(txTypeStats);
  await writeCSV(
    fileName,
    txTypeStats.map((e) => ({
      numSignatures: e._id.sigCount,
      accountId: `0.0.${e._id.accountId}`,
      txCount: e.txCount,
      avgFee: e.avgAmount,
      maxFee: e.maxAmount,
      minFee: e.minAmount,
      avgLength: e.avgLength,
      maxLength: e.maxLength,
      minLength: e.minLength
    }))
  );

  await client.close();
}

analyzeTransactions();
