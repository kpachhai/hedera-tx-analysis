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

  const fileName = "output/tx_token_transfer_by_accountId_inc_fees.csv";
  const txTypeStats = await collection
    .aggregate(  [
        {
            $project: {
                sigCount: 1,
                body: 1,
                record: 1
            }
        },
        {
            $match:
            /**
             * query: The query in MQL.
             */
                {
                    "body.cryptoTransfer": {
                        $exists: {}
                    },
                    "body.cryptoTransfer.tokenTransfers": {
                        $exists: {}
                    }
                    // sigCount: {
                    //   $gt: 1
                    // }
                }
        },
        // {
        //   $match:
        //     /**
        //      * query: The query in MQL.
        //      */
        //     {
        //       "body.cryptoTransfer": {
        //         $exists: {}
        //       },
        //       "body.cryptoTransfer.tokenTransfers": {
        //         $exists: {}
        //       },
        //       "body.transactionID.accountID.accountNum": {
        //         $eq: "610168"
        //       },
        //       "body.transactionID.transactionValidStart.seconds":
        //         {
        //           $eq: "1747312529"
        //         },
        //       "body.transactionID.transactionValidStart.nanos":
        //         {
        //           $eq: 105541049
        //         }
        //     }
        // }
        {
            $project: {
                sigCount: "$sigCount",
                tokenTransfers: {
                    $size:
                        "$body.cryptoTransfer.tokenTransfers"
                },
                nftTransfers: {
                    $size:
                        "$body.cryptoTransfer.tokenTransfers.nftTransfers"
                },
                autoAssociations: {
                    $cond: {
                        if: {
                            $eq: [
                                0,
                                {
                                    $ifNull: [
                                        "$record.automaticTokenAssociations",
                                        0
                                    ]
                                }
                            ]
                        },
                        then: 0,
                        else: {
                            $size:
                                "$record.automaticTokenAssociations"
                        }
                    }
                },
                body: 1,
                record: 1,
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
            $match: {
                $or: [
                    {
                        tokenTransfers: {
                            $gte: 1
                        }
                    },
                    {
                        nftTransfers: {
                            $gte: 1
                        }
                    }
                ]
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
                    tokenTransfers: "$tokenTransfers",
                    nftTransfers: "$nftTransfers",
                    exchRate: "$exchRate",
                    transactionFee: "$transactionFee",
                    autoAssociations: "$autoAssociations",
                    cost: {
                        $multiply: [
                            "$exchRate",
                            "$transactionFee",
                            0.0000000001
                        ]
                    }
                    // autoAssociations: {
                    //   $cond: {
                    //     if: {
                    //       $eq: [
                    //         0,
                    //         {
                    //           $ifNull: [
                    //             "$record.automaticTokenAssociations",
                    //             0
                    //           ]
                    //         }
                    //       ]
                    //     },
                    //     then: 0,
                    //     else: {
                    //       $size: "$record.automaticTokenAssociations"
                    //     }
                    //   }
                    // }
                }
        },
        {
            $match:
            /**
             * query: The query in MQL.
             */
                {
                    cost: {
                        $ne: null
                    }
                }
        },
        {
            $group: {
                _id: {
                    sigCount: "$sigCount",
                    accountId:
                        "$body.transactionID.accountID.accountNum",
                    nftTransfers: "$nftTransfers",
                    tokenTransfers: "$tokenTransfers",
                    autoAssociations: "$autoAssociations"
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
                }
            }
        },
        {
            $match: {
                txCount: {
                    $gt: 1
                }
            }
        }
    ])
    .toArray();

  console.table(txTypeStats);
  await writeCSV(
    fileName,
    txTypeStats.map((e) => ({
      numSignatures: e._id.sigCount,
      accountId: `0.0.${e._id.accountId}`,
      nftTransfers: e._id.nftTransfers,
      tokenTransfers: e._id.tokenTransfers,
        autoAssociations: e._id.autoAssociations,
      txCount: e.txCount,
      avgFee: e.avgAmount,
      maxFee: e.maxAmount,
      minFee: e.minAmount
    }))
  );

  await client.close();
}

analyzeTransactions();
