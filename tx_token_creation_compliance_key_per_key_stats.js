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

  // 2. Token Creation Compliance Key Stats
  console.log("\nToken Creation Compliance Key Stats (per key per line):");
  const fileName = "output/token_creation_compliance_key_per_key_stats.csv";

  const keyPresenceStats = await collection
    .aggregate(
        [
          {
            $match: {
              "body.tokenCreation": {
                $exists: true
              }
            }
          },
          {
            $project: {
              tokenKeys: {
                $map: {
                  input: {
                    $objectToArray: "$body.tokenCreation"
                  },
                  as: "field",
                  in: "$$field.k"
                }
              }
            }
          },
          {
            $project: {
              presentKeys: {
                $filter: {
                  input: [
                    "supplyKey",
                    "kycKey",
                    "freezeKey",
                    "wipeKey",
                    "pauseKey",
                    "feeScheduleKey",
                    "adminKey",
                    "metadataKey"
                  ],
                  as: "key",
                  cond: {
                    $in: ["$$key", "$tokenKeys"]
                  }
                }
              }
            }
          },
          {
            $unwind: "$presentKeys"
          },
          {
            $group:
            /**
             * _id: The id of the group.
             * fieldN: The first field name.
             */
                {
                  _id: "$_id",
                  numKeys: {
                    $count: {}
                  }
                }
          },
          {
            $project:
            /**
             * specifications: The fields to
             *   include or exclude.
             */
                {
                  _id: "$_id",
                  numKeys: "$numKeys"
                }
          },
          {
            $group:
                {
                  _id: "$numKeys",
                  numTx: {
                    $count: {}
                  }
                }
          },
          {
            $sort: {
              numTx: 1
            }
          }
        ]
        )
    .toArray();

  // Transform for display/export
  const flattenedStats = keyPresenceStats.map((stat) => ({
    numKeys: stat._id,
    txCount: stat.numTx,
  }));

  console.table(flattenedStats);

  await writeCSV(
    fileName,
    flattenedStats
  );
  await client.close();
}

analyzeTransactions();
