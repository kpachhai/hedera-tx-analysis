const { MongoClient } = require("mongodb");
const { createObjectCsvWriter } = require("csv-writer");
require('dotenv').config();

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

  console.log("\nNumber of FT approvals per transaction count:");
  const fileName = "output/ft_approvals_transaction_count_stats.csv";

  const keyPresenceStats = await collection
    .aggregate(
        [
            {
                $match:
                    {
                        "body.cryptoApproveAllowance.tokenAllowances":
                            {
                                $exists: {}
                            }
                    }
            },
            {
                $project:
                    {
                        _id: 0,
                        body: 1,
                        numTokens: {
                            $size:
                                "$body.cryptoApproveAllowance.tokenAllowances"
                        }
                    }
            },
            {
                $match:
                    {
                        numTokens: {
                            $gte: 1
                        }
                    }
            },
            {
                $group:
                    {
                        _id: {
                            numTokens: "$numTokens"
                        },
                        txCount: {
                            $count: {}
                        }
                    }
            }
            ]
        )
    .toArray();

  // Transform for display/export
  const flattenedStats = keyPresenceStats.map((stat) => ({
    numTokens: stat._id.numTokens,
    txCount: stat.txCount,
  }));

  console.table(flattenedStats);

  await writeCSV(
    fileName,
    flattenedStats
  );
  await client.close();
}

analyzeTransactions();
