# Hedera Transaction Analysis

Analyze Hedera `transactions.json` data exported from MongoDB with deep insights into:

- Transaction types and signature counts
- Token creation compliance key usage (supplyKey, kycKey, etc.)
- CSV exports for all queries

Works with any MongoDB instance using Docker or remote URI. Supports analysis-only mode (no file import) and optional MongoDB resets.

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/kpachhai/hedera-tx-analysis.git
cd hedera-tx-analysis
npm install
```

### 2. Add your MongoDB export

Put your `transactions.json` file (one JSON document per line) in the project root. Look at the `sample.json` file to look at how the data should be formatted.

---

## Run the Full Pipeline

```bash
./run.sh transactions.json
```

This will:

- Spin up MongoDB via Docker (if using local URI)
- Import `transactions.json`
- Run analytics
- Export CSV files to the `output/` directory
- Print results to the console

---

## Reset MongoDB Before Importing

```bash
./run.sh transactions.json --reset
```

This drops the `hedera` database before importing fresh data.

---

## Analysis Only (No Import)

```bash
./run.sh
```

This runs the analysis on the current contents of the MongoDB `hedera.transactions` collection.

---

## Configuration

### Use Remote MongoDB URI

1. Copy a environment file:

```
cp .env.example .env
```

Then, configure your own mongodb(remote mongodb or whatever you prefer). Leave the default as it is if you want to use the local mongodb running via docker container.

```
MONGO_URI=mongodb+srv://youruser:yourpass@yourcluster.mongodb.net
```

2. Or export the variable:

```bash
export MONGO_URI=mongodb://localhost:27017
```

---

## Output Files (CSV)

All results are written to `output/`:

| Filename                                          | Description                                                     |
| ------------------------------------------------- | --------------------------------------------------------------- |
| `transaction_type_signature_stats.csv`            | Transaction type vs. number of signatures                       |
| `token_creation_compliance_key_per_key_stats.csv` | Per-key breakdown (supplyKey, kycKey, etc.) per signature count |
| `distinct_transaction_types.csv`                  | All unique transaction types detected                           |

---

## Sample Output

### Transaction Type vs Signature Count

| transactionType | numSignatures | count |
| --------------- | ------------- | ----- |
| cryptoTransfer  | 1             | 103   |
| tokenCreation   | 2             | 12    |

---

### Token Creation Compliance Key Stats (Per Key)

| numSignatures | key       | count |
| ------------- | --------- | ----- |
| 1             | supplyKey | 10    |
| 1             | kycKey    | 5     |
| 2             | adminKey  | 3     |

---

## Development Notes

- Supports MongoDB exports with `$oid` fields
- Handles blank/malformed JSON lines gracefully
- Modular pipeline (each script can run standalone)

---

## Testing

To just import or analyze manually:

```bash
node import.js transactions.json
node analyze.js
```

---

## Cleanup

If needed manually, stop and remove the Docker container:

```bash
docker-compose down
```

---

## License

MIT – use it freely.

---
