const { MongoClient } = require("mongodb");
require("dotenv").config();

// Connection URL
const client = new MongoClient(
  process.env.MONGO_DB_URL
    ? process.env.MONGO_DB_URL
    : "mongodb://localhost:27017"
);

const db = client.db(process.env.MONGO_DB_NAME || "nftconomy-cardano");

// Export db and client
export { db, client };
