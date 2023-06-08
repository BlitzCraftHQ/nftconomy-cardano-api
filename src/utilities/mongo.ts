import { MongoClient } from "mongodb";
require("dotenv").config();

// Connection URL
export const client = new MongoClient(
  process.env.MONGO_DB_URL
    ? process.env.MONGO_DB_URL
    : "mongodb://localhost:27017"
);

export const db = client.db(process.env.MONGO_DB_NAME || "nftconomy");
