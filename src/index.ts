import * as dotenv from "dotenv";
import { client } from "./utilities/mongo";

import { redisClient } from "./utilities/redis";
redisClient.connect();

// const cron = require("node-cron");
// import { cache } from "./utilities/preCache";

dotenv.config();

import server from "./api";
let environment = process.env.DEPLOY_ENV || "DEVELOPMENT";

// cron.schedule("0 */6 * * *", function () {
//   if (environment == "PRODUCTION") {
//     cache();
//   }
// });

server.listen(parseInt(process.env.PORT || "5000"), "0.0.0.0", () => {
  console.log(
    `The API server has successfully started. \nListening at http://localhost:${
      process.env.PORT || "5000"
    }`
  );
});

process.on("SIGINT", function () {
  //  redisClient.quit();
  console.log("Redis Disconnected");
  client.close();
  process.exit(0);
});
