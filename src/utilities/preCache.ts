import axios from "axios";

import { getCache } from "./redis";

let baseUrl = "http://localhost:3005";
let environment = process.env.ENVIRONMENT;

let endpoints = [
  "/v2/collections/listing?sortBy=tokenomic_score&category=All&pageSize=15&cache=false",

  "/v2/collections/listing?sortBy=tokenomic_score&page=1&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=1&category=All&cache=false",

  "/v2/market/top-collections?sortBy=market_cap&cache=false",
  "/v2/market/top-collections?sortBy=volume&cache=false",

  "/v2/market/category-stats?by=market_cap&cache=false",
  "/v2/market/category-stats?by=volume&cache=false",
  "/v2/market/category-stats?by=liquidity&cache=false",

  "/v2/market/collection-distribution?by=volume&cache=false",
  "/v2/market/collection-distribution?by=market_cap&cache=false",

  "/v2/market/sentiment?cache=false",

  "/v2/market/top-sales?time=all&category=All&page=1&cache=false",

  "/v2/market/price?time=30d&cache=false",
  "/v2/market/volume?time=30d&cache=false",
  "/v2/market/floor-price?time=30d&cache=false",
  "/v2/market/sales-liquidity?time=30d&cache=false",
  "/v2/market/transfers?time=30d&cache=false",
  "/v2/market/market-cap?time=30d&cache=false",
  "/v2/market/traders-count?cache=false",

  "/v2/news/google-news?with_images=1&keyword=nft&cache=false",

  "/v2/whales/list?cache=false",

  "/v2/whales/activity/trend?time=30d&cache=false",

  "/v2/whales/trade/whales-involved?time=30d&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=30d&type=sell&page=1&cache=false",

  "/v2/whales/trade/whales-bought?time=30d&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=30d&page=1&type=sellcache=false",

  "/v2/whales/trade/top-buyers?time=30d&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=30d&page=1&cache=false",

  "/v2/whales/mint/most-minted?time=24h&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=24h&page=1&cache=false",

  "/v2/whales/activity/all?type=all&page=1&cache=false",
  "/v2/whales/activity/all?type=sell&page=1&cache=false",
  "/v2/whales/activity/all?type=buy&page=1&cache=false",
  "/v2/whales/activity/all?type=mint&page=1&cache=false",
  "/v2/whales/activity/all?type=burn&page=1&cache=false",

  "/v2/collections/listing?sortBy=tokenomic_score&page=2&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=3&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=4&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=5&category=All&cache=false",

  "/v2/collections/listing?sortBy=created_date&page=2&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=3&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=4&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=5&category=All&cache=false",

  "/v2/collections/listing?sortBy=tokenomic_score&page=6&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=7&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=8&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=9&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=10&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=11&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=12&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=13&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=14&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=15&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=16&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=17&category=All&cache=false",
  "/v2/collections/listing?sortBy=tokenomic_score&page=18&category=All&cache=false",

  "/v2/collections/listing?sortBy=created_date&page=6&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=7&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=8&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=9&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=10&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=11&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=12&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=13&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=14&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=15&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=16&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=17&category=All&cache=false",
  "/v2/collections/listing?sortBy=created_date&page=18&category=All&cache=false",

  "/v2/whales/activity/trend?time=24h&cache=false",
  "/v2/whales/activity/trend?time=7d&cache=false",

  "/v2/whales/trade/whales-involved?time=5m&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=10m&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=30m&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=1h&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=24h&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=7d&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=30d&type=sell&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=5m&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=10m&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=30m&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=1h&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=24h&type=buy&page=1&cache=false",
  "/v2/whales/trade/whales-involved?time=7d&type=buy&page=1&cache=false",

  "/v2/whales/trade/whales-bought?time=5m&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=10m&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=30m&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=1h&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=12h&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=24h&page=1&type=buy&cache=false",
  "/v2/whales/trade/whales-bought?time=7d&page=1&type=buy&cache=false",

  "/v2/whales/trade/whales-bought?time=5m&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=10m&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=30m&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=1h&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=12h&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=24h&page=1&type=sell&cache=false",
  "/v2/whales/trade/whales-bought?time=7d&page=1&type=sell&cache=false",

  "/v2/whales/trade/top-buyers?time=5m&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=10m&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=30m&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=1h&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=12h&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=24h&page=1&cache=false",
  "/v2/whales/trade/top-buyers?time=7d&page=1&cache=false",

  "/v2/whales/trade/top-sellers?time=5m&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=10m&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=30m&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=1h&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=12h&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=24h&page=1&cache=false",
  "/v2/whales/trade/top-sellers?time=7d&page=1&cache=false",

  "/v2/whales/mint/most-minted?time=5m&page=1&cache=false",
  "/v2/whales/mint/most-minted?time=10m&page=1&cache=false",
  "/v2/whales/mint/most-minted?time=30m&page=1&cache=false",
  "/v2/whales/mint/most-minted?time=1h&page=1&cache=false",
  "/v2/whales/mint/most-minted?time=12h&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=5m&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=10m&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=30m&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=1h&page=1&cache=false",
  "/v2/whales/mint/top-minters?time=12h&page=1&cache=false",
];

// import { redisClient } from "../utilities/redis-helper";
// redisClient.connect();

export async function cache() {
  for (let i = 0; i < endpoints.length; i++) {
    let key = `${endpoints[i]}`.replace("&cache=false", "");
    let axiosKey = decodeURIComponent(key);
    let cacheKey = axiosKey.replace(/[^A-Z0-9]/gi, "_");

    let cache = await getCache(cacheKey);

    let hardCache;

    if (environment == "PRODUCTION") {
      hardCache = true;
    } else {
      hardCache = false;
    }

    if (!cache || hardCache) {
      console.log(`Caching ${baseUrl}${endpoints[i]}`);
      await axios.get(`${baseUrl}${endpoints[i]}`).catch((err) => {
        console.log(err);
      });
    } else {
      console.log("Already cached");
    }
  }
}

// cache();
