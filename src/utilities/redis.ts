import { Request } from "express";
const { createClient } = require("redis");
require("dotenv").config();

export const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on("error", (err) => console.log("Redis Client Error", err));

export function uniqueKey(req: Request) {
  let key = removeURLParameter(req.originalUrl, "cache");

  let axiosKey = decodeURIComponent(key);
  let cacheKey = axiosKey.replace(/[^A-Z0-9]/gi, "_");
  return cacheKey;
}

export async function setCache(key, value, minutes = 1440) {
  await redisClient.set(key, value, {
    EX: minutes * 60, // cache time in seconds
  });
  console.log("Cache Set");
}

export async function getCache(key) {
  let data = await redisClient.get(key);
  return data;
}

function removeURLParameter(url, parameter) {
  //prefer to use l.search if you have a location/link object
  var urlparts = url.split("?");
  if (urlparts.length >= 2) {
    var prefix = encodeURIComponent(parameter) + "=";
    var pars = urlparts[1].split(/[&;]/g);

    // reverse iteration as may be destructive
    for (var i = pars.length; i-- > 0; ) {
      // idiom for string.startsWith
      if (pars[i].lastIndexOf(prefix, 0) !== -1) {
        pars.splice(i, 1);
      }
    }

    return urlparts[0] + (pars.length > 0 ? "?" + pars.join("&") : "");
  }
  return url;
}
