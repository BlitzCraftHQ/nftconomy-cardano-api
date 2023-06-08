import { Request, Response, NextFunction } from "express";
import { uniqueKey } from "../utilities/redis";
import { getCache } from "../utilities/redis";

export const redisCache = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<any> => {
  let redisKey = uniqueKey(req);
  let isCache = req.query.cache;
  if (isCache == "false") {
    next();
  } else {
    const cachedData = await getCache(redisKey);
    if (cachedData) {
      console.log("From Cache");
      return res.status(200).send(JSON.parse(cachedData));
    } else {
      next();
    }
  }
};
