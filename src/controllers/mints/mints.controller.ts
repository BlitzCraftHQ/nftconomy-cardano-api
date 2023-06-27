//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class MintsController {
  public GetAvgMintsPerWallet = async (req: Request, res: Response) => {
    const { slug } = req.body;
    try {
      const data = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug,
              from_address: "0x0000000000000000000000000000000000000000",
            },
          },
          {
            $group: {
              _id: {
                wallet: "$to_address",
              },
              count: {
                $sum: 1,
              },
            },
          },
          {
            $group: {
              _id: null,
              avg_mints: {
                $avg: "$count",
              },
            },
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data,
        }),
        2 * 1440
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (e) {
      res.status(500).send({
        success: false,
        error: e,
      });
    }
  };
}
