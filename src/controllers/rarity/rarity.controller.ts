//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class RarityController {
  public GetDistribution = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let data = await db
        .collection("tokens")
        .aggregate([
          {
            $match: {
              slug,
            },
          },
          {
            $facet: {
              rarity_type: [
                {
                  $group: {
                    _id: "$rarity_type",
                    tokens: {
                      $addToSet: "$token_id",
                    },
                  },
                },
                {
                  $project: {
                    _id: 0,
                    type: "$_id",
                    count: {
                      $size: "$tokens",
                    },
                  },
                },
              ],
              rarity_range: [
                {
                  $project: {
                    range: {
                      $concat: [
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 0],
                                },
                                {
                                  $lt: ["$normalized_score", 10],
                                },
                              ],
                            },
                            "0 - 10",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 10],
                                },
                                {
                                  $lt: ["$normalized_score", 20],
                                },
                              ],
                            },
                            "10 - 20",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 20],
                                },
                                {
                                  $lt: ["$normalized_score", 30],
                                },
                              ],
                            },
                            "20 - 30",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 30],
                                },
                                {
                                  $lt: ["$normalized_score", 40],
                                },
                              ],
                            },
                            "30 - 40",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 40],
                                },
                                {
                                  $lt: ["$normalized_score", 50],
                                },
                              ],
                            },
                            "40 - 50",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 50],
                                },
                                {
                                  $lt: ["$normalized_score", 60],
                                },
                              ],
                            },
                            "50 - 60",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 60],
                                },
                                {
                                  $lt: ["$normalized_score", 70],
                                },
                              ],
                            },
                            "60 - 70",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 70],
                                },
                                {
                                  $lt: ["$normalized_score", 80],
                                },
                              ],
                            },
                            "70 - 80",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 80],
                                },
                                {
                                  $lt: ["$normalized_score", 90],
                                },
                              ],
                            },
                            "80 - 90",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            {
                              $and: [
                                {
                                  $gte: ["$normalized_score", 90],
                                },
                                {
                                  $lte: ["$normalized_score", 100],
                                },
                              ],
                            },
                            "90 - 100",
                            "",
                          ],
                        },
                      ],
                    },
                  },
                },
                {
                  $group: {
                    _id: "$range",
                    count: {
                      $sum: 1,
                    },
                  },
                },
                {
                  $project: {
                    _id: 0,
                    range: "$_id",
                    count: "$count",
                  },
                },
                {
                  $sort: {
                    range: -1,
                  },
                },
              ],
            },
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetRarityVsVolume = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let data = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              event_type: "successful",
              slug: slug,
              total_price: {
                $nin: [null, "0", 0],
              },
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              token_id: 1,
              total_price: 1,
              slug: 1,
            },
          },
          {
            $group: {
              _id: "$token_id",
              volume: {
                $sum: {
                  $divide: [
                    {
                      $convert: {
                        input: "$total_price",
                        to: "double",
                      },
                    },
                    1000000000000000000,
                  ],
                },
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              localField: "_id",
              foreignField: "token_id",
              as: "token",
            },
          },
          {
            $unwind: {
              path: "$token",
            },
          },
          {
            $match: {
              "token.slug": slug,
            },
          },
          {
            $group: {
              _id: "$token.token_score",
              volume: {
                $push: "$volume",
              },
            },
          },
          {
            $project: {
              _id: 0,
              score: "$_id",
              volume: {
                $sum: "$volume",
              },
            },
          },
          {
            $sort: {
              score: 1,
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
        720
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetRarityVsMarketCap = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let data = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              slug: slug,
              event_type: "successful",
            },
          },
          {
            $project: {
              total_price: 1,
              token_id: 1,
            },
          },
          {
            $group: {
              _id: "$token_id",
              last_traded_price: {
                $last: {
                  $convert: {
                    input: "$total_price",
                    to: "double",
                  },
                },
              },
              floor_price: {
                $min: {
                  $convert: {
                    input: "$total_price",
                    to: "double",
                  },
                },
              },
            },
          },
          {
            $project: {
              token_id: "$_id",
              market_cap: {
                $add: ["$floor_price", "$last_traded_price"],
              },
            },
          },
          {
            $group: {
              _id: "$token_id",
              total_market_cap: {
                $sum: {
                  $divide: ["$market_cap", 1000000000000000000],
                },
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              localField: "_id",
              foreignField: "token_id",
              as: "token",
            },
          },
          {
            $unwind: {
              path: "$token",
            },
          },
          {
            $match: {
              "token.slug": slug,
            },
          },
          {
            $group: {
              _id: "$token.token_score",
              market_cap: {
                $push: "$total_market_cap",
              },
            },
          },
          {
            $project: {
              _id: 0,
              score: "$_id",
              market_cap: {
                $sum: "$market_cap",
              },
            },
          },
          {
            $sort: {
              token_score: 1,
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
        720
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetRarityVsFloor = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let data = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              slug: slug,
              event_type: "created",
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              ending_price: 1,
              slug: 1,
              token_id: 1,
            },
          },
          {
            $group: {
              _id: "$token_id",
              floor_price: {
                $min: {
                  $divide: [
                    {
                      $toDouble: "$ending_price",
                    },
                    1000000000000000000,
                  ],
                },
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              localField: "_id",
              foreignField: "token_id",
              as: "token",
            },
          },
          {
            $unwind: {
              path: "$token",
            },
          },
          {
            $match: {
              "token.slug": slug,
            },
          },
          {
            $group: {
              _id: "$token.token_score",
              floor_price: {
                $push: "$floor_price",
              },
            },
          },
          {
            $project: {
              _id: 0,
              score: "$_id",
              floor_price: {
                $min: "$floor_price",
              },
            },
          },
          {
            $sort: {
              score: 1,
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
        720
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetRarityVsSales = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let data = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              event_type: "successful",
              slug: slug,
              total_price: {
                $nin: [null, "0", 0],
              },
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              slug: 1,
              token_id: 1,
            },
          },
          {
            $group: {
              _id: "$token_id",
              sales: {
                $sum: 1,
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              localField: "_id",
              foreignField: "token_id",
              as: "token",
            },
          },
          {
            $unwind: {
              path: "$token",
            },
          },
          {
            $match: {
              "token.slug": slug,
            },
          },
          {
            $group: {
              _id: "$token.token_score",
              sales: {
                $push: "$sales",
              },
            },
          },
          {
            $project: {
              _id: 0,
              score: "$_id",
              sales: {
                $sum: "$sales",
              },
            },
          },
          {
            $sort: {
              score: 1,
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
        720
      );

      res.status(200).send({
        success: true,
        data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };
}
