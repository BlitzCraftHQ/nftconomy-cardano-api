import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { structure } from "../../helpers/marketOverview";
import * as dayjs from "dayjs";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";
import axios from "axios";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class MarketController {
  public GetMarketSentiment = async (req: Request, res: Response) => {
    try {
      const volumeSentiment = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              event_type: "successful",
            },
          },
          {
            $group: {
              _id: {
                year: {
                  $year: {
                    $toDate: "$created_date",
                  },
                },
                month: {
                  $month: {
                    $toDate: "$created_date",
                  },
                },
                day: {
                  $dayOfMonth: {
                    $toDate: "$created_date",
                  },
                },
              },
              volume: {
                $sum: {
                  $divide: [{ $toDouble: "$total_price" }, 1000000000000000000],
                },
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $group: {
              _id: null,
              avg_volume: {
                $avg: "$volume",
              },
              last_volume: {
                $last: "$volume",
              },
            },
          },
        ])
        .toArray();

      const googleTrends = await db
        .collection("google_trends")
        .aggregate([
          {
            $match: {
              value: {
                $nin: [null, 0],
              },
            },
          },
          {
            $project: {
              date: {
                $toDate: {
                  $multiply: ["$timestamp", 1000],
                },
              },
              value: 1,
            },
          },
          {
            $group: {
              _id: {
                year: {
                  $year: "$date",
                },
                month: {
                  $month: "$date",
                },
                day: {
                  $dayOfMonth: "$date",
                },
              },
              avg_trend: {
                $avg: "$value",
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $group: {
              _id: null,
              avg_trend: {
                $avg: "$avg_trend",
              },
              last_trend: {
                $last: "$avg_trend",
              },
            },
          },
        ])
        .toArray();

      // console.log("googleTrends", googleTrends);

      let a =
        (volumeSentiment[0].last_volume / volumeSentiment[0].avg_volume) * 100;

      let b = (googleTrends[0].last_trend / googleTrends[0].avg_trend) * 100;

      let market_sentiment = (a + b) / 2 / 8;

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: market_sentiment,
        }),
        2 * 1440
      );

      res.status(200).json({
        success: true,
        data: market_sentiment,
      });
    } catch (err) {
      console.log(err);
      res.status(500).json({ error: "Internal Server Error" });
    }
  };

  public GetPrice = async (req: Request, res: Response) => {
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"]
      );

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time).matchFormat,
            ...(time
              ? {
                  created_date: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {}),
          },
        },
        {
          $project: {
            created_date: {
              $toDate: "$created_date",
            },
            total_price: 1,
            slug: 1,
          },
        },
        {
          $group: {
            _id: structure(time).idFormat,
            average_total_price: {
              $avg: {
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
            min_total_price: {
              $min: {
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
            max_total_price: {
              $max: {
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
          $project: {
            _id: getDateFormat(time),
            average_total_price: 1,
            min_total_price: 1,
            max_total_price: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      let priceData = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      let data = [];

      const dafaultValue = {
        average_total_price: 0,
        min_total_price: 0,
        max_total_price: 0,
      };

      var startFrom = !time
        ? priceData.length
          ? dayjs(priceData[0]._id)
          : dayjs()
        : subtractedTime;

      // Fix missing date in the range
      priceData.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(
          data,
          !time ? "1y" : time,
          startFrom,
          date,
          dafaultValue
        );
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), dafaultValue);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: priceData,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetVolume = async (req: Request, res: Response) => {
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"]
      );

    try {
      let volumeData = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              ...structure(time).matchFormat,
              ...(time
                ? {
                    created_date: {
                      $gte: subtractedTime.toISOString(),
                    },
                  }
                : {}),
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              total_price: 1,
              slug: 1,
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
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
            $project: {
              _id: getDateFormat(time),
              volume: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let data = [];

      var startFrom = !time
        ? volumeData.length
          ? dayjs(volumeData[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        volume: 0,
      };

      // Convert id objects to datetime
      volumeData.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: volumeData,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetSalesAndLiquidity = async (req: Request, res: Response) => {
    let { time } = req.query;
    try {
      let totalSupply = await db.collection("tokens").countDocuments();

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"]
        );

      let salesData = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              ...structure(time).matchFormat,
              ...(time
                ? {
                    created_date: {
                      $gte: subtractedTime.toISOString(),
                    },
                  }
                : {}),
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              slug: 1,
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
              sales: { $sum: 1 },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              sales: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let data = [];
      var startFrom = !time
        ? salesData.length
          ? dayjs(salesData[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        liquidity: 0,
        sales: 0,
      };

      salesData.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        (item.liquidity = (item.sales / totalSupply) * 100), data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetFloorPrice = async (req: Request, res: Response) => {
    try {
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"]
        );

      let floorPrice = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: time
              ? {
                  event_type: "created",
                  created_date: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {
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
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
              floor_price: {
                $min: {
                  $divide: [
                    {
                      $convert: {
                        input: "$ending_price",
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
            $project: {
              _id: getDateFormat(time),
              floor_price: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let data = [];
      var startFrom = !time
        ? floorPrice.length
          ? dayjs(floorPrice[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        floor_price: 0,
      };

      // Convert id objects to datetime
      floorPrice.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: floorPrice,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTransfers = async (req: Request, res: Response) => {
    try {
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );

      let transfers = await db
        .collection("transfers")
        .aggregate([
          ...(time
            ? [
                {
                  $match: {
                    block_timestamp: {
                      $gte: subtractedTime.toISOString(),
                    },
                  },
                },
              ]
            : []),
          {
            $project: {
              created_date: {
                $toDate: "$block_timestamp",
              },
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
              transfer_count: {
                $count: {},
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              transfer_count: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let data = [];
      var startFrom = !time
        ? transfers.length
          ? dayjs(transfers[0]._id)
          : dayjs()
        : subtractedTime;

      const defaultValue = {
        transfer_count: 0,
      };

      // Convert id objects to datetime
      transfers.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(
          data,
          !time ? "1y" : time,
          startFrom,
          date,
          defaultValue
        );
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), defaultValue);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetMarketCap = async (req: Request, res: Response) => {
    try {
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"]
        );

      let matchFormat: any = {
        event_type: "successful",
      };

      if (time) {
        matchFormat = {
          event_type: "successful",
          created_date: {
            $gte: subtractedTime.toISOString(),
          },
        };
      }

      let finalGroupFormat: any = {
        year: "$_id.year",
        month: "$_id.month",
        day: "$_id.day",
      };
      if (time === "24h") {
        finalGroupFormat = {
          year: "$_id.year",
          month: "$_id.month",
          day: "$_id.day",
          hour: "$_id.hour",
        };
      } else if (time === "7d") {
        finalGroupFormat = {
          year: "$_id.year",
          month: "$_id.month",
          day: "$_id.day",
          hour: "$_id.hour",
        };
      }

      let tokensMarketcap = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: matchFormat,
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              total_price: 1,
              token_id: 1,
            },
          },
          {
            $group: {
              _id: {
                ...structure(time).idFormat,
                token_id: {
                  $convert: {
                    input: "$token_id",
                    to: "double",
                  },
                },
              },
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
              token_id: "$_id.token_id",
              market_cap: {
                $add: ["$floor_price", "$last_traded_price"],
              },
            },
          },
          {
            $group: {
              _id: finalGroupFormat,
              total_market_cap: {
                $sum: {
                  $divide: ["$market_cap", 1000000000000000000],
                },
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              total_market_cap: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let data = [];
      var startFrom = !time
        ? tokensMarketcap.length
          ? dayjs(tokensMarketcap[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        total_market_cap: 0,
      };

      // Convert id objects to datetime
      tokensMarketcap.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);
      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetCollectionDistribution = async (req: Request, res: Response) => {
    try {
      const by = req.query?.by || "volume";

      let volume_pipeline = [
        {
          $match: {
            event_type: "successful",
          },
        },
        {
          $project: {
            slug: 1,
            total_price: {
              $divide: [
                {
                  $toDouble: "$total_price",
                },
                1000000000000000000,
              ],
            },
          },
        },
        {
          $group: {
            _id: "$slug",
            data: {
              $sum: "$total_price",
            },
          },
        },
        {
          $sort: {
            data: -1,
          },
        },
        {
          $facet: {
            top10: [
              {
                $limit: 10,
              },
            ],
            others: [
              {
                $skip: 10,
              },
              {
                $group: {
                  _id: null,
                  data: {
                    $sum: "$data",
                  },
                },
              },
            ],
            overall: [
              {
                $group: {
                  _id: null,
                  data: {
                    $sum: "$data",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            top10: 1,
            others: {
              $arrayElemAt: ["$others.data", 0],
            },
            overall: {
              $arrayElemAt: ["$overall.data", 0],
            },
          },
        },
      ];

      let market_cap_pipeline = [
        {
          $match: {
            event_type: "successful",
          },
        },
        {
          $project: {
            slug: 1,
            total_price: {
              $divide: [
                {
                  $toDouble: "$total_price",
                },
                1000000000000000000,
              ],
            },
            created_date: 1,
          },
        },
        {
          $group: {
            _id: {
              slug: "$slug",
              token_id: {
                $convert: {
                  input: "$token_id",
                  to: "double",
                },
              },
            },
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
            slug: "$_id.slug",
            token_id: "$_id.token_id",
            market_cap: {
              $add: ["$floor_price", "$last_traded_price"],
            },
          },
        },
        {
          $group: {
            _id: "$slug",
            data: {
              $sum: {
                $divide: ["$market_cap", 1000000000000000000],
              },
            },
          },
        },
        {
          $sort: {
            data: -1,
          },
        },
        {
          $facet: {
            top10: [
              {
                $limit: 10,
              },
            ],
            others: [
              {
                $skip: 10,
              },
              {
                $group: {
                  _id: null,
                  data: {
                    $sum: "$data",
                  },
                },
              },
            ],
            overall: [
              {
                $group: {
                  _id: null,
                  data: {
                    $sum: "$data",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            top10: 1,
            others: {
              $arrayElemAt: ["$others.data", 0],
            },
            overall: {
              $arrayElemAt: ["$overall.data", 0],
            },
          },
        },
      ];

      let data = await db
        .collection("rarible_events")
        .aggregate(by === "volume" ? volume_pipeline : market_cap_pipeline)
        .toArray();

      let result = {
        top10: data[0].top10.map((item: any) => {
          return {
            slug: item._id,
            data: (item.data / data[0].overall) * 100, // Percentage
          };
        }),
        others: (data[0].others / data[0].overall) * 100, // Percentage
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: result,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: result,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetCategoryStats = async (req: Request, res: Response) => {
    try {
      const by = req.query?.by || "volume";

      // For Volume
      let volume_pipeline: any = [
        {
          $group: {
            _id: "$slug",
            volume: {
              $sum: {
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
        },
        {
          $lookup: {
            from: "collections",
            localField: "_id",
            foreignField: "slug",
            pipeline: [
              {
                $project: {
                  slug: 1,
                  categories: 1,
                },
              },
            ],
            as: "result",
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $unwind: {
            path: "$result.categories",
          },
        },
        {
          $group: {
            _id: "$result.categories",
            volume: {
              $sum: "$volume",
            },
          },
        },
      ];

      let market_cap_pipeline = [
        {
          $group: {
            _id: {
              slug: "$slug",
              token_id: {
                $convert: {
                  input: "$token_id",
                  to: "double",
                },
              },
            },
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
            slug: "$_id.slug",
            token_id: "$_id.token_id",
            market_cap: {
              $add: ["$floor_price", "$last_traded_price"],
            },
          },
        },
        {
          $group: {
            _id: "$slug",
            market_cap: {
              $sum: {
                $divide: ["$market_cap", 1000000000000000000],
              },
            },
          },
        },
        {
          $lookup: {
            from: "collections",
            localField: "_id",
            foreignField: "slug",
            pipeline: [
              {
                $project: {
                  slug: 1,
                  categories: 1,
                },
              },
            ],
            as: "result",
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $unwind: {
            path: "$result.categories",
          },
        },
        {
          $group: {
            _id: "$result.categories",
            market_cap: {
              $sum: "$market_cap",
            },
          },
        },
      ];

      let liquidity_pipeline = [
        {
          $group: {
            _id: "$slug",
            sales: {
              $sum: 1,
            },
          },
        },
        {
          $lookup: {
            from: "collections",
            localField: "_id",
            foreignField: "slug",
            pipeline: [
              {
                $project: {
                  slug: 1,
                  categories: 1,
                  total_supply: 1,
                },
              },
            ],
            as: "result",
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $unwind: {
            path: "$result.categories",
          },
        },
        {
          $group: {
            _id: "$result.categories",
            sales: {
              $sum: "$sales",
            },
            total_supply: {
              $sum: "$result.total_supply",
            },
          },
        },
        {
          $project: {
            _id: 1,
            liquidity: {
              $multiply: [
                {
                  $divide: ["$sales", "$total_supply"],
                },
                100,
              ],
            },
          },
        },
      ];

      let data = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              event_type: "successful",
            },
          },
          {
            $facet: {
              pipeline:
                by === "volume"
                  ? volume_pipeline
                  : by === "market_cap"
                  ? market_cap_pipeline
                  : liquidity_pipeline,
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
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetTopCollectionsByMCorVolume = async (
    req: Request,
    res: Response
  ) => {
    try {
      let sortBy: any = req.query.type || "volume";

      const topCollections = await db
        .collection("collections")
        .aggregate([
          {
            $match: {
              slug: {
                $nin: ["theshiboshis"],
              },
            },
          },
          {
            $project: {
              slug: 1,
              total_supply: 1,
              created_date: 1,
              image_url: 1,
              name: 1,
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "slug",
              foreignField: "slug",
              pipeline: [
                {
                  $match: {
                    event_type: "successful",
                  },
                },
              ],
              as: "events",
            },
          },
          {
            $unwind: {
              path: "$events",
            },
          },
          {
            $project: {
              slug: 1,
              event_type: "$events.event_type",
              total_supply: 1,
              created_date: 1,
              token_id: "$events.token_id",
              image_url: 1,
              name: 1,
              total_price: "$events.total_price",
            },
          },
          {
            $facet: {
              collection_info: [
                {
                  $group: {
                    _id: "$slug",
                    total_supply: {
                      $first: "$total_supply",
                    },
                    created_date: {
                      $first: "$created_date",
                    },
                    image_url: {
                      $first: "$image_url",
                    },
                    name: {
                      $first: "$name",
                    },
                  },
                },
              ],
              volume: [
                {
                  $group: {
                    _id: "$slug",
                    volume: {
                      $sum: {
                        $convert: {
                          input: "$total_price",
                          to: "double",
                        },
                      },
                    },
                  },
                },
              ],
              market_cap: [
                {
                  $group: {
                    _id: {
                      slug: "$slug",
                      token_id: {
                        $convert: {
                          input: "$token_id",
                          to: "double",
                        },
                      },
                    },
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
                    slug: "$_id.slug",
                    token_id: "$_id.token_id",
                    market_cap: {
                      $add: ["$floor_price", "$last_traded_price"],
                    },
                  },
                },
                {
                  $group: {
                    _id: "$slug",
                    market_cap: {
                      $sum: {
                        $divide: ["$market_cap", 1000000000000000000],
                      },
                    },
                  },
                },
              ],
            },
          },
          {
            $project: {
              _id: 1,
              all: {
                $concatArrays: ["$volume", "$market_cap", "$collection_info"],
              },
            },
          },
          {
            $unwind: {
              path: "$all",
            },
          },
          {
            $group: {
              _id: "$all._id",
              volume: {
                $max: "$all.volume",
              },
              market_cap: {
                $max: "$all.market_cap",
              },
              total_supply: {
                $max: "$all.total_supply",
              },
              image_url: {
                $max: "$all.image_url",
              },
              name: {
                $max: "$all.name",
              },
              created_date: {
                $max: "$all.created_date",
              },
            },
          },
          {
            $project: {
              volume: {
                $divide: ["$volume", 1000000000000000000],
              },
              market_cap: 1,
              total_supply: 1,
              created_date: 1,
              name: 1,
              image_url: 1,
              owners: 1,
            },
          },
          {
            $sort:
              sortBy === "volume"
                ? {
                    volume: -1,
                  }
                : {
                    market_cap: -1,
                  },
          },
          {
            $limit: 10,
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: topCollections,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: topCollections,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  // NEED REVIEW
  public GetBuyersAndSellers = async (req: Request, res: Response) => {
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["transfers"],
        ["block_timestamp"]
      );

    let groupFormat: any = {
      year: {
        $year: "$time",
      },
      month: {
        $month: "$time",
      },
      day: {
        $dayOfMonth: "$time",
      },
    };

    if (time == "7d") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $multiply: [
            {
              $floor: {
                $divide: [
                  {
                    $hour: "$time",
                  },
                  2,
                ],
              },
            },
            2,
          ],
        },
      };
    } else if (time == "24h") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $hour: "$time",
        },
      };
    }

    try {
      let buyersAndSellers = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              time: {
                $toDate: "$block_timestamp",
              },
              token_id: {
                $concat: ["$slug", "_", "$token_id"],
              },
              seller: "$from_address",
              buyer: "$to_address",
              value: 1,
            },
          },
          {
            $group: {
              _id: groupFormat,
              sellers: {
                $accumulator: {
                  init: "function() { return { sellers: []}}",
                  accumulate: `function(state, seller, value, time) { ${
                    !time
                      ? ""
                      : `if(time >= new Date('${subtractedTime.toISOString()}')) {\n`
                  } if (!state.sellers.includes(seller) && seller !== "0x0000000000000000000000000000000000000000" && value !== "0") {state.sellers.push(seller);\n }\n ${
                    !time ? "" : "}\n"
                  } return state; }`,
                  accumulateArgs: ["$seller", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { sellers: state1.sellers.concat(state2.sellers), }; }",
                  finalize: "function(state) { return state.sellers.length; }",
                  lang: "js",
                },
              },
              buyers: {
                $accumulator: {
                  init: "function() { return { buyers: []} }",
                  accumulate: `function(state, buyer, value,time) { ${
                    !time
                      ? ""
                      : `if(time >= new Date('${subtractedTime.toISOString()}')) {\n`
                  }  if (!state.buyers.includes(buyer) && value !== "0") { state.buyers.push(buyer);}\n ${
                    !time ? "" : "}\n"
                  } return state; }`,
                  accumulateArgs: ["$buyer", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { buyers: state1.buyers.concat(state2.buyers),};}",
                  finalize: "function(state) { return state.buyers.length; }",
                  lang: "js",
                },
              },
              token_transactions: {
                $accumulator: {
                  init: "function() { return { token_trans: {}}}",
                  accumulate:
                    "function(state, token_id, buyer, time) { if (!state.token_trans.hasOwnProperty(token_id) || state.token_trans[token_id].time.getTime() < time.getTime() ) {state.token_trans[token_id] = {buyer,time}; } return state; }",
                  accumulateArgs: ["$token_id", "$buyer", "$time"],
                  merge:
                    "function(state1, state2) { Object.assign(state1.token_trans, state2.token_trans); return state1; }",
                  finalize: "function(state) { return state.token_trans; }",
                  lang: "js",
                },
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $group: {
              _id: null,
              records: {
                $accumulator: {
                  init: "function() { return { records: [], tokens: {}}; }",
                  accumulate:
                    "function(state,token_transactions, _id, buyers, sellers) { Object.assign(state.tokens, token_transactions);\
                     let set = new Set(); Object.keys(state.tokens).forEach(k => {set.add(state.tokens[k].buyer); });\
                     let res = { _id, buyers, sellers, holders: set.size}; state.records.push(res);\
                     return {records: state.records,tokens: state.tokens};}",
                  accumulateArgs: [
                    "$token_transactions",
                    "$_id",
                    "$buyers",
                    "$sellers",
                  ],
                  merge:
                    "function(state1, state2) { return state1.records.concat(state2.records); }",
                  finalize: "function(state) { return state.records; }",
                  lang: "js",
                },
              },
            },
          },
          {
            $unwind: {
              path: "$records",
            },
          },
          {
            $project: {
              buyers: "$records.buyers",
              sellers: "$records.sellers",
              holders: "$records.holders",
              _id: {
                $dateFromParts: {
                  year: "$records._id.year",
                  month: "$records._id.month",
                  day: "$records._id.day",
                  ...(time == "24h" || time == "7d"
                    ? { hour: "$records._id.hour" }
                    : {}),
                },
              },
            },
          },
          ...(time
            ? [
                {
                  $match: {
                    _id: {
                      $gte: subtractedTime.toDate(),
                    },
                  },
                },
              ]
            : []),
        ])
        .toArray();

      // Data Formatting
      let data = [];
      var startFrom = !time
        ? buyersAndSellers.length
          ? dayjs(buyersAndSellers[0]._id)
          : dayjs()
        : subtractedTime;
      var prevHolders = buyersAndSellers.length
        ? buyersAndSellers[0].holders
        : 0;

      buyersAndSellers.forEach((day) => {
        const date = dayjs(day._id);

        const value = {
          buyers: 0,
          sellers: 0,
          holders: prevHolders,
        };

        // Fix sparse date ranges.
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);

        data.push(day);
        startFrom = date;
        prevHolders = day.holders;
      });

      // const value = {
      //   buyers: 0,
      //   sellers: 0,
      //   holders: prevHolders,
      // }
      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetHoldersTraders = async (req: Request, res: Response) => {
    const { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["transfers"],
        ["block_timestamp"]
      );

    let groupFormat: any = {
      year: {
        $year: "$time",
      },
      month: {
        $month: "$time",
      },
      day: {
        $dayOfMonth: "$time",
      },
    };

    if (time == "7d") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $multiply: [
            {
              $floor: {
                $divide: [
                  {
                    $hour: "$time",
                  },
                  2,
                ],
              },
            },
            2,
          ],
        },
      };
    } else if (time == "24h") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $hour: "$time",
        },
      };
    }

    try {
      const holdersAndTraders = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              time: {
                $toDate: "$block_timestamp",
              },
              token_id: {
                $concat: ["$slug", "_", "$token_id"],
              },
              seller: "$from_address",
              buyer: "$to_address",
              value: 1,
            },
          },
          {
            $group: {
              _id: groupFormat,
              sellers: {
                $accumulator: {
                  init: "function() { return { sellers: []}}",
                  accumulate: `function(state, seller, value,time) {  \n          if (!state.sellers.includes(seller) \n          && seller !== "0x0000000000000000000000000000000000000000" \n          && value !== "0" && \n          ${
                    !time
                      ? ""
                      : `time >= new Date('${subtractedTime.toISOString()}')`
                  }\n          ) {\n            state.sellers.push(seller);\n          }\n          return state;\n        }`,
                  accumulateArgs: ["$seller", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { sellers: state1.sellers.concat(state2.sellers), }; }",
                  finalize: "function(state) { return state.sellers.length; }",
                  lang: "js",
                },
              },
              buyers: {
                $accumulator: {
                  init: "function() { return { buyers: []} }",
                  accumulate: `function(state, buyer, value, time) {  \n          if (!state.buyers.includes(buyer) \n          && value !== "0" && \n          ${
                    !time
                      ? ""
                      : `time >= new Date('${subtractedTime.toISOString()}')`
                  }\n          ) {\n            state.buyers.push(buyer);\n          }\n          return state;\n        }`,
                  accumulateArgs: ["$buyer", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { buyers: state1.buyers.concat(state2.buyers),};}",
                  finalize: "function(state) { return state.buyers.length; }",
                  lang: "js",
                },
              },
              traders: {
                $accumulator: {
                  init: "function() { return { traders:[]} }",
                  accumulate:
                    'function(state, seller, buyer, value, time){ \n if(!state.traders.includes(seller) \n && value !== "0" && \n time>=new Date("2022-07-21")\n) {\n state.traders.push(seller);\n} \n if(!state.traders.includes(buyer) \n && value !== "0" \n && time >= new Date("2022-07-21")\n) {\n state.traders.includes(buyer)} \n return state;\n}',
                  accumulateArgs: ["$seller", "$buyer", "$value", "$time"],
                  merge:
                    "function(state1,state2) {return {traders: state1.traders.concat(state2.traders),};}",
                  finalize: "function(state) { return state.traders.length; }",
                  lang: "js",
                },
              },
              token_transactions: {
                $accumulator: {
                  init: "function() { return { token_trans: {}}}",
                  accumulate:
                    "function(state, token_id, buyer, time) {  \n          if (\n          !state.token_trans.hasOwnProperty(token_id) ||\n          state.token_trans[token_id].time.getTime() < time.getTime()\n          ) {\n            state.token_trans[token_id] = {\n              buyer,\n              time\n            };\n          } \n          return state;\n        }",
                  accumulateArgs: ["$token_id", "$buyer", "$time"],
                  merge:
                    "function(state1, state2) { Object.assign(state1.token_trans, state2.token_trans); return state1; }",
                  finalize: "function(state) { return state.token_trans; }",
                  lang: "js",
                },
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $group: {
              _id: null,
              records: {
                $accumulator: {
                  init: "function() { return { records: [], tokens: {}}; }",
                  accumulate:
                    "function(state,token_transactions, _id, buyers, sellers,traders) { Object.assign(state.tokens, token_transactions);                     let set = new Set(); Object.keys(state.tokens).forEach(k => {set.add(state.tokens[k].buyer); });                     let res = { _id, buyers, sellers,traders, holders: set.size}; state.records.push(res);                     return {records: state.records,tokens: state.tokens};}",
                  accumulateArgs: [
                    "$token_transactions",
                    "$_id",
                    "$buyers",
                    "$sellers",
                    "$traders",
                  ],
                  merge:
                    "function(state1, state2) { return state1.records.concat(state2.records); }",
                  finalize: "function(state) { return state.records; }",
                  lang: "js",
                },
              },
            },
          },
          {
            $unwind: {
              path: "$records",
            },
          },
          {
            $project: {
              buyers: "$records.buyers",
              sellers: "$records.sellers",
              holders: "$records.holders",
              traders: "$records.traders",
              _id: {
                $dateFromParts: {
                  year: "$records._id.year",
                  month: "$records._id.month",
                  day: "$records._id.day",
                },
              },
            },
          },
          ...(time
            ? [
                {
                  $match: {
                    _id: {
                      $gte: subtractedTime.toDate(),
                    },
                  },
                },
              ]
            : []),
        ])
        .toArray();
      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          holdersAndTraders,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        holdersAndTraders,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTopSales = async (req: Request, res: Response) => {
    try {
      const time = req.query.time || "all";
      const category = req.query.category || "All";

      let pageSize = 100;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      let events_match = async (time) => {
        let subtractedTime;
        if (time != "all")
          subtractedTime = await getSubtractedtime(
            time,
            ["rarible_events"],
            ["created_date"]
          );
        let match =
          time === "all"
            ? {
                event_type: "successful",
              }
            : {
                event_type: "successful",
                created_date: {
                  $gte: subtractedTime.toDate(),
                },
              };
        return match;
      };

      if (!page || page <= 0) {
        page = 1;
      }

      const topSalesGlobal = await db
        .collection("collections")
        .aggregate([
          {
            $match:
              category === "All"
                ? {}
                : {
                    categories: {
                      $in: [category],
                    },
                  },
          },
          {
            $project: {
              slug: 1,
              collection_name: "$name",
              collection_img_url: "$image_url",
              collection_address: "$address",
              categories: 1,
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "slug",
              foreignField: "slug",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "events",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    event_type: 1,
                    token_id: 1,
                    total_price: 1,
                    created_date: {
                      $toDate: "$created_date",
                    },
                  },
                },
                {
                  $match: events_match(time),
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$events",
            },
          },
          {
            $project: {
              slug: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              categories: 1,
              token_id: "$events.token_id",
              total_price: {
                $divide: [
                  {
                    $convert: {
                      input: "$events.total_price",
                      to: 1,
                    },
                  },
                  1000000000000000000,
                ],
              },
              created_date: {
                $toDate: "$events.created_date",
              },
            },
          },
          {
            $sort: {
              total_price: -1,
            },
          },
          {
            $skip: (page - 1) * pageSize,
          },
          {
            $limit: pageSize,
          },
          {
            $lookup: {
              from: "tokens",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "result",
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ["$slug", "$$slug"],
                        },
                        {
                          $eq: ["$token_id", "$$token_id"],
                        },
                      ],
                    },
                  },
                },
              ],
            },
          },
          {
            $replaceRoot: {
              newRoot: {
                $mergeObjects: [
                  {
                    $arrayElemAt: ["$result", 0],
                  },
                  {
                    name: "$$ROOT.name",
                    image_url: "$$ROOT.image_url",
                    collection_name: "$$ROOT.collection_name",
                    collection_img_url: "$$ROOT.collection_img_url",
                    collection_address: "$$ROOT.collection_address",
                    total_price: "$$ROOT.total_price",
                    categories: "$$ROOT.categories",
                    created_date: "$$ROOT.created_date",
                  },
                ],
              },
            },
          },
          {
            $project: {
              slug: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              categories: 1,
              total_price: {
                $divide: ["$total_price", 1000000000000000000],
              },
              created_date: 1,
              name: 1,
              image_url: 1,
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "owner",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    event_type: 1,
                    created_date: {
                      $toDate: "$created_date",
                    },
                    owner_name: "$winner_account.user.username",
                    owner_address: "$winner_account.address",
                    owner_img_url: "$winner_account.profile_img_url",
                  },
                },
                {
                  $match: {
                    event_type: "successful",
                    $expr: {
                      $and: [
                        {
                          $eq: ["$slug", "$$slug"],
                        },
                        {
                          $eq: ["$token_id", "$$token_id"],
                        },
                      ],
                    },
                  },
                },
                {
                  $sort: {
                    created_date: -1,
                  },
                },
                {
                  $limit: 1,
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$owner",
            },
          },
          {
            $project: {
              slug: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              total_price: 1,
              created_date: 1,
              categories: 1,
              token_name: "$name",
              token_img_url: "$image_url",
              owner_name: "$owner.owner_name",
              owner_address: "$owner.owner_address",
              owner_img_url: "$owner.owner_img_url",
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "slug",
              foreignField: "slug",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "last_price",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    value: {
                      $toDouble: "$value",
                    },
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                  },
                },
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    value: {
                      $divide: ["$value", 1000000000000000000],
                    },
                    block_timestamp: 1,
                    categories: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ["$slug", "$$slug"],
                        },
                        {
                          $eq: ["$token_id", "$$token_id"],
                        },
                        {
                          $ne: ["$value", 0],
                        },
                      ],
                    },
                  },
                },
                {
                  $sort: {
                    block_timestamp: -1,
                  },
                },
                {
                  $limit: 2,
                },
                {
                  $group: {
                    _id: null,
                    data: {
                      $addToSet: {
                        value: "$value",
                      },
                    },
                    count1: {
                      $first: "$value",
                    },
                    count2: {
                      $last: "$value",
                    },
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$last_price",
            },
          },
          {
            $project: {
              slug: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              total_price: 1,
              created_date: 1,
              token_name: 1,
              token_img_url: 1,
              owner_name: 1,
              owner_address: 1,
              owner_img_url: 1,
              categories: 1,
              size: {
                $size: "$last_price.data",
              },
              last_price: 1,
            },
          },
          {
            $project: {
              slug: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              total_price: 1,
              created_date: 1,
              token_name: 1,
              token_img_url: 1,
              owner_name: 1,
              owner_address: 1,
              owner_img_url: 1,
              categories: 1,
              last_price: "$last_price.count1",
              change: {
                $cond: {
                  if: {
                    $eq: ["$size", 2],
                  },
                  then: {
                    $subtract: ["$last_price.count1", "$last_price.count2"],
                  },
                  else: "$last_price.count1",
                },
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "slug",
              foreignField: "slug",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "transfers",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    value: {
                      $toDouble: "$value",
                    },
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                  },
                },
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    categories: 1,
                    value: {
                      $divide: ["$value", 1000000000000000000],
                    },
                    block_timestamp: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ["$slug", "$$slug"],
                        },
                        {
                          $eq: ["$token_id", "$$token_id"],
                        },
                        {
                          $ne: ["$value", 0],
                        },
                      ],
                    },
                  },
                },
                {
                  $match: {
                    $expr: {
                      $gte: [
                        "$block_timestamp",
                        {
                          $subtract: ["$$NOW", 86400000],
                        },
                      ],
                    },
                  },
                },
                {
                  $facet: {
                    highest_price: [
                      {
                        $group: {
                          _id: null,
                          data: {
                            $addToSet: {
                              value: "$value",
                            },
                          },
                          highest_price: {
                            $max: "$value",
                          },
                        },
                      },
                      {
                        $project: {
                          highest_price: 1,
                        },
                      },
                    ],
                    sales: [
                      {
                        $group: {
                          _id: null,
                          data: {
                            $addToSet: {
                              value: "$value",
                            },
                          },
                        },
                      },
                      {
                        $unwind: {
                          path: "$data",
                        },
                      },
                      {
                        $count: "sales",
                      },
                      {
                        $project: {
                          sales: 1,
                        },
                      },
                    ],
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$transfers",
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "slug",
              foreignField: "slug",
              let: {
                slug: "$slug",
                token_id: "$token_id",
              },
              as: "last_deal",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    categories: 1,
                    value: {
                      $toDouble: "$value",
                    },
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                  },
                },
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    categories: 1,
                    value: {
                      $divide: ["$value", 1000000000000000000],
                    },
                    block_timestamp: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ["$slug", "$$slug"],
                        },
                        {
                          $eq: ["$token_id", "$$token_id"],
                        },
                        {
                          $ne: ["$value", 0],
                        },
                      ],
                    },
                  },
                },
                {
                  $group: {
                    _id: ["$slug", "$token_id"],
                    data: {
                      $addToSet: {
                        block_timestamp: "$block_timestamp",
                      },
                    },
                    recent_deal: {
                      $max: "$block_timestamp",
                    },
                  },
                },
                {
                  $project: {
                    last_deal: "$recent_deal",
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$last_deal",
            },
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: topSalesGlobal,
        })
      );

      res.status(200).send({
        success: true,
        data: topSalesGlobal,
      });
    } catch (err) {
      console.log(err);
      return res.status(500).json({
        message: err.message,
      });
    }
  };

  public GetTradersCount = async (req: Request, res: Response) => {
    try {
      let subtractedTime = await getSubtractedtime(
        "24h",
        ["transfers"],
        ["block_timestamp"]
      );

      let pipeline = [
        {
          $match: {
            block_timestamp: {
              $gte: subtractedTime.toISOString(),
            },
          },
        },
        {
          $facet: {
            sellers: [
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                },
              },
              {
                $project: {
                  sellers: {
                    $size: "$sellers",
                  },
                },
              },
            ],
            buyers: [
              {
                $group: {
                  _id: null,
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  buyers: {
                    $size: "$buyers",
                  },
                },
              },
            ],
          },
        },
      ];

      const data = await db
        .collection("transfers")
        .aggregate(pipeline)
        .toArray();

      let traders = [
        {
          sellers_count: data[0].sellers[0].sellers,
          buyers_count: data[0].buyers[0].buyers,
        },
      ];

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: traders,
        })
      );

      res.status(200).send({
        success: true,
        data: traders,
      });
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };

  public GetBlueChipIndex = async (req: Request, res: Response) => {
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"]
      );

    try {
      let volumeData = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              ...structure(time).matchFormat,
              ...(time
                ? {
                    created_date: {
                      $gte: subtractedTime.toISOString(),
                    },
                  }
                : {}),
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              total_price: 1,
              slug: 1,
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
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
            $project: {
              _id: getDateFormat(time),
              volume: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $limit: 100,
          },
        ])
        .toArray();

      let data = [];

      var startFrom = !time
        ? volumeData.length
          ? dayjs(volumeData[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        volume: 0,
      };

      // Convert id objects to datetime
      volumeData.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: volumeData,
        })
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetPlatformVolumeAndSales = async (req: Request, res: Response) => {
    try {
      let result = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              ...structure().matchFormat,
            },
          },
          {
            $project: {
              marketplace_id: 1,
              total_price: 1,
            },
          },
          {
            $group: {
              _id: "$marketplace_id",
              no_of_sales: {
                $sum: 1,
              },
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
            $sort: {
              volume: 1,
            },
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: result,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: result,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetCurrencyVolumeAndSales = async (req: Request, res: Response) => {
    try {
      const { time } = req.query;

      let pipeline = [
        {
          $match: {
            ...structure(time).matchFormat,
          },
        },
        {
          $project: {
            marketplace_id: 1,
            total_price: 1,
            payment_token: 1,
            rarible_price: 1,
          },
        },
        {
          $group: {
            _id: {
              payment_token: "$payment_token.symbol",
              name: "$payment_token.name",
              symbol: "$payment_token.symbol",
              address: "$payment_token.address",
            },
            no_of_sales: {
              $sum: 1,
            },
            volume_in_usd: {
              $sum: {
                $toDouble: "$rarible_price.usd_price",
              },
            },
          },
        },
        {
          $match: {
            volume_in_usd: {
              $gt: 0,
            },
          },
        },
        {
          $sort: {
            volume_in_usd: 1,
          },
        },
      ];

      let result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      for (let i = 0; i < result.length; i++) {
        if (result[i]._id.address) {
          let contract_address = result[i]._id.address;
          let currentDate = new Date().toISOString();

          if (contract_address.toLowerCase().replaceAll(`${"ETHEREUM"}:`, "")) {
            let query = `https://api.rarible.org/v0.1/currencies/${contract_address}/rates/usd?at=${currentDate}`;
            console.log(query);
            await axios
              .get(query)
              .then((response: any) => {
                result[i]._id = {
                  ...result[i]._id,
                  name: response.data.symbol,
                  more_data: response.data,
                };
              })
              .catch((err: any) => {
                console.log(err);
              });
          }
        } else {
        }
      }

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: result,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: result,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };
}
