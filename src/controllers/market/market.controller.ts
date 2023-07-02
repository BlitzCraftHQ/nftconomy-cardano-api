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
        .collection("sales")
        .aggregate([
          {
            $project: {
              created_date: "$timestamp",
              price: 1,
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
                $sum: { $toDouble: "$price" },
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
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"]);

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time).matchFormat,
            ...(time
              ? {
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {}),
          },
        },
        {
          $project: {
            timestamp: {
              $toDate: "$timestamp",
            },
            price: 1,
          },
        },
        {
          $group: {
            _id: structure(time).idFormat,
            average_total_price: {
              $avg: {
                $toDouble: "$price",
              },
            },
            min_total_price: {
              $min: {
                $toDouble: "$price",
              },
            },
            max_total_price: {
              $max: {
                $toDouble: "$price",
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

      console.log("pipeline", JSON.stringify(pipeline));

      let priceData = await db
        .collection("sales")
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

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   720
      // );

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
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"]);

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time).matchFormat,
            ...(time
              ? {
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {}),
          },
        },
        {
          $project: {
            timestamp: {
              $toDate: "$timestamp",
            },
            price: 1,
            name: 1,
          },
        },
        {
          $group: {
            _id: structure(time).idFormat,
            volume: {
              $sum: {
                $toDouble: "$price",
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
      ];

      let volumeData = await db
        .collection("sales")
        .aggregate(pipeline)
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

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   720
      // );

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
      let totalSupply = await db
        .collection("policies")
        .aggregate([
          {
            $group: {
              _id: null,
              totalSupply: {
                $sum: "$nftsInCirculation",
              },
            },
          },
        ])
        .toArray();

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["sales"],
          ["timestamp"]
        );

      let salesData = await db
        .collection("sales")
        .aggregate([
          {
            $match: {
              ...structure(time).matchFormat,
              ...(time
                ? {
                    timestamp: {
                      $gte: subtractedTime.toISOString(),
                    },
                  }
                : {}),
            },
          },
          {
            $project: {
              timestamp: {
                $toDate: "$timestamp",
              },
              name: 1,
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
        (item.liquidity = (item.sales / totalSupply[0].totalSupply) * 100),
          data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);
      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   720
      // );

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
          ["listings"],
          ["timestamp"]
        );

      let floorPrice = await db
        .collection("listings")
        .aggregate([
          {
            $match: time
              ? {
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {},
          },
          {
            $project: {
              timestamp: {
                $toDate: "$timestamp",
              },
              price: 1,
            },
          },
          {
            $group: {
              _id: structure(time).idFormat,
              floor_price: {
                $min: {
                  $toDouble: "$price",
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

      var prevFloorPrice = floorPrice.length ? floorPrice[0].floor_price : 0;

      // Convert id objects to datetime
      floorPrice.forEach((item, index) => {
        const date = dayjs(item._id);
        const value = {
          floor_price: prevFloorPrice,
        };
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
        prevFloorPrice = item.floor_price;
      });

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   720
      // );

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
          ["sales"],
          ["timestamp"]
        );

      let transfers = await db
        .collection("sales")
        .aggregate([
          ...(time
            ? [
                {
                  $match: {
                    timestamp: {
                      $gte: subtractedTime.toISOString(),
                    },
                  },
                },
              ]
            : []),
          {
            $project: {
              timestamp: {
                $toDate: "$timestamp",
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

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   1440
      // );

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
      let volume_pipeline = [
        {
          $group: {
            _id: "$collection",
            data: {
              $sum: "$price",
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
        .collection("sales")
        .aggregate(volume_pipeline)
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

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: result,
      //   }),
      //   1440
      // );

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
      let pageSize = 20;
      // console.log(req.query);
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      const topCollections = await db
        .collection("policies")
        .aggregate([
          {
            $sort: {
              [sortBy]: -1,
            },
          },
          {
            $facet: {
              data: [
                {
                  $skip: (page - 1) * pageSize,
                },
                {
                  $limit: pageSize,
                },
              ],
              totalCount: [
                {
                  $count: "count",
                },
              ],
            },
          },
        ])
        .toArray();

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: topCollections,
      //   }),
      //   1440
      // );

      res.status(200).send({
        success: true,
        data: {
          paginatedData: {
            pageSize: pageSize,
            currentPage: page,
            totalPages: topCollections[0].totalCount[0].count / pageSize,
          },
          data: topCollections[0].data,
        },
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
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"]);

    try {
      let buyersAndSellers = await db
        .collection("sales")
        .aggregate([
          {
            $facet: {
              buyers: [
                {
                  $match: {
                    timestamp: {
                      $gte: subtractedTime,
                    },
                  },
                },
                {
                  $group: {
                    _id: "$toAddress",
                    count: {
                      $sum: 1,
                    },
                  },
                },
                {
                  $count: "count",
                },
              ],
              sellers: [
                {
                  $match: {
                    timestamp: {
                      $gte: subtractedTime,
                    },
                  },
                },
                {
                  $group: {
                    _id: "$fromAddress",
                    count: {
                      $sum: 1,
                    },
                  },
                },
                {
                  $count: "count",
                },
              ],
            },
          },
        ])
        .toArray();

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data,
      //   }),
      //   1440
      // );

      res.status(200).send({
        success: true,
        data: buyersAndSellers,
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
    let { time } = req.query;
    let pageSize = 12;
    // console.log(req.query);
    let pageString = req.query.page;
    let page = Number(pageString) || 1;

    if (!page || page <= 0) {
      page = 1;
    }

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"]);

    try {
      let pipeline = [
        {
          $match: {
            ...(time
              ? {
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            price: 1,
          },
        },
        {
          $facet: {
            data: [
              {
                $skip: (page - 1) * pageSize,
              },
              {
                $limit: pageSize,
              },
            ],
            totalCount: [
              {
                $count: "count",
              },
            ],
          },
        },
      ];

      console.log(JSON.stringify(pipeline));

      let topSalesData = await db
        .collection("sales")
        .aggregate(pipeline)
        .toArray();

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: data,
      //   }),
      //   720
      // );

      res.status(200).send({
        success: true,
        data: {
          paginatedData: {
            pageSize: pageSize,
            currentPage: page,
            totalPages: topSalesData[0].totalCount[0].count / pageSize,
          },
          data: topSalesData[0].data,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTradersCount = async (req: Request, res: Response) => {
    try {
      let subtractedTime = await getSubtractedtime(
        "24h",
        ["sales"],
        ["timestamp"]
      );

      let pipeline = [
        {
          $match: {
            timestamp: {
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
                    $addToSet: "$fromAddress",
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
                    $addToSet: "$toAddress",
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

      const data = await db.collection("sales").aggregate(pipeline).toArray();

      let traders = [
        {
          sellers_count: data[0].sellers[0].sellers,
          buyers_count: data[0].buyers[0].buyers,
        },
      ];

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: traders,
      //   })
      // );

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
