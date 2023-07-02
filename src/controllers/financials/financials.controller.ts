import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { structure } from "../../helpers/stats";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";
import * as dayjs from "dayjs";
import axios from "axios";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class FinancialsController {
  public GetVolume = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"], {
        collection: name,
      });

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
            _id: structure(time, name).idFormat,
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

  public GetPrice = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"], {
        collection: name,
      });

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
            _id: structure(time, name).idFormat,
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

  public GetSalesAndLiquidity = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;
    try {
      let collectionData = await db.collection("policies").findOne({
        name,
      });

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["sales"],
          ["timestamp"],
          { collection: name }
        );

      let salesData = await db
        .collection("sales")
        .aggregate([
          {
            $match: {
              ...structure(time, name).matchFormat,
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
              _id: structure(time, name).idFormat,
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
        (item.liquidity =
          (item.sales / collectionData.nftsInCirculation) * 100),
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

  public GetNoOfListings = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;
    try {
      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["sales"],
          ["timestamp"],
          { collection: name }
        );

      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
            _id: structure(time, name).idFormat,
            listings: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            listings: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      let result = await db.collection("sales").aggregate(pipeline).toArray();

      let data = [];
      var startFrom = !time
        ? result.length
          ? dayjs(result[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        listings: 0,
      };

      result.forEach((item, index) => {
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

  public GetFloorPrice = async (req: Request, res: Response) => {
    try {
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["listings"],
          ["timestamp"],
          { collection: name }
        );

      let floorPrice = await db
        .collection("listings")
        .aggregate([
          {
            $match: time
              ? {
                  collection: name,
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {
                  collection: name,
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
              _id: structure(time, name).idFormat,
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
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["sales"],
          ["timestamp"],
          { collection: name }
        );

      let transfers = await db
        .collection("sales")
        .aggregate([
          {
            $match: time
              ? {
                  collection: name,
                  timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {
                  collection: name,
                },
          },
          {
            $project: {
              timestamp: {
                $toDate: "$timestamp",
              },
            },
          },
          {
            $group: {
              _id: structure(time, name).idFormat,
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

  public GetMarketCap = async (req: Request, res: Response) => {
    try {
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["sales"],
          ["timestamp"],
          { collection: name }
        );

      let matchFormat: any = {
        collection: name,
      };

      if (time) {
        matchFormat = {
          collection: name,
          timestamp: {
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
        .collection("sales")
        .aggregate([
          {
            $match: matchFormat,
          },
          {
            $project: {
              timestamp: {
                $toDate: "$timestamp",
              },
              total_price: "$price",
              token_id: "$assetNameHex",
            },
          },
          {
            $group: {
              _id: {
                ...structure(time, name).idFormat,
                token_id: "$token_id",
              },
              last_traded_price: {
                $last: {
                  $toDouble: "$total_price",
                },
              },
              floor_price: {
                $min: {
                  $toDouble: "$total_price",
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
                $sum: "$market_cap",
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

      var prevMarketCap = tokensMarketcap.length
        ? tokensMarketcap[0].total_market_cap
        : 0;

      // Convert id objects to datetime
      tokensMarketcap.forEach((item, index) => {
        const date = dayjs(item._id);
        const value = {
          total_market_cap: prevMarketCap,
        };
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
        prevMarketCap = item.total_market_cap;
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

  public GetTopSales = async (req: Request, res: Response) => {
    let { name } = req.params;
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
      subtractedTime = await getSubtractedtime(time, ["sales"], ["timestamp"], {
        collection: name,
      });

    try {
      let pipeline = [
        {
          $match: {
            collection: name,
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
}
