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
      let volumeData = await db
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
              total_price: 1,
              name: 1,
            },
          },
          {
            $group: {
              _id: structure(time, name).idFormat,
              volume: {
                $sum: {
                  $divide: [
                    {
                      $toDouble: "$selling_order.price",
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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

  public GetPrice = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"],
        { name: name }
      );

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
            name: 1,
          },
        },
        {
          $group: {
            _id: structure(time, name).idFormat,
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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
    let { name } = req.params;
    let { time } = req.query;
    try {
      let totalSupply = await db.collection("collections").findOne({
        name,
      });

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      let salesData = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: {
              ...structure(time, name).matchFormat,
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
        (item.liquidity = (item.sales / totalSupply.total_supply) * 100),
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
        720
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

  public GetNoOfListings = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;
    try {
      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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

      let result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      let floorPrice = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: time
              ? {
                  event_type: "created",
                  name: name,
                  created_date: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {
                  event_type: "created",
                  name: name,
                },
          },
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              ending_price: 1,
              name: 1,
            },
          },
          {
            $group: {
              _id: structure(time, name).idFormat,
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

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);
      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"],
          { name: name }
        );

      let transfers = await db
        .collection("transfers")
        .aggregate([
          {
            $match: time
              ? {
                  name,
                  block_timestamp: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {
                  name,
                },
          },
          {
            $project: {
              created_date: {
                $toDate: "$block_timestamp",
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

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), defaultValue);

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), defaultValue);
      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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
      let { name } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      let matchFormat: any = {
        name: name,
        event_type: "successful",
      };

      if (time) {
        matchFormat = {
          name: name,
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
                ...structure(time, name).idFormat,
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

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
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

  public GetTopSales = async (req: Request, res: Response) => {
    try {
      const time = req.query.time || "all";
      const name = req.params.name;

      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      let events_match = async (time) => {
        let subtractedTime;
        if (time)
          subtractedTime = await getSubtractedtime(
            time,
            ["rarible_events"],
            ["created_date"],
            { name: name }
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

      const topSalesByCollection = await db
        .collection("collections")
        .aggregate([
          {
            $match: {
              name: name,
            },
          },
          {
            $project: {
              name: 1,
              collection_name: "$name",
              collection_img_url: "$image_url",
              collection_address: "$address",
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "name",
              foreignField: "name",
              let: {
                name: "$name",
                token_id: "$token_id",
              },
              as: "events",
              pipeline: [
                {
                  $project: {
                    name: 1,
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
              name: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: "$events.token_id",
              total_price: {
                $divide: [
                  {
                    $convert: {
                      input: "$events.created_date",
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
                name: "$name",
                token_id: "$token_id",
              },
              as: "result",
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ["$name", "$$name"],
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
                    created_date: "$$ROOT.created_date",
                  },
                ],
              },
            },
          },
          {
            $project: {
              name: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              total_price: {
                $divide: ["$total_price", 1000000000000000000],
              },
              created_date: 1,
              image_url: 1,
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              let: {
                name: "$name",
                token_id: "$token_id",
              },
              as: "owner",
              pipeline: [
                {
                  $project: {
                    name: 1,
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
                          $eq: ["$name", "$$name"],
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
              name: 1,
              collection_name: 1,
              collection_img_url: 1,
              collection_address: 1,
              token_id: 1,
              total_price: 1,
              created_date: 1,
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
              localField: "name",
              foreignField: "name",
              let: {
                name: "$name",
                token_id: "$token_id",
              },
              as: "last_price",
              pipeline: [
                {
                  $project: {
                    name: 1,
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
                    name: 1,
                    token_id: 1,
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
                          $eq: ["$name", "$$name"],
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
              name: 1,
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
              size: {
                $size: "$last_price.data",
              },
              last_price: 1,
            },
          },
          {
            $project: {
              name: 1,
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
              localField: "name",
              foreignField: "name",
              let: {
                name: "$name",
                token_id: "$token_id",
              },
              as: "transfers",
              pipeline: [
                {
                  $project: {
                    name: 1,
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
                    name: 1,
                    token_id: 1,
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
                          $eq: ["$name", "$$name"],
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
              localField: "name",
              foreignField: "name",
              let: {
                name: "$name",
                token_id: "$token_id",
              },
              as: "last_deal",
              pipeline: [
                {
                  $project: {
                    name: 1,
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
                    name: 1,
                    token_id: 1,
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
                          $eq: ["$name", "$$name"],
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
                    _id: ["$name", "$token_id"],
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
                    last_deal: {
                      $dateDiff: {
                        startDate: "$recent_deal",
                        endDate: "$$NOW",
                        unit: "hour",
                      },
                    },
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
          data: topSalesByCollection,
        })
      );

      res.status(200).send({
        success: true,
        data: topSalesByCollection,
      });
    } catch (err) {
      console.log(err);
      return res.status(500).json({
        message: err.message,
      });
    }
  };

  public GetSalesCurrency = async (req: Request, res: Response) => {
    try {
      // TODO: Add the pipeline
      const data = await db.collection("transfers").aggregate().toArray();
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

  public GetVolumeSimilar = async (req: Request, res: Response) => {
    let { name } = req.params;
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"],
        { name: name }
      );

    try {
      const pipeline = [
        {
          $match: {
            name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            volume: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $divide: [
                            {
                              $toDouble: "$total_price",
                            },
                            1000000000000000000,
                          ],
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  volume: {
                    $sum: "$events.total_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $divide: [
                            {
                              $toDouble: "$total_price",
                            },
                            1000000000000000000,
                          ],
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                        name: 1,
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  competitor_volume: {
                    $sum: "$events.total_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
            ],
            categoryVolume: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $divide: [
                            {
                              $toDouble: "$total_price",
                            },
                            1000000000000000000,
                          ],
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  categories_volume: {
                    $sum: "$events.total_price",
                  },
                  categories_avg_volume: {
                    $avg: "$events.total_price",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$volume._id", 0],
            },
            final: {
              $concatArrays: ["$competitor", "$volume", "$categoryVolume"],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            volume: {
              $sum: "$final.volume",
            },
            categories_volume: {
              $sum: "$final.categories_volume",
            },
            categories_avg_volume: {
              $avg: "$final.categories_avg_volume",
            },
            competitor_volume: {
              $sum: "$final.competitor_volume",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            volume: 1,
            categories_volume: 1,
            categories_avg_volume: 1,
            competitor_volume: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            value: "$data.volume",
            categories_value: "$data.categories_volume",
            categories_avg_value: "$data.categories_avg_volume",
            competitor_value: "$data.competitor_volume",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      let result = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];

      var startFrom = !time
        ? result.length
          ? dayjs(result[0]._id)
          : dayjs()
        : subtractedTime;

      let value = {
        value: 0,
        categories_value: 0,
        categories_avg_value: 0,
        competitor_value: 0,
        competitor: [],
      };

      let competitor: any = null;

      // Fix missing date range
      result.forEach((item) => {
        const date = dayjs(item._id);
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }

        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetFloorPriceSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            floorPrice: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "created",
                      },
                    },
                    {
                      $project: {
                        ending_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  floor_price: {
                    $min: "$events.ending_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "created",
                      },
                    },
                    {
                      $project: {
                        ending_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                        name: 1,
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  competitor_floor_price: {
                    $min: "$events.ending_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
            ],
            categoryFloorPrice: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "created",
                      },
                    },
                    {
                      $project: {
                        ending_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  categories_floor_price: {
                    $min: "$events.ending_price",
                  },
                  categories_avg_floor_price: {
                    $avg: "$events.ending_price",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$floorPrice._id", 0],
            },
            final: {
              $concatArrays: [
                "$competitor",
                "$floorPrice",
                "$categoryFloorPrice",
              ],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            floor_price: {
              $sum: "$final.floor_price",
            },
            categories_floor_price: {
              $sum: "$final.categories_floor_price",
            },
            categories_avg_floor_price: {
              $avg: "$final.categories_avg_floor_price",
            },
            competitor_floor_price: {
              $sum: "$final.competitor_floor_price",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            floor_price: 1,
            categories_floor_price: 1,
            categories_avg_floor_price: 1,
            competitor_floor_price: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            collection_starting_date: "$data.collection_starting_date",
            value: "$data.floor_price",
            categories_value: "$data.categories_floor_price",
            categories_avg_value: "$data.categories_avg_floor_price",
            competitor_value: "$data.competitor_floor_price",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const floorPrice = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? floorPrice.length
          ? dayjs(floorPrice[0]._id)
          : dayjs()
        : subtractedTime;

      var prevCollectionFloorPrice = floorPrice.length
        ? floorPrice[0].value
        : 0;
      var prevCategoryFloorPrice = floorPrice.length
        ? floorPrice[0].categories_value
        : 0;
      var prevCategoryAvgFloorPrice = floorPrice.length
        ? floorPrice[0].categories_avg_value
        : 0;
      var prevCompetitorFloorPrice = floorPrice.length
        ? floorPrice[0].competitor_value
        : 0;
      var prevCategories = floorPrice.length ? floorPrice[0].categories : [];

      let competitor: any = null;

      // Fix missing date range
      floorPrice.forEach((item, index) => {
        const date = dayjs(item._id);
        const value = {
          value: prevCollectionFloorPrice,
          categories_value: prevCategoryFloorPrice,
          categories: prevCategories,
          categories_avg_value: prevCategoryAvgFloorPrice,
          competitor_value: prevCompetitorFloorPrice,
        };
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
        prevCollectionFloorPrice = item.value;
        prevCategoryFloorPrice = item.categories_value;
        prevCategoryAvgFloorPrice = item.categories_avg_value;
        prevCompetitorFloorPrice = item.competitor_value;
        prevCategories = item.categories;
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      res.status(500).send(error);
    }
  };

  public GetMarketCapSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

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

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            marketcap: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $convert: {
                            input: "$total_price",
                            to: "double",
                          },
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                        token_id: {
                          $convert: {
                            input: "$token_id",
                            to: "double",
                          },
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    token_id: "$token_id",
                  },
                  last_traded_price: {
                    $last: "$events.total_price",
                  },
                  floor_price: {
                    $min: "$events.total_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $project: {
                  market_cap: {
                    $add: ["$floor_price", "$last_traded_price"],
                  },
                  categories: 1,
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  market_cap: {
                    $sum: {
                      $divide: ["$market_cap", 1000000000000000000],
                    },
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $convert: {
                            input: "$total_price",
                            to: "double",
                          },
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                        token_id: {
                          $convert: {
                            input: "$token_id",
                            to: "double",
                          },
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    token_id: "$token_id",
                  },
                  competitor_last_traded_price: {
                    $last: "$events.total_price",
                  },
                  competitor_floor_price: {
                    $min: "$events.total_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
              {
                $project: {
                  market_cap: {
                    $add: [
                      "$competitor_floor_price",
                      "$competitor_last_traded_price",
                    ],
                  },
                  competitor: 1,
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  competitor_market_cap: {
                    $sum: {
                      $divide: ["$market_cap", 1000000000000000000],
                    },
                  },
                  competitor: {
                    $max: "$competitor",
                  },
                },
              },
            ],
            categoryMarketcap: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                      },
                    },
                    {
                      $project: {
                        total_price: {
                          $convert: {
                            input: "$total_price",
                            to: "double",
                          },
                        },
                        created_date: {
                          $toDate: "$created_date",
                        },
                        token_id: {
                          $convert: {
                            input: "$token_id",
                            to: "double",
                          },
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    token_id: "$token_id",
                    name: "$name",
                  },
                  categories_last_traded_price: {
                    $last: "$events.total_price",
                  },
                  categories_floor_price: {
                    $min: "$events.total_price",
                  },
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  categories_last_traded_price: {
                    $sum: "$categories_last_traded_price",
                  },
                  categories_floor_price: {
                    $sum: "$categories_floor_price",
                  },
                  categories_avg_last_traded_price: {
                    $avg: "$categories_last_traded_price",
                  },
                  categories_avg_floor_price: {
                    $avg: "$categories_floor_price",
                  },
                },
              },
              {
                $project: {
                  _id: 1,
                  market_cap: {
                    $add: [
                      "$categories_floor_price",
                      "$categories_last_traded_price",
                    ],
                  },
                  avg_market_cap: {
                    $add: [
                      "$categories_avg_floor_price",
                      "$categories_avg_last_traded_price",
                    ],
                  },
                },
              },
              {
                $project: {
                  _id: 1,
                  category_market_cap: {
                    $sum: {
                      $divide: ["$market_cap", 1000000000000000000],
                    },
                  },
                  category_avg_market_cap: {
                    $sum: {
                      $divide: ["$avg_market_cap", 1000000000000000000],
                    },
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$marketcap._id", 0],
            },
            final: {
              $concatArrays: [
                "$competitor",
                "$marketcap",
                "$categoryMarketcap",
              ],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            market_cap: {
              $sum: "$final.market_cap",
            },
            category_market_cap: {
              $sum: "$final.category_market_cap",
            },
            competitor_market_cap: {
              $sum: "$final.competitor_market_cap",
            },
            category_avg_market_cap: {
              $sum: "$final.category_avg_market_cap",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            market_cap: 1,
            category_market_cap: 1,
            category_avg_market_cap: 1,
            competitor_market_cap: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            collection_starting_date: "$data.collection_starting_date",
            value: "$data.market_cap",
            categories_value: "$data.category_market_cap",
            categories_avg_value: "$data.category_avg_market_cap",
            competitor_value: "$data.competitor_market_cap",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const tokensMarketcap = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? tokensMarketcap.length
          ? dayjs(tokensMarketcap[0]._id)
          : dayjs()
        : subtractedTime;

      var prevCategoryMarketCap = tokensMarketcap.length
        ? tokensMarketcap[0].categories_value
        : 0;

      var prevCollectionMarketCap = tokensMarketcap.length
        ? tokensMarketcap[0].value
        : 0;

      var prevCategoryAvgMarketCap = tokensMarketcap.length
        ? tokensMarketcap[0].categories_avg_value
        : 0;

      var prevCompetitorMarketCap = tokensMarketcap.length
        ? tokensMarketcap[0].competitor_value
        : 0;

      var prevCategories = tokensMarketcap.length
        ? tokensMarketcap[0].categories
        : [];

      var prevCompetitor = tokensMarketcap.length
        ? tokensMarketcap[0].competitor
        : [];

      let competitor: any = null;

      // fix missing date range
      tokensMarketcap.forEach((item, index) => {
        const date = dayjs(item._id);
        const value = {
          categories_value: prevCategoryMarketCap,
          value: prevCollectionMarketCap,
          categories_avg_value: prevCategoryAvgMarketCap,
          competitor_value: prevCompetitorMarketCap,
          competitor: prevCompetitor,
          categories: prevCategories,
        };
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
        prevCategoryMarketCap = item.categories_value;
        prevCollectionMarketCap = item.value;
        prevCategoryAvgMarketCap = item.categories_avg_value;
        prevCompetitorMarketCap = item.competitor_value;
        prevCompetitor = item.competitor;
        prevCategories = item.categories;
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetSalesSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            categorySales: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  let: {
                    category: "$categories",
                  },
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        categories: 1,
                      },
                    },
                  ],
                  as: "output",
                },
              },
              {
                $unwind: {
                  path: "$output",
                },
              },
              {
                $project: {
                  name: "$output.name",
                  main_category: "$categories",
                  categories: "$output.categories",
                  intersect: {
                    $setIntersection: ["$categories", "$output.categories"],
                  },
                },
              },
              {
                $project: {
                  name: 1,
                  main_category: 1,
                  hasCategory: {
                    $cond: [
                      {
                        $gt: [
                          {
                            $size: "$intersect",
                          },
                          0,
                        ],
                      },
                      true,
                      false,
                    ],
                  },
                },
              },
              {
                $match: {
                  hasCategory: {
                    $eq: true,
                  },
                  name: {
                    $ne: name,
                  },
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
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
                  created_date: "$events.created_date",
                  name: "$events.name",
                  main_category: 1,
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  sales: {
                    $sum: 1,
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categorySales: "$sales",
                  categoryAvgSales: {
                    $avg: "$sales",
                  },
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            collectionSales: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
                    },
                  ],
                  as: "results",
                },
              },
              {
                $unwind: {
                  path: "$results",
                },
              },
              {
                $project: {
                  name: 1,
                  created_date: {
                    $toDate: "$results.created_date",
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  sales: {
                    $sum: 1,
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  collectionSales: "$sales",
                },
              },
            ],
            competitorSales: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
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
                  created_date: "$events.created_date",
                  next_top_collection: "$next_top_collection._id",
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  competitorSales: {
                    $sum: 1,
                  },
                  competitor: {
                    $last: "$next_top_collection",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  competitorSales: 1,
                  competitor: 1,
                },
              },
            ],
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: [
                "$categorySales",
                "$collectionSales",
                "$competitorSales",
              ],
            },
          },
        },
        {
          $unwind: {
            path: "$finalArray",
          },
        },
        {
          $group: {
            _id: "$finalArray._id",
            categorySales: {
              $sum: "$finalArray.categorySales",
            },
            categoryAvgSales: {
              $avg: "$finalArray.categoryAvgSales",
            },
            collectionSales: {
              $sum: "$finalArray.collectionSales",
            },
            categories: {
              $addToSet: "$finalArray.categories",
            },
            competitorSales: {
              $sum: "$finalArray.competitorSales",
            },
            competitor: {
              $last: "$finalArray.competitor",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories_value: "$categorySales",
            categories_avg_value: "$categoryAvgSales",
            value: "$collectionSales",
            competitor_value: "$competitorSales",
            competitor: 1,
            categories: {
              $arrayElemAt: ["$categories", 0],
            },
          },
        },
      ];

      const salesData = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? salesData.length
          ? dayjs(salesData[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        value: 0,
        categories_value: 0,
        competitor_value: 0,
        categories_avg_value: 0,
        categories: [],
      };

      let competitor: any = null;

      salesData.forEach((item, index) => {
        const date = dayjs(item._id);

        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor;
        }
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        data.push(item);
        startFrom = date;
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetLiquiditySimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let totalSupply = await db.collection("collections").findOne({
        name,
      });

      let categorySupply = await db
        .collection("collections")
        .aggregate([
          {
            $match: {
              name: name,
            },
          },
          {
            $project: {
              categories: 1,
            },
          },
          {
            $lookup: {
              from: "collections",
              localField: "categories",
              foreignField: "categories",
              let: {
                category: "$categories",
              },
              pipeline: [
                {
                  $project: {
                    name: 1,
                    total_supply: 1,
                    categories: 1,
                  },
                },
              ],
              as: "output",
            },
          },
          {
            $unwind: {
              path: "$output",
            },
          },
          {
            $project: {
              name: "$output.name",
              intersect: {
                $setIntersection: ["$categories", "$output.categories"],
              },
              total_supply: "$output.total_supply",
            },
          },
          {
            $project: {
              name: 1,
              total_supply: 1,
              hasCategory: {
                $cond: [
                  {
                    $gt: [
                      {
                        $size: "$intersect",
                      },
                      0,
                    ],
                  },
                  true,
                  false,
                ],
              },
            },
          },
          {
            $match: {
              hasCategory: {
                $eq: true,
              },
            },
          },
          {
            $group: {
              _id: null,
              avg_total_supply: {
                $avg: "$total_supply",
              },
              total_supply: {
                $sum: "$total_supply",
              },
            },
          },
        ])
        .toArray();

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            categorySales: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  let: {
                    category: "$categories",
                  },
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        categories: 1,
                      },
                    },
                  ],
                  as: "output",
                },
              },
              {
                $unwind: {
                  path: "$output",
                },
              },
              {
                $project: {
                  name: "$output.name",
                  main_category: "$categories",
                  categories: "$output.categories",
                  intersect: {
                    $setIntersection: ["$categories", "$output.categories"],
                  },
                },
              },
              {
                $project: {
                  name: 1,
                  main_category: 1,
                  hasCategory: {
                    $cond: [
                      {
                        $gt: [
                          {
                            $size: "$intersect",
                          },
                          0,
                        ],
                      },
                      true,
                      false,
                    ],
                  },
                },
              },
              {
                $match: {
                  hasCategory: {
                    $eq: true,
                  },
                  name: {
                    $ne: name,
                  },
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
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
                  created_date: "$events.created_date",
                  name: "$events.name",
                  main_category: 1,
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  sales: {
                    $sum: 1,
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categorySales: "$sales",
                  categoryAvgSales: {
                    $avg: "$sales",
                  },
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            collectionSales: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
                    },
                  ],
                  as: "results",
                },
              },
              {
                $unwind: {
                  path: "$results",
                },
              },
              {
                $project: {
                  name: 1,
                  created_date: {
                    $toDate: "$results.created_date",
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  sales: {
                    $sum: 1,
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  collectionSales: "$sales",
                },
              },
            ],
            competitorSales: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
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
                        name: 1,
                      },
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
                  created_date: "$events.created_date",
                  next_top_collection: "$next_top_collection._id",
                },
              },
              {
                $group: {
                  _id: structure(time, name).idFormat,
                  competitorSales: {
                    $sum: 1,
                  },
                  competitor: {
                    $last: "$next_top_collection",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  competitorSales: 1,
                  competitor: 1,
                },
              },
            ],
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: [
                "$categorySales",
                "$collectionSales",
                "$competitorSales",
              ],
            },
          },
        },
        {
          $unwind: {
            path: "$finalArray",
          },
        },
        {
          $group: {
            _id: "$finalArray._id",
            categorySales: {
              $sum: "$finalArray.categorySales",
            },
            categoryAvgSales: {
              $avg: "$finalArray.categoryAvgSales",
            },
            collectionSales: {
              $sum: "$finalArray.collectionSales",
            },
            categories: {
              $addToSet: "$finalArray.categories",
            },
            competitorSales: {
              $sum: "$finalArray.competitorSales",
            },
            competitor: {
              $last: "$finalArray.competitor",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories_sales: "$categorySales",
            categories_avg_sales: "$categoryAvgSales",
            sales: "$collectionSales",
            competitor_sales: "$competitorSales",
            competitor: 1,
            categories: {
              $arrayElemAt: ["$categories", 0],
            },
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const salesData = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? salesData.length
          ? dayjs(salesData[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        value: 0,
        categories_value: 0,
        categories_avg_value: 0,
        competitor_value: 0,
        sales: 0,
        categories_sales: 0,
        competitor_sales: 0,
        categories: [],
      };

      let competitor: any = null;

      salesData.forEach((item) => {
        const date = dayjs(item._id);
        if (!competitor && item.competitor) {
          competitor = item.competitor;
        }
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        (item.value = (item.sales / totalSupply.total_supply) * 100),
          (item.categories_value =
            (item.categories_sales / categorySupply[0].total_supply) * 100),
          (item.categories_avg_value =
            (item.categories_avg_sales / categorySupply[0].avg_total_supply) *
            100),
          // TODO: Should use competitor total supply
          (item.competitor_value =
            (item.competitor_sales / totalSupply.total_supply) * 100);
        data.push(item);
        startFrom = date;
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetAvgPriceSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

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

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            price: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  value: {
                    $avg: "$events.total_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  competitor_value: {
                    $avg: "$events.total_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
            ],
            categoryPrice: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    name: "$name",
                  },
                  avg_value: {
                    $avg: "$events.total_price",
                  },
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  categories_value: {
                    $avg: "$avg_value",
                  },
                  categories_avg_value: {
                    $avg: "$average_total_price",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$price._id", 0],
            },
            final: {
              $concatArrays: ["$competitor", "$price", "$categoryPrice"],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            value: {
              $sum: "$final.value",
            },
            competitor_value: {
              $sum: "$final.competitor_value",
            },
            categories_value: {
              $sum: "$final.categories_value",
            },
            categories_avg_value: {
              $sum: "$final.categories_avg_value",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            value: 1,
            competitor_value: 1,
            categories_value: 1,
            categories_avg_value: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            collection_starting_date: "$data.collection_starting_date",
            value: "$data.value",
            competitor_value: "$data.competitor_value",
            categories_value: "$data.categories_value",
            categories_avg_value: "$data.categories_avg_value",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const priceData = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();
      let data = [];

      const dafaultValue = {
        categories_value: 0,
        categories_avg_value: 0,
        value: 0,
        competitor_value: 0,
        competitor: [],
        categories: priceData.length ? priceData[0].categories : [],
      };

      var startFrom = !time
        ? priceData.length
          ? dayjs(priceData[0]._id)
          : dayjs()
        : subtractedTime;

      let competitor: any = null;

      // Fix missing date in the range
      priceData.forEach((item, index) => {
        const date = dayjs(item._id);
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetMinPriceSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

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

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            price: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  value: {
                    $min: "$events.total_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  competitor_value: {
                    $min: "$events.total_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
            ],
            categoryPrice: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    name: "$name",
                  },
                  value: {
                    $min: "$events.total_price",
                  },
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  categories_value: {
                    $min: "$value",
                  },
                  categories_avg_value: {
                    $avg: "$value",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$price._id", 0],
            },
            final: {
              $concatArrays: ["$competitor", "$price", "$categoryPrice"],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            value: {
              $sum: "$final.value",
            },
            competitor_value: {
              $sum: "$final.competitor_value",
            },
            categories_value: {
              $sum: "$final.categories_value",
            },
            categories_avg_value: {
              $sum: "$final.categories_avg_value",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            value: 1,
            competitor_value: 1,
            categories_value: 1,
            categories_avg_value: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            collection_starting_date: "$data.collection_starting_date",
            value: "$data.value",
            competitor_value: "$data.competitor_value",
            categories_value: "$data.categories_value",
            categories_avg_value: "$data.categories_avg_value",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const priceData = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();
      let data = [];

      const dafaultValue = {
        categories_value: 0,
        categories_avg_value: 0,
        value: 0,
        competitor_value: 0,
        competitor: [],
        categories: priceData.length ? priceData[0].categories : [],
      };

      var startFrom = !time
        ? priceData.length
          ? dayjs(priceData[0]._id)
          : dayjs()
        : subtractedTime;

      let competitor: any = null;

      // Fix missing date in the range
      priceData.forEach((item, index) => {
        const date = dayjs(item._id);
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetMaxPriceSimilar = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { name: name }
        );

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

      const pipeline = [
        {
          $match: {
            name: name,
          },
        },
        {
          $project: {
            _id: 0,
            name: 1,
            categories: 1,
          },
        },
        {
          $facet: {
            price: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  value: {
                    $max: "$events.total_price",
                  },
                  categories: {
                    $max: "$categories",
                  },
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            competitor: [
              {
                $lookup: {
                  from: "collections_volume_sorted",
                  localField: "categories",
                  foreignField: "meta.categories",
                  as: "next_top_collection",
                  pipeline: [
                    {
                      $sort: {
                        volume: -1,
                      },
                    },
                    {
                      $match: {
                        _id: {
                          $ne: name,
                        },
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
                  path: "$next_top_collection",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "next_top_collection._id",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: structure(time, name).idFormat,
                  competitor_value: {
                    $max: "$events.total_price",
                  },
                  competitor: {
                    $last: "$next_top_collection._id",
                  },
                },
              },
            ],
            categoryPrice: [
              {
                $lookup: {
                  from: "collections",
                  localField: "categories",
                  foreignField: "categories",
                  as: "similar",
                  pipeline: [
                    {
                      $project: {
                        name: 1,
                        _id: 0,
                      },
                    },
                    {
                      $match: {
                        name: {
                          $ne: name,
                        },
                      },
                    },
                  ],
                },
              },
              {
                $unwind: {
                  path: "$similar",
                },
              },
              {
                $project: {
                  name: "$similar.name",
                },
              },
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "name",
                  foreignField: "name",
                  as: "events",
                  pipeline: [
                    {
                      $match: {
                        event_type: "successful",
                        total_price: {
                          $nin: [null, "0", 0],
                        },
                      },
                    },
                    {
                      $project: {
                        total_price: {
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
                        created_date: {
                          $toDate: "$created_date",
                        },
                      },
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
                $group: {
                  _id: {
                    ...structure(time, name).idFormat,
                    name: "$name",
                  },
                  value: {
                    $max: "$events.total_price",
                  },
                },
              },
              {
                $group: {
                  _id: finalGroupFormat,
                  categories_value: {
                    $max: "$value",
                  },
                  categories_avg_value: {
                    $avg: "$value",
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            collection_starting_date: {
              $arrayElemAt: ["$price._id", 0],
            },
            final: {
              $concatArrays: ["$competitor", "$price", "$categoryPrice"],
            },
          },
        },
        {
          $unwind: {
            path: "$final",
          },
        },
        {
          $group: {
            _id: "$final._id",
            value: {
              $sum: "$final.value",
            },
            competitor_value: {
              $sum: "$final.competitor_value",
            },
            categories_value: {
              $sum: "$final.categories_value",
            },
            categories_avg_value: {
              $sum: "$final.categories_avg_value",
            },
            competitor: {
              $addToSet: "$final.competitor",
            },
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            categories: {
              $max: "$final.categories",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            collection_starting_date: getDateFormat(
              time,
              "$collection_starting_date"
            ),
            value: 1,
            competitor_value: 1,
            categories_value: 1,
            categories_avg_value: 1,
            competitor: 1,
            categories: 1,
          },
        },
        {
          $group: {
            _id: "$_id",
            collection_starting_date: {
              $first: "$collection_starting_date",
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            data: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $gte: ["$$i._id", "$collection_starting_date"],
                    },
                  },
                },
                as: "maxOccur",
                in: "$$maxOccur",
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
          $project: {
            _id: "$_id",
            collection_starting_date: "$data.collection_starting_date",
            value: "$data.value",
            competitor_value: "$data.competitor_value",
            categories_value: "$data.categories_value",
            categories_avg_value: "$data.categories_avg_value",
            competitor: "$data.competitor",
            categories: "$data.categories",
          },
        },
        {
          $match: {
            ...(time
              ? {
                  _id: {
                    $gte: subtractedTime.toDate(),
                  },
                }
              : {}),
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const priceData = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();
      let data = [];

      const dafaultValue = {
        categories_value: 0,
        categories_avg_value: 0,
        value: 0,
        competitor_value: 0,
        competitor: [],
        categories: priceData.length ? priceData[0].categories : [],
      };

      var startFrom = !time
        ? priceData.length
          ? dayjs(priceData[0]._id)
          : dayjs()
        : subtractedTime;

      let competitor: any = null;

      // Fix missing date in the range
      priceData.forEach((item, index) => {
        const date = dayjs(item._id);
        if (!competitor && item.competitor && item.competitor.length) {
          competitor = item.competitor[0];
        }
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          competitor: competitor,
          data: data,
        }),
        720
      );

      res.status(200).send({
        success: true,
        competitor: competitor,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send({
        success: false,
        message: error.message,
      });
    }
  };

  public GetPlatformVolumeAndSales = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
      ];

      let result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
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
      const { name } = req.params;
      const { time } = req.query;

      let pipeline = [
        {
          $match: {
            ...structure(time, name).matchFormat,
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
                // console.log(err);
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
