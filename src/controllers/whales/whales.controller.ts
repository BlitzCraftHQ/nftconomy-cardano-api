//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import * as dayjs from "dayjs";
import { setCache, uniqueKey } from "../../utilities/redis";
import {
  fixMissingDateRange,
  getSubtractedtime,
  getDateFormat,
} from "../../helpers/formatter";

export default class WhalesController {
  public GetListing = async (req: Request, res: Response) => {
    let pageSize = 20;
    let pageString = req.query.page;
    let page = Number(pageString) || 1;
    const { sort } = req.query;
    const { order } = req.query;

    if (!page || page <= 0) {
      page = 1;
    }

    let sortType: any = {
      value: -1,
    };

    if (sort === "sellVolume" && order === "-1") {
      sortType = {
        sellVolume: -1,
      };
    }
    if (sort === "buyVolume" && order === "-1") {
      sortType = {
        value: -1,
      };
    }
    if (sort === "holdingValue" && order === "-1") {
      sortType = {
        value: -1,
      };
    }
    if (sort === "nfts" && order === "-1") {
      sortType = {
        tokens: -1,
      };
    }
    if (sort === "collections" && order === "-1") {
      sortType = {
        collections: -1,
      };
    }
    if (sort === "activities" && order === "-1") {
      sortType = {
        activities: -1,
      };
    }
    if (sort === "sellVolume" && order === "1") {
      sortType = {
        sellVolume: 1,
      };
    }
    if (sort === "buyVolume" && order === "1") {
      sortType = {
        value: 1,
      };
    }
    if (sort === "holdingValue" && order === "1") {
      sortType = {
        value: 1,
      };
    }
    if (sort === "nfts" && order === "1") {
      sortType = {
        tokens: 1,
      };
    }
    if (sort === "collections" && order === "1") {
      sortType = {
        collections: 1,
      };
    }
    if (sort === "activities" && order === "1") {
      sortType = {
        activities: 1,
      };
    }
    try {
      let whalesData = await db
        .collection("sales")
        .aggregate([
          {
            $project: {
              value: {
                $toDouble: "$price",
              },
              block_timestamp: "$timestamp",
              slug: "$collection",
              token_id: "$assetNameHex",
              to_address: "$toAddress",
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: "$value",
              },
              collectionsList: {
                $addToSet: "$slug",
              },
              tokensList: {
                $addToSet: {
                  tokenId: "$token_id",
                  slug: "$slug",
                },
              },
              activities: {
                $sum: 1,
              },
              lastDeal: {
                $max: "$block_timestamp",
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 10000,
              },
            },
          },
          {
            $project: {
              topHolding: {
                $max: "$tokensList.slug",
              },
              value: 1,
              collections: {
                $size: "$collectionsList",
              },
              tokens: {
                $size: "$tokensList",
              },
              activities: 1,
              lastDeal: 1,
              records: 1,
            },
          },
          {
            $lookup: {
              from: "policies",
              localField: "topHolding",
              foreignField: "name",
              pipeline: [
                {
                  $project: {
                    featuredImages: 1,
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
              topHolding: 1,
              topHoldingUrl: "$output.featuredImages",
              value: 1,
              collections: 1,
              tokens: 1,
              activities: 1,
              lastDeal: 1,
            },
          },

          {
            $lookup: {
              from: "sales",
              localField: "_id",
              foreignField: "fromAddress",
              pipeline: [
                {
                  $project: {
                    value: {
                      $toDouble: "$price",
                    },
                    from_address: "$fromAddress",
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
            $group: {
              _id: "$output.from_address",
              sellVolume: {
                $sum: "$output.value",
              },
              value: {
                $max: "$value",
              },
              slug: {
                $max: "$topHolding",
              },
              topHolding: {
                $max: "$value",
              },
              topHoldingUrl: {
                $max: "$topHoldingUrl",
              },
              collections: {
                $max: "$collections",
              },
              tokens: {
                $max: "$tokens",
              },
              activities: {
                $max: "$activities",
              },
              lastDeal: {
                $max: "$lastDeal",
              },
            },
          },
          {
            $facet: {
              sellerData: [
                {
                  $sort: sortType,
                },
                {
                  $skip: (page - 1) * pageSize,
                },
                {
                  $limit: pageSize,
                },
              ],
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: { $sum: 1 },
                  },
                },
              ],
            },
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(
          whalesData[0].totalCount[0]?.count || 0 / pageSize
        ),
      };

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: {
      //       paginatedData,
      //       list: whalesData[0].sellerData,
      //     },
      //   }),
      //   15 * 1440
      // );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          list: whalesData[0].sellerData,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetWhalesInCollection = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"],
          { slug: slug }
        );

      let matchFormat = {
        ...(time
          ? {
              created_date: {
                $gte: subtractedTime.toDate(),
              },
            }
          : {}),
      };

      // if (time == "24h") {
      //   subtractedTime = dayjs().subtract(1, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "7d") {
      //   subtractedTime = dayjs().subtract(7, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "30d") {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "3m") {
      //   subtractedTime = dayjs().subtract(3, "month");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "1y") {
      //   subtractedTime = dayjs().subtract(1, "year");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // }

      // if (time == "24h") {
      //   subtractedTime = dayjs().subtract(1, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "7d") {
      //   subtractedTime = dayjs().subtract(7, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "30d") {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "3m") {
      //   subtractedTime = dayjs().subtract(3, "month");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else if (time == "1y") {
      //   subtractedTime = dayjs().subtract(1, "year");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // } else {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      // }

      const data = await db
        .collection("whales_list")
        .aggregate([
          {
            $lookup: {
              from: "transfers",
              localField: "address",
              foreignField: "to_address",
              pipeline: [
                {
                  $project: {
                    block_timestamp: 1,
                  },
                },
              ],
              as: "string",
            },
          },
          {
            $unwind: {
              path: "$string",
            },
          },
          {
            $project: {
              created_date: {
                $toDate: "$string.block_timestamp",
              },
              address: 1,
            },
          },
          {
            $match: matchFormat,
          },
          {
            $group: {
              _id: {
                year: {
                  $year: "$created_date",
                },
                month: {
                  $month: "$created_date",
                },
                day: {
                  $dayOfMonth: "$created_date",
                },
              },
              whales: {
                $addToSet: "$address",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              whales: {
                $size: "$whales",
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      let _data = [];
      var startFrom = !time
        ? data.length
          ? dayjs(data[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        whales: 0,
      };

      data.forEach((item, index) => {
        const date = dayjs(item._id);
        fixMissingDateRange(_data, !time ? "1y" : time, startFrom, date, value);
        _data.push(item);
        startFrom = date;
      });

      // fixMissingDateRange(_data, !time ? "1y" : time, startFrom, dayjs(), value);

      res.status(200).send({
        success: true,
        data: _data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetMostMintedCollections = async (req: Request, res: Response) => {
    let pageSize = 20;
    let sortFormat;
    let subtractedTime;
    let matchFormat;
    let pageString = req.query.page;
    let page = Number(pageString) || 1;

    if (!page || page <= 0) {
      page = 1;
    }
    try {
      const { time, sort, order } = req.query;

      if (sort && order) {
        sortFormat = { [String(sort)]: Number(order) };
      } else {
        sortFormat = { whales: -1 };
      }

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "24h",
          ["transfers"],
          ["block_timestamp"]
        );
      }

      let topCollections = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              block_timestamp: {
                $toDate: "$block_timestamp",
              },
              slug: 1,
              token_id: 1,
              to_address: 1,
              from_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
              from_address: "0x0000000000000000000000000000000000000000",
              block_timestamp: {
                $gte: subtractedTime.toDate(),
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
              collectionsList: {
                $addToSet: "$slug",
              },
              firstMint: {
                $min: "$block_timestamp",
              },
            },
          },
          {
            $unwind: {
              path: "$collectionsList",
            },
          },
          {
            $group: {
              _id: "$collectionsList",
              address: {
                $addToSet: {
                  address: "$_id",
                  value: "$value",
                },
              },
              firstMint: {
                $min: "$firstMint",
              },
            },
          },
          {
            $unwind: {
              path: "$address",
            },
          },
          {
            $group: {
              _id: "$_id",
              whales: {
                $push: {
                  $cond: [
                    {
                      $gt: ["$address.value", 10000],
                    },
                    "$address.address",
                    "$$REMOVE",
                  ],
                },
              },
              minters: {
                $push: "$address.address",
              },
              first_mint: {
                $min: "$firstMint",
              },
              minted_activites: {
                $sum: {
                  $cond: [
                    {
                      $gt: ["$address.value", 10000],
                    },
                    1,
                    0,
                  ],
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
                    image_url: 1,
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
            $project: {
              _id: 0,
              slug: "$_id",
              minters: {
                $size: "$minters",
              },
              whales: {
                $size: "$whales",
              },
              first_mint: "$first_mint",
              whales_mint: "$minted_activites",
              image_url: "$result.image_url",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: sortFormat,
                },
                {
                  $skip: (page - 1) * pageSize,
                },
                {
                  $limit: pageSize,
                },
              ],
            },
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(
          topCollections[0].totalCount[0]?.count || 0 / pageSize
        ),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            top_collection: topCollections[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          top_collection: topCollections[0].data,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTopMinters = async (req: Request, res: Response) => {
    let pageSize = 20;
    let pageString = req.query.page;
    let page = Number(pageString) || 1;

    if (!page || page <= 0) {
      page = 1;
    }
    try {
      let subtractedTime;
      let sortFormat;
      const { time, sort, order } = req.query;

      if (sort && order) {
        sortFormat = { [String(sort)]: Number(order) };
      } else {
        sortFormat = { activities: -1 };
      }

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "24h",
          ["transfers"],
          ["block_timestamp"]
        );
      }

      let whalesData = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              block_timestamp: {
                $toDate: "$block_timestamp",
              },
              slug: 1,
              token_id: 1,
              to_address: 1,
              from_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
              from_address: "0x0000000000000000000000000000000000000000",
              block_timestamp: {
                $gte: subtractedTime.toDate(),
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
              collectionsList: {
                $addToSet: "$slug",
              },
              activities: {
                $sum: 1,
              },
              lastDeal: {
                $max: "$block_timestamp",
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 10000,
              },
            },
          },
          {
            $project: {
              _id: 0,
              whale_address: "$_id",
              value: 1,
              collections: {
                $size: "$collectionsList",
              },
              activities: 1,
              last_deal: "$lastDeal",
              records: 1,
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                { $sort: sortFormat },
                { $skip: (page - 1) * pageSize },
                { $limit: pageSize },
              ],
            },
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(
          whalesData[0].totalCount[0]?.count || 0 / pageSize
        ),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            whales_data: whalesData[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          whales_data: whalesData,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTrends = async (req: Request, res: Response) => {
    let subtractedTime;
    let matchFormat;
    let idFormat: any = {
      year: {
        $year: "$block_timestamp",
      },
      month: {
        $month: "$block_timestamp",
      },
      day: {
        $dayOfMonth: "$block_timestamp",
      },
    };
    let concatFormat: any = {
      $toDate: {
        $concat: [
          {
            $toString: "$_id.year",
          },
          "-",
          {
            $toString: "$_id.month",
          },
          "-",
          {
            $toString: "$_id.day",
          },
        ],
      },
    };

    const { time } = req.query;

    if (time) {
      subtractedTime = await getSubtractedtime(
        time,
        ["transfers"],
        ["block_timestamp"]
      );
    } else {
      subtractedTime = await getSubtractedtime(
        "30d",
        ["transfers"],
        ["block_timestamp"]
      );
    }

    if (time == "24h") {
      idFormat = {
        year: {
          $year: "$block_timestamp",
        },
        month: {
          $month: "$block_timestamp",
        },
        day: {
          $dayOfMonth: "$block_timestamp",
        },
        hour: {
          $hour: "$block_timestamp",
        },
      };
      concatFormat = {
        $toDate: {
          $concat: [
            {
              $toString: "$_id.year",
            },
            "-",
            {
              $toString: "$_id.month",
            },
            "-",
            {
              $toString: "$_id.day",
            },
            " ",
            {
              $toString: "$_id.hour",
            },
            ":00:00",
          ],
        },
      };
    } else if (time == "7d") {
      idFormat = {
        year: {
          $year: "$block_timestamp",
        },
        month: {
          $month: "$block_timestamp",
        },
        day: {
          $dayOfMonth: "$block_timestamp",
        },
        hour: {
          $multiply: [
            {
              $floor: {
                $divide: [{ $hour: "$block_timestamp" }, 2],
              },
            },
            2,
          ],
        },
      };
      concatFormat = {
        $toDate: {
          $concat: [
            {
              $toString: "$_id.year",
            },
            "-",
            {
              $toString: "$_id.month",
            },
            "-",
            {
              $toString: "$_id.day",
            },
            " ",
            {
              $toString: "$_id.hour",
            },
            ":00:00",
          ],
        },
      };
    }

    if (time) {
      matchFormat = {
        block_timestamp: {
          $gte: subtractedTime.toDate(),
        },
      };
    }
    try {
      const trends = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              value: {
                $ne: "0",
              },
            },
          },
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 10000,
              },
            },
          },
          {
            $facet: {
              sold: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "from_address",
                    pipeline: [
                      {
                        $match: {
                          to_address: {
                            $ne: "0x0000000000000000000000000000000000000000",
                          },
                          value: {
                            $ne: "0",
                          },
                        },
                      },
                      {
                        $project: {
                          to_address: 1,
                          block_timestamp: 1,
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
                    to_address: "$output.to_address",
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    to_address_array: {
                      $push: "$to_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    sold: {
                      $size: "$to_address_array",
                    },
                  },
                },
              ],
              bought: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "to_address",
                    pipeline: [
                      {
                        $match: {
                          from_address: {
                            $ne: "0x0000000000000000000000000000000000000000",
                          },
                          value: {
                            $ne: "0",
                          },
                        },
                      },
                      {
                        $project: {
                          from_address: 1,
                          block_timestamp: 1,
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
                    from_address: "$output.from_address",
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    from_address_array: {
                      $push: "$from_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    bought: {
                      $size: "$from_address_array",
                    },
                  },
                },
              ],
              mint: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "to_address",
                    pipeline: [
                      {
                        $match: {
                          from_address: {
                            $eq: "0x0000000000000000000000000000000000000000",
                          },
                          value: {
                            $eq: "0",
                          },
                        },
                      },
                      {
                        $project: {
                          to_address: 1,
                          from_address: 1,
                          block_timestamp: 1,
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
                    from_address: "$output.from_address",
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    from_address_array: {
                      $push: "$from_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    mint: {
                      $size: "$from_address_array",
                    },
                  },
                },
              ],
              burn: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "from_address",
                    pipeline: [
                      {
                        $match: {
                          to_address: {
                            $eq: "0x0000000000000000000000000000000000000000",
                          },
                          value: {
                            $eq: "0",
                          },
                        },
                      },
                      {
                        $project: {
                          to_address: 1,
                          from_address: 1,
                          block_timestamp: 1,
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
                    to_address: "$output.to_address",
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    to_address_array: {
                      $push: "$to_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    burn: {
                      $size: "$to_address_array",
                    },
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              buyers: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "to_address",
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
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                    to_address: "$output.to_address",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    address: {
                      $addToSet: "$to_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    buy_count: {
                      $size: "$address",
                    },
                  },
                },
              ],
              sellers: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "_id",
                    foreignField: "from_address",
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
                    block_timestamp: {
                      $toDate: "$output.block_timestamp",
                    },
                    from_address: "$output.from_address",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: idFormat,
                    address: {
                      $addToSet: "$from_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    sold_count: {
                      $size: "$address",
                    },
                  },
                },
              ],
            },
          },
          {
            $project: {
              all: {
                $concatArrays: [
                  "$sold",
                  "$bought",
                  "$mint",
                  "$burn",
                  "$buyers",
                  "$sellers",
                ],
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
              sold: {
                $sum: "$all.sold",
              },
              buy_count: {
                $sum: "$all.buy_count",
              },
              sell_count: {
                $sum: "$all.sold_count",
              },
              mint: {
                $sum: "$all.mint",
              },
              burn: {
                $sum: "$all.burn",
              },
              bought: {
                $sum: "$all.bought",
              },
            },
          },
          {
            $project: {
              _id: concatFormat,
              sold: 1,
              active_whales: {
                $sum: ["$buy_count", "$sell_count"],
              },
              mint: 1,
              burn: 1,
              bought: 1,
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
        ])
        .toArray();

      // Data Formatting
      let data = [];
      var startFrom = !time
        ? trends.length
          ? dayjs(trends[0]._id)
          : dayjs()
        : subtractedTime;

      trends.forEach((day) => {
        const date = dayjs(day._id);

        const value = {
          sold: 0,
          mint: 0,
          burn: 0,
          bought: 0,
          active_whales: 0,
        };

        // Fix sparse date ranges.
        fixMissingDateRange(data, !time ? "30d" : time, startFrom, date, value);

        data.push(day);
        startFrom = date;
      });

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

  public GetActivities = async (req: Request, res: Response) => {
    const type = req.params.type;

    try {
      if (type === "all") {
        const pageSize = 5;
        const { pageString } = req.query;
        let page = Number(pageString) || 1;

        if (!page || page <= 0) {
          page = 1;
        }

        const sold = db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                value: {
                  $ne: "0",
                },
              },
            },
            {
              $project: {
                value: {
                  $toDouble: "$value",
                },
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
                from_address: 1,
              },
            },
            {
              $group: {
                _id: "$to_address",
                value: {
                  $sum: {
                    $divide: ["$value", 1000000000000000000],
                  },
                },
              },
            },
            {
              $match: {
                value: {
                  $gte: 10000,
                },
              },
            },
            {
              $lookup: {
                from: "transfers",
                localField: "_id",
                foreignField: "from_address",
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
                _id: 0,
                from_address: "$output.from_address",
                to_address: "$output.to_address",
                value: {
                  $toDouble: "$output.value",
                },
                block_timestamp: {
                  $toDate: "$output.block_timestamp",
                },
                slug: "$output.slug",
                token_id: "$output.token_id",
              },
            },
            {
              $match: {
                to_address: {
                  $ne: "0x0000000000000000000000000000000000000000",
                },
                value: {
                  $ne: 0,
                },
              },
            },
            {
              $lookup: {
                from: "collections",
                localField: "slug",
                foreignField: "slug",
                as: "result",
              },
            },
            {
              $unwind: {
                path: "$result",
              },
            },
            {
              $project: {
                _id: 0,
                to_address: 1,
                from_address: 1,
                value: {
                  $divide: ["$value", 1000000000000000000],
                },
                block_timestamp: 1,
                slug: 1,
                token_id: 1,
                collection_img_url: "$result.image_url",
                type: "sold",
              },
            },
            {
              $facet: {
                totalCount: [
                  {
                    $group: {
                      _id: null,
                      totalCount: {
                        $sum: 1,
                      },
                    },
                  },
                ],
                data: [
                  {
                    $sort: {
                      block_timestamp: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                ],
              },
            },
          ])
          .toArray();

        const mint = db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                value: {
                  $ne: "0",
                },
              },
            },
            {
              $project: {
                value: {
                  $toDouble: "$value",
                },
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
                from_address: 1,
              },
            },
            {
              $group: {
                _id: "$to_address",
                value: {
                  $sum: {
                    $divide: ["$value", 1000000000000000000],
                  },
                },
              },
            },
            {
              $match: {
                value: {
                  $gte: 10000,
                },
              },
            },
            {
              $lookup: {
                from: "transfers",
                localField: "_id",
                foreignField: "to_address",
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
                _id: 0,
                to_address: "$output.to_address",
                from_address: "$output.from_address",
                value: {
                  $toDouble: "$output.value",
                },
                block_timestamp: {
                  $toDate: "$output.block_timestamp",
                },
                slug: "$output.slug",
                token_id: "$output.token_id",
              },
            },
            {
              $match: {
                from_address: {
                  $eq: "0x0000000000000000000000000000000000000000",
                },
                value: {
                  $eq: 0,
                },
              },
            },
            {
              $lookup: {
                from: "collections",
                localField: "slug",
                foreignField: "slug",
                as: "result",
              },
            },
            {
              $unwind: {
                path: "$result",
              },
            },
            {
              $project: {
                _id: 0,
                to_address: 1,
                from_address: 1,
                value: 1,
                block_timestamp: 1,
                slug: 1,
                token_id: 1,
                collection_img_url: "$result.image_url",
                type: "minted",
              },
            },
            {
              $facet: {
                totalCount: [
                  {
                    $group: {
                      _id: null,
                      count: {
                        $sum: 1,
                      },
                    },
                  },
                ],
                data: [
                  {
                    $sort: {
                      block_timestamp: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                ],
              },
            },
          ])
          .toArray();

        const burn = db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                value: {
                  $ne: "0",
                },
              },
            },
            {
              $project: {
                value: {
                  $toDouble: "$value",
                },
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
                from_address: 1,
              },
            },
            {
              $group: {
                _id: "$to_address",
                value: {
                  $sum: {
                    $divide: ["$value", 1000000000000000000],
                  },
                },
              },
            },
            {
              $match: {
                value: {
                  $gte: 10000,
                },
              },
            },
            {
              $lookup: {
                from: "transfers",
                localField: "_id",
                foreignField: "from_address",
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
                _id: 0,
                to_address: "$output.to_address",
                from_address: "$output.from_address",
                value: {
                  $toDouble: "$output.value",
                },
                block_timestamp: {
                  $toDate: "$output.block_timestamp",
                },
                slug: "$output.slug",
                token_id: "$output.token_id",
              },
            },
            {
              $match: {
                to_address: {
                  $eq: "0x0000000000000000000000000000000000000000",
                },
                value: {
                  $eq: 0,
                },
              },
            },
            {
              $lookup: {
                from: "collections",
                localField: "slug",
                foreignField: "slug",
                as: "result",
              },
            },
            {
              $unwind: {
                path: "$result",
              },
            },
            {
              $project: {
                _id: 0,
                to_address: 1,
                from_address: 1,
                value: 1,
                block_timestamp: 1,
                slug: 1,
                token_id: 1,
                collection_img_url: "$result.image_url",
                type: "burned",
              },
            },
            {
              $facet: {
                totalCount: [
                  {
                    $group: {
                      _id: null,
                      count: {
                        $sum: 1,
                      },
                    },
                  },
                ],
                data: [
                  {
                    $sort: {
                      block_timestamp: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                ],
              },
            },
          ])
          .toArray();

        const bought = db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                value: {
                  $ne: "0",
                },
              },
            },
            {
              $project: {
                value: {
                  $toDouble: "$value",
                },
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
                from_address: 1,
              },
            },
            {
              $group: {
                _id: "$to_address",
                value: {
                  $sum: {
                    $divide: ["$value", 1000000000000000000],
                  },
                },
              },
            },
            {
              $match: {
                value: {
                  $gte: 10000,
                },
              },
            },
            {
              $lookup: {
                from: "transfers",
                localField: "_id",
                foreignField: "to_address",
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
                _id: 0,
                to_address: "$output.to_address",
                from_address: "$output.from_address",
                value: {
                  $toDouble: "$output.value",
                },
                block_timestamp: {
                  $toDate: "$output.block_timestamp",
                },
                slug: "$output.slug",
                token_id: "$output.token_id",
              },
            },
            {
              $match: {
                from_address: {
                  $ne: "0x0000000000000000000000000000000000000000",
                },
                value: {
                  $ne: 0,
                },
              },
            },
            {
              $lookup: {
                from: "collections",
                localField: "slug",
                foreignField: "slug",
                as: "result",
              },
            },
            {
              $unwind: {
                path: "$result",
              },
            },
            {
              $project: {
                _id: 0,
                to_address: 1,
                from_address: 1,
                value: 1,
                block_timestamp: 1,
                slug: 1,
                token_id: 1,
                collection_img_url: "$result.image_url",
                type: "bought",
              },
            },
            {
              $facet: {
                totalCount: [
                  {
                    $group: {
                      _id: null,
                      count: {
                        $sum: 1,
                      },
                    },
                  },
                ],
                data: [
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                ],
              },
            },
          ])
          .toArray();

        const [soldRes, mintRes, burnRes, boughtRes] = await Promise.allSettled(
          [sold, mint, burn, bought]
        );

        let newArray = [];

        let totalRecords = 0;

        if (soldRes.status === "fulfilled") {
          totalRecords += soldRes.value[0].totalCount[0]?.count || 0;
          for (let j = 0; j < soldRes.value[0].data.length; j++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: soldRes.value[0].data[j].token_id,
                  slug: soldRes.value[0].data[j].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...soldRes.value[0].data[j],
              token_img_url: token_img,
            });
          }
        }
        if (mintRes.status === "fulfilled") {
          totalRecords += mintRes.value[0].totalCount[0]?.count || 0;
          for (let j = 0; j < mintRes.value[0].data.length; j++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: mintRes.value[0].data[j].token_id,
                  slug: mintRes.value[0].data[j].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...mintRes.value[0].data[j],
              token_img_url: token_img,
            });
          }
        }
        if (burnRes.status === "fulfilled") {
          totalRecords += burnRes.value[0].totalCount[0]?.count || 0;
          for (let j = 0; j < burnRes.value[0].data.length; j++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: burnRes.value[0].data[j].token_id,
                  slug: burnRes.value[0].data[j].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...burnRes.value[0].data[j],
              token_img_url: token_img,
            });
          }
        }
        if (boughtRes.status === "fulfilled") {
          totalRecords += boughtRes.value[0].totalCount[0]?.count || 0;
          for (let j = 0; j < boughtRes.value[0].data.length; j++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: boughtRes.value[0].data[j].token_id,
                  slug: boughtRes.value[0].data[j].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...boughtRes.value[0].data[j],
              token_img_url: token_img,
            });
          }
        }

        newArray.sort((a, b) => {
          return b.block_timestamp - a.block_timestamp;
        });

        const totalPages = Math.ceil(totalRecords / 20);

        let paginatedData = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: totalPages,
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            data: {
              activities: newArray,
              paginatedData,
            },
          }),
          1440
        );

        res.status(200).send({
          success: true,
          data: {
            activities: newArray,
            paginatedData,
          },
        });
      } else if (type === "sell") {
        const pageSize = 15;
        const { pageString } = req.query;
        let page = Number(pageString) || 1;

        if (!page || page <= 0) {
          page = 1;
        }
        try {
          const activities = await db
            .collection("transfers")
            .aggregate([
              {
                $match: {
                  value: {
                    $ne: "0",
                  },
                },
              },
              {
                $project: {
                  value: {
                    $toDouble: "$value",
                  },
                  to_address: 1,
                  block_timestamp: {
                    $toDate: "$block_timestamp",
                  },
                  from_address: 1,
                },
              },
              {
                $group: {
                  _id: "$to_address",
                  value: {
                    $sum: {
                      $divide: ["$value", 1000000000000000000],
                    },
                  },
                },
              },
              {
                $match: {
                  value: {
                    $gte: 10000,
                  },
                },
              },
              {
                $lookup: {
                  from: "transfers",
                  localField: "_id",
                  foreignField: "from_address",
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
                  _id: 0,
                  from_address: "$output.from_address",
                  to_address: "$output.to_address",
                  value: {
                    $toDouble: "$output.value",
                  },
                  block_timestamp: {
                    $toDate: "$block_timestamp",
                  },
                  slug: "$output.slug",
                  token_id: "$output.token_id",
                },
              },
              {
                $match: {
                  to_address: {
                    $ne: "0x0000000000000000000000000000000000000000",
                  },
                  value: {
                    $ne: 0,
                  },
                },
              },
              {
                $lookup: {
                  from: "collections",
                  localField: "slug",
                  foreignField: "slug",
                  as: "result",
                },
              },
              {
                $unwind: {
                  path: "$result",
                },
              },
              {
                $facet: {
                  totalCount: [
                    {
                      $group: {
                        _id: null,
                        count: {
                          $sum: 1,
                        },
                      },
                    },
                  ],
                  sold: [
                    {
                      $project: {
                        _id: 0,
                        to_address: 1,
                        from_address: 1,
                        value: {
                          $divide: ["$value", 1000000000000000000],
                        },
                        block_timestamp: 1,
                        slug: 1,
                        token_id: 1,
                        collection_img_url: "$result.image_url",
                        type: "sold",
                      },
                    },
                    {
                      $sort: {
                        block_timestamp: -1,
                      },
                    },
                    {
                      $skip: (page - 1) * pageSize,
                    },
                    {
                      $limit: pageSize,
                    },
                  ],
                },
              },
            ])
            .toArray();

          let newArray = [];
          let soldCount = activities[0].sold.length;

          for (let i = 0; i < soldCount; i++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: activities[0].sold[i].token_id,
                  slug: activities[0].sold[i].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            newArray.push({
              ...activities[0].sold[i],
              token_img_url: token_url[0].image_url,
            });
          }

          const totalCount = activities[0].totalCount[0]
            ? activities[0].totalCount[0].count
            : 0;
          const totalPages = Math.ceil(totalCount / pageSize);

          let paginatedData = {
            currentPge: page,
            totalPages,
          };

          setCache(
            uniqueKey(req),
            JSON.stringify({
              success: true,
              data: {
                activities: newArray,
                paginatedData,
              },
            }),
            1440
          );

          res.status(200).send({
            success: true,
            data: {
              activities: newArray,
              paginatedData,
            },
          });
        } catch (err) {
          console.log(err);
          res.status(500).send({
            success: false,
            message: err.message,
          });
        }
      } else if (type === "mint") {
        const pageSize = 15;
        const { pageString } = req.query;
        let page = Number(pageString) || 1;

        if (!page || page <= 0) {
          page = 1;
        }
        try {
          const activities = await db
            .collection("transfers")
            .aggregate([
              {
                $match: {
                  value: {
                    $ne: "0",
                  },
                },
              },
              {
                $project: {
                  value: {
                    $toDouble: "$value",
                  },
                  to_address: 1,
                  block_timestamp: {
                    $toDate: "$block_timestamp",
                  },
                  from_address: 1,
                },
              },
              {
                $group: {
                  _id: "$to_address",
                  value: {
                    $sum: {
                      $divide: ["$value", 1000000000000000000],
                    },
                  },
                },
              },
              {
                $match: {
                  value: {
                    $gte: 10000,
                  },
                },
              },
              {
                $lookup: {
                  from: "transfers",
                  localField: "_id",
                  foreignField: "to_address",
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
                  _id: 0,
                  to_address: "$output.to_address",
                  from_address: "$output.from_address",
                  value: {
                    $toDouble: "$output.value",
                  },
                  block_timestamp: {
                    $toDate: "$output.block_timestamp",
                  },
                  slug: "$output.slug",
                  token_id: "$output.token_id",
                },
              },
              {
                $match: {
                  from_address: {
                    $eq: "0x0000000000000000000000000000000000000000",
                  },
                  value: {
                    $eq: 0,
                  },
                },
              },
              {
                $lookup: {
                  from: "collections",
                  localField: "slug",
                  foreignField: "slug",
                  as: "result",
                },
              },
              {
                $unwind: {
                  path: "$result",
                },
              },
              {
                $facet: {
                  totalCount: [
                    {
                      $group: {
                        _id: null,
                        count: {
                          $sum: 1,
                        },
                      },
                    },
                  ],
                  mint: [
                    {
                      $project: {
                        _id: 0,
                        to_address: 1,
                        from_address: 1,
                        value: 1,
                        block_timestamp: 1,
                        slug: 1,
                        token_id: 1,
                        collection_img_url: "$result.image_url",
                        type: "minted",
                      },
                    },
                    {
                      $sort: {
                        block_timestamp: -1,
                      },
                    },
                    {
                      $skip: (page - 1) * pageSize,
                    },
                    {
                      $limit: pageSize,
                    },
                  ],
                },
              },
            ])
            .toArray();

          let newArray = [];
          const mintCount = activities[0].mint.length;

          for (let i = 0; i < mintCount; i++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: activities[0].mint[i].token_id,
                  slug: activities[0].mint[i].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            if (token_url[0]) {
              newArray.push({
                ...activities[0].mint[i],
                token_img_url: token_url[0].image_url,
              });
            } else {
              newArray.push({
                ...activities[0].mint[i],
                token_img_url: null,
              });
            }
          }

          const totalCount = activities[0].totalCount[0]
            ? activities[0].totalCount[0].count
            : 0;

          const totalPages = Math.ceil(totalCount / pageSize);

          let paginatedData = {
            pageSize: pageSize,
            currentPage: page,
            totalPages,
          };

          setCache(
            uniqueKey(req),
            JSON.stringify({
              success: true,
              data: {
                activities: newArray,
                paginatedData,
              },
            }),
            1440
          );

          res.status(200).send({
            success: true,
            data: {
              activities: newArray,
              paginatedData,
            },
          });
        } catch (err) {
          console.log(err);
          res.status(500).send(err);
        }
      } else if (type === "burn") {
        const pageSize = 15;
        const { pageString } = req.query;
        let page = Number(pageString) || 1;

        if (!page || page <= 0) {
          page = 1;
        }
        try {
          const activities = await db
            .collection("transfers")
            .aggregate([
              {
                $match: {
                  value: {
                    $ne: "0",
                  },
                },
              },
              {
                $project: {
                  value: {
                    $toDouble: "$value",
                  },
                  to_address: 1,
                  block_timestamp: {
                    $toDate: "$block_timestamp",
                  },
                  from_address: 1,
                },
              },
              {
                $group: {
                  _id: "$to_address",
                  value: {
                    $sum: {
                      $divide: ["$value", 1000000000000000000],
                    },
                  },
                },
              },
              {
                $match: {
                  value: {
                    $gte: 10000,
                  },
                },
              },
              {
                $lookup: {
                  from: "transfers",
                  localField: "_id",
                  foreignField: "from_address",
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
                  _id: 0,
                  to_address: "$output.to_address",
                  from_address: "$output.from_address",
                  value: {
                    $toDouble: "$output.value",
                  },
                  block_timestamp: {
                    $toDate: "$output.block_timestamp",
                  },
                  slug: "$output.slug",
                  token_id: "$output.token_id",
                },
              },
              {
                $match: {
                  to_address: {
                    $eq: "0x0000000000000000000000000000000000000000",
                  },
                  value: {
                    $eq: 0,
                  },
                },
              },
              {
                $lookup: {
                  from: "collections",
                  localField: "slug",
                  foreignField: "slug",
                  as: "result",
                },
              },
              {
                $unwind: {
                  path: "$result",
                },
              },
              {
                $facet: {
                  totalCount: [
                    {
                      $group: {
                        _id: null,
                        count: {
                          $sum: 1,
                        },
                      },
                    },
                  ],
                  burn: [
                    {
                      $project: {
                        _id: 0,
                        to_address: 1,
                        from_address: 1,
                        value: 1,
                        block_timestamp: 1,
                        slug: 1,
                        token_id: 1,
                        collection_img_url: "$result.image_url",
                        type: "burned",
                      },
                    },
                    {
                      $sort: {
                        block_timestamp: -1,
                      },
                    },
                    {
                      $skip: (page - 1) * pageSize,
                    },
                    {
                      $limit: pageSize,
                    },
                  ],
                },
              },
            ])
            .toArray();

          let newArray = [];
          const burnCount = activities[0].burn.length;

          for (let i = 0; i < burnCount; i++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: activities[0].burn[i].token_id,
                  slug: activities[0].burn[i].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...activities[0].burn[i],
              token_img_url: token_img,
            });
          }

          const totalCount = activities[0].totalCount[0]
            ? activities[0].totalCount[0].count
            : 0;

          const totalPages = Math.ceil(totalCount / pageSize);

          let paginatedData = {
            pageSize: pageSize,
            currentPage: page,
            totalPages,
          };

          setCache(
            uniqueKey(req),
            JSON.stringify({
              success: true,
              data: {
                activities: newArray,
                paginatedData,
              },
            }),
            1440
          );

          res.status(200).send({
            success: true,
            data: {
              activities: newArray,
              paginatedData,
            },
          });
        } catch (err) {
          console.log(err);
          res.status(500).send(err);
        }
      } else if (type === "buy") {
        const pageSize = 15;
        const { pageString } = req.query;
        let page = Number(pageString) || 1;

        if (!page || page <= 0) {
          page = 1;
        }
        try {
          const activities = await db
            .collection("transfers")
            .aggregate([
              {
                $match: {
                  value: {
                    $ne: "0",
                  },
                },
              },
              {
                $project: {
                  value: {
                    $toDouble: "$value",
                  },
                  to_address: 1,
                  block_timestamp: {
                    $toDate: "$block_timestamp",
                  },
                  from_address: 1,
                },
              },
              {
                $group: {
                  _id: "$to_address",
                  value: {
                    $sum: {
                      $divide: ["$value", 1000000000000000000],
                    },
                  },
                },
              },
              {
                $match: {
                  value: {
                    $gte: 10000,
                  },
                },
              },
              {
                $lookup: {
                  from: "transfers",
                  localField: "_id",
                  foreignField: "to_address",
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
                  _id: 0,
                  to_address: "$output.to_address",
                  from_address: "$output.from_address",
                  value: {
                    $toDouble: "$output.value",
                  },
                  block_timestamp: {
                    $toDate: "$output.block_timestamp",
                  },
                  slug: "$output.slug",
                  token_id: "$output.token_id",
                },
              },
              {
                $match: {
                  from_address: {
                    $ne: "0x0000000000000000000000000000000000000000",
                  },
                  value: {
                    $ne: 0,
                  },
                },
              },
              {
                $lookup: {
                  from: "collections",
                  localField: "slug",
                  foreignField: "slug",
                  as: "result",
                },
              },
              {
                $unwind: {
                  path: "$result",
                },
              },
              {
                $facet: {
                  totalCount: [
                    {
                      $group: {
                        _id: null,
                        count: {
                          $sum: 1,
                        },
                      },
                    },
                  ],
                  bought: [
                    {
                      $project: {
                        _id: 0,
                        to_address: 1,
                        from_address: 1,
                        value: 1,
                        block_timestamp: 1,
                        slug: 1,
                        token_id: 1,
                        collection_img_url: "$result.image_url",
                        type: "bought",
                      },
                    },
                    {
                      $sort: {
                        block_timestamp: -1,
                      },
                    },
                    {
                      $skip: (page - 1) * pageSize,
                    },
                    {
                      $limit: pageSize,
                    },
                  ],
                },
              },
            ])
            .toArray();

          let newArray = [];
          const boughtCount = activities[0].bought.length;

          for (let i = 0; i < boughtCount; i++) {
            let token_url = await db
              .collection("tokens")
              .find(
                {
                  token_id: activities[0].bought[i].token_id,
                  slug: activities[0].bought[i].slug,
                },
                {
                  projection: {
                    image_url: 1,
                  },
                }
              )
              .toArray();

            let token_img = token_url[0] ? token_url[0].image_url : null;

            newArray.push({
              ...activities[0].bought[i],
              token_img_url: token_img,
            });
          }

          const totalCount = activities[0].totalCount[0]
            ? activities[0].totalCount[0].count
            : 0;

          const totalPages = Math.ceil(totalCount / pageSize);

          let paginatedData = {
            pageSize: pageSize,
            currentPage: page,
            totalPages,
          };

          setCache(
            uniqueKey(req),
            JSON.stringify({
              success: true,
              data: {
                activities: newArray,
                paginatedData,
              },
            }),
            1440
          );

          res.status(200).send({
            success: true,
            data: {
              activities: newArray,
              paginatedData,
            },
          });
        } catch (err) {
          console.log(err);
          res.status(500).send(err);
        }
      }
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetTopBuyers = async (req: Request, res: Response) => {
    try {
      let pageSize = 20;
      let sortFormat;
      let subtractedTime;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      const { time, sort, order } = req.query;

      if (sort && order) {
        sortFormat = { [String(sort)]: Number(order) };
      } else {
        sortFormat = { buy_volume: -1 };
      }

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "24h",
          ["transfers"],
          ["block_timestamp"]
        );
      }

      const traders = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 1000,
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "_id",
              foreignField: "to_address",
              pipeline: [
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    value: 1,
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $ne: ["$value", "0"],
                        },
                        {
                          $ne: [
                            "$from_address",
                            "0x0000000000000000000000000000000000000000",
                          ],
                        },
                        {
                          $gte: [
                            "$$CURRENT.block_timestamp",
                            subtractedTime.toDate(),
                          ],
                        },
                      ],
                    },
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
            $project: {
              slug: "$result.slug",
              token_id: "$result.token_id",
              to_address: "$_id",
              block_timestamp: {
                $toDate: "$result.block_timestamp",
              },
              value: {
                $toDouble: "$result.value",
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              bought: {
                $addToSet: {
                  slug: "$slug",
                  token_id: "$token_id",
                },
              },
              collection: {
                $addToSet: {
                  slug: "$slug",
                },
              },
              buyVolume: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $project: {
              _id: 0,
              to_address: "$_id",
              bought_count: {
                $size: "$bought",
              },
              collection: {
                $size: "$collection",
              },
              buy_volume: "$buyVolume",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: sortFormat,
                },
                {
                  $skip: (page - 1) * pageSize,
                },
                {
                  $limit: pageSize,
                },
              ],
            },
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(traders[0].totalCount[0]?.count || 0 / pageSize),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            traders: traders[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          traders: traders[0].data,
        },
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetTopSellers = async (req: Request, res: Response) => {
    try {
      let pageSize = 20;
      let sortFormat;
      let subtractedTime;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      const { time, sort, order } = req.query;

      if (sort && order) {
        sortFormat = { [String(sort)]: Number(order) };
      } else {
        sortFormat = { sell_volume: -1 };
      }

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "24h",
          ["transfers"],
          ["block_timestamp"]
        );
      }

      const sellers = await db
        .collection("transfers")
        .aggregate([
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 1000,
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "_id",
              foreignField: "from_address",
              pipeline: [
                {
                  $project: {
                    value: 1,
                    slug: 1,
                    token_id: 1,
                    to_address: 1,
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $ne: ["$value", "0"],
                        },
                        {
                          $ne: [
                            "$to_address",
                            "0x0000000000000000000000000000000000000000",
                          ],
                        },
                        {
                          $gte: [
                            "$$CURRENT.block_timestamp",
                            subtractedTime.toDate(),
                          ],
                        },
                      ],
                    },
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
            $project: {
              slug: "$result.slug",
              token_id: "$result.token_id",
              from_address: "$_id",
              to_address: "$result.to_address",
              block_timestamp: {
                $toDate: "$result.block_timestamp",
              },
              value: {
                $toDouble: "$result.value",
              },
            },
          },
          {
            $group: {
              _id: "$from_address",
              sold: {
                $addToSet: {
                  slug: "$slug",
                  token_id: "$token_id",
                },
              },
              collection: {
                $addToSet: {
                  slug: "$slug",
                },
              },
              sellVolume: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $project: {
              _id: 0,
              from_address: "$_id",
              sold_count: {
                $size: "$sold",
              },
              collections: {
                $size: "$collection",
              },
              sell_volume: "$sellVolume",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: sortFormat,
                },
                {
                  $skip: (page - 1) * pageSize,
                },
                {
                  $limit: pageSize,
                },
              ],
            },
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(sellers[0].totalCount[0]?.count || 0 / pageSize),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            sellers: sellers[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          sellers: sellers[0].data,
        },
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetMostWhalesInvolved = async (req: Request, res: Response) => {
    try {
      let pageSize = 20;
      let sortFormat: any;
      let subtractedTime;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;
      const { type } = req.query;

      if (!page || page <= 0) {
        page = 1;
      }

      const { time, sort, order } = req.query;

      if (sort && order) {
        sortFormat = [{ $sort: { [String(sort)]: Number(order) } }];
      } else {
        sortFormat = [{ $sort: { whales: -1 } }];
      }

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers", "rarible_events"],
          ["block_timestamp", "created_date"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "24h",
          ["transfers", "rarible_events"],
          ["block_timestamp", "created_date"]
        );
      }

      let mostwhales: any;

      if (type == "sell") {
        let pipeline = [
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 1000,
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "_id",
              foreignField: "from_address",
              pipeline: [
                {
                  $project: {
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                    value: 1,
                    slug: 1,
                    token_id: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $ne: [
                            "$to_address",
                            "0x0000000000000000000000000000000000000000",
                          ],
                        },
                        {
                          $gte: [
                            "$$CURRENT.block_timestamp",
                            subtractedTime.toDate(),
                          ],
                        },
                      ],
                    },
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
            $project: {
              slug: "$result.slug",
              token_id: "$result.token_id",
              from_address: "$_id",
              block_timestamp: {
                $toDate: "$result.block_timestamp",
              },
              value: {
                $toDouble: "$result.value",
              },
            },
          },
          {
            $group: {
              _id: "$slug",
              sold: {
                $addToSet: {
                  slug: "$slug",
                  token_id: "$token_id",
                },
              },
              whaleVolume: {
                $sum: {
                  $divide: [
                    {
                      $convert: {
                        input: "$value",
                        to: "double",
                      },
                    },
                    1000000000000000000,
                  ],
                },
              },
              whales: {
                $addToSet: "$from_address",
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
                    image_url: 1,
                  },
                },
              ],
              as: "res",
            },
          },
          {
            $unwind: {
              path: "$res",
            },
          },
          {
            $project: {
              _id: 0,
              slug: "$_id",
              whales: {
                $size: "$whales",
              },
              soldCount: {
                $size: "$sold",
              },
              whaleVolume: "$whaleVolume",
              img_url: "$res.image_url",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $count: {},
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: {
                    whales: -1,
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
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          token_id: 1,
                          total_price: 1,
                          created_date: {
                            $toDate: "$created_date",
                          },
                          event_type: 1,
                        },
                      },
                      {
                        $match: {
                          $expr: {
                            $and: [
                              {
                                $eq: ["$event_type", "successful"],
                              },
                              {
                                $not: {
                                  $in: ["$total_price", [null, "0", 0]],
                                },
                              },
                              {
                                $gte: [
                                  "$$CURRENT.created_date",
                                  subtractedTime.toDate(),
                                ],
                              },
                            ],
                          },
                        },
                      },
                    ],
                    as: "res",
                  },
                },
                {
                  $unwind: {
                    path: "$res",
                  },
                },
                {
                  $group: {
                    _id: "$slug",
                    whales: {
                      $max: "$whales",
                    },
                    soldCount: {
                      $max: "$soldCount",
                    },
                    whaleVolume: {
                      $max: "$whaleVolume",
                    },
                    img_url: {
                      $max: "$img_url",
                    },
                    volume: {
                      $sum: {
                        $divide: [
                          {
                            $convert: {
                              input: "$res.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    average_total_price: {
                      $avg: {
                        $convert: {
                          input: "$res.total_price",
                          to: "double",
                        },
                      },
                    },
                    last_traded_price: {
                      $last: {
                        $divide: [
                          {
                            $convert: {
                              input: "$res.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    floor_price: {
                      $min: {
                        $divide: [
                          {
                            $convert: {
                              input: "$res.total_price",
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
                    _id: 0,
                    slug: "$_id",
                    whales: 1,
                    soldCount: 1,
                    whaleVolume: 1,
                    img_url: 1,
                    volume: "$volume",
                    floor_price: "$floor_price",
                    avg_price: "$average_total_price",
                    market_cap: {
                      $add: ["$floor_price", "$last_traded_price"],
                    },
                  },
                },
                ...sortFormat,
              ],
            },
          },
        ];

        mostwhales = await db
          .collection("transfers")
          .aggregate(pipeline)
          .toArray();
      } else if (type === "buy") {
        mostwhales = await db
          .collection("transfers")
          .aggregate([
            {
              $project: {
                value: {
                  $toDouble: "$value",
                },
                to_address: 1,
              },
            },
            {
              $match: {
                value: {
                  $ne: 0,
                },
              },
            },
            {
              $group: {
                _id: "$to_address",
                value: {
                  $sum: {
                    $divide: ["$value", 1000000000000000000],
                  },
                },
              },
            },
            {
              $match: {
                value: {
                  $gte: 1000,
                },
              },
            },
            {
              $lookup: {
                from: "transfers",
                localField: "_id",
                foreignField: "to_address",
                pipeline: [
                  {
                    $project: {
                      block_timestamp: {
                        $toDate: "$block_timestamp",
                      },
                      value: 1,
                      slug: 1,
                      token_id: 1,
                    },
                  },
                  {
                    $match: {
                      $expr: {
                        $and: [
                          {
                            $ne: [
                              "$from_address",
                              "0x0000000000000000000000000000000000000000",
                            ],
                          },
                          {
                            $gte: [
                              "$$CURRENT.block_timestamp",
                              subtractedTime.toDate(),
                            ],
                          },
                        ],
                      },
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
              $project: {
                slug: "$result.slug",
                token_id: "$result.token_id",
                to_address: "$_id",
                block_timestamp: {
                  $toDate: "$result.block_timestamp",
                },
                value: {
                  $toDouble: "$result.value",
                },
              },
            },
            {
              $group: {
                _id: "$slug",
                sold: {
                  $addToSet: {
                    slug: "$slug",
                    token_id: "$token_id",
                  },
                },
                whaleVolume: {
                  $sum: {
                    $divide: [
                      {
                        $convert: {
                          input: "$value",
                          to: "double",
                        },
                      },
                      1000000000000000000,
                    ],
                  },
                },
                whales: {
                  $addToSet: "$to_address",
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
                      image_url: 1,
                    },
                  },
                ],
                as: "res",
              },
            },
            {
              $unwind: {
                path: "$res",
              },
            },
            {
              $project: {
                _id: 0,
                slug: "$_id",
                whales: {
                  $size: "$whales",
                },
                boughtCount: {
                  $size: "$sold",
                },
                whaleVolume: "$whaleVolume",
                img_url: "$res.image_url",
              },
            },
            {
              $facet: {
                totalCount: [
                  {
                    $group: {
                      _id: null,
                      count: {
                        $count: {},
                      },
                    },
                  },
                ],
                data: [
                  {
                    $sort: {
                      whales: -1,
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
                      from: "rarible_events",
                      localField: "slug",
                      foreignField: "slug",
                      pipeline: [
                        {
                          $project: {
                            token_id: 1,
                            total_price: 1,
                            created_date: {
                              $toDate: "$created_date",
                            },
                            event_type: 1,
                          },
                        },
                        {
                          $match: {
                            $expr: {
                              $and: [
                                {
                                  $eq: ["$event_type", "successful"],
                                },
                                {
                                  $not: {
                                    $in: ["$total_price", [null, "0", 0]],
                                  },
                                },
                                {
                                  $gte: [
                                    "$$CURRENT.created_date",
                                    subtractedTime.toDate(),
                                  ],
                                },
                              ],
                            },
                          },
                        },
                      ],
                      as: "res",
                    },
                  },
                  {
                    $unwind: {
                      path: "$res",
                    },
                  },
                  {
                    $group: {
                      _id: "$slug",
                      whales: {
                        $max: "$whales",
                      },
                      boughtCount: {
                        $max: "$boughtCount",
                      },
                      whaleVolume: {
                        $max: "$whaleVolume",
                      },
                      img_url: {
                        $max: "$img_url",
                      },
                      volume: {
                        $sum: {
                          $divide: [
                            {
                              $convert: {
                                input: "$res.total_price",
                                to: "double",
                              },
                            },
                            1000000000000000000,
                          ],
                        },
                      },
                      average_total_price: {
                        $avg: {
                          $convert: {
                            input: "$res.total_price",
                            to: "double",
                          },
                        },
                      },
                      last_traded_price: {
                        $last: {
                          $divide: [
                            {
                              $convert: {
                                input: "$res.total_price",
                                to: "double",
                              },
                            },
                            1000000000000000000,
                          ],
                        },
                      },
                      floor_price: {
                        $min: {
                          $divide: [
                            {
                              $convert: {
                                input: "$res.total_price",
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
                      _id: 0,
                      slug: "$_id",
                      whales: 1,
                      boughtCount: 1,
                      whaleVolume: 1,
                      img_url: 1,
                      volume: "$volume",
                      floor_price: "$floor_price",
                      avg_price: "$average_total_price",
                      market_cap: {
                        $add: ["$floor_price", "$last_traded_price"],
                      },
                    },
                  },
                  ...sortFormat,
                ],
              },
            },
          ])
          .toArray();
      }

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(
          mostwhales[0].totalCount[0]?.count || 0 / pageSize
        ),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            mostwhales: mostwhales[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          mostwhales: mostwhales[0].data,
        },
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetWhalesBought = async (req: Request, res: Response) => {
    try {
      let pageSize = 20;
      let sortFormat;
      let subtractedTime;
      let type = req.query.type;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      const { time, sort, order } = req.query;

      if (time) {
        subtractedTime = await getSubtractedtime(
          time,
          ["transfers"],
          ["block_timestamp"]
        );
      } else {
        subtractedTime = await getSubtractedtime(
          "10m",
          ["transfers"],
          ["block_timestamp"]
        );
      }

      if (sort && order) {
        sortFormat = { [String(sort)]: Number(order) };
      } else {
        sortFormat = { price: -1 };
      }

      let pipeline = null;
      if (type == "sell") {
        let pipeline = [
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 1000,
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "_id",
              foreignField: "from_address",
              pipeline: [
                {
                  $project: {
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                    from_address: 1,
                    to_address: 1,
                    slug: 1,
                    token_id: 1,
                    value: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $ne: ["$value", 0],
                        },
                        {
                          $ne: [
                            "$to_address",
                            "0x0000000000000000000000000000000000000000",
                          ],
                        },
                        {
                          $gte: [
                            "$$CURRENT.block_timestamp",
                            subtractedTime.toDate(),
                          ],
                        },
                      ],
                    },
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
            $project: {
              from_address: "$result.from_address",
              to_address: "$result.to_address",
              time: {
                $toDate: "$result.block_timestamp",
              },
              collection: "$result.slug",
              token_id: "$result.token_id",
              price: {
                $divide: [
                  {
                    $toDouble: "$result.value",
                  },
                  1000000000000000000,
                ],
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              let: {
                slug: "$collection",
                token_id: "$token_id",
              },
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
                {
                  $project: {
                    token_img_url: "$image_url",
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
            $project: {
              from_address: 1,
              to_address: 1,
              time: 1,
              collection: 1,
              token_id: 1,
              price: 1,
              image_url: "$result.token_img_url",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: sortFormat,
                },
                {
                  $skip: pageSize * (page - 1),
                },
                {
                  $limit: pageSize,
                },
              ],
            },
          },
        ];
      } else if (type == "buy") {
        let pipeline = [
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $ne: 0,
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 1000,
              },
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "_id",
              foreignField: "to_address",
              pipeline: [
                {
                  $project: {
                    block_timestamp: {
                      $toDate: "$block_timestamp",
                    },
                    from_address: 1,
                    to_address: 1,
                    slug: 1,
                    token_id: 1,
                    value: 1,
                  },
                },
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $ne: ["$value", 0],
                        },
                        {
                          $ne: [
                            "$from_address",
                            "0x0000000000000000000000000000000000000000",
                          ],
                        },
                        {
                          $gte: [
                            "$$CURRENT.block_timestamp",
                            subtractedTime.toDate(),
                          ],
                        },
                      ],
                    },
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
            $project: {
              from_address: "$result.from_address",
              to_address: "$result.to_address",
              time: {
                $toDate: "$result.block_timestamp",
              },
              collection: "$result.slug",
              token_id: "$result.token_id",
              price: {
                $divide: [
                  {
                    $toDouble: "$result.value",
                  },
                  1000000000000000000,
                ],
              },
            },
          },
          {
            $lookup: {
              from: "tokens",
              let: {
                slug: "$collection",
                token_id: "$token_id",
              },
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
                {
                  $project: {
                    token_img_url: "$image_url",
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
            $project: {
              from_address: 1,
              to_address: 1,
              time: 1,
              collection: 1,
              token_id: 1,
              price: 1,
              image_url: "$result.token_img_url",
            },
          },
          {
            $facet: {
              totalCount: [
                {
                  $group: {
                    _id: null,
                    count: {
                      $sum: 1,
                    },
                  },
                },
              ],
              data: [
                {
                  $sort: sortFormat,
                },
                {
                  $skip: pageSize * (page - 1),
                },
                {
                  $limit: pageSize,
                },
              ],
            },
          },
        ];
      }

      const result = await db
        .collection("transfers")
        .aggregate(pipeline)
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(result[0].totalCount[0]?.count || 0 / pageSize),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData,
            result: result[0].data,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData,
          result: result[0].data,
        },
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetWhalesList = async (req: Request, res: Response) => {
    try {
      const list = await db
        .collection("transfers")
        .aggregate([
          {
            $group: {
              _id: {
                slug: "$slug",
                token_id: "$token_id",
              },
              last_transcation: {
                $max: "$transaction.timestamp",
              },
              items: {
                $push: "$$CURRENT",
              },
            },
          },
          {
            $project: {
              _id: 1,
              last_transaction: "$last_transaction",
              recent_transaction: {
                $map: {
                  input: {
                    $filter: {
                      input: "$items",
                      as: "i",
                      cond: {
                        $eq: [
                          "$$i.transaction.block_timestamp",
                          "$block_timestamp",
                        ],
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
              path: "$recent_transaction",
            },
          },
          {
            $replaceRoot: {
              newRoot: "$recent_transaction",
            },
          },
          {
            $project: {
              value: {
                $toDouble: "$value",
              },
              to_address: 1,
            },
          },
          {
            $match: {
              value: {
                $nin: [0],
              },
              to_address: {
                $nin: ["0x0000000000000000000000000000000000000000"],
              },
            },
          },
          {
            $group: {
              _id: "$to_address",
              value: {
                $sum: {
                  $divide: ["$value", 1000000000000000000],
                },
              },
            },
          },
          {
            $match: {
              value: {
                $gte: 10000,
              },
            },
          },
          {
            $project: {
              _id: 0,
              address: "$_id",
              value: 1,
            },
          },
          {
            $out: "whales_list",
          },
        ])
        .toArray();
      res.status(200).send({
        success: true,
        data: list,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };
}
