//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";
import * as dayjs from "dayjs";

export default class ActivitiesController {
  public GetHistoricalMints = async (req: Request, res: Response) => {
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

      let pipeline = [
        {
          $match: time
            ? {
                slug,
                block_timestamp: {
                  $gte: subtractedTime.toISOString(),
                },
                from_address: "0x0000000000000000000000000000000000000000",
              }
            : {
                slug,
                from_address: "0x0000000000000000000000000000000000000000",
              },
        },
        {
          $group: {
            _id: {
              year: {
                $year: {
                  $toDate: "$block_timestamp",
                },
              },
              month: {
                $month: {
                  $toDate: "$block_timestamp",
                },
              },
              day: {
                $dayOfMonth: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            count: {
              $sum: 1,
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            count: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const result = await db
        .collection("transfers")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? result.length
          ? dayjs(result[0]._id)
          : dayjs()
        : subtractedTime;

      const defaultValue = {
        count: 0,
      };

      // Convert id objects to datetime
      result.forEach((item, index) => {
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

  public GetHistoricalBurns = async (req: Request, res: Response) => {
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

      let pipeline = [
        {
          $match: time
            ? {
                slug,
                block_timestamp: {
                  $gte: subtractedTime.toISOString(),
                },
                to_address: "0x0000000000000000000000000000000000000000",
              }
            : {
                slug,
                to_address: "0x0000000000000000000000000000000000000000",
              },
        },
        {
          $group: {
            _id: {
              year: {
                $year: {
                  $toDate: "$block_timestamp",
                },
              },
              month: {
                $month: {
                  $toDate: "$block_timestamp",
                },
              },
              day: {
                $dayOfMonth: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            count: {
              $sum: 1,
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            count: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const result = await db
        .collection("transfers")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? result.length
          ? dayjs(result[0]._id)
          : dayjs()
        : subtractedTime;

      const defaultValue = {
        count: 0,
      };

      // Convert id objects to datetime
      result.forEach((item, index) => {
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

  public GetCollectionActivity = async (req: Request, res: Response) => {
    try {
      let { slug, type } = req.params;

      let pipeline = [];

      switch (type) {
        case "list":
          pipeline = [
            {
              $match: {
                slug,
                event_type: "created",
              },
            },
            {
              $project: {
                _id: 0,
                timestamp: "$created_date",
                value: "$ending_price",
                token_id: 1,
                seller: "$seller.address",
              },
            },
            {
              $sort: {
                timestamp: -1,
              },
            },
            {
              $limit: 5000,
            },
          ];
          break;
        case "mint":
          pipeline = [
            {
              $match: {
                slug,
                from_address: "0x0000000000000000000000000000000000000000",
              },
            },
            {
              $project: {
                _id: 0,
                timestamp: "$block_timestamp",
                transaction_hash: 1,
                token_id: 1,
                to_address: 1,
              },
            },
            {
              $sort: {
                timestamp: -1,
              },
            },
            {
              $limit: 5000,
            },
          ];
          break;
        case "transfer":
          pipeline = [
            {
              $match: {
                slug,
                from_address: {
                  $ne: "0x0000000000000000000000000000000000000000",
                },
                value: "0",
              },
            },
            {
              $project: {
                _id: 0,
                timestamp: "$block_timestamp",
                transaction_hash: 1,
                token_id: 1,
                from_address: 1,
                to_address: 1,
              },
            },
            {
              $sort: {
                timestamp: -1,
              },
            },
            {
              $limit: 5000,
            },
          ];
          break;
        case "burn":
          pipeline = [
            {
              $match: {
                slug,
                to_address: "0x0000000000000000000000000000000000000000",
              },
            },
            {
              $project: {
                _id: 0,
                timestamp: "$block_timestamp",
                transaction_hash: 1,
                token_id: 1,
                from_address: 1,
              },
            },
            {
              $sort: {
                timestamp: -1,
              },
            },
            {
              $limit: 5000,
            },
          ];
          break;
        // case "sale":
        default:
          pipeline = [
            {
              $match: {
                slug,
                value: { $ne: "0" },
              },
            },
            {
              $project: {
                _id: 0,
                timestamp: "$block_timestamp",
                transaction_hash: 1,
                token_id: 1,
                from_address: 1,
                to_address: 1,
                value: {
                  $divide: [{ $toDouble: "$value" }, 1000000000000000000],
                },
              },
            },
            {
              $sort: {
                timestamp: -1,
              },
            },
            {
              $limit: 5000,
            },
          ];
          break;
      }

      const data = await db
        .collection(type === "list" ? "rarible_events" : "transfers")
        .aggregate(pipeline)
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        12 * 1440
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

  public GetActivities = async (req: Request, res: Response) => {
    try {
      // Getting slug Name
      let slugName = req.params.name;

      // Pagination declaration
      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      // Sorting type for transfers: lowest-price, highest-price, newest, oldest
      let sortType = req.query.sort || "newest";
      let sortOptions = (sortType) => {
        switch (sortType) {
          case "lowest-price":
            return { price: 1 };
          case "highest-price":
            return { price: -1 };
          case "newest":
            return { timestamp: -1 };
          case "oldest":
            return { timestamp: 1 };
          default:
            return { timestamp: -1 };
        }
      };

      //Transfers
      const activities = await db
        .collection("sales")
        .aggregate([
          {
            $match: {
              collection: slugName,
            },
          },
          {
            $unionWith: {
              coll: "listings",
              pipeline: [
                {
                  $match: {
                    action: "list",
                  },
                },
              ],
            },
          },
          {
            $sort: sortOptions(sortType),
          },
          {
            $skip: (page - 1) * pageSize,
          },
          {
            $limit: pageSize,
          },
        ])
        .toArray();

      // Get count of activities
      let totalCount = await db
        .collection("sales")
        .aggregate([
          {
            $match: {
              collection: slugName,
            },
          },
          {
            $unionWith: {
              coll: "listings",
              pipeline: [
                {
                  $match: {
                    action: "list",
                  },
                },
              ],
            },
          },
          {
            $count: "count",
          },
        ])
        .toArray();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(totalCount[0].count / pageSize),
      };

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: {
      //       activity: transfers,
      //       paginatedData,
      //     },
      //   }),
      //   1440
      // );

      res.status(200).send({
        success: true,
        data: {
          activity: activities,
          paginatedData,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };
}
