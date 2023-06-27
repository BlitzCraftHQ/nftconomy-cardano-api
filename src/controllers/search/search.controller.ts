//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
const googleTrends = require("google-trends-api");
import * as dayjs from "dayjs";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class SearchController {
  public SearchAll = async (req: Request, res: Response) => {
    let { searchKey }: any = req.query;
    searchKey = searchKey.replace(/[+]/g, "");
    let partialStr = searchKey.replace(/\s+/g, "");

    try {
      let pageTokensSize = 12;
      let pageUsersSize = 12;
      let pageCollectionsSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      if (searchKey) {
        const totalCollectionsCount = await db
          .collection("collections")
          .countDocuments({
            $or: [
              {
                slug: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
              {
                slug: {
                  $regex: partialStr,
                  $options: "i",
                },
              },
              {
                name: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
              {
                category: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
              {
                twitter_userame: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
            ],
          });

        let collections = await db
          .collection("collections")
          .find(
            {
              $or: [
                {
                  slug: {
                    $regex: searchKey,
                  },
                },
                {
                  slug: {
                    $regex: partialStr,
                    $options: "i",
                  },
                },
                {
                  name: {
                    $regex: searchKey,
                  },
                },
                {
                  category: {
                    $regex: searchKey,
                  },
                },
                {
                  twitter_userame: {
                    $regex: searchKey,
                  },
                },
              ],
            },
            {
              projection: {
                _id: 0,
                slug: 1,
                name: 1,
                image_url: 1,
                address: 1,
                created_date: 1,
                description: 1,
                categories: 1,
              },
            }
          )
          .sort({ total_supply: -1 })
          .skip((page - 1) * pageCollectionsSize)
          .limit(pageCollectionsSize)
          .toArray();

        const users = await db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                to_address: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
            },
            {
              $project: {
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            {
              $group: {
                _id: "$to_address",
                date: {
                  $last: "$block_timestamp",
                },
              },
            },
            {
              $lookup: {
                from: "rarible_events",
                localField: "_id",
                foreignField: "seller.address",
                pipeline: [
                  {
                    $project: {
                      seller: 1,
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
                _id: "$_id",
                output: {
                  $addToSet: {
                    profile_img_url: "$output.seller.profile_img_url",
                    username: "$output.seller.user.username",
                  },
                },
                date: {
                  $last: "$date",
                },
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
                users: [
                  {
                    $sort: {
                      date: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageUsersSize,
                  },
                  {
                    $limit: pageUsersSize,
                  },
                  {
                    $project: {
                      _id: 0,
                      address: "$_id",
                      username: {
                        $arrayElemAt: ["$output.username", 0],
                      },
                      image_url: {
                        $arrayElemAt: ["$output.profile_img_url", 0],
                      },
                    },
                  },
                ],
              },
            },
          ])
          .toArray();

        const totalTokensCount = await db.collection("tokens").countDocuments({
          $or: [
            {
              name: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              slug: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              slug: {
                $regex: partialStr,
                $options: "i",
              },
            },
            {
              token_id: {
                $regex: searchKey,
                $options: "i",
              },
            },
          ],
        });

        const tokens = await db
          .collection("tokens")
          .find(
            {
              $or: [
                {
                  name: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
                {
                  slug: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
                {
                  slug: {
                    $regex: partialStr,
                    $options: "i",
                  },
                },
                {
                  token_id: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
              ],
            },
            {
              projection: {
                _id: 0,
                name: 1,
                token_id: 1,
                image_url: 1,
                slug: 1,
              },
            }
          )
          .sort({ normalized_score: -1 })
          .skip((page - 1) * pageTokensSize)
          .limit(pageTokensSize)
          .toArray();

        const usersCount = users[0].totalCount[0]
          ? users[0].totalCount[0].count
          : 0;

        const totalCount =
          totalCollectionsCount + usersCount + totalTokensCount;
        const totalPageSize = pageUsersSize + pageTokensSize + pageUsersSize;

        let pagination = {
          currentPage: page,
          totalPages: Math.ceil(totalCount / totalPageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: {
              collections,
              users: users[0].users,
              tokens,
            },
            pagination,
            totalCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: {
            collections,
            users: users[0].users,
            tokens,
          },
          pagination,
          totalCount,
        });
      } else {
        const totalCollectionsCount = await db
          .collection("collections")
          .countDocuments();

        const collectionData = await db
          .collection("collections")
          .find(
            {},
            {
              projection: {
                _id: 0,
                slug: 1,
                name: 1,
                image_url: 1,
                address: 1,
                created_date: 1,
                description: 1,
              },
            }
          )
          .sort({ total_supply: -1 })
          .skip((page - 1) * pageCollectionsSize)
          .limit(pageCollectionsSize)
          .toArray();

        const totalTokensCount = await db.collection("tokens").countDocuments();

        const tokenData = await db
          .collection("tokens")
          .find(
            {},
            {
              projection: {
                _id: 0,
                name: 1,
                token_id: 1,
                image_url: 1,
                slug: 1,
              },
            }
          )
          .sort({ normalized_score: -1 })
          .skip((page - 1) * pageTokensSize)
          .limit(pageTokensSize)
          .toArray();

        const subtractedTime = dayjs().subtract(1, "month");

        const users = await db
          .collection("transfers")
          .aggregate([
            {
              $project: {
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            {
              $match: {
                block_timestamp: {
                  $gte: new Date(dayjs(subtractedTime).toISOString()),
                },
              },
            },
            {
              $group: {
                _id: "$to_address",
                date: {
                  $last: "$block_timestamp",
                },
              },
            },
            {
              $lookup: {
                from: "rarible_events",
                localField: "_id",
                foreignField: "seller.address",
                pipeline: [
                  {
                    $project: {
                      seller: 1,
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
                _id: "$output.seller.address",
                output: {
                  $addToSet: {
                    profile_img_url: "$output.seller.profile_img_url",
                    username: "$output.seller.user.username",
                  },
                },
                date: {
                  $last: "$date",
                },
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
                users: [
                  {
                    $sort: {
                      date: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageUsersSize,
                  },
                  {
                    $limit: pageUsersSize,
                  },
                  {
                    $project: {
                      _id: 0,
                      address: "$_id",
                      username: {
                        $arrayElemAt: ["$output.username", 0],
                      },
                      image_url: {
                        $arrayElemAt: ["$output.profile_img_url", 0],
                      },
                    },
                  },
                ],
              },
            },
          ])
          .toArray();

        const usersCount = users[0].totalCount[0].count;

        const totalCount =
          totalCollectionsCount + totalTokensCount + usersCount;
        const totalPageSize =
          pageCollectionsSize + pageTokensSize + pageUsersSize;
        const totalPages = Math.ceil(totalCount / totalPageSize);

        let pagination = {
          currentPage: page,
          totalPages: totalPages,
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: {
              collections: collectionData,
              tokens: tokenData,
              users: users[0].users,
            },
            pagination,
            totalCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: {
            collections: collectionData,
            tokens: tokenData,
            users: users[0].users,
          },
          pagination,
          totalCount,
        });
      }
    } catch (err) {
      console.log(err);
      res.status(500).send({
        success: false,
        message: err.message,
      });
    }
  };

  public SearchTokens = async (req: Request, res: Response) => {
    let { searchKey }: any = req.query;
    searchKey = searchKey.replace(/[+]/g, "");
    let partialStr = searchKey.replace(/\s+/g, "");

    try {
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      if (searchKey) {
        const totalCount = await db.collection("tokens").countDocuments({
          $or: [
            {
              name: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              slug: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              slug: {
                $regex: partialStr,
                $options: "i",
              },
            },
            {
              token_id: {
                $regex: searchKey,
                $options: "i",
              },
            },
          ],
        });

        const tokens = await db
          .collection("tokens")
          .find(
            {
              $or: [
                {
                  name: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
                {
                  slug: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
                {
                  slug: {
                    $regex: partialStr,
                    $options: "i",
                  },
                },
                {
                  token_id: {
                    $regex: searchKey,
                    $options: "i",
                  },
                },
              ],
            },
            {
              projection: {
                _id: 0,
                name: 1,
                token_id: 1,
                image_url: 1,
                slug: 1,
              },
            }
          )
          .sort({ normalized_score: -1 })
          .skip((page - 1) * pageSize)
          .limit(pageSize)
          .toArray();

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(totalCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: tokens,
            pagination,
            totalCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: tokens,
          pagination,
          totalCount,
        });
      } else {
        const totalTokensCount = await db.collection("tokens").countDocuments();

        const tokenData = await db
          .collection("tokens")
          .find(
            {},
            {
              projection: {
                _id: 0,
                name: 1,
                token_id: 1,
                image_url: 1,
                slug: 1,
              },
            }
          )
          .sort({ normalized_score: -1 })
          .skip((page - 1) * pageSize)
          .limit(pageSize)
          .toArray();

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(totalTokensCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: tokenData,
            totalResults: totalTokensCount,
            pagination,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: tokenData,
          totalResults: totalTokensCount,
          pagination,
        });
      }
    } catch (err) {
      console.log(err);
      res.status(500).send({
        success: false,
        message: err.message,
      });
    }
  };

  public SearchCollections = async (req: Request, res: Response) => {
    let { searchKey }: any = req.query;

    searchKey = searchKey.replace(/[+]/g, "");
    let partialStr = searchKey.replace(/\s+/g, "");

    try {
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      if (searchKey) {
        const totalCount = await db.collection("collections").countDocuments({
          $or: [
            {
              slug: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              slug: {
                $regex: partialStr,
                $options: "i",
              },
            },
            {
              name: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              category: {
                $regex: searchKey,
                $options: "i",
              },
            },
            {
              twitter_userame: {
                $regex: searchKey,
                $options: "i",
              },
            },
          ],
        });

        let collections = await db
          .collection("collections")
          .find(
            {
              $or: [
                {
                  slug: {
                    $regex: searchKey,
                    $options: "si",
                  },
                },
                {
                  slug: {
                    $regex: partialStr,
                    $options: "i",
                  },
                },
                {
                  name: {
                    $regex: searchKey,
                    $options: "si",
                  },
                },
                {
                  category: {
                    $regex: searchKey,
                    $options: "si",
                  },
                },
                {
                  twitter_userame: {
                    $regex: searchKey,
                    $options: "si",
                  },
                },
              ],
            },
            {
              projection: {
                _id: 0,
                slug: 1,
                name: 1,
                image_url: 1,
                address: 1,
                created_date: 1,
                description: 1,
                categories: 1,
              },
            }
          )
          .sort({ total_supply: -1 })
          .skip((page - 1) * pageSize)
          .limit(pageSize)
          .toArray();

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(totalCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: collections,
            pagination,
            totalCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: collections,
          pagination,
          totalCount,
        });
      } else {
        const totalCollectionsCount = await db
          .collection("collections")
          .countDocuments();

        const collectionData = await db
          .collection("collections")
          .find(
            {},
            {
              projection: {
                _id: 0,
                slug: 1,
                name: 1,
                image_url: 1,
                address: 1,
                created_date: 1,
                description: 1,
                categories: 1,
              },
            }
          )
          .sort({ total_supply: -1 })
          .skip((page - 1) * pageSize)
          .limit(pageSize)
          .toArray();

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(totalCollectionsCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: collectionData,
            pagination,
            totalCount: totalCollectionsCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: collectionData,
          pagination,
          totalCount: totalCollectionsCount,
        });
      }
    } catch (err) {
      console.log(err);
      res.status(500).send({
        success: false,
        message: err.message,
      });
    }
  };

  public SearchUsers = async (req: Request, res: Response) => {
    let { searchKey }: any = req.query;
    searchKey = searchKey.replace(/[+]/g, "");
    try {
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      if (searchKey) {
        const users = await db
          .collection("transfers")
          .aggregate([
            {
              $match: {
                to_address: {
                  $regex: searchKey,
                  $options: "i",
                },
              },
            },
            {
              $project: {
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            {
              $group: {
                _id: "$to_address",
                date: {
                  $last: "$block_timestamp",
                },
              },
            },
            {
              $lookup: {
                from: "rarible_events",
                localField: "_id",
                foreignField: "seller.address",
                pipeline: [
                  {
                    $project: {
                      seller: 1,
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
                _id: "$_id",
                output: {
                  $addToSet: {
                    profile_img_url: "$output.seller.profile_img_url",
                    username: "$output.seller.user.username",
                  },
                },
                date: {
                  $last: "$date",
                },
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
                users: [
                  {
                    $sort: {
                      date: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                  {
                    $project: {
                      _id: 0,
                      address: "$_id",
                      username: {
                        $arrayElemAt: ["$output.username", 0],
                      },
                      image_url: {
                        $arrayElemAt: ["$output.profile_img_url", 0],
                      },
                    },
                  },
                ],
              },
            },
          ])
          .toArray();

        const usersCount = users[0].totalCount[0]
          ? users[0].totalCount[0].count
          : 0;

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(usersCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: users[0].users,
            pagination,
            totalCount: usersCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: users[0].users,
          pagination,
          totalCount: usersCount,
        });
      } else {
        const subtractedTime = dayjs().subtract(1, "month");
        const users = await db
          .collection("transfers")
          .aggregate([
            {
              $project: {
                to_address: 1,
                block_timestamp: {
                  $toDate: "$block_timestamp",
                },
              },
            },
            {
              $match: {
                block_timestamp: {
                  $gte: new Date(dayjs(subtractedTime).toISOString()),
                },
              },
            },
            {
              $group: {
                _id: "$to_address",
                date: {
                  $last: "$block_timestamp",
                },
              },
            },
            {
              $lookup: {
                from: "rarible_events",
                localField: "_id",
                foreignField: "seller.address",
                pipeline: [
                  {
                    $project: {
                      seller: 1,
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
                _id: "$output.seller.address",
                output: {
                  $addToSet: {
                    profile_img_url: "$output.seller.profile_img_url",
                    username: "$output.seller.user.username",
                  },
                },
                date: {
                  $last: "$date",
                },
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
                users: [
                  {
                    $sort: {
                      date: -1,
                    },
                  },
                  {
                    $skip: (page - 1) * pageSize,
                  },
                  {
                    $limit: pageSize,
                  },
                  {
                    $project: {
                      _id: 0,
                      address: "$_id",
                      username: {
                        $arrayElemAt: ["$output.username", 0],
                      },
                      image_url: {
                        $arrayElemAt: ["$output.profile_img_url", 0],
                      },
                    },
                  },
                ],
              },
            },
          ])
          .toArray();

        const usersCount = users[0].totalCount[0]
          ? users[0].totalCount[0].count
          : 0;

        let pagination = {
          pageSize: pageSize,
          currentPage: page,
          totalPages: Math.ceil(usersCount / pageSize),
        };

        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            results: users[0].users,
            pagination,
            totalCount: usersCount,
          }),
          2 * 1440
        );

        res.status(200).send({
          success: true,
          results: users[0].users,
          pagination,
          totalCount: usersCount,
        });
      }
    } catch (err) {
      console.log(err);
      res.status(500).send({
        success: false,
        message: err.message,
      });
    }
  };

  public SearchTrends = async (req: Request, res: Response) => {
    const { searchKey }: any = req.query;
    try {
      const trends = await googleTrends.relatedQueries({
        keyword: searchKey,
      });

      let arr = [];

      for (
        let i = 0;
        i < JSON.parse(trends).default.rankedList[0].rankedKeyword.length;
        i++
      ) {
        let query =
          JSON.parse(trends).default.rankedList[0].rankedKeyword[i].query;
        let link = `www.google.com/search?q=${query.replace(/ /g, "+")}`;
        arr.push({
          query,
          link,
        });
      }

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          results: arr,
        }),
        2 * 1440
      );

      res.status(200).send({
        success: true,
        results: arr,
      });
    } catch (err) {
      console.log(err);

      res.status(500).send({
        success: false,
        message: err.message,
      });
    }
  };
}
