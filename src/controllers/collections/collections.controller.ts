//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import * as dayjs from "dayjs";
const fs = require("fs");
import { setCache, uniqueKey } from "../../utilities/redis";
import {
  fixMissingDateRange,
  getDateFormat,
  getSubtractedtime,
} from "../../helpers/formatter";
import { structure } from "../../helpers/stats";

export default class CollectionController {
  public GetAllCollections = async (req: Request, res: Response) => {
    try {
      let page: any = req.query.page || 1;
      let sort: any = req.query.sort || "volume"; // volume, owners, floorPrice, name, nftsInCirculation
      let order: any = req.query.order || "desc";

      const collections = await db
        .collection("policies")
        .find({})
        .sort({ [sort]: order == "desc" ? -1 : 1 })
        .skip((page - 1) * 10)
        .limit(10)
        .toArray();

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: collections,
      //   }),
      //   2 * 1440
      // );

      res.status(200).send({
        success: true,
        data: collections,
      });
    } catch (error) {
      res.status(500).send(error);
    }
  };

  public GetCollectionsListing = async (req: Request, res: Response) => {
    try {
      let pageString = req.query.page;
      let page = Number(pageString) || 1;
      let pageSize = Number(req.query.pageSize) || 10;
      let category = req.query.category || "All";

      let weightage = {
        volume: 1,
        holders: 0.2,
        buyers: 1,
        sellers: -0.2,
        no_of_transfers: 0.8,
        twitter_engagement: 0.8,
        reddit_engagement: 0.8,
        floor_price: 1,
        avg_price: 1,
        min_price: 1,
        max_price: 1,
        no_of_sales: 1.1,
        liquidity: 1,
        market_cap: 1,
      };

      if (!page || page <= 0) {
        page = 1;
      }

      let sortBy: any = req.query.sortBy || "volume_all";

      const collections = await db
        .collection("collections")
        .aggregate([
          {
            $project: {
              slug: 1,
              total_supply: 1,
              created_date: 1,
              image_url: 1,
              name: 1,
              categories: 1,
              address: 1,
            },
          },
          {
            $match: {
              ...(category === "All"
                ? {}
                : {
                    categories: {
                      $in: [category],
                    },
                  }),
              name: { $exists: true },
              slug: {
                $nin: ["theshiboshis"],
              },
            },
          },
          {
            $facet: {
              collection_info: [
                {
                  $project: {
                    _id: "$slug",
                    name: 1,
                    created_date: 1,
                    total_supply: 1,
                    image_url: 1,
                    categories: 1,
                    address: 1,
                  },
                },
              ],
              twitter_engagement: [
                {
                  $lookup: {
                    from: "tweets",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          like_count: 1,
                          retweet_count: 1,
                        },
                      },
                    ],
                    as: "tweets",
                  },
                },
                {
                  $unwind: {
                    path: "$tweets",
                  },
                },
                {
                  $group: {
                    _id: "$slug",
                    avg_likes: {
                      $avg: "$tweets.like_count",
                    },
                    avg_retweet: {
                      $avg: "$tweets.retweet_count",
                    },
                  },
                },
                {
                  $project: {
                    twitter_engagement: {
                      $sum: ["$avg_likes", "$avg_retweet"],
                    },
                  },
                },
              ],
              reddit_engagement: [
                {
                  $lookup: {
                    from: "reddit_posts",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          score: 1,
                          num_comments: 1,
                        },
                      },
                    ],
                    as: "subreddit",
                  },
                },
                {
                  $unwind: {
                    path: "$subreddit",
                  },
                },
                {
                  $group: {
                    _id: "$slug",
                    average_score: {
                      $avg: "$subreddit.score",
                    },
                    average_comments: {
                      $avg: "$subreddit.num_comments",
                    },
                  },
                },
                {
                  $project: {
                    reddit_engagement: {
                      $sum: ["$average_score", "$average_comments"],
                    },
                  },
                },
              ],
              volume_all: [
                {
                  $lookup: {
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $match: {
                          event_type: "successful",
                          total_price: {
                            $ne: "0",
                          },
                        },
                      },
                      {
                        $project: {
                          total_price: 1,
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
                  $group: {
                    _id: "$slug",
                    volume_all: {
                      $sum: {
                        $divide: [
                          {
                            $convert: {
                              input: "$events.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    total_sales: {
                      $sum: 1,
                    },
                    avg_price: {
                      $avg: {
                        $divide: [
                          {
                            $convert: {
                              input: "$events.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    min_price: {
                      $min: {
                        $divide: [
                          {
                            $convert: {
                              input: "$events.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    max_price: {
                      $max: {
                        $divide: [
                          {
                            $convert: {
                              input: "$events.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                  },
                },
              ],
              owners: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          to_address: 1,
                        },
                      },
                    ],
                    as: "transfers",
                  },
                },
                {
                  $unwind: {
                    path: "$transfers",
                  },
                },
                {
                  $group: {
                    _id: "$slug",
                    total_owners: {
                      $addToSet: "$transfers.to_address",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
                    total_owners: {
                      $size: "$total_owners",
                    },
                  },
                },
              ],
              volume_7d: [
                {
                  $lookup: {
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $match: {
                          event_type: "successful",
                          total_price: {
                            $nin: [null],
                          },
                        },
                      },
                      {
                        $project: {
                          created_date: 1,
                          total_price: 1,
                          slug: 1,
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
                    created_date: {
                      $toDate: "$events.created_date",
                    },
                    total_price: {
                      $toDouble: "$events.total_price",
                    },
                    slug: "$events.slug",
                  },
                },
                {
                  $match: {
                    created_date: {
                      $gte: dayjs().subtract(7, "day").toDate(),
                    },
                  },
                },
                {
                  $group: {
                    _id: {
                      slug: "$slug",
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
                  $group: {
                    _id: "$_id.slug",
                    values: {
                      $addToSet: {
                        date: {
                          $concat: [
                            { $toString: "$_id.year" },
                            "-",
                            { $toString: "$_id.month" },
                            "-",
                            { $toString: "$_id.day" },
                          ],
                        },
                        volume: "$volume",
                      },
                    },
                  },
                },
                {
                  $sort: {
                    "values.date": -1,
                  },
                },
              ],
              // floor_price: [
              //   {
              //     $lookup: {
              //       from: "rarible_events",
              //       localField: "slug",
              //       foreignField: "slug",
              //       pipeline: [
              //         {
              //           $match: {
              //             event_type: "created",
              //             ending_price: {
              //               $nin: [null, "0"],
              //             },
              //           },
              //         },
              //         {
              //           $project: {
              //             created_date: {
              //               $toDate: "$created_date",
              //             },
              //             ending_price: {
              //               $toDouble: "$ending_price",
              //             },
              //             slug: 1,
              //           },
              //         },
              //       ],
              //       as: "events",
              //     },
              //   },
              //   {
              //     $unwind: {
              //       path: "$events",
              //     },
              //   },
              //   {
              //     $group: {
              //       _id: {
              //         slug: "$events.slug",
              //       },
              //       floor_price: {
              //         $min: {
              //           $divide: [
              //             {
              //               $convert: {
              //                 input: "$events.ending_price",
              //                 to: "double",
              //               },
              //             },
              //             1000000000000000000,
              //           ],
              //         },
              //       },
              //     },
              //   },
              //   {
              //     $group: {
              //       _id: "$_id.slug",
              //       floor_price: {
              //         $min: "$floor_price",
              //       },
              //     },
              //   },
              //   {
              //     $sort: {
              //       _id: -1,
              //     },
              //   },
              // ],
              twitter: [
                {
                  $lookup: {
                    from: "tweets",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          created_date: {
                            $toDate: "$created_date",
                          },
                          slug: 1,
                          sentiment: 1,
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
                  $group: {
                    _id: "$result.slug",
                    twitter_sentiment: {
                      $avg: "$result.sentiment",
                    },
                  },
                },
              ],
              reddit: [
                {
                  $lookup: {
                    from: "reddit_posts",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          craeted_date: {
                            $toDate: {
                              $multiply: ["$created_utc", 1000],
                            },
                          },
                          slug: 1,
                          sentiment: 1,
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
                  $group: {
                    _id: "$result.slug",
                    reddit_sentiment: {
                      $avg: "$result.sentiment",
                    },
                  },
                },
              ],
              transfers: [
                {
                  $lookup: {
                    from: "transfers",
                    localField: "slug",
                    foreignField: "slug",
                    pipeline: [
                      {
                        $project: {
                          from_address: 1,
                          slug: 1,
                        },
                      },
                      {
                        $match: {
                          from_address: {
                            $ne: "0x0000000000000000000000000000000000000000",
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
                  $group: {
                    _id: "$result.slug",
                    no_of_transfers: {
                      $count: {},
                    },
                  },
                },
              ],
              market_cap: [
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
                      {
                        $project: {
                          total_price: 1,
                          slug: 1,
                          token_id: 1,
                          created_date: 1,
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
                  $group: {
                    _id: { slug: "$events.slug", token_id: "$events.token_id" },
                    last_traded_price: {
                      $last: {
                        $divide: [
                          {
                            $convert: {
                              input: "$events.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                  },
                  // floor_price: {
                  //   $min: {
                  //     $convert: {
                  //       input: "$events.total_price",
                  //       to: "double",
                  //     },
                  //   },
                  // },
                  // },
                },
                {
                  $group: {
                    _id: "$_id.slug",
                    // market_cap_count: { $sum: 1 },
                    market_cap: {
                      $sum: {
                        $max: [0, "$last_traded_price"],
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
                $concatArrays: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$volume_all",
                  "$collection_info",
                  "$owners",
                  "$volume_7d",
                  // "$floor_price",
                  "$twitter",
                  "$reddit",
                  "$transfers",
                  "$market_cap",
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
              reddit_engagement: {
                $max: "$all.reddit_engagement",
              },
              twitter_engagement: {
                $max: "$all.twitter_engagement",
              },
              volume_all: {
                $max: "$all.volume_all",
              },
              total_sales: {
                $max: "$all.total_sales",
              },
              avg_price: {
                $max: "$all.avg_price",
              },
              min_price: {
                $max: "$all.min_price",
              },
              max_price: {
                $max: "$all.max_price",
              },
              total_supply: {
                $max: "$all.total_supply",
              },
              image_url: {
                $max: "$all.image_url",
              },
              address: {
                $max: "$all.address",
              },
              name: {
                $max: "$all.name",
              },
              created_date: {
                $max: "$all.created_date",
              },
              owners: {
                $max: "$all.total_owners",
              },
              volume_7d: {
                $addToSet: "$all.values",
              },
              categories: {
                $addToSet: "$all.categories",
              },
              // floor_price: {
              //   $max: "$all.floor_price",
              // },
              twitter: {
                $max: "$all.twitter_sentiment",
              },
              reddit: {
                $max: "$all.reddit_sentiment",
              },
              market_cap: {
                $max: "$all.market_cap",
              },
              no_of_transfers: {
                $max: "$all.no_of_transfers",
              },
            },
          },
          {
            $project: {
              community_engagement: {
                $sum: ["$reddit_engagement", "$twitter_engagement"],
              },
              community_sentiment: {
                $sum: ["$reddit", "$twitter"],
              },
              community_score: {
                $sum: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$reddit",
                  "$twitter",
                ],
              },
              volume_all: 1,
              total_sales: 1,
              total_supply: 1,
              created_date: 1,
              name: 1,
              image_url: 1,
              address: 1,
              categories: { $arrayElemAt: ["$categories", 0] },
              owners: 1,
              volume_7d: { $arrayElemAt: ["$volume_7d", 0] },
              // floor_price: alchemy_floor_price,
              market_cap: 1,
              // market_cap_count: 1,
              tokenomic_score: {
                $round: [
                  {
                    $sum: [
                      {
                        $multiply: [
                          "$no_of_transfers",
                          weightage.no_of_transfers,
                        ],
                      },
                      {
                        $multiply: ["$total_sales", weightage.no_of_sales],
                      },
                      {
                        $multiply: ["$volume_all", weightage.volume],
                      },
                      {
                        $multiply: ["$avg_price", weightage.avg_price],
                      },
                      {
                        $multiply: ["$min_price", weightage.min_price],
                      },
                      {
                        $multiply: ["$max_price", weightage.max_price],
                      },
                      // {
                      //   $multiply: [
                      //     "$reddit_engagement",
                      //     weightage.reddit_engagement,
                      //   ],
                      // },
                      // {
                      //   $multiply: [
                      //     "$twitter_engagement",
                      //     weightage.twitter_engagement,
                      //   ],
                      // },
                      // {
                      //   $multiply: ["$floor_price", weightage.floor_price],
                      // },
                    ],
                  },
                  2,
                ],
              },
            },
          },
          {
            $sort: {
              [sortBy]: -1,
            },
          },
          { $skip: (page - 1) * pageSize },
          { $limit: pageSize },
        ])
        .toArray();

      let totalCount = await db.collection("collections").countDocuments();

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(totalCount / pageSize),
      };

      for (var i = 0; i < collections.length; i++) {
        const address = collections[i]?.address;

        let alchemy_result;

        try {
          const sdk = require("api")("@alchemy-docs/v1.0#1ae9z2il7zo8f1u");
          sdk.server("https://eth-mainnet.g.alchemy.com/nft/v2");

          alchemy_result = await sdk.getFloorPrice({
            contractAddress: address,
            apiKey: process.env.ALCHEMY_API_KEY,
          });
        } catch (e) {
          console.log(e);
        }

        const floor_price = address ? alchemy_result?.openSea?.floorPrice : 0;

        collections[i].floor_price = floor_price;
        // collections[i].market_cap = floor_price;
        collections[i].tokenomic_score =
          floor_price * weightage.floor_price + collections[i].tokenomic_score;
      }

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            paginatedData: paginatedData,
            totalCount: totalCount,
            collections: collections,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          paginatedData: paginatedData,
          totalCount: totalCount,
          collections: collections,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetCollection = async (req: Request, res: Response) => {
    let { name } = req.params;
    console.log(name);

    try {
      let collectionDetails = await db.collection("policies").findOne({ name });

      // setCache(
      //   uniqueKey(req),
      //   JSON.stringify({
      //     success: true,
      //     data: {
      //       ...collectionDetails,
      //     },
      //   }),
      //   1440
      // );

      res.status(200).send({
        success: true,
        data: {
          ...collectionDetails,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetNumberofNftsListed = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["listings"],
          ["timestamp"],
          { collection: name }
        );

      const pipeline = [
        {
          $match: {
            collection: name,
            timestamp: {
              $gte: new Date(subtractedTime).toISOString(),
            },
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
            nfts: {
              $sum: 1,
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            nfts: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      let nftsListed = await db
        .collection("listings")
        .aggregate(pipeline)
        .toArray();

      let data = [];

      var startFrom = !time
        ? nftsListed.length
          ? dayjs(nftsListed[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        nfts: 0,
      };

      // Convert id objects to datetime
      nftsListed.forEach((item, index) => {
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

      res.status(200).send({ success: true, data: data });
    } catch (error) {
      res.status(500).send(error);
    }
  };

  public GetCurrentListing = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      const pipeline = [
        {
          $match: {
            slug: "cryptopunks",
          },
        },
        {
          $project: {
            slug: 1,
            event_type: 1,
            token_id: 1,
            created_date: 1,
          },
        },
        {
          $group: {
            _id: "$slug",
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            _id: 1,
            recent_transaction: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $eq: ["$$i.event_type", "successful"],
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
          $lookup: {
            from: "rarible_events",
            localField: "recent_transaction.token_id",
            foreignField: "token_id",
            pipeline: [
              {
                $match: {
                  slug: "cryptopunks",
                  created_date: {
                    $ne: "$recent_transaction.created_date",
                  },
                  token_id: {
                    $ne: "$recent_transaction.token_id",
                  },
                  event_type: "created",
                },
              },
              {
                $project: {
                  slug: 1,
                  token_id: 1,
                  created_date: 1,
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
            _id: 0,
            slug: "$_id",
            token_id: "$output.token_id",
            created_date: "$output.created_date",
          },
        },
        {
          $lookup: {
            from: "tokens",
            localField: "token_id",
            foreignField: "token_id",
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      {
                        $eq: ["$token_id", "$$CURRENT.token_id"],
                      },
                      {
                        $eq: ["$slug", "$$CURRENT.slug"],
                      },
                    ],
                  },
                },
              },
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
          $sort: {
            created_date: -1,
          },
        },
        {
          $skip: (page - 1) * pageSize,
        },
        {
          $limit: 10,
        },
        {
          $project: {
            slug: 1,
            token_id: 1,
            created_date: {
              $toDate: "$created_date",
            },
            image_url: "$result.image_url",
          },
        },
      ];

      const currentListing = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      res.status(200).send({ success: true, data: currentListing });
    } catch (error) {
      res.status(500).send(error);
    }
  };
}
