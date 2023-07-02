//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";
import { structure } from "../../helpers/stats";
import { rarityNameToScore, rarityScoreToName } from "../../helpers";
import { CardanoJamOnBreadAsset } from "../../../types";
import * as dayjs from "dayjs";

// Import lodash
import * as _ from "lodash";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";

export default class TokenController {
  public GetTokens = async (req: Request, res: Response) => {
    try {
      const { name } = req.params;
      let {
        search,
        rarity,
        trait_type,
        trait_value,
        min_price,
        max_price,
        sort,
        order,
      } = req.query;
      let pageSize = 12;
      // console.log(req.query);
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      let findQuery: any = { $and: [{ "collection.name": name }] };

      if (search) {
        findQuery["$and"].push({
          displayName: { $regex: String(search), $options: "i" },
        });
      }

      if (trait_type && trait_value) {
        let trait_types = String(trait_type).split(",");
        let trait_values = String(trait_value).split(",");
        for (let i = 0; i < String(trait_type).split(",").length; i++) {
          findQuery["$and"].push({
            properties: {
              $elemMatch: {
                name: trait_types[i],
                value: trait_values[i],
              },
            },
          });
        }
      }

      if (min_price || max_price) {
        if (min_price) {
          findQuery.$and.push({
            "sellOrder.price": {
              $gte: Number(min_price),
            },
          });
        }
        if (max_price) {
          findQuery.$and.push({
            "sellOrder.price": {
              $lte: Number(max_price),
            },
          });
        }
      }

      // append rarity_distrubution to findquery
      if (rarity) {
        findQuery["$and"].push({
          "rarity.score": {
            $gte: rarityNameToScore(rarity),
          },
        });
      }

      let data: any = await db
        .collection("assets")
        .aggregate([
          {
            $match: findQuery,
          },
          {
            $facet: {
              data: [
                {
                  $sort: {
                    [sort || ("displayName" as any)]: order === "asc" ? 1 : -1,
                  },
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
                  $count: "count",
                },
              ],
            },
          },
        ])
        .toArray();

      // if (data.length === 0) {
      //   setCache(
      //     uniqueKey(req),
      //     JSON.stringify({
      //       success: true,
      //       data: {
      //         paginatedData,
      //         data,
      //       },
      //     }),
      //     1440
      //   );
      // }

      res.status(200).send({
        success: true,
        data: {
          paginatedData: {
            pageSize: pageSize,
            currentPage: page,
            totalPages: data[0].totalCount[0].count / pageSize,
          },
          data: data[0].data,
        },
      });
    } catch (error) {
      console.log(error.toString());
      res.status(500).send(error);
    }
  };

  public GetTokenActivity = async (req: Request, res: Response) => {
    try {
      //Getting slug Name
      let slugName = req.params.slug;
      let token_id = req.params.token_id;

      //Pagination declaration
      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      //Sorting type for transfers
      let transfersSortType;
      transfersSortType = {
        block_timestamp: 1,
      };
      if (req.query.sort === "price") {
        transfersSortType = { value: 1 };
      }
      if (req.query.sort === "date") {
        transfersSortType = { block_timestamp: 1 };
      }

      //Transfers
      const transfers = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug: slugName,
              token_id: token_id,
            },
          },
          {
            $project: {
              event_type: {
                $cond: {
                  if: {
                    $eq: [
                      {
                        $convert: {
                          input: "$value",
                          to: 1,
                        },
                      },
                      0,
                    ],
                  },
                  then: "transfers",
                  else: "sale",
                },
              },
              block_timestamp: 1,
              slug: 1,
              from_address: 1,
              to_address: 1,
              token_id: 1,
              eth_price: {
                $divide: [
                  {
                    $convert: {
                      input: "$value",
                      to: 1,
                    },
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
                slug: "$slug",
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
                    token_name: "$name",
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
              slug: 1,
              token_id: 1,
              from_address: 1,
              to_address: 1,
              block_timestamp: 1,
              event_type: 1,
              eth_price: 1,
              token_name: "$result.token_name",
              token_img_url: "$result.token_img_url",
            },
          },

          {
            $sort: transfersSortType,
          },
          {
            $skip: (page - 1) * pageSize,
          },
          {
            $limit: pageSize,
          },
        ])
        .toArray();

      //Get count of Transfers
      let totalCount = await db
        .collection("transfers")
        .countDocuments({ slug: slugName, token_id: token_id });

      let paginatedData = {
        pageSize: pageSize,
        currentPage: page,
        totalPages: Math.ceil(totalCount / pageSize),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: {
            activity: transfers,
            paginatedData,
          },
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: {
          activity: transfers,
          paginatedData,
        },
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTokenPrice = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { token_id } = req.params;
    let { time } = req.query;

    let subtractedTime;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );

    try {
      let pipeline = [
        {
          $match: {
            ...structure(time, slug).matchFormat,
            ...(time
              ? {
                  created_date: {
                    $gte: subtractedTime.toISOString(),
                  },
                }
              : {}),
            token_id,
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
            _id: structure(time, slug).idFormat,
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
          data,
        }),
        1440
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

  public GetTokenOverview = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;
      let { token_id } = req.params;

      let weightage = {
        volume: 1,
        holders: 0.2,
        buyers: 0.4,
        sellers: 0.4,
        no_of_transfers: 0.3,
        twitter_engagement: 0.8,
        reddit_engagement: 0.6,
        // trends_engagement: 0.8,
        floor_price: 0.7,
        avg_price: 0.5,
        min_price: 0.2,
        max_price: 0.2,
        no_of_sales: 0.7,
        liquidity: 0.9,
        market_cap: 0.8,
      };

      let collectionDetails = await db
        .collection("collections")
        .findOne({ slug });

      let alchemy_result;

      try {
        const sdk = require("api")("@alchemy-docs/v1.0#1ae9z2il7zo8f1u");
        sdk.server("https://eth-mainnet.g.alchemy.com/nft/v2");

        alchemy_result = await sdk.getFloorPrice({
          contractAddress: collectionDetails?.address,
          apiKey: process.env.ALCHEMY_API_KEY,
        });
      } catch (e) {
        console.log(e);
      }

      const floor_price = collectionDetails?.address
        ? alchemy_result?.openSea?.floorPrice
        : 0;

      let tokenomic_score_data = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug: slug,
              // value: {
              //   $nin: ["0"],
              // },
              from_address: {
                $ne: "0x0000000000000000000000000000000000000000",
              },
            },
          },
          {
            $project: {
              slug: 1,
              token_id: 1,
            },
          },
          {
            $group: {
              _id: "$slug",
              tokens: {
                $push: "$token_id",
              },
              no_of_transfers: {
                $count: {},
              },
            },
          },
          {
            $unwind: {
              path: "$tokens",
            },
          },
          {
            $match: {
              tokens: token_id,
            },
          },
          {
            $group: {
              _id: {
                token: "$tokens",
                slug: "$_id",
              },
              no_of_transfers: {
                $max: "$no_of_transfers",
              },
              token_no_of_transfers: {
                $sum: 1,
              },
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "_id.slug",
              foreignField: "slug",
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
                  $group: {
                    _id: "$slug",
                    tokens: {
                      $push: {
                        token_id: "$token_id",
                        total_price: "$total_price",
                      },
                    },
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
                    avg_price: {
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
                    min_price: {
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
                    max_price: {
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
                  $unwind: {
                    path: "$tokens",
                  },
                },
                {
                  $match: {
                    "tokens.token_id": token_id,
                  },
                },
                {
                  $group: {
                    _id: {
                      slug: "$_id",
                      token_id: "$tokens.token_id",
                    },
                    collection: {
                      $first: "$$ROOT",
                    },
                    token_no_of_sales: {
                      $sum: 1,
                    },
                    token_volume: {
                      $sum: {
                        $divide: [
                          {
                            $convert: {
                              input: "$tokens.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    token_avg_price: {
                      $avg: {
                        $divide: [
                          {
                            $convert: {
                              input: "$tokens.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    token_min_price: {
                      $min: {
                        $divide: [
                          {
                            $convert: {
                              input: "$tokens.total_price",
                              to: "double",
                            },
                          },
                          1000000000000000000,
                        ],
                      },
                    },
                    token_max_price: {
                      $max: {
                        $divide: [
                          {
                            $convert: {
                              input: "$tokens.total_price",
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
              as: "events",
            },
          },
          // {
          //   $lookup: {
          //     from: "rarible_events",
          //     localField: "_id.slug",
          //     foreignField: "slug",
          //     pipeline: [
          //       {
          //         $match: {
          //           event_type: "created",
          //           ending_price: {
          //             $nin: [null, 0, "0"],
          //           },
          //         },
          //       },
          //       {
          //         $group: {
          //           _id: "$slug",
          //           tokens: {
          //             $push: {
          //               token_id: "$token_id",
          //               ending_price: "$ending_price",
          //             },
          //           },
          //           floor_price: {
          //             $min: {
          //               $divide: [
          //                 {
          //                   $convert: {
          //                     input: "$ending_price",
          //                     to: "double",
          //                   },
          //                 },
          //                 1000000000000000000,
          //               ],
          //             },
          //           },
          //         },
          //       },
          //       {
          //         $unwind: {
          //           path: "$tokens",
          //         },
          //       },
          //       {
          //         $match: {
          //           "tokens.token_id": token_id,
          //         },
          //       },
          //       {
          //         $group: {
          //           _id: {
          //             slug: "$_id",
          //             token_id: "$tokens.token_id",
          //           },
          //           collection: {
          //             $first: "$$ROOT",
          //           },
          //           token_floor_price: {
          //             $min: {
          //               $divide: [
          //                 {
          //                   $convert: {
          //                     input: "$tokens.ending_price",
          //                     to: "double",
          //                   },
          //                 },
          //                 1000000000000000000,
          //               ],
          //             },
          //           },
          //         },
          //       },
          //     ],
          //     as: "listings",
          //   },
          // },
          // {
          //   $lookup: {
          //     from: "tweets",
          //     localField: "_id.slug",
          //     foreignField: "slug",
          //     pipeline: [
          //       {
          //         $group: {
          //           _id: "$slug",
          //           avg_likes: {
          //             $avg: "$like_count",
          //           },
          //           avg_reply: {
          //             $avg: "$reply_count",
          //           },
          //           avg_retweet: {
          //             $avg: "$retweet_count",
          //           },
          //           avg_quote: {
          //             $avg: "$quote_count",
          //           },
          //         },
          //       },
          //       {
          //         $project: {
          //           _id: "$slug",
          //           twitter_engagement: {
          //             $sum: [
          //               "$avg_likes",
          //               "$avg_reply",
          //               "$avg_retweet",
          //               "$avg_quote",
          //             ],
          //           },
          //         },
          //       },
          //     ],
          //     as: "twitter",
          //   },
          // },
          // {
          //   $lookup: {
          //     from: "reddit_posts",
          //     localField: "_id.slug",
          //     foreignField: "slug",
          //     pipeline: [
          //       {
          //         $project: {
          //           created_date: {
          //             $toDate: {
          //               $multiply: ["$created_utc", 1000],
          //             },
          //           },
          //           score: "$score",
          //           num_comments: "$num_comments",
          //         },
          //       },
          //       {
          //         $group: {
          //           _id: "$slug",
          //           average_score: {
          //             $avg: "$score",
          //           },
          //           average_comments: {
          //             $avg: "$num_comments",
          //           },
          //         },
          //       },
          //       {
          //         $project: {
          //           _id: "$slug",
          //           reddit_engagement: {
          //             $sum: ["$average_score", "$average_comments"],
          //           },
          //         },
          //       },
          //     ],
          //     as: "reddit",
          //   },
          // },
          {
            $project: {
              no_of_transfers: 1,
              token_no_of_transfers: 1,
              events: {
                $arrayElemAt: ["$events", 0],
              },
              // listings: {
              //   $arrayElemAt: ["$listings", 0],
              // },
              // twitter: {
              //   $arrayElemAt: ["$twitter", 0],
              // },
              // reddit: {
              //   $arrayElemAt: ["$reddit", 0],
              // },
            },
          },
          {
            $project: {
              tokenomic_score: {
                $add: [
                  {
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
                            $multiply: [
                              "$events.collection.no_of_sales",
                              weightage.no_of_sales,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.collection.volume",
                              weightage.volume,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.collection.avg_price",
                              weightage.avg_price,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.collection.min_price",
                              weightage.min_price,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.collection.max_price",
                              weightage.max_price,
                            ],
                          },
                          // {
                          //   $multiply: [
                          //     "$reddit.reddit_engagement",
                          //     weightage.reddit_engagement,
                          //   ],
                          // },
                          // {
                          //   $multiply: [
                          //     "$twitter.twitter_engagement",
                          //     weightage.reddit_engagement,
                          //   ],
                          // },
                          // {
                          //   $multiply: [
                          //     "$listings.collection.floor_price",
                          //     weightage.floor_price,
                          //   ],
                          // },
                        ],
                      },
                      2,
                    ],
                  },
                  {
                    $round: [
                      {
                        $sum: [
                          {
                            $multiply: [
                              "$token_no_of_transfers",
                              weightage.no_of_transfers,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.token_no_of_sales",
                              weightage.no_of_sales,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.token_volume",
                              weightage.volume,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.token_avg_price",
                              weightage.avg_price,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.token_min_price",
                              weightage.min_price,
                            ],
                          },
                          {
                            $multiply: [
                              "$events.token_max_price",
                              weightage.max_price,
                            ],
                          },
                          // {
                          //   $multiply: [
                          //     "$reddit.reddit_engagement",
                          //     weightage.reddit_engagement,
                          //   ],
                          // },
                          // {
                          //   $multiply: [
                          //     "$twitter.twitter_engagement",
                          //     weightage.twitter_engagement,
                          //   ],
                          // },
                          {
                            $multiply: [floor_price, weightage.floor_price],
                          },
                        ],
                      },
                      2,
                    ],
                  },
                ],
              },
            },
          },
        ])
        .toArray();

      let tokenProfile = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug: slug,
              token_id: token_id,
              // value: {
              //   $nin: ["0"],
              // },
            },
          },
          {
            $lookup: {
              from: "tokens",
              let: {
                slug: "$slug",
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
              ],
              as: "token",
            },
          },
          {
            $unwind: {
              path: "$token",
            },
          },
          {
            $project: {
              slug: 1,
              token_id: 1,
              block_timestamp: {
                $toDate: "$block_timestamp",
              },
              value: {
                $divide: [
                  {
                    $convert: {
                      input: "$value",
                      to: 1,
                    },
                  },
                  1000000000000000000,
                ],
              },
              from_address: 1,
              to_address: 1,
              token_image_url: "$token.image_url",
              token_name: "$token.name",
              token_traits: "$token.traits",
              token_last_updated_timestamp: "$token.last_updated_timestamp",
              token_score: "$token.token_score",
              token_normalized_score: "$token.normalized_score",
              token_rarity_rank: "$token.rarity_rank",
              token_rarity_type: "$token.rarity_type",
            },
          },
          {
            $sort: {
              block_timestamp: -1,
            },
          },
          {
            $group: {
              _id: "$token_id",
              data: {
                $addToSet: {
                  slug: "$slug",
                  token_id: "$token_id",
                  from_address: "$from_address",
                  to_address: "$to_address",
                  block_timestamp: "$block_timestamp",
                  value: "$value",
                  token_image_url: "$token_image_url",
                  token_name: "$token_name",
                  token_traits: "$token_traits",
                  token_last_updated_timestamp: "$token_last_updated_timestamp",
                  token_score: "$token_score",
                  token_normalized_score: "$token_normalized_score",
                  token_rarity_rank: "$token_rarity_rank",
                  token_rarity_type: "$token_rarity_type",
                },
              },
              total_owners: {
                $sum: 1,
              },
            },
          },
          {
            $unwind: {
              path: "$data",
            },
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "data.to_address",
              foreignField: "seller.address",
              as: "owner",
            },
          },
          {
            $unwind: {
              path: "$owner",
            },
          },
          {
            $group: {
              _id: "$data.to_address",
              data: {
                $addToSet: {
                  slug: "$data.slug",
                  token_id: "$data.token_id",
                  from_address: "$data.from_address",
                  to_address: "$data.to_address",
                  block_timestamp: "$data.block_timestamp",
                  value: "$data.value",
                  token_image_url: "$data.token_image_url",
                  token_name: "$data.token_name",
                  token_traits: "$data.token_traits",
                  token_last_updated_timestamp:
                    "$data.token_last_updated_timestamp",
                  token_score: "$data.token_score",
                  token_normalized_score: "$data.token_normalized_score",
                  token_rarity_rank: "$data.token_rarity_rank",
                  token_rarity_type: "$data.token_rarity_type",
                  total_owners: "$total_owners",
                  user_name: "$owner.seller.user.username",
                  user_profile_img_url: "$owner.seller.profile_img_url",
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
              slug: "$data.slug",
              token_id: "$data.token_id",
              from_address: "$data.from_address",
              to_address: "$data.to_address",
              block_timestamp: "$data.block_timestamp",
              value: "$data.value",
              token_name: "$data.token_name",
              token_traits: "$data.token_traits",
              token_last_updated_timestamp:
                "$data.token_last_updated_timestamp",
              token_score: "$data.token_score",
              token_normalized_score: "$data.token_normalized_score",
              token_rarity_rank: "$data.token_rarity_rank",
              token_rarity_type: "$data.token_rarity_type",
              token_image_url: "$data.token_image_url",
              past_owners: {
                $subtract: ["$data.total_owners", 1],
              },
              user_name: "$data.user_name",
              user_profile_img_url: "$data.user_profile_img_url",
            },
          },
          {
            $group: {
              _id: {
                slug: "$slug",
                token_id: "$token_id",
              },
              data: {
                $push: "$$ROOT",
              },
              mint_date: {
                $min: "$block_timestamp",
              },
            },
          },
          {
            $unwind: {
              path: "$data",
            },
          },
          {
            $replaceRoot: {
              newRoot: {
                $mergeObjects: [
                  {
                    mint_date: "$mint_date",
                  },
                  "$data",
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
            $limit: 1,
          },
          {
            $lookup: {
              from: "rarible_events",
              localField: "to_address",
              foreignField: "seller.address",
              let: {
                slug: "$slug",
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
                    created_date: {
                      $toDate: "$created_date",
                    },
                    ending_price: {
                      $toDouble: "$ending_price",
                    },
                  },
                },
              ],
              as: "listing_price",
            },
          },
          {
            $unwind: {
              path: "$listing_price",
              preserveNullAndEmptyArrays: true,
            },
          },
          {
            $sort: {
              "listing_price.created_date": -1,
            },
          },
          {
            $limit: 1,
          },
          {
            $lookup: {
              from: "collections",
              localField: "slug",
              foreignField: "slug",
              pipeline: [
                {
                  $project: {
                    name: 1,
                    image_url: 1,
                    address: 1,
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
              slug: 1,
              token_id: 1,
              token_name: 1,
              current_owner: "$to_address",
              block_timestamp: 1,
              owner_user_name: "$user_name",
              owner_user_profile_img_url: "$user_profile_img_url",
              last_price: "$value",
              token_traits: 1,
              token_last_updated_timestamp: 1,
              token_normalized_score: 1,
              token_score: 1,
              token_rarity_rank: 1,
              token_rarity_type: 1,
              token_image_url: 1,
              past_owners: 1,
              mint_date: 1,
              holding_hours: {
                $dateDiff: {
                  startDate: "$block_timestamp",
                  endDate: "$$NOW",
                  unit: "hour",
                },
              },
              listingPrice: {
                $cond: {
                  if: {
                    $gte: ["$listing_price.created_date", "$block_timestamp"],
                  },
                  then: {
                    $divide: [
                      "$listing_price.ending_price",
                      1000000000000000000,
                    ],
                  },
                  else: null,
                },
              },
              collection_name: "$result.name",
              collection_img_url: "$result.image_url",
              collection_address: "$result.address",
            },
          },
        ])
        .toArray();

      if (tokenProfile.length) {
        tokenProfile[0].tokenomic_score =
          tokenomic_score_data[0].tokenomic_score;
      }

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: tokenProfile,
        }),
        1440
      );

      res.status(200).json({
        success: true,
        data: tokenProfile,
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };
}
