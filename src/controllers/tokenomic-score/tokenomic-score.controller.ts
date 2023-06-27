import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { structure } from "../../helpers/stats";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";
import * as dayjs from "dayjs";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class TokenomicScoreController {
  public GetTokenomicScore = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;
      let { time: timeString } = req.query;

      // let collection = await db.collection("collections").findOne({ slug });
      // let total_supply = collection.total_supply;

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

      const timeEnum = {
        NaN: 12,
        "1y": 11,
        "3m": 10,
        "30d": 9,
        "7d": 8,
        "24h": 7,
        "12h": 6,
        "6h": 5,
        "1h": 4,
        "30m": 3,
        "15m": 2,
        "5m": 1,
        "1m": 0,
      };
      let matchFormat = {};

      const time = timeEnum[`${!timeString ? "NaN" : timeString}`];

      if (timeString && time > 6) {
        matchFormat = {
          $gte: new Date(
            structure(timeString, slug).matchFormat.created_date.$gte
          ),
        };
      } else if (time < 7) {
        let today = new Date();
        let subtractedTime;

        switch (timeString) {
          case "12h":
            subtractedTime = today.setHours(today.getHours() - 12);
            break;
          case "6h":
            subtractedTime = today.setHours(today.getHours() - 6);
            break;
          case "1h":
            subtractedTime = today.setHours(today.getHours() - 1);
            break;
          case "30m":
            subtractedTime = today.setMinutes(today.getMinutes() - 30);
            break;
          case "15m":
            subtractedTime = today.setMinutes(today.getMinutes() - 15);
            break;
          case "5m":
            subtractedTime = today.setMinutes(today.getMinutes() - 5);
            break;
          case "1m":
            subtractedTime = today.setMinutes(today.getMinutes() - 1);
            break;
        }
        matchFormat = { $gte: subtractedTime };
      }

      const format = {
        year: true,
        month: true,
        day: true,
        hour: {
          isHour: !(time > 9),
          value: time > 9 ? 0 : time === 9 ? 8 : time === 8 ? 2 : 1,
        },
        minute: {
          isMinute: !(time > 7),
          value:
            time > 7
              ? 0
              : time === 7
              ? 20
              : time === 6
              ? 10
              : time === 5
              ? 5
              : 1,
        },
        seconds: {
          isSeconds: !(time > 4),
          value:
            time > 4
              ? 0
              : time === 4
              ? 30
              : time === 3
              ? 20
              : time === 2
              ? 10
              : time === 1
              ? 5
              : 1,
        },
      };

      const facetFormat = {
        year: true,
        month: true,
        day: true,
        hour: {
          isHour: true,
          value: time > 9 ? 4 : time === 9 ? 2 : 1,
        },
        minute: {
          isMinute: !(time > 8),
          value:
            time > 8
              ? 0
              : time === 8
              ? 30
              : time === 7
              ? 5
              : time === 6
              ? 2
              : 1,
        },
        seconds: {
          isSeconds: !(time > 4),
          value:
            time > 4 ? 0 : time === 4 ? 7 : time === 3 ? 5 : time === 2 ? 2 : 1,
        },
      };

      const dateFormat = {
        year: {
          $year: "$created_date",
        },
        month: {
          $month: "$created_date",
        },
        day: {
          $dayOfMonth: "$created_date",
        },

        ...(facetFormat.hour.isHour
          ? {
              hour: {
                $multiply: [
                  {
                    $floor: {
                      $divide: [
                        {
                          $hour: "$created_date",
                        },
                        facetFormat.hour.value,
                      ],
                    },
                  },
                  facetFormat.hour.value,
                ],
              },
            }
          : {}),
        ...(facetFormat.minute.isMinute
          ? {
              minute: {
                $multiply: [
                  {
                    $floor: {
                      $divide: [
                        {
                          $minute: "$created_date",
                        },
                        facetFormat.minute.value,
                      ],
                    },
                  },
                  facetFormat.minute.value,
                ],
              },
            }
          : {}),
        ...(facetFormat.seconds.isSeconds
          ? {
              seconds: {
                $multiply: [
                  {
                    $floor: {
                      $divide: [
                        {
                          $second: "$created_date",
                        },
                        facetFormat.seconds.value,
                      ],
                    },
                  },
                  facetFormat.seconds.value,
                ],
              },
            }
          : {}),
      };

      let idFormat = {
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
            "T",
            ...(facetFormat.hour.isHour
              ? [
                  {
                    $toString: "$_id.hour",
                  },
                ]
              : ["00"]),
            ":",
            ...(facetFormat.minute.isMinute
              ? [
                  {
                    $toString: "$_id.minute",
                  },
                ]
              : ["00"]),
            ":",
            ...(facetFormat.seconds.isSeconds
              ? [
                  {
                    $toString: "$_id.seconds",
                  },
                ]
              : ["00"]),
          ],
        },
      };

      const facet = await db
        .collection("collections")
        .aggregate([
          {
            $match: {
              slug,
            },
          },
          {
            $project: {
              slug: 1,
            },
          },
          {
            $facet: {
              listings: [
                {
                  $lookup: {
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    as: "result",
                  },
                },
                {
                  $unwind: "$result",
                },
                {
                  $project: {
                    event_type: "$result.event_type",
                    total_price: "$result.total_price",
                    created_date: {
                      $toDate: "$result.created_date",
                    },
                    ending_price: "$result.ending_price",
                  },
                },
                {
                  $match: {
                    event_type: "created",
                    ending_price: {
                      $nin: [null, 0, "0"],
                    },
                    ...(timeString ? { created_date: matchFormat } : {}),
                  },
                },
                {
                  $group: {
                    _id: dateFormat,
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
                    _id: idFormat,
                    floor_price: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              sales: [
                {
                  $lookup: {
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    as: "result",
                  },
                },
                {
                  $unwind: "$result",
                },
                {
                  $project: {
                    event_type: "$result.event_type",
                    total_price: "$result.total_price",
                    created_date: {
                      $toDate: "$result.created_date",
                    },
                    ending_price: "$result.ending_price",
                  },
                },
                {
                  $match: {
                    event_type: "successful",
                    total_price: {
                      $nin: [null, "0", 0],
                    },
                    ...(timeString ? { created_date: matchFormat } : {}),
                  },
                },
                {
                  $group: {
                    _id: dateFormat,
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
                  $project: {
                    _id: idFormat,
                    max_price: 1,
                    min_price: 1,
                    avg_price: 1,
                    volume: 1,
                    no_of_sales: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              // twitter: [
              //   {
              //     $lookup: {
              //       from: "tweets",
              //       localField: "slug",
              //       foreignField: "slug",
              //       as: "result",
              //     },
              //   },
              //   {
              //     $unwind: {
              //       path: "$result",
              //     },
              //   },
              //   {
              //     $project: {
              //       created_date: {
              //         $toDate: "$result.created_date",
              //       },
              //       slug: 1,
              //       like_count: "$result.like_count",
              //       reply_count: "$result.reply_count",
              //       retweet_count: "$result.retweet_count",
              //       quote_count: "$result.quote_count",
              //     },
              //   },
              //   ...(timeString
              //     ? [{ $match: { created_date: matchFormat } }]
              //     : []),
              //   {
              //     $group: {
              //       _id: dateFormat,
              //       avg_likes: {
              //         $avg: "$like_count",
              //       },
              //       avg_reply: {
              //         $avg: "$reply_count",
              //       },
              //       avg_retweet: {
              //         $avg: "$retweet_count",
              //       },
              //       avg_quote: {
              //         $avg: "$quote_count",
              //       },
              //     },
              //   },
              //   {
              //     $project: {
              //       _id: idFormat,
              //       twitter_engagement: {
              //         $sum: [
              //           "$avg_likes",
              //           "$avg_reply",
              //           "$avg_retweet",
              //           "$avg_quote",
              //         ],
              //       },
              //     },
              //   },
              //   {
              //     $sort: {
              //       _id: 1,
              //     },
              //   },
              // ],
              // reddit: [
              //   {
              //     $lookup: {
              //       from: "reddit_posts",
              //       localField: "slug",
              //       foreignField: "slug",
              //       as: "result",
              //     },
              //   },
              //   {
              //     $unwind: {
              //       path: "$result",
              //     },
              //   },
              //   {
              //     $project: {
              //       created_date: {
              //         $toDate: {
              //           $multiply: ["$result.created_utc", 1000],
              //         },
              //       },
              //       score: "$result.score",
              //       num_comments: "$result.num_comments",
              //     },
              //   },
              //   ...(timeString
              //     ? [{ $match: { created_date: matchFormat } }]
              //     : []),
              //   {
              //     $group: {
              //       _id: dateFormat,
              //       average_score: {
              //         $avg: "$score",
              //       },
              //       average_comments: {
              //         $avg: "$num_comments",
              //       },
              //     },
              //   },
              //   {
              //     $project: {
              //       _id: idFormat,
              //       reddit_engagement: {
              //         $sum: ["$average_score", "$average_comments"],
              //       },
              //     },
              //   },
              //   {
              //     $sort: {
              //       _id: 1,
              //     },
              //   },
              // ],
              transfers: [
                {
                  $lookup: {
                    from: "transfers",
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
                    created_date: {
                      $toDate: "$result.block_timestamp",
                    },
                    from_address: "$result.from_address",
                  },
                },
                {
                  $match: {
                    from_address: {
                      $ne: "0x0000000000000000000000000000000000000000",
                    },
                    ...(timeString ? { created_date: matchFormat } : {}),
                  },
                },
                {
                  $group: {
                    _id: dateFormat,
                    no_of_transfers: {
                      $count: {},
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    no_of_transfers: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
            },
          },
          {
            $project: {
              all: {
                $concatArrays: [
                  "$listings",
                  "$sales",
                  // "$twitter",
                  // "$reddit",
                  "$transfers",
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
              no_of_transfers: {
                $max: "$all.no_of_transfers",
              },
              no_of_sales: {
                $max: "$all.no_of_sales",
              },
              volume: {
                $max: "$all.volume",
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
              // reddit_engagement: {
              //   $max: "$all.reddit_engagement",
              // },
              // twitter_engagement: {
              //   $max: "$all.twitter_engagement",
              // },
              floor_price: {
                $max: "$all.floor_price",
              },
            },
          },
          {
            $project: {
              no_of_transfers: {
                $cond: [
                  {
                    $eq: ["$no_of_transfers", null],
                  },
                  "$$REMOVE",
                  "$no_of_transfers",
                ],
              },
              no_of_sales: {
                $cond: [
                  {
                    $eq: ["$no_of_sales", null],
                  },
                  "$$REMOVE",
                  "$no_of_sales",
                ],
              },
              volume: {
                $cond: [
                  {
                    $eq: ["$volume", null],
                  },
                  "$$REMOVE",
                  "$volume",
                ],
              },
              avg_price: {
                $cond: [
                  {
                    $eq: ["$avg_price", null],
                  },
                  "$$REMOVE",
                  "$avg_price",
                ],
              },
              min_price: {
                $cond: [
                  {
                    $eq: ["$min_price", null],
                  },
                  "$$REMOVE",
                  "$min_price",
                ],
              },
              max_price: {
                $cond: [
                  {
                    $eq: ["$max_price", null],
                  },
                  "$$REMOVE",
                  "$max_price",
                ],
              },
              // reddit_engagement: {
              //   $cond: [
              //     {
              //       $eq: ["$reddit_engagement", null],
              //     },
              //     "$$REMOVE",
              //     "$reddit_engagement",
              //   ],
              // },
              // twitter_engagement: {
              //   $cond: [
              //     {
              //       $eq: ["$twitter_engagement", null],
              //     },
              //     "$$REMOVE",
              //     "$twitter_engagement",
              //   ],
              // },
              floor_price: {
                $cond: [
                  {
                    $eq: ["$floor_price", null],
                  },
                  "$$REMOVE",
                  "$floor_price",
                ],
              },
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
                        $multiply: ["$no_of_sales", weightage.no_of_sales],
                      },
                      {
                        $multiply: ["$volume", weightage.volume],
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
                      {
                        $multiply: ["$floor_price", weightage.floor_price],
                      },
                    ],
                  },
                  2,
                ],
              },
            },
          },
          {
            $group: {
              _id: {
                year: {
                  $year: "$_id",
                },
                month: {
                  $month: "$_id",
                },
                day: {
                  $dayOfMonth: "$_id",
                },

                ...(format.hour.isHour
                  ? {
                      hour: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $hour: "$_id",
                                },
                                format.hour.value,
                              ],
                            },
                          },
                          format.hour.value,
                        ],
                      },
                    }
                  : {}),
                ...(format.minute.isMinute
                  ? {
                      minute: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $minute: "$_id",
                                },
                                format.minute.value,
                              ],
                            },
                          },
                          format.minute.value,
                        ],
                      },
                    }
                  : {}),
                ...(format.seconds.isSeconds
                  ? {
                      seconds: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $second: "$_id",
                                },
                                format.seconds.value,
                              ],
                            },
                          },
                          format.seconds.value,
                        ],
                      },
                    }
                  : {}),
              },
              open_and_close: {
                $accumulator: {
                  init: "function() { \
                     return { open: {}, close: {} };\n\
                    //  return [];\n\
                   }",
                  accumulate:
                    "function(state, doc) {\n\
                    let st = state;\n\
                    if (Object.keys(state.open).length == 0 || new Date(doc._id) < new Date(state.open._id))\n\
                      st.open = doc;\n\
                    if (Object.keys(state.close).length == 0 || new Date(doc._id) > new Date(state.close._id))\n\
                       st.close = doc;\n\
                    return st;\n\
                  }",
                  accumulateArgs: ["$$ROOT"],
                  merge:
                    "function(state1, state2) {\n\
                    Object.assign(state1, state2);\n\
                    return state1;\n\
                  }",
                  finalize:
                    "function(state) {\n\
                    return {\n\
                      open: state.open.tokenomic_score,\n\
                      close: state.close.tokenomic_score\n\
                    };\n\
                  }",
                  lang: "js",
                },
              },
              volume: {
                $sum: "$volume",
              },
              lowest: {
                $min: "$tokenomic_score",
              },
              highest: {
                $max: "$tokenomic_score",
              },
            },
          },
          {
            $project: {
              _id: {
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
                    "T",
                    ...(format.hour.isHour
                      ? [
                          {
                            $toString: "$_id.hour",
                          },
                        ]
                      : ["00"]),
                    ":",
                    ...(format.minute.isMinute
                      ? [
                          {
                            $toString: "$_id.minute",
                          },
                        ]
                      : ["00"]),
                    ":",
                    ...(format.seconds.isSeconds
                      ? [
                          {
                            $toString: "$_id.seconds",
                          },
                        ]
                      : ["00"]),
                  ],
                },
              },
              open: "$open_and_close.open",
              close: "$open_and_close.close",
              lowest: "$lowest",
              highest: "$highest",
              volume: {
                $cond: [
                  {
                    $eq: ["$volume", null],
                  },
                  0,
                  "$volume",
                ],
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          // {
          //   $fill: {
          //     sortBy: {
          //       _id: 1,
          //     },
          //     output: {
          //       no_of_transfers: {
          //         method: "linear",
          //       },
          //       no_of_sales: {
          //         method: "linear",
          //       },
          //       volume: {
          //         method: "linear",
          //       },
          //       avg_price: {
          //         method: "linear",
          //       },
          //       min_price: {
          //         method: "linear",
          //       },
          //       max_price: {
          //         method: "linear",
          //       },
          //       reddit_engagement: {
          //         method: "linear",
          //       },
          //       twitter_engagement: {
          //         method: "linear",
          //       },
          //       floor_price: {
          //         method: "linear",
          //       },
          //     },
          //   },
          // },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: facet,
        }),
        360
      );

      res.status(200).send({
        success: true,
        data: facet,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetTokenomicScoreDistribution = async (
    req: Request,
    res: Response
  ) => {
    try {
      let { slug } = req.params;
      let { time: timeString } = req.query;
      let { isoTimeString } = req.query;
      const utcTime = dayjs(`${isoTimeString}`);

      const timeEnum = {
        NaN: 12,
        "1y": 11,
        "3m": 10,
        "30d": 9,
        "7d": 8,
        "24h": 7,
        "12h": 6,
        "6h": 5,
        "1h": 4,
        "30m": 3,
        "15m": 2,
        "5m": 1,
        "1m": 0,
      };

      const time = timeEnum[`${!timeString ? "NaN" : timeString}`];

      const format = {
        year: true,
        month: true,
        day: true,
        hour: {
          isHour: !(time > 9),
          value: time > 9 ? 0 : time === 9 ? 8 : time === 8 ? 2 : 1,
        },
        minute: {
          isMinute: !(time > 7),
          value:
            time > 7
              ? 0
              : time === 7
              ? 20
              : time === 6
              ? 10
              : time === 5
              ? 5
              : 1,
        },
        seconds: {
          isSeconds: !(time > 4),
          value:
            time > 4
              ? 0
              : time === 4
              ? 30
              : time === 3
              ? 20
              : time === 2
              ? 10
              : time === 1
              ? 5
              : 1,
        },
      };

      const facetFormat = {
        year: true,
        month: true,
        day: true,
        hour: true,
        minute: {
          isMinute: !(time >= 9),
        },
        seconds: {
          isSeconds: !(time >= 6),
        },
      };

      const dateFormat = (dateString) => {
        return {
          year: {
            $year: {
              $toDate: dateString,
            },
          },
          month: {
            $month: {
              $toDate: dateString,
            },
          },
          day: {
            $dayOfMonth: {
              $toDate: dateString,
            },
          },
          hour: {
            $hour: {
              $toDate: dateString,
            },
          },
          ...(facetFormat.minute.isMinute
            ? {
                minute: {
                  $minute: {
                    $toDate: dateString,
                  },
                },
              }
            : {}),
          ...(facetFormat.seconds.isSeconds
            ? {
                seconds: {
                  $second: {
                    $toDate: dateString,
                  },
                },
              }
            : {}),
        };
      };

      let matchFormat = {
        "date.year": {
          $eq: utcTime.year(),
        },
        "date.month": {
          $eq: utcTime.month() + 1,
        },
        "date.day": {
          $eq: utcTime.date(),
        },
        ...(format.hour.isHour
          ? {
              "date.hour": {
                $gte:
                  Math.floor(utcTime.hour() / format.hour.value) *
                  format.hour.value,
                $lte:
                  Math.floor(utcTime.hour() / format.hour.value) *
                    format.hour.value +
                  format.hour.value -
                  1,
              },
            }
          : {}),
        ...(format.minute.isMinute
          ? {
              "date.minute": {
                $gte:
                  Math.floor(utcTime.minute() / format.minute.value) *
                  format.minute.value,
                $lte:
                  Math.floor(utcTime.minute() / format.minute.value) *
                    format.minute.value +
                  format.minute.value -
                  1,
              },
            }
          : {}),
      };

      let idFormat = {
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
            "T",
            {
              $toString: "$_id.hour",
            },
            ":",
            ...(facetFormat.minute.isMinute
              ? [
                  {
                    $toString: "$_id.minute",
                  },
                ]
              : ["00"]),
            ":",
            ...(facetFormat.seconds.isSeconds
              ? [
                  {
                    $toString: "$_id.seconds",
                  },
                ]
              : ["00"]),
          ],
        },
      };

      let groupFormat = {
        year: "$date.year",
        month: "$date.month",
        day: "$date.day",
        hour: "$date.hour",
        ...(facetFormat.minute.isMinute ? { minute: "$date.minute" } : {}),
        ...(facetFormat.seconds.isSeconds ? { seconds: "$date.seconds" } : {}),
      };

      const getNumeratorAndDenominator = (metric) => {
        return {
          numerator: {
            $subtract: [
              {
                $multiply: ["$elems", `$sum_${metric}_x_volume`],
              },
              {
                $multiply: [`$sum_${metric}`, "$sum_volume"],
              },
            ],
          },
          denominator: {
            $sqrt: {
              $multiply: [
                {
                  $subtract: [
                    {
                      $multiply: ["$elems", `$sum_${metric}_sq`],
                    },
                    {
                      $multiply: [`$sum_${metric}`, `$sum_${metric}`],
                    },
                  ],
                },
                {
                  $subtract: [
                    {
                      $multiply: ["$elems", "$sum_volume_sq"],
                    },
                    {
                      $multiply: ["$sum_volume", "$sum_volume"],
                    },
                  ],
                },
              ],
            },
          },
        };
      };

      const divideNumeratorAndDenominator = (metric) => {
        return {
          $cond: [
            {
              $ne: [`$${metric}.denominator`, 0],
            },
            {
              $abs: {
                $round: [
                  {
                    $multiply: [
                      {
                        $divide: [
                          `$${metric}.numerator`,
                          `$${metric}.denominator`,
                        ],
                      },
                      100,
                    ],
                  },
                  2,
                ],
              },
            },
            0,
          ],
        };
      };

      const lookupFrom = (collection) => {
        return [
          {
            $lookup: {
              from: collection,
              localField: "slug",
              foreignField: "slug",
              as: "result",
            },
          },
          {
            $unwind: "$result",
          },
        ];
      };

      const convertToEth = (input) => {
        return {
          $divide: [
            {
              $convert: {
                input,
                to: "double",
              },
            },
            1000000000000000000,
          ],
        };
      };

      const facet = await db
        .collection("collections")
        .aggregate([
          {
            $match: {
              slug,
            },
          },
          {
            $project: {
              slug: 1,
            },
          },
          {
            $facet: {
              listings: [
                ...lookupFrom("rarible_events"),
                {
                  $project: {
                    event_type: "$result.event_type",
                    total_price: "$result.total_price",
                    date: dateFormat("$result.created_date"),
                    ending_price: "$result.ending_price",
                  },
                },
                {
                  $match: {
                    event_type: "created",
                    ending_price: {
                      $nin: [null, 0, "0"],
                    },
                    ...matchFormat,
                  },
                },
                {
                  $group: {
                    _id: groupFormat,
                    floor_price: {
                      $min: convertToEth("$ending_price"),
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    floor_price: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              sales: [
                ...lookupFrom("rarible_events"),
                {
                  $project: {
                    event_type: "$result.event_type",
                    total_price: "$result.total_price",
                    date: dateFormat("$result.created_date"),
                    ending_price: "$result.ending_price",
                  },
                },
                {
                  $match: {
                    event_type: "successful",
                    total_price: {
                      $nin: [null, "0", 0],
                    },
                    ...matchFormat,
                  },
                },
                {
                  $group: {
                    _id: groupFormat,
                    no_of_sales: {
                      $sum: 1,
                    },
                    volume: {
                      $sum: convertToEth("$total_price"),
                    },
                    avg_price: {
                      $avg: convertToEth("$total_price"),
                    },
                    min_price: {
                      $min: convertToEth("$total_price"),
                    },
                    max_price: {
                      $max: convertToEth("$total_price"),
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    max_price: 1,
                    min_price: 1,
                    avg_price: 1,
                    volume: 1,
                    no_of_sales: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              twitter: [
                ...lookupFrom("tweets"),
                {
                  $project: {
                    date: dateFormat("$result.created_date"),
                    slug: 1,
                    like_count: "$result.like_count",
                    retweet_count: "$result.retweet_count",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: groupFormat,
                    avg_likes: {
                      $avg: "$like_count",
                    },
                    avg_retweet: {
                      $avg: "$retweet_count",
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    twitter_engagement: {
                      $sum: ["$avg_likes", "$avg_retweet"],
                    },
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              reddit: [
                ...lookupFrom("reddit_posts"),
                {
                  $project: {
                    date: dateFormat({
                      $multiply: ["$result.created_utc", 1000],
                    }),
                    score: "$result.score",
                    num_comments: "$result.num_comments",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: groupFormat,
                    average_score: {
                      $avg: "$score",
                    },
                    average_comments: {
                      $avg: "$num_comments",
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    reddit_engagement: {
                      $sum: ["$average_score", "$average_comments"],
                    },
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
              transfers: [
                ...lookupFrom("transfers"),
                {
                  $project: {
                    date: dateFormat("$result.block_timestamp"),
                    from_address: "$result.from_address",
                  },
                },
                {
                  $match: {
                    from_address: {
                      $ne: "0x0000000000000000000000000000000000000000",
                    },
                    ...matchFormat,
                  },
                },
                {
                  $group: {
                    _id: groupFormat,
                    no_of_transfers: {
                      $count: {},
                    },
                  },
                },
                {
                  $project: {
                    _id: idFormat,
                    no_of_transfers: 1,
                  },
                },
                {
                  $sort: {
                    _id: 1,
                  },
                },
              ],
            },
          },
          {
            $project: {
              all: {
                $concatArrays: [
                  "$listings",
                  "$sales",
                  "$twitter",
                  "$reddit",
                  "$transfers",
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
              count: {
                $count: {},
              },
              no_of_transfers: {
                $max: "$all.no_of_transfers",
              },
              no_of_sales: {
                $max: "$all.no_of_sales",
              },
              volume: {
                $max: "$all.volume",
              },
              avg_price: {
                $max: "$all.avg_price",
              },
              min_price: {
                $min: "$all.min_price",
              },
              max_price: {
                $max: "$all.max_price",
              },
              reddit_engagement: {
                $max: "$all.reddit_engagement",
              },
              twitter_engagement: {
                $max: "$all.twitter_engagement",
              },
              floor_price: {
                $max: "$all.floor_price",
              },
            },
          },
          {
            $group: {
              _id: {
                year: {
                  $year: "$_id",
                },
                month: {
                  $month: "$_id",
                },
                day: {
                  $dayOfMonth: "$_id",
                },

                ...(format.hour.isHour
                  ? {
                      hour: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $hour: "$_id",
                                },
                                format.hour.value,
                              ],
                            },
                          },
                          format.hour.value,
                        ],
                      },
                    }
                  : {}),
                ...(format.minute.isMinute
                  ? {
                      minute: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $minute: "$_id",
                                },
                                format.minute.value,
                              ],
                            },
                          },
                          format.minute.value,
                        ],
                      },
                    }
                  : {}),
                ...(format.seconds.isSeconds
                  ? {
                      seconds: {
                        $multiply: [
                          {
                            $floor: {
                              $divide: [
                                {
                                  $second: "$_id",
                                },
                                format.seconds.value,
                              ],
                            },
                          },
                          format.seconds.value,
                        ],
                      },
                    }
                  : {}),
              },
              elems: {
                $count: {},
              },
              sum_no_of_transfers: {
                $sum: "$no_of_transfers",
              },
              sum_no_of_transfers_x_volume: {
                $sum: {
                  $multiply: ["$no_of_transfers", "$volume"],
                },
              },
              sum_no_of_transfers_sq: {
                $sum: {
                  $multiply: ["$no_of_transfers", "$no_of_transfers"],
                },
              },
              sum_no_of_sales: {
                $sum: "$no_of_sales",
              },
              sum_no_of_sales_x_volume: {
                $sum: {
                  $multiply: ["$no_of_sales", "$volume"],
                },
              },
              sum_no_of_sales_sq: {
                $sum: {
                  $multiply: ["$no_of_sales", "$no_of_sales"],
                },
              },
              sum_volume: {
                $sum: "$volume",
              },
              sum_volume_sq: {
                $sum: {
                  $multiply: ["$volume", "$volume"],
                },
              },
              sum_avg_price: {
                $sum: "$avg_price",
              },
              sum_avg_price_x_volume: {
                $sum: {
                  $multiply: ["$avg_price", "$volume"],
                },
              },
              sum_avg_price_sq: {
                $sum: {
                  $multiply: ["$avg_price", "$avg_price"],
                },
              },
              sum_min_price: {
                $sum: "$min_price",
              },
              sum_min_price_x_volume: {
                $sum: {
                  $multiply: ["$min_price", "$volume"],
                },
              },
              sum_min_price_sq: {
                $sum: {
                  $multiply: ["$min_price", "$min_price"],
                },
              },
              sum_max_price: {
                $sum: "$max_price",
              },
              sum_max_price_x_volume: {
                $sum: {
                  $multiply: ["$max_price", "$volume"],
                },
              },
              sum_max_price_sq: {
                $sum: {
                  $multiply: ["$max_price", "$max_price"],
                },
              },
              sum_floor_price: {
                $sum: "$floor_price",
              },
              sum_floor_price_x_volume: {
                $sum: {
                  $multiply: ["$floor_price", "$volume"],
                },
              },
              sum_floor_price_sq: {
                $sum: {
                  $multiply: ["$floor_price", "$floor_price"],
                },
              },
              sum_reddit_engagement: {
                $sum: "$reddit_engagement",
              },
              sum_reddit_engagement_x_volume: {
                $sum: {
                  $multiply: ["$reddit_engagement", "$volume"],
                },
              },
              sum_reddit_engagement_sq: {
                $sum: {
                  $multiply: ["$reddit_engagement", "$reddit_engagement"],
                },
              },
              sum_twitter_engagement: {
                $sum: "$twitter_engagement",
              },
              sum_twitter_engagement_x_volume: {
                $sum: {
                  $multiply: ["$twitter_engagement", "$volume"],
                },
              },
              sum_twitter_engagement_sq: {
                $sum: {
                  $multiply: ["$twitter_engagement", "$twitter_engagement"],
                },
              },
            },
          },
          {
            $project: {
              no_of_transfers: {
                sum: "$sum_no_of_transfers",
                ...getNumeratorAndDenominator("no_of_transfers"),
              },
              no_of_sales: {
                ...getNumeratorAndDenominator("no_of_sales"),
              },
              avg_price: {
                ...getNumeratorAndDenominator("avg_price"),
              },
              min_price: {
                ...getNumeratorAndDenominator("min_price"),
              },
              max_price: {
                ...getNumeratorAndDenominator("max_price"),
              },
              floor_price: {
                ...getNumeratorAndDenominator("floor_price"),
              },
              reddit_engagement: {
                sum: "$sum_reddit_engagement",
                ...getNumeratorAndDenominator("reddit_engagement"),
              },
              twitter_engagement: {
                sum: "$sum_twitter_engagement",
                ...getNumeratorAndDenominator("twitter_engagement"),
              },
              volume: "$sum_volume",
            },
          },
          {
            $project: {
              volume: {
                $cond: [
                  {
                    $eq: ["$volume", 0],
                  },
                  0,
                  100,
                ],
              },
              no_of_transfers: {
                $cond: [
                  {
                    $and: [
                      {
                        $eq: ["$volume", 0],
                      },
                      {
                        $ne: ["$no_of_transfers.sum", 0],
                      },
                    ],
                  },
                  1,
                  divideNumeratorAndDenominator("no_of_transfers"),
                ],
              },
              no_of_sales: divideNumeratorAndDenominator("no_of_sales"),
              average_price: divideNumeratorAndDenominator("avg_price"),
              min_price: divideNumeratorAndDenominator("min_price"),
              max_price: divideNumeratorAndDenominator("max_price"),
              // floor_price: divideNumeratorAndDenominator("floor_price"),
              reddit_engagement: {
                $cond: [
                  {
                    $and: [
                      {
                        $eq: ["$volume", 0],
                      },
                      {
                        $ne: ["$reddit_engagement.sum", 0],
                      },
                    ],
                  },
                  1,
                  divideNumeratorAndDenominator("reddit_engagement"),
                ],
              },
              twitter_engagement: {
                $cond: [
                  {
                    $and: [
                      {
                        $eq: ["$volume", 0],
                      },
                      {
                        $ne: ["$twitter_engagement.sum", 0],
                      },
                    ],
                  },
                  1,
                  divideNumeratorAndDenominator("twitter_engagement"),
                ],
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

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: facet,
        }),
        360
      );

      res.status(200).send({
        success: true,
        data: facet,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };
}
