//import * as bcrypt from 'bcrypt';
import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { structure } from "../../helpers/stats";
import * as dayjs from "dayjs";
const googleTrends = require("google-trends-api");
import axios from "axios";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";
import { setCache, uniqueKey } from "../../utilities/redis";
import { pipeline } from "stream";

export default class CommunityController {
  public GetTotalSubReddits = async (req: Request, res: Response) => {
    try {
      let slug = req.params.slug;

      // Get total subreddits in the db count
      const totalSubReddits = await db
        .collection("reddit_posts")
        .countDocuments({ slug: slug });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          succcess: true,
          data: totalSubReddits,
        }),
        720
      );

      res.status(200).json({
        succcess: true,
        data: totalSubReddits,
      });
    } catch (err) {
      console.log(err);
      res.status(500).json({ err });
    }
  };

  public GetTotalTweets = async (req: Request, res: Response) => {
    try {
      let slug = req.params.slug;

      // Get total tweets in the db count
      const totalTweets = await db.collection("tweets").countDocuments({
        slug,
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: totalTweets,
        }),
        720
      );

      res.status(200).json({ success: true, data: totalTweets });
    } catch (err) {
      console.log(err);
      res.status(500).json({ err });
    }
  };

  public GetDiscordPresence = async (req: Request, res: Response) => {
    try {
      let slug = req.params.slug;

      const collection_data = await db
        .collection("collections")
        .findOne({ slug: slug }, { projection: { discord_url: 1 } });

      let discord_url = collection_data.discord_url.split("/").pop();

      let data = await axios({
        method: "get",
        url: `https://discord.com/api/v9/invites/${discord_url}?with_counts=true&with_expiration=true`,
        headers: {
          Cookie:
            "__dcfduid=d0f8f4e8270111ed9c7b5af7c4ba7e9f; __sdcfduid=d0f8f4e8270111ed9c7b5af7c4ba7e9fec27c919f7d926fd191dd159eb019daa5c0ca1a194497fae8fcda3833f2b4d4e",
        },
      });

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data.data,
        }),
        720
      );

      res.status(200).json({ success: true, data: data.data });
    } catch (err) {
      console.log(err);
      res.status(500).json({ err });
    }
  };

  public GetEngagementVsVolume = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );

      // let subtractedTime = dayjs().subtract(30, "day");
      let matchFormat = {
        ...(time
          ? {
              created_date: {
                $gte: new Date(subtractedTime.toISOString()),
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
      // }

      let pipeline = [
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
            twitter_engagement: [
              {
                $lookup: {
                  from: "tweets",
                  localField: "slug",
                  foreignField: "slug",
                  as: "tweets",
                },
              },
              {
                $unwind: {
                  path: "$tweets",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: "$tweets.created_date",
                  },
                  likes_count: "$tweets.likes_count",
                  retweet_count: "$tweets.retweet_count",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_likes: {
                    $avg: "$likes_count",
                  },
                  avg_retweet: {
                    $avg: "$retweet_count",
                  },
                },
              },
              {
                $project: {
                  _id: 1,
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
                  as: "subreddit",
                },
              },
              {
                $unwind: {
                  path: "$subreddit",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$subreddit.created_utc", 1000],
                    },
                  },
                  score: "$subreddit.score",
                  num_comments: "$subreddit.num_comments",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
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
                  _id: 1,
                  reddit_engagement: {
                    $sum: ["$average_score", "$average_comments"],
                  },
                },
              },
            ],
            trends_engagement: [
              {
                $lookup: {
                  from: "google_trends",
                  localField: "slug",
                  foreignField: "slug",
                  as: "trends",
                },
              },
              {
                $unwind: {
                  path: "$trends",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$trends.timestamp", 1000],
                    },
                  },
                  value: "$trends.value",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  trends_engagement: {
                    $avg: "$value",
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
                  created_date: {
                    $toDate: "$events.created_date",
                  },
                  total_price: {
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
              {
                $match: time
                  ? {
                      event_type: "successful",
                      slug: slug,
                      created_date: {
                        $gte: subtractedTime.toDate(),
                      },
                      total_price: {
                        $nin: [null, "0", 0, "", "null"],
                      },
                    }
                  : {
                      event_type: "successful",
                      slug: slug,
                      total_price: {
                        $nin: [null, "0", 0, "", "null"],
                      },
                    },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  volume_all: {
                    $sum: "$total_price",
                  },
                  total_sales: {
                    $sum: 1,
                  },
                },
              },
              {
                $project: {
                  volume_all: 1,
                  total_sales: 1,
                  _id: 1,
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
                "$trends_engagement",
                "$volume_all",
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
              $sum: "$all.reddit_engagement",
            },
            twitter_engagement: {
              $sum: "$all.twitter_engagement",
            },
            trends_engagement: {
              $sum: "$all.trends_engagement",
            },
            volume_all: {
              $sum: "$all.volume_all",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            community_engagement: {
              $sum: [
                "$reddit_engagement",
                "$twitter_engagement",
                "$trends_engagement",
              ],
            },
            volume_all: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const collections = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? collections.length
          ? dayjs(collections[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        community_engagement: 0,
        volume_all: 0,
      };

      // Convert id objects to datetime
      collections.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetEngagementVsLiquidity = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );

      let total_supply = (
        await db
          .collection("collections")
          .find(
            {
              slug,
            },
            {
              projection: {
                total_supply: 1,
              },
            }
          )
          .toArray()
      )[0].total_supply;

      // let subtractedTime = dayjs().subtract(30, "day");
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
      // }

      const collections = await db
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
              twitter_engagement: [
                {
                  $lookup: {
                    from: "tweets",
                    localField: "slug",
                    foreignField: "slug",
                    as: "tweets",
                  },
                },
                {
                  $unwind: {
                    path: "$tweets",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: "$tweets.created_date",
                    },
                    likes_count: "$tweets.likes_count",
                    retweet_count: "$tweets.retweet_count",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    avg_likes: {
                      $avg: "$likes_count",
                    },
                    avg_retweet: {
                      $avg: "$retweet_count",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
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
                    as: "subreddit",
                  },
                },
                {
                  $unwind: {
                    path: "$subreddit",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$subreddit.created_utc", 1000],
                      },
                    },
                    score: "$subreddit.score",
                    num_comments: "$subreddit.num_comments",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
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
                    _id: 1,
                    reddit_engagement: {
                      $sum: ["$average_score", "$average_comments"],
                    },
                  },
                },
              ],
              trends_engagement: [
                {
                  $lookup: {
                    from: "google_trends",
                    localField: "slug",
                    foreignField: "slug",
                    as: "trends",
                  },
                },
                {
                  $unwind: {
                    path: "$trends",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$trends.timestamp", 1000],
                      },
                    },
                    value: "$trends.value",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    trends_engagement: {
                      $avg: "$value",
                    },
                  },
                },
              ],
              sales: [
                {
                  $lookup: {
                    from: "rarible_events",
                    localField: "slug",
                    foreignField: "slug",
                    as: "sales",
                  },
                },
                {
                  $unwind: {
                    path: "$sales",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: "$sales.created_date",
                    },
                    event_type: "$sales.event_type",
                    slug: "$sales.slug",
                    total_price: "$sales.total_price",
                  },
                },
                {
                  $match: {
                    event_type: "successful",
                    slug,
                    created_date: {
                      $gte: subtractedTime.toDate(),
                    },
                    total_price: {
                      $nin: [null, "0", 0, "", "null"],
                    },
                  },
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    sales: { $sum: 1 },
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
                  "$trends_engagement",
                  "$sales",
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
                $sum: "$all.reddit_engagement",
              },
              twitter_engagement: {
                $sum: "$all.twitter_engagement",
              },
              trends_engagement: {
                $sum: "$all.trends_engagement",
              },
              sales: {
                $sum: "$all.sales",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              community_engagement: {
                $sum: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$trends_engagement",
                ],
              },
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
        ? collections.length
          ? dayjs(collections[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        sales: 0,
        community_engagement: 0,
        liquidity: 0,
      };

      // Convert id objects to datetime
      collections.forEach((item, index) => {
        const date = dayjs(item._id);

        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);
        item["liquidity"] = (item.sales / total_supply) * 100;
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

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetEngagementVsSales = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );

      // let subtractedTime = dayjs().subtract(30, "day");
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
      // }

      const collections = await db
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
              twitter_engagement: [
                {
                  $lookup: {
                    from: "tweets",
                    localField: "slug",
                    foreignField: "slug",
                    as: "tweets",
                  },
                },
                {
                  $unwind: {
                    path: "$tweets",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: "$tweets.created_date",
                    },
                    likes_count: "$tweets.likes_count",
                    retweet_count: "$tweets.retweet_count",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    avg_likes: {
                      $avg: "$likes_count",
                    },
                    avg_retweet: {
                      $avg: "$retweet_count",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
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
                    as: "subreddit",
                  },
                },
                {
                  $unwind: {
                    path: "$subreddit",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$subreddit.created_utc", 1000],
                      },
                    },
                    score: "$subreddit.score",
                    num_comments: "$subreddit.num_comments",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
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
                    _id: 1,
                    reddit_engagement: {
                      $sum: ["$average_score", "$average_comments"],
                    },
                  },
                },
              ],
              trends_engagement: [
                {
                  $lookup: {
                    from: "google_trends",
                    localField: "slug",
                    foreignField: "slug",
                    as: "trends",
                  },
                },
                {
                  $unwind: {
                    path: "$trends",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$trends.timestamp", 1000],
                      },
                    },
                    value: "$trends.value",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    trends_engagement: {
                      $avg: "$value",
                    },
                  },
                },
              ],
              sales: [
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
                    as: "sales",
                  },
                },
                {
                  $unwind: {
                    path: "$sales",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: "$sales.created_date",
                    },
                    event_type: "$sales.event_type",
                    slug: "$sales.slug",
                    total_price: "$sales.total_price",
                  },
                },
                {
                  $match: {
                    event_type: "successful",
                    slug,
                    created_date: {
                      $gte: subtractedTime.toDate(),
                    },
                    total_price: {
                      $nin: [null, "0", 0, "", "null"],
                    },
                  },
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    sales: { $sum: 1 },
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
              _id: 1,
              all: {
                $concatArrays: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$trends_engagement",
                  "$sales",
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
                $sum: "$all.reddit_engagement",
              },
              twitter_engagement: {
                $sum: "$all.twitter_engagement",
              },
              trends_engagement: {
                $sum: "$all.trends_engagement",
              },
              sales: {
                $sum: "$all.sales",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              community_engagement: {
                $sum: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$trends_engagement",
                ],
              },
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
        ? collections.length
          ? dayjs(collections[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        sales: 0,
        community_engagement: 0,
      };

      // Convert id objects to datetime
      collections.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetEngagementVsFloorPrice = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );

      // let subtractedTime = dayjs().subtract(30, "day");
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
      // }

      let pipeline = [
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
            twitter_engagement: [
              {
                $lookup: {
                  from: "tweets",
                  localField: "slug",
                  foreignField: "slug",
                  as: "tweets",
                },
              },
              {
                $unwind: {
                  path: "$tweets",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: "$tweets.created_date",
                  },
                  likes_count: "$tweets.likes_count",
                  retweet_count: "$tweets.retweet_count",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_likes: {
                    $avg: "$likes_count",
                  },
                  avg_retweet: {
                    $avg: "$retweet_count",
                  },
                },
              },
              {
                $project: {
                  _id: 1,
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
                  as: "subreddit",
                },
              },
              {
                $unwind: {
                  path: "$subreddit",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$subreddit.created_utc", 1000],
                    },
                  },
                  score: "$subreddit.score",
                  num_comments: "$subreddit.num_comments",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
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
                  _id: 1,
                  reddit_engagement: {
                    $sum: ["$average_score", "$average_comments"],
                  },
                },
              },
            ],
            trends_engagement: [
              {
                $lookup: {
                  from: "google_trends",
                  localField: "slug",
                  foreignField: "slug",
                  as: "trends",
                },
              },
              {
                $unwind: {
                  path: "$trends",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$trends.timestamp", 1000],
                    },
                  },
                  value: "$trends.value",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  trends_engagement: {
                    $avg: "$value",
                  },
                },
              },
            ],
            floor_price: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "slug",
                  foreignField: "slug",
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
                  created_date: {
                    $toDate: "$events.created_date",
                  },
                  ending_price: "$events.ending_price",
                },
              },
              {
                $match: {
                  event_type: "created",
                  slug: slug,
                  ending_price: {
                    $nin: [null, "0", 0, "null", "", NaN],
                  },
                  ...matchFormat,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
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
                "$trends_engagement",
                "$floor_price",
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
              $sum: "$all.reddit_engagement",
            },
            twitter_engagement: {
              $sum: "$all.twitter_engagement",
            },
            trends_engagement: {
              $sum: "$all.trends_engagement",
            },
            floor_price: {
              $sum: "$all.floor_price",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            community_engagement: {
              $sum: [
                "$reddit_engagement",
                "$twitter_engagement",
                "$trends_engagement",
              ],
            },
            floor_price: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const collections = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? collections.length
          ? dayjs(collections[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        floor_price: 0,
        community_engagement: 0,
      };

      // Convert id objects to datetime
      collections.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetEngagementVsMarketCap = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let { time } = req.query;

      let subtractedTime;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );

      // let subtractedTime = dayjs().subtract(30, "day");
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
      // }

      let pipeline = [
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
            twitter_engagement: [
              {
                $lookup: {
                  from: "tweets",
                  localField: "slug",
                  foreignField: "slug",
                  as: "tweets",
                },
              },
              {
                $unwind: {
                  path: "$tweets",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: "$tweets.created_date",
                  },
                  likes_count: "$tweets.likes_count",
                  retweet_count: "$tweets.retweet_count",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_likes: {
                    $avg: "$likes_count",
                  },
                  avg_retweet: {
                    $avg: "$retweet_count",
                  },
                },
              },
              {
                $project: {
                  _id: 1,
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
                  as: "subreddit",
                },
              },
              {
                $unwind: {
                  path: "$subreddit",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$subreddit.created_utc", 1000],
                    },
                  },
                  score: "$subreddit.score",
                  num_comments: "$subreddit.num_comments",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
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
                  _id: 1,
                  reddit_engagement: {
                    $sum: ["$average_score", "$average_comments"],
                  },
                },
              },
            ],
            trends_engagement: [
              {
                $lookup: {
                  from: "google_trends",
                  localField: "slug",
                  foreignField: "slug",
                  as: "trends",
                },
              },
              {
                $unwind: {
                  path: "$trends",
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$trends.timestamp", 1000],
                    },
                  },
                  value: "$trends.value",
                },
              },
              {
                $match: matchFormat,
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  trends_engagement: {
                    $avg: "$value",
                  },
                },
              },
            ],
            marketcap: [
              {
                $lookup: {
                  from: "rarible_events",
                  localField: "slug",
                  foreignField: "slug",
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
                  created_date: {
                    $toDate: "$events.created_date",
                  },
                  total_price: "$events.total_price",
                  token_id: "$events.token_id",
                },
              },
              {
                $match: {
                  event_type: "successful",
                  slug,
                  total_price: {
                    $nin: [null, "0", 0, "", "null"],
                  },
                  ...matchFormat,
                },
              },
              {
                $group: {
                  _id: {
                    ...structure(time, slug).idFormat,
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
                  _id: {
                    year: "$_id.year",
                    month: "$_id.month",
                    day: "$_id.day",
                    hour: "$_id.hour",
                  },
                  total_market_cap: {
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
              $concatArrays: [
                "$reddit_engagement",
                "$twitter_engagement",
                "$trends_engagement",
                "$marketcap",
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
              $sum: "$all.reddit_engagement",
            },
            twitter_engagement: {
              $sum: "$all.twitter_engagement",
            },
            trends_engagement: {
              $sum: "$all.trends_engagement",
            },
            marketcap: {
              $sum: "$all.total_market_cap",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            community_engagement: {
              $sum: [
                "$reddit_engagement",
                "$twitter_engagement",
                "$trends_engagement",
              ],
            },
            marketcap: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const collections = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? collections.length
          ? dayjs(collections[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        community_engagement: 0,
        marketcap: 0,
      };

      // Convert id objects to datetime
      collections.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetRecentTweets = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;

      let data = await db
        .collection("tweets")
        .find({
          slug,
        })
        .sort({
          created_date: -1,
        })
        .limit(10)
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetRedditEngagement = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["reddit_posts"],
        ["created_utc"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            created_utc: {
              $gte: subtractedTime.toDate().getTime() / 1000,
            },
          }
        : {}),
    };

    // if (time == "24h") {
    //   subtractedTime = dayjs().subtract(1, "day");
    //   matchFormat = {
    //     slug,
    //     created_utc: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "7d") {
    //   subtractedTime = dayjs().subtract(7, "day");
    //   matchFormat = {
    //     slug,
    //     created_utc: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "30d") {
    //   subtractedTime = dayjs().subtract(30, "day");
    //   matchFormat = {
    //     slug,
    //     created_utc: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "3m") {
    //   subtractedTime = dayjs().subtract(3, "month");
    //   matchFormat = {
    //     slug,
    //     created_utc: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "1y") {
    //   subtractedTime = dayjs().subtract(1, "year");
    //   matchFormat = {
    //     slug,
    //     created_utc: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // }
    try {
      const redditEngagement = await db
        .collection("reddit_posts")
        .aggregate([
          {
            $match: matchFormat,
          },
          {
            $project: {
              created_date: {
                $toDate: {
                  $multiply: ["$created_utc", 1000],
                },
              },
              score: "$score",
              num_comments: "$num_comments",
            },
          },
          {
            $group: {
              _id: structure(time, slug).idFormat,
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
              _id: getDateFormat(time),
              engagement: {
                $sum: ["$average_score", "$average_comments"],
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

      let data = [];
      var startFrom = !time
        ? redditEngagement.length
          ? dayjs(redditEngagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        engagement: 0,
      };

      // Convert id objects to datetime
      redditEngagement.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTwitterEngagement = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["tweets"],
        ["created_date"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            created_date: {
              $gte: subtractedTime.toDate(),
            },
          }
        : {}),
    };

    // let idFormat;
    // if (time == "24h") {
    //   // subtractedTime = dayjs().subtract(1, "day");
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //     hour: {
    //       $hour: "$created_date",
    //     },
    //   };
    // } else if (time == "7d") {
    //   // subtractedTime = dayjs().subtract(7, "day");
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //     hour: {
    //       $multiply: [
    //         {
    //           $floor: {
    //             $divide: [{ $hour: "$created_date" }, 2],
    //           },
    //         },
    //         2,
    //       ],
    //     },
    //   };
    // } else if (time == "30d") {
    //   // subtractedTime = dayjs().subtract(30, "day");
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //   };
    // } else if (time == "3m") {
    //   // subtractedTime = dayjs().subtract(3, "month");
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //   };
    // } else if (time == "1y") {
    //   // subtractedTime = dayjs().subtract(1, "year");
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //   };
    // }

    // if (!time) {
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //   };
    // } else {
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // }

    try {
      const twitter_engagement = await db
        .collection("tweets")
        .aggregate([
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              slug: 1,
              like_count: 1,
              retweet_count: 1,
            },
          },
          {
            $match: matchFormat,
          },
          {
            $group: {
              _id: structure(time, slug).idFormat,
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
              _id: getDateFormat(time),
              engagement: {
                $sum: ["$avg_likes", "$avg_retweet"],
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

      let data = [];
      var startFrom = !time
        ? twitter_engagement.length
          ? dayjs(twitter_engagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        engagement: 0,
      };

      // Convert id objects to datetime
      twitter_engagement.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetRedditSentiment = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["reddit_posts"],
        ["created_utc"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            created_utc: {
              $gte: subtractedTime.toDate().getTime() / 1000,
            },
          }
        : {}),
    };

    try {
      // let subtractedTime;
      // let matchFormat: any = {
      //   slug,
      // };
      // if (time == "24h") {
      //   subtractedTime = dayjs().subtract(1, "day");
      //   matchFormat = {
      //     slug,
      //     created_utc: {
      //       $gte: new Date(subtractedTime).getTime() / 1000,
      //     },
      //   };
      // } else if (time == "7d") {
      //   subtractedTime = dayjs().subtract(7, "day");
      //   matchFormat = {
      //     slug,
      //     created_utc: {
      //       $gte: new Date(subtractedTime).getTime() / 1000,
      //     },
      //   };
      // } else if (time == "30d") {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     slug,
      //     created_utc: {
      //       $gte: new Date(subtractedTime).getTime() / 1000,
      //     },
      //   };
      // } else if (time == "3m") {
      //   subtractedTime = dayjs().subtract(3, "month");
      //   matchFormat = {
      //     slug,
      //     created_utc: {
      //       $gte: new Date(subtractedTime).getTime() / 1000,
      //     },
      //   };
      // } else if (time == "1y") {
      //   subtractedTime = dayjs().subtract(1, "year");
      //   matchFormat = {
      //     slug,
      //     created_utc: {
      //       $gte: new Date(subtractedTime).getTime() / 1000,
      //     },
      //   };
      // }

      const sentiment = await db
        .collection("reddit_posts")
        .aggregate([
          {
            $match: matchFormat,
          },
          {
            $project: {
              created_date: {
                $toDate: {
                  $multiply: ["$created_utc", 1000],
                },
              },
              sentiment: 1,
              slug: 1,
            },
          },
          {
            $group: {
              _id: structure(time, slug).idFormat,
              sentiment_score: {
                $avg: "$sentiment",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              sentiment_score: 1,
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
        ? sentiment.length
          ? dayjs(sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        sentiment_score: 0,
      };

      // Convert id objects to datetime
      sentiment.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetTwitterSentiment = async (req: Request, res: Response) => {
    const { slug } = req.params;
    const { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["tweets"],
        ["created_date"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            created_date: {
              $gte: subtractedTime.toDate(),
            },
          }
        : {}),
    };

    // let subtractedTime = dayjs().subtract(1, "day");
    // let concatFormat: any = {
    //   $toDate: {
    //     $concat: [
    //       {
    //         $toString: "$_id.year",
    //       },
    //       "-",
    //       {
    //         $toString: "$_id.month",
    //       },
    //       "-",
    //       {
    //         $toString: "$_id.day",
    //       },
    //       " ",
    //       {
    //         $toString: "$_id.hour",
    //       },
    //       ":00:00",
    //     ],
    //   },
    // };
    // let matchFormat: any = {
    //   slug: slug,
    //   created_date: {
    //     $gte: new Date(dayjs(subtractedTime).toISOString()),
    //   },
    // };
    // if (time == "24h") {
    //   subtractedTime = dayjs().subtract(1, "day");
    //   concatFormat = {
    //     $toDate: {
    //       $concat: [
    //         {
    //           $toString: "$_id.year",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.month",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.day",
    //         },
    //         " ",
    //         {
    //           $toString: "$_id.hour",
    //         },
    //         ":00:00",
    //       ],
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // } else if (time == "7d") {
    //   subtractedTime = dayjs().subtract(7, "day");
    //   concatFormat = {
    //     $toDate: {
    //       $concat: [
    //         {
    //           $toString: "$_id.year",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.month",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.day",
    //         },
    //         " ",
    //         {
    //           $toString: "$_id.hour",
    //         },
    //         ":00:00",
    //       ],
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // } else if (time == "30d") {
    //   subtractedTime = dayjs().subtract(30, "day");
    //   concatFormat = {
    //     $toDate: {
    //       $concat: [
    //         {
    //           $toString: "$_id.year",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.month",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.day",
    //         },
    //       ],
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // } else if (time == "3m") {
    //   subtractedTime = dayjs().subtract(3, "month");
    //   concatFormat = {
    //     $toDate: {
    //       $concat: [
    //         {
    //           $toString: "$_id.year",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.month",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.day",
    //         },
    //       ],
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // } else if (time == "1y") {
    //   subtractedTime = dayjs().subtract(1, "year");
    //   concatFormat = {
    //     $toDate: {
    //       $concat: [
    //         {
    //           $toString: "$_id.year",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.month",
    //         },
    //         "-",
    //         {
    //           $toString: "$_id.day",
    //         },
    //       ],
    //     },
    //   };
    //   matchFormat = {
    //     slug: slug,
    //     created_date: {
    //       $gte: new Date(dayjs(subtractedTime).toISOString()),
    //     },
    //   };
    // }
    try {
      const sentiment = await db
        .collection("tweets")
        .aggregate([
          {
            $project: {
              created_date: {
                $toDate: "$created_date",
              },
              sentiment: 1,
              slug: 1,
            },
          },
          {
            $match: matchFormat,
          },
          {
            $group: {
              _id: structure(time, slug).idFormat,
              sentiment: {
                $avg: "$sentiment",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              sentiment: 1,
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
        ? sentiment.length
          ? dayjs(sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        sentiment: 0,
      };

      // Convert id objects to datetime
      sentiment.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (err) {
      res.status(500).send(err);
    }
  };

  public GetOverallSentiment = async (req: Request, res: Response) => {
    const { slug } = req.params;
    const { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["tweets", "reddit_posts"],
        ["created_date", "created_utc"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            created_date: {
              $gte: subtractedTime.toDate(),
            },
          }
        : {}),
    };

    try {
      // let subtractedTime: any = dayjs().subtract(30, "day");
      // let matchFormat: any = {
      //   created_date: {
      //     $gte: new Date(dayjs(subtractedTime).toISOString()),
      //   },
      // };
      // let concatFormat: any = {
      //   $toDate: {
      //     $concat: [
      //       {
      //         $toString: "$_id.year",
      //       },
      //       "-",
      //       {
      //         $toString: "$_id.month",
      //       },
      //       "-",
      //       {
      //         $toString: "$_id.day",
      //       },
      //     ],
      //   },
      // };
      // if (time == "24h") {
      //   subtractedTime = dayjs().subtract(1, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      //   concatFormat = {
      //     $toDate: {
      //       $concat: [
      //         {
      //           $toString: "$_id.year",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.month",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.day",
      //         },
      //         " ",
      //         {
      //           $toString: "$_id.hour",
      //         },
      //         ":00:00",
      //       ],
      //     },
      //   };
      // } else if (time == "7d") {
      //   subtractedTime = dayjs().subtract(7, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      //   concatFormat = {
      //     $toDate: {
      //       $concat: [
      //         {
      //           $toString: "$_id.year",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.month",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.day",
      //         },
      //         " ",
      //         {
      //           $toString: "$_id.hour",
      //         },
      //         ":00:00",
      //       ],
      //     },
      //   };
      // } else if (time == "30d") {
      //   subtractedTime = dayjs().subtract(30, "day");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      //   concatFormat = {
      //     $toDate: {
      //       $concat: [
      //         {
      //           $toString: "$_id.year",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.month",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.day",
      //         },
      //       ],
      //     },
      //   };
      // } else if (time == "3m") {
      //   subtractedTime = dayjs().subtract(3, "month");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      //   concatFormat = {
      //     $toDate: {
      //       $concat: [
      //         {
      //           $toString: "$_id.year",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.month",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.day",
      //         },
      //       ],
      //     },
      //   };
      // } else if (time == "1y") {
      //   subtractedTime = dayjs().subtract(1, "year");
      //   matchFormat = {
      //     created_date: {
      //       $gte: new Date(dayjs(subtractedTime).toISOString()),
      //     },
      //   };
      //   concatFormat = {
      //     $toDate: {
      //       $concat: [
      //         {
      //           $toString: "$_id.year",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.month",
      //         },
      //         "-",
      //         {
      //           $toString: "$_id.day",
      //         },
      //       ],
      //     },
      //   };
      // }

      const overall_sentiment = await db
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
              twitter: [
                {
                  $lookup: {
                    from: "tweets",
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
                      $toDate: "$result.created_date",
                    },
                    slug: 1,
                    sentiment: "$result.sentiment",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    avg_sentiment: {
                      $avg: "$sentiment",
                    },
                  },
                },
                {
                  $project: {
                    _id: getDateFormat(time),
                    twitter_sentiment: "$avg_sentiment",
                  },
                },
              ],
              reddit: [
                {
                  $lookup: {
                    from: "reddit_posts",
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
                      $toDate: {
                        $multiply: ["$result.created_utc", 1000],
                      },
                    },
                    sentiment: "$result.sentiment",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    average_sentiment: {
                      $avg: "$sentiment",
                    },
                  },
                },
                {
                  $project: {
                    _id: getDateFormat(time),
                    reddit_sentiment: "$average_sentiment",
                  },
                },
              ],
            },
          },
          {
            $project: {
              all: {
                $concatArrays: ["$twitter", "$reddit"],
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
              reddit_sentiment: {
                $max: "$all.reddit_sentiment",
              },
              twitter_sentiment: {
                $max: "$all.twitter_sentiment",
              },
            },
          },
          {
            $project: {
              reddit_sentiment: {
                $cond: [
                  {
                    $eq: ["$reddit_sentiment", null],
                  },
                  "$$REMOVE",
                  "$reddit_sentiment",
                ],
              },
              twitter_sentiment: {
                $cond: [
                  {
                    $eq: ["$twitter_sentiment", null],
                  },
                  "$$REMOVE",
                  "$twitter_sentiment",
                ],
              },
              overall_sentiment: {
                $sum: ["$reddit_sentiment", "$twitter_sentiment"],
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

      let data = [];
      var startFrom = !time
        ? overall_sentiment.length
          ? dayjs(overall_sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        overall_sentiment: 0,
      };

      // Convert id objects to datetime
      overall_sentiment.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (err) {
      console.log(err);
      res.status(500).send(err);
    }
  };

  public GetTrendsEngagement = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["google_trends"],
        ["timestamp"],
        { slug: slug }
      );

    let matchFormat = {
      slug,
      ...(time
        ? {
            timestamp: {
              $gte: subtractedTime.toDate().getTime() / 1000,
            },
          }
        : {}),
    };

    // let subtractedTime;
    // let matchFormat: any = {
    //   slug,
    // };
    // let idFormat: any = {
    //   year: {
    //     $year: "$created_date",
    //   },
    //   month: {
    //     $month: "$created_date",
    //   },
    //   day: {
    //     $dayOfMonth: "$created_date",
    //   },
    // };

    // if (time == "24h") {
    //   subtractedTime = dayjs().subtract(1, "day");
    //   matchFormat = {
    //     slug,
    //     timestamp: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //     hour: {
    //       $hour: "$created_date",
    //     },
    //   };
    // } else if (time == "7d") {
    //   subtractedTime = dayjs().subtract(7, "day");
    //   matchFormat = {
    //     slug,
    //     timestamp: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    //   idFormat = {
    //     year: {
    //       $year: "$created_date",
    //     },
    //     month: {
    //       $month: "$created_date",
    //     },
    //     day: {
    //       $dayOfMonth: "$created_date",
    //     },
    //     hour: {
    //       $multiply: [
    //         {
    //           $floor: {
    //             $divide: [{ $hour: "$created_date" }, 2],
    //           },
    //         },
    //         2,
    //       ],
    //     },
    //   };
    // } else if (time == "30d") {
    //   subtractedTime = dayjs().subtract(30, "day");
    //   matchFormat = {
    //     slug,
    //     timestamp: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "3m") {
    //   subtractedTime = dayjs().subtract(3, "month");
    //   matchFormat = {
    //     slug,
    //     timestamp: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // } else if (time == "1y") {
    //   subtractedTime = dayjs().subtract(1, "year");
    //   matchFormat = {
    //     slug,
    //     timestamp: {
    //       $gte: new Date(subtractedTime).getTime() / 1000,
    //     },
    //   };
    // }
    try {
      const trendsEngagement = await db
        .collection("google_trends")
        .aggregate([
          {
            $match: matchFormat,
          },
          {
            $project: {
              created_date: {
                $toDate: {
                  $multiply: ["$timestamp", 1000],
                },
              },
              value: 1,
            },
          },
          {
            $group: {
              _id: structure(time, slug).idFormat,
              avg_engagement: {
                $avg: "$value",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              avg_engagement: 1,
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
        ? trendsEngagement.length
          ? dayjs(trendsEngagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        avg_engagement: 0,
      };

      // Convert id objects to datetime
      trendsEngagement.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetTrendsByCountry = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let finalData = [];
      let interestByRegion = JSON.parse(
        await googleTrends.interestByRegion({
          keyword: slug,
        })
      );
      interestByRegion.default.geoMapData.map((item) => {
        if (item.hasData.includes(true)) {
          finalData.push({
            code: item.geoCode,
            country: item.geoName,
            value: item.value[0],
          });
        }
      });

      if (finalData) {
        setCache(
          uniqueKey(req),
          JSON.stringify({
            success: true,
            data: finalData,
          }),
          1440 //
        );
      }

      res.status(200).json({
        success: true,
        data: finalData,
      });
    } catch (error) {
      console.log(error);
      res.status(500).send(error);
    }
  };

  public GetOverallEngagement = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["tweets", "reddit_posts", "google_trends"],
          ["created_date", "created_utc", "timestamp"],
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

      // let subtractedTime: any = dayjs().subtract(30, "day");
      // let matchFormat: any = {
      //   created_date: {
      //     $gte: new Date(dayjs(subtractedTime).toISOString()),
      //   },
      // };
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
      // }

      const engagement = await db
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
              twitter_engagement: [
                {
                  $lookup: {
                    from: "tweets",
                    localField: "slug",
                    foreignField: "slug",
                    as: "tweets",
                  },
                },
                {
                  $unwind: {
                    path: "$tweets",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: "$tweets.created_date",
                    },
                    likes_count: "$tweets.likes_count",
                    retweet_count: "$tweets.retweet_count",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    avg_likes: {
                      $avg: "$likes_count",
                    },
                    avg_retweet: {
                      $avg: "$retweet_count",
                    },
                  },
                },
                {
                  $project: {
                    _id: 1,
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
                    as: "subreddit",
                  },
                },
                {
                  $unwind: {
                    path: "$subreddit",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$subreddit.created_utc", 1000],
                      },
                    },
                    score: "$subreddit.score",
                    num_comments: "$subreddit.num_comments",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
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
                    _id: 1,
                    reddit_engagement: {
                      $sum: ["$average_score", "$average_comments"],
                    },
                  },
                },
              ],
              trends_engagement: [
                {
                  $lookup: {
                    from: "google_trends",
                    localField: "slug",
                    foreignField: "slug",
                    as: "trends",
                  },
                },
                {
                  $unwind: {
                    path: "$trends",
                  },
                },
                {
                  $project: {
                    created_date: {
                      $toDate: {
                        $multiply: ["$trends.timestamp", 1000],
                      },
                    },
                    value: "$trends.value",
                  },
                },
                {
                  $match: matchFormat,
                },
                {
                  $group: {
                    _id: structure(time, slug).idFormat,
                    trends_engagement: {
                      $avg: "$value",
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
                  "$trends_engagement",
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
                $sum: "$all.reddit_engagement",
              },
              twitter_engagement: {
                $sum: "$all.twitter_engagement",
              },
              trends_engagement: {
                $sum: "$all.trends_engagement",
              },
            },
          },
          {
            $project: {
              _id: getDateFormat(time),
              community_engagement: {
                $sum: [
                  "$reddit_engagement",
                  "$twitter_engagement",
                  "$trends_engagement",
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

      let data = [];
      var startFrom = !time
        ? engagement.length
          ? dayjs(engagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        community_engagement: 0,
      };

      // Convert id objects to datetime
      engagement.forEach((item, index) => {
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
        720
      );

      res.status(200).json({
        success: true,
        data: data,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetRedditSentimentSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;
      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["reddit_posts"],
          ["created_utc"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
        {
          $lookup: {
            from: "reddit_posts",
            localField: "slug",
            foreignField: "slug",
            pipeline: [
              {
                $match: {
                  ...(time
                    ? {
                        created_utc: {
                          $gte: subtractedTime.toDate().getTime() / 1000,
                        },
                      }
                    : {}),
                },
              },
              {
                $project: {
                  created_utc: {
                    $toDate: {
                      $multiply: ["$created_utc", 1000],
                    },
                  },
                  sentiment: 1,
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
            slug: 1,
            created_date: {
              $toDate: "$results.created_utc",
            },
            sentiment: "$results.sentiment",
            main_category: 1,
          },
        },
        {
          $facet: {
            categorialSentiment: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  sentiment_score: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  categorial_sentiment_score: "$sentiment_score",
                },
              },
              {
                $sort: {
                  _id: 1,
                },
              },
            ],
            sentiment: [
              {
                $match: {
                  slug: slug,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  sentiment_score: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  sentiment_score: 1,
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
            finalArray: {
              $concatArrays: ["$categorialSentiment", "$sentiment"],
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
            main_category: {
              $addToSet: "$finalArray.categories",
            },
            categorialSentiment: {
              $sum: "$finalArray.categorial_sentiment_score",
            },
            collectionSentiment: {
              $sum: "$finalArray.sentiment_score",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories: {
              $arrayElemAt: ["$main_category", 0],
            },
            categorialSentiment: 1,
            collectionSentiment: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const sentiment = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? sentiment.length
          ? dayjs(sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        collectionSentiment: 0,
        categorialSentiment: 0,
        categories: sentiment.length ? sentiment[0].categories : [],
      };

      // Convert id objects to datetime
      sentiment.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetTwitterSentimentSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["tweets"],
          ["created_date"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
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
              {
                $match: {
                  ...(time
                    ? {
                        created_date: {
                          $gte: subtractedTime.toDate(),
                        },
                      }
                    : {}),
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
            slug: 1,
            main_category: 1,
            created_date: {
              $toDate: "$results.created_date",
            },
            sentiment: "$results.sentiment",
          },
        },
        {
          $facet: {
            categorySentiment: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  categorySentiment: "$sentiment",
                },
              },
            ],
            collectionSentiment: [
              {
                $match: {
                  slug: slug,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  collectionSentiment: "$sentiment",
                },
              },
            ],
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: ["$categorySentiment", "$collectionSentiment"],
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
            categories: {
              $addToSet: "$finalArray.categories",
            },
            categorySentiment: {
              $sum: "$finalArray.categorySentiment",
            },
            collectionSentiment: {
              $sum: "$finalArray.collectionSentiment",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories: {
              $arrayElemAt: ["$categories", 0],
            },
            categorySentiment: 1,
            collectionSentiment: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const sentiment = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? sentiment.length
          ? dayjs(sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        collectionSentiment: 0,
        categorySentiment: 0,
        categories: sentiment.length ? sentiment[0].categories : [],
      };

      // Convert id objects to datetime
      sentiment.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetTwitterEngagementSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["tweets"],
          ["created_date"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
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
                  retweet_count: 1,
                  like_count: 1,
                },
              },
              {
                $match: {
                  ...(time
                    ? {
                        created_date: {
                          $gte: subtractedTime.toDate(),
                        },
                      }
                    : {}),
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
            slug: 1,
            main_category: 1,
            created_date: {
              $toDate: "$results.created_date",
            },
            like_count: "$results.like_count",
            retweet_count: "$results.retweet_count",
          },
        },
        {
          $facet: {
            categoryEngagement: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_likes: {
                    $avg: "$like_count",
                  },
                  avg_retweet: {
                    $avg: "$retweet_count",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  categoryEngagement: {
                    $sum: ["$avg_likes", "$avg_retweet"],
                  },
                },
              },
            ],
            collectionEngagement: [
              {
                $match: {
                  slug: slug,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_likes: {
                    $avg: "$like_count",
                  },
                  avg_retweet: {
                    $avg: "$retweet_count",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  collectionEngagement: {
                    $sum: ["$avg_likes", "$avg_retweet"],
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: ["$categoryEngagement", "$collectionEngagement"],
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
            categories: {
              $addToSet: "$finalArray.categories",
            },
            collectionEngagement: {
              $sum: "$finalArray.collectionEngagement",
            },
            categoryEngagement: {
              $sum: "$finalArray.categoryEngagement",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories: {
              $arrayElemAt: ["$categories", 0],
            },
            collectionEngagement: 1,
            categoryEngagement: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const twitter_engagement = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? twitter_engagement.length
          ? dayjs(twitter_engagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        categoryEngagement: 0,
        collectionEngagement: 0,
        categories: twitter_engagement.length
          ? twitter_engagement[0].categories
          : [],
      };

      // Convert id objects to datetime
      twitter_engagement.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetRedditEngagementSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["reddit_posts"],
          ["created_utc"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
        {
          $lookup: {
            from: "reddit_posts",
            localField: "slug",
            foreignField: "slug",
            pipeline: [
              {
                $match: {
                  ...(time
                    ? {
                        created_utc: {
                          $gte: subtractedTime.toDate().getTime() / 1000,
                        },
                      }
                    : {}),
                },
              },
              {
                $project: {
                  created_utc: {
                    $toDate: {
                      $multiply: ["$created_utc", 1000],
                    },
                  },
                  score: 1,
                  num_comments: 1,
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
            slug: 1,
            created_date: {
              $toDate: "$results.created_utc",
            },
            score: "$results.score",
            num_comments: "$results.num_comments",
            main_category: 1,
          },
        },
        {
          $facet: {
            categorialEnagement: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  average_score: {
                    $avg: "$score",
                  },
                  average_comments: {
                    $avg: "$num_comments",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  categorialEngagement: {
                    $sum: ["$average_score", "$average_comments"],
                  },
                },
              },
            ],
            engagement: [
              {
                $match: {
                  slug: slug,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  average_score: {
                    $avg: "$score",
                  },
                  average_comments: {
                    $avg: "$num_comments",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                  engagement: {
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
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: ["$categorialEnagement", "$engagement"],
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
            main_category: {
              $addToSet: "$finalArray.categories",
            },
            categorialEngagement: {
              $sum: "$finalArray.categorialEngagement",
            },
            collectionEngagement: {
              $sum: "$finalArray.engagement",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categories: {
              $arrayElemAt: ["$main_category", 0],
            },
            categorialEngagement: 1,
            collectionEngagement: 1,
          },
        },
      ];

      const engagement = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? engagement.length
          ? dayjs(engagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        collectionEngagement: 0,
        categorialEngagement: 0,
        categories: engagement.length ? engagement[0].categories : [],
      };

      // Convert id objects to datetime
      engagement.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetTrendsEngagementSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["google_trends"],
          ["timestamp"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
        {
          $lookup: {
            from: "google_trends",
            localField: "slug",
            foreignField: "slug",
            pipeline: [
              {
                $match: {
                  ...(time
                    ? {
                        timestamp: {
                          $gte: subtractedTime.toDate().getTime() / 1000,
                        },
                      }
                    : {}),
                },
              },
              {
                $project: {
                  created_date: {
                    $toDate: {
                      $multiply: ["$timestamp", 1000],
                    },
                  },
                  value: 1,
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
            created_date: "$results.created_date",
            value: "$results.value",
            main_category: 1,
            slug: 1,
          },
        },
        {
          $facet: {
            categorialEngagement: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_engagement: {
                    $avg: "$value",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  categoryEngagement: "$avg_engagement",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            collectionEngagement: [
              {
                $match: {
                  slug: slug,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_engagement: {
                    $avg: "$value",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  collectionEngagement: "$avg_engagement",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            finalArray: {
              $concatArrays: ["$categorialEngagement", "$collectionEngagement"],
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
            categoryEngagement: {
              $sum: "$finalArray.categoryEngagement",
            },
            collectionEngagement: {
              $sum: "$finalArray.collectionEngagement",
            },
            categories: {
              $addToSet: "$finalArray.categories",
            },
          },
        },
        {
          $project: {
            _id: 1,
            categoryEngagement: 1,
            collectionEngagement: 1,
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

      const trendsEngagement = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? trendsEngagement.length
          ? dayjs(trendsEngagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        categoryEngagement: 0,
        collectionEngagement: 0,
        categories: trendsEngagement.length
          ? trendsEngagement[0].categories
          : [],
      };

      // Convert id objects to datetime
      trendsEngagement.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetOverallSentimentSimilar = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["tweets", "reddit_posts"],
          ["created_date", "created_utc"],
          { slug: slug }
        );

      const pipeline = [
        {
          $match: {
            slug: slug,
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
                  slug: 1,
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
            slug: "$output.slug",
            main_category: "$categories",
            categories: "$output.categories",
            intersect: {
              $setIntersection: ["$categories", "$output.categories"],
            },
          },
        },
        {
          $project: {
            slug: 1,
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
          },
        },
        {
          $facet: {
            categoryTwitter: [
              {
                $match: {
                  slug: { $ne: slug },
                },
              },
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
                        sentiment: 1,
                      },
                    },
                    {
                      $match: {
                        ...(time
                          ? {
                              created_date: {
                                $gte: subtractedTime.toDate(),
                              },
                            }
                          : {}),
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
                  created_date: "$result.created_date",
                  slug: 1,
                  main_category: 1,
                  sentiment: "$result.sentiment",
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  category_twitter_sentiment: "$avg_sentiment",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            categoryReddit: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $lookup: {
                  from: "reddit_posts",
                  localField: "slug",
                  foreignField: "slug",
                  pipeline: [
                    {
                      $project: {
                        created_date: {
                          $toDate: {
                            $multiply: ["$created_utc", 1000],
                          },
                        },
                        sentiment: 1,
                      },
                    },
                    {
                      $match: {
                        ...(time
                          ? {
                              created_date: {
                                $gte: subtractedTime.toDate(),
                              },
                            }
                          : {}),
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
                  created_date: "$result.created_date",
                  sentiment: "$result.sentiment",
                  main_category: 1,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  average_sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  category_reddit_sentiment: "$average_sentiment",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            collectionTwitter: [
              {
                $match: {
                  slug: slug,
                },
              },
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
                        sentiment: 1,
                      },
                    },
                    {
                      $match: {
                        ...(time
                          ? {
                              created_date: {
                                $gte: subtractedTime.toDate(),
                              },
                            }
                          : {}),
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
                  created_date: "$result.created_date",
                  slug: 1,
                  main_category: 1,
                  sentiment: "$result.sentiment",
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  avg_sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  collection_twitter_sentiment: "$avg_sentiment",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
                  },
                },
              },
            ],
            collectionReddit: [
              {
                $match: {
                  slug: {
                    $ne: slug,
                  },
                },
              },
              {
                $lookup: {
                  from: "reddit_posts",
                  localField: "slug",
                  foreignField: "slug",
                  pipeline: [
                    {
                      $project: {
                        created_date: {
                          $toDate: {
                            $multiply: ["$created_utc", 1000],
                          },
                        },
                        sentiment: 1,
                      },
                    },
                    {
                      $match: {
                        ...(time
                          ? {
                              created_date: {
                                $gte: subtractedTime.toDate(),
                              },
                            }
                          : {}),
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
                  created_date: "$result.created_date",
                  sentiment: "$result.sentiment",
                  main_category: 1,
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  average_sentiment: {
                    $avg: "$sentiment",
                  },
                  main_category: {
                    $addToSet: "$main_category",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  collection_reddit_sentiment: "$average_sentiment",
                  categories: {
                    $arrayElemAt: ["$main_category", 0],
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
                "$categoryTwitter",
                "$categoryReddit",
                "$collectionTwitter",
                "$collectionReddit",
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
            category_reddit_sentiment: {
              $max: "$all.category_reddit_sentiment",
            },
            category_twitter_sentiment: {
              $max: "$all.category_twitter_sentiment",
            },
            collection_reddit_sentiment: {
              $max: "$all.collection_reddit_sentiment",
            },
            collection_twitter_sentiment: {
              $max: "$all.collection_twitter_sentiment",
            },
            categories: {
              $addToSet: "$all.categories",
            },
          },
        },
        {
          $project: {
            collection_reddit_sentiment: {
              $cond: [
                {
                  $eq: ["$collection_reddit_sentiment", null],
                },
                "$$REMOVE",
                "$collection_reddit_sentiment",
              ],
            },
            collection_twitter_sentiment: {
              $cond: [
                {
                  $eq: ["$collection_twitter_sentiment", null],
                },
                "$$REMOVE",
                "$collection_twitter_sentiment",
              ],
            },
            category_reddit_sentiment: {
              $cond: [
                {
                  $eq: ["$category_reddit_sentiment", null],
                },
                "$$REMOVE",
                "$category_reddit_sentiment",
              ],
            },
            category_twitter_sentiment: {
              $cond: [
                {
                  $eq: ["$category_twitter_sentiment", null],
                },
                "$$REMOVE",
                "$category_twitter_sentiment",
              ],
            },
            category_overall_sentiment: {
              $sum: [
                "$category_reddit_sentiment",
                "$category_twitter_sentiment",
              ],
            },
            collection_overall_sentiment: {
              $sum: [
                "$collection_reddit_sentiment",
                "$collection_twitter_sentiment",
              ],
            },
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

      const overall_sentiment = await db
        .collection("collections")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? overall_sentiment.length
          ? dayjs(overall_sentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        collection_reddit_sentiment: 0,
        category_reddit_sentiment: 0,
        category_overall_sentiment: 0,
        collection_overall_sentiment: 0,
        categories: overall_sentiment.length
          ? overall_sentiment[0].categories
          : [],
      };

      // Convert id objects to datetime
      overall_sentiment.forEach((item, index) => {
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
      res.status(500).send(error);
    }
  };

  public GetDiscordEngagement = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["discord_messages"],
          ["created_date_utc"],
          { slug: slug }
        );

      let matchFormat = {
        slug,
        ...(time
          ? {
              created_date_utc: {
                $gte: subtractedTime.toDate().getTime() / 1000,
              },
            }
          : {}),
      };

      const pipeline = [
        {
          $match: matchFormat,
        },
        {
          $project: {
            created_date: {
              $toDate: {
                $multiply: ["$created_date_utc", 1000],
              },
            },
            content: 1,
          },
        },
        {
          $group: {
            _id: structure(time, slug).idFormat,
            engagement: {
              $sum: 1,
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            engagement: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const discordEngagement = await db
        .collection("discord_messages")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? discordEngagement.length
          ? dayjs(discordEngagement[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        engagement: 0,
      };

      // Convert id objects to datetime
      discordEngagement.forEach((item, index) => {
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

      return res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      return res.status(500).send(error);
    }
  };

  public GetDiscordSentiment = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      const { time } = req.query;

      let subtractedTime: dayjs.Dayjs;
      if (time)
        subtractedTime = await getSubtractedtime(
          time,
          ["discord_messages"],
          ["created_date_utc"],
          { slug: slug }
        );

      let matchFormat = {
        slug,
        ...(time
          ? {
              created_date_utc: {
                $gte: subtractedTime.toDate().getTime() / 1000,
              },
            }
          : {}),
      };

      const pipeline = [
        {
          $match: matchFormat,
        },
        {
          $project: {
            created_date: {
              $toDate: {
                $multiply: ["$created_date_utc", 1000],
              },
            },
            sentimentScore: {
              $cond: {
                if: {
                  $gt: ["$sentiment.pos", "$sentiment.neg"],
                },
                then: 1,
                else: {
                  $cond: {
                    if: {
                      $gt: ["$sentiment.neg", "$sentiment.pos"],
                    },
                    then: -1,
                    else: 0,
                  },
                },
              },
            },
          },
        },
        {
          $group: {
            _id: structure(time, slug).idFormat,
            sentiment_score: {
              $avg: "$sentimentScore",
            },
          },
        },
        {
          $project: {
            _id: getDateFormat(time),
            sentiment_score: 1,
          },
        },
        {
          $sort: {
            _id: 1,
          },
        },
      ];

      const discordSentiment = await db
        .collection("discord_messages")
        .aggregate(pipeline)
        .toArray();

      let data = [];
      var startFrom = !time
        ? discordSentiment.length
          ? dayjs(discordSentiment[0]._id)
          : dayjs()
        : subtractedTime;

      const value = {
        sentiment_score: 0,
      };

      // Convert id objects to datetime
      discordSentiment.forEach((item, index) => {
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

      return res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      console.log(error);
      return res.status(500).send(error);
    }
  };
}
