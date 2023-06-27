import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";

export default class leaderboardController {
  public GetProfitLeaderboard = async (req: Request, res: Response) => {
    try {
      const time = req.query.time || "all";

      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      const leaderboard = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              event_type: "successful",
            },
          },
          {
            $project: {
              user_address: "$winner_account.address",
              user_name: "$winner_account.user.username",
              user_img_url: "$winner_account.profile_img_url",
              created_date: {
                $toDate: "$created_date",
              },
              slug: 1,
              token_id: 1,
              total_price: {
                $divide: [
                  {
                    $convert: {
                      input: "$total_price",
                      to: 1,
                    },
                  },
                  1000000000000000000,
                ],
              },
            },
          },
          {
            $group: {
              _id: "$user_address",
              user_details: {
                $addToSet: {
                  user_name: "$user_name",
                  user_img_url: "$user_img_url",
                  user_address: "$user_address",
                },
              },
              tokens: {
                $addToSet: {
                  token_id: "$token_id",
                  slug: "$slug",
                },
              },
              slugs: {
                $addToSet: {
                  slug: "$slug",
                },
              },
              total_price: {
                $addToSet: {
                  created_date: "$created_date",
                  total_price: "$total_price",
                },
              },
              buy_volume: {
                $sum: "$total_price",
              },
            },
          },
          {
            $unwind: {
              path: "$user_details",
            },
          },
          {
            $project: {
              user_name: "$user_details.user_name",
              user_address: "$user_details.user_address",
              user_img_url: "$user_details.user_img_url",
              buy_volume: 1,
              collection_count: {
                $size: "$slugs",
              },
              purchase_count: {
                $size: "$total_price",
              },
            },
          },
          {
            $skip: (page - 1) * pageSize,
          },
          {
            $limit: pageSize,
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: leaderboard,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: leaderboard,
      });
    } catch (err) {
      res.status(500).send({ error: err.message });
    }
  };

  public GetTopBuyers = async (req: Request, res: Response) => {
    try {
      const time = req.query.time || "all";

      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;
      let sortType: any = req.query.sortBy || "buy_volume";

      //SORT TYPE
      if (sortType == "buy_volume") {
        sortType = {
          buy_volume: -1,
        };
      }
      if (sortType == "purchase_count") {
        sortType = {
          purchase_count: -1,
        };
      }
      if (sortType == "collection_count") {
        sortType = {
          collection_count: -1,
        };
      }

      let time_match = (time) => {
        let subtractedTime;
        let today = new Date();

        if (time == "1h") {
          subtractedTime = today.setHours(today.getHours() - 1);
        }
        if (time == "6h") {
          subtractedTime = today.setHours(today.getHours() - 6);
        }
        if (time == "12h") {
          subtractedTime = today.setHours(today.getHours() - 12);
        }
        if (time == "24h") {
          subtractedTime = today.setDate(today.getDate() - 1);
        }

        if (time == "7d") {
          subtractedTime = today.setDate(today.getDate() - 7);
        }

        if (time == "30d") {
          subtractedTime = today.setDate(today.getDate() - 30);
        }

        let match =
          time === "all"
            ? {
                event_type: "successful",
                winner_account: { $ne: null },
              }
            : {
                winner_account: { $ne: null },
                event_type: "successful",
                created_date: {
                  $gte: new Date(subtractedTime).toISOString(),
                },
              };

        return match;
      };

      const topBuyers = await db
        .collection("rarible_events")
        .aggregate([
          {
            $match: time_match(time),
          },
          {
            $project: {
              user_address: "$winner_account.address",
              user_name: "$winner_account.user.username",
              user_img_url: "$winner_account.profile_img_url",
              created_date: {
                $toDate: "$created_date",
              },
              slug: 1,
              token_id: 1,
              total_price: {
                $divide: [
                  {
                    $convert: {
                      input: "$total_price",
                      to: 1,
                    },
                  },
                  1000000000000000000,
                ],
              },
            },
          },
          {
            $group: {
              _id: "$user_address",
              user_details: {
                $addToSet: {
                  user_name: "$user_name",
                  user_img_url: "$user_img_url",
                  user_address: "$user_address",
                },
              },
              tokens: {
                $addToSet: {
                  token_id: "$token_id",
                  slug: "$slug",
                },
              },
              slugs: {
                $addToSet: {
                  slug: "$slug",
                },
              },
              total_price: {
                $addToSet: {
                  created_date: "$created_date",
                  total_price: "$total_price",
                },
              },
              buy_volume: {
                $sum: "$total_price",
              },
            },
          },
          {
            $unwind: {
              path: "$user_details",
            },
          },
          {
            $project: {
              user_name: "$user_details.user_name",
              user_address: "$user_details.user_address",
              user_img_url: "$user_details.user_img_url",
              buy_volume: 1,
              collection_count: {
                $size: "$slugs",
              },
              purchase_count: {
                $size: "$total_price",
              },
            },
          },
          {
            $sort: sortType,
          },
          {
            $skip: (page - 1) * pageSize,
          },
          {
            $limit: pageSize,
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: topBuyers,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: topBuyers,
      });
    } catch (err) {
      res.status(500).send({ error: err.message });
    }
  };

  public GetTopSellers = async (req: Request, res: Response) => {
    try {
      const time = req.query.time || "all";
      let sortType: any = req.query.sortBy || "sell_volume";

      let pageSize = 10;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      //SORT TYPE
      if (sortType == "buy_volume") {
        sortType = {
          sell_volume: -1,
        };
      }
      if (sortType == "sold_count") {
        sortType = {
          sold_count: -1,
        };
      }
      if (sortType == "collection_count") {
        sortType = {
          collection_count: -1,
        };
      }

      let time_match = (time) => {
        let subtractedTime;
        let today = new Date();

        if (time == "1h") {
          subtractedTime = today.setHours(today.getHours() - 1);
        }
        if (time == "6h") {
          subtractedTime = today.setHours(today.getHours() - 6);
        }
        if (time == "12h") {
          subtractedTime = today.setHours(today.getHours() - 12);
        }
        if (time == "24h") {
          subtractedTime = today.setDate(today.getDate() - 1);
        }

        if (time == "7d") {
          subtractedTime = today.setDate(today.getDate() - 7);
        }

        if (time == "30d") {
          subtractedTime = today.setDate(today.getDate() - 30);
        }

        let match =
          time === "all"
            ? {
                event_type: "successful",
              }
            : {
                event_type: "successful",
                created_date: {
                  $gte: new Date(subtractedTime).toISOString(),
                },
              };

        return match;
      };

      let pipeline = [
        {
          $match: time_match(time),
        },
        {
          $project: {
            user_address: "$seller.address",
            user_name: "$seller.user.username",
            user_img_url: "$seller.profile_img_url",
            created_date: {
              $toDate: "$created_date",
            },
            slug: 1,
            token_id: 1,
            total_price: {
              $divide: [
                {
                  $convert: {
                    input: "$total_price",
                    to: 1,
                  },
                },
                1000000000000000000,
              ],
            },
          },
        },
        {
          $group: {
            _id: "$user_address",
            user_details: {
              $addToSet: {
                user_name: "$user_name",
                user_img_url: "$user_img_url",
                user_address: "$user_address",
              },
            },
            tokens: {
              $addToSet: {
                token_id: "$token_id",
                slug: "$slug",
              },
            },
            slugs: {
              $addToSet: {
                slug: "$slug",
              },
            },
            total_price: {
              $addToSet: {
                created_date: "$created_date",
                total_price: "$total_price",
              },
            },
            sell_volume: {
              $sum: "$total_price",
            },
            created_date: {
              $addToSet: "$created_date",
            },
          },
        },
        {
          $unwind: {
            path: "$user_details",
          },
        },
        {
          $project: {
            user_name: "$user_details.user_name",
            user_address: "$user_details.user_address",
            user_img_url: "$user_details.user_img_url",
            sell_volume: 1,
            collection_count: {
              $size: "$slugs",
            },
            sold_count: {
              $size: "$total_price",
            },
            created_date: 1,
          },
        },
        {
          $sort: sortType,
        },
        {
          $skip: (page - 1) * pageSize,
        },
        {
          $limit: pageSize,
        },
      ];

      const topSellers = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: topSellers,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: topSellers,
      });
    } catch (err) {
      res.status(500).send({ error: err.message });
    }
  };
}
