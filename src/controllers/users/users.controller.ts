import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";
import axios from "axios";

export default class UsersController {
  public GetUserProfile = async (req: Request, res: Response) => {
    try {
      const { userAddress } = req.query;

      // Get user profile - Data[Tokens, Slugs, TokensCount, SlugsCount], BuyVolume and SellVolume
      const userProfile = await db
        .collection("rarible_events")
        .aggregate([
          {
            $facet: {
              data: [
                {
                  $match: {
                    event_type: "successful",
                    "winner_account.address": userAddress,
                  },
                },
                {
                  $project: {
                    slug: 1,
                    token_id: 1,
                    created_date: {
                      $toDate: "$created_date",
                    },
                    user_address: "$winner_account.address",
                    user_name: "$winner_account.user.username",
                    user_img_url: "$winner_account.profile_img_url",
                  },
                },
                {
                  $lookup: {
                    from: "transfers",
                    let: {
                      slug: "$slug",
                      token_id: "$token_id",
                      user_address: "$user_address",
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
                              {
                                $ne: ["$value", "0"],
                              },
                            ],
                          },
                        },
                      },
                      {
                        $project: {
                          slug: 1,
                          token_id: 1,
                          created_date: {
                            $toDate: "$block_timestamp",
                          },
                          to_address: 1,
                          last_price: {
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
                        $sort: {
                          created_date: -1,
                        },
                      },
                      {
                        $limit: 1,
                      },
                      {
                        $match: {
                          $expr: {
                            $eq: ["$to_address", "$$user_address"],
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
                  $match: {
                    "result.to_address": userAddress,
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
                          name: 1,
                          image_url: 1,
                          slug: 1,
                          token_id: 1,
                        },
                      },
                    ],
                    as: "token_info",
                  },
                },
                {
                  $unwind: {
                    path: "$token_info",
                  },
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
                          slug: 1,
                          address: 1,
                        },
                      },
                    ],
                    as: "collection_info",
                  },
                },
                {
                  $unwind: {
                    path: "$collection_info",
                  },
                },
                {
                  $group: {
                    _id: "$owner",
                    tokens: {
                      $addToSet: {
                        token_id: "$token_id",
                        slug: "$slug",
                        token_name: "$token_info.name",
                        token_img_url: "$token_info.image_url",
                      },
                    },
                    slugs: {
                      $addToSet: {
                        slug: "$slug",
                        collection_name: "$collection_info.name",
                        collection_img_url: "$collection_info.image_url",
                        collection_address: "$collection_info.address",
                      },
                    },
                    holding_value: {
                      $sum: "$result.last_price",
                    },
                    user_details: {
                      $addToSet: {
                        user_name: "$user_name",
                        user_img_url: "$user_img_url",
                        user_address: "$user_address",
                      },
                    },
                  },
                },
                {
                  $unwind: {
                    path: "$user_details",
                  },
                },
                {
                  $unwind: {
                    path: "$holding_value",
                  },
                },
                {
                  $project: {
                    user_name: "$user_details.user_name",
                    user_img_url: "$user_details.user_img_url",
                    user_address: "$user_details.user_address",
                    tokens: 1,
                    slugs: 1,
                    tokens_count: {
                      $size: "$tokens",
                    },
                    slugs_count: {
                      $size: "$slugs",
                    },
                    holding_value: 1,
                  },
                },
              ],
              sellVolume: [
                {
                  $match: {
                    event_type: "successful",
                    "seller.address": userAddress,
                  },
                },
                {
                  $project: {
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
                    _id: null,
                    sellVolume: {
                      $sum: "$total_price",
                    },
                  },
                },
              ],
              buyVolume: [
                {
                  $match: {
                    event_type: "successful",
                    "winner_account.address": userAddress,
                  },
                },
                {
                  $project: {
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
                    _id: null,
                    buyVolume: {
                      $sum: "$total_price",
                    },
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$data",
            },
          },
          {
            $unwind: {
              path: "$sellVolume",
            },
          },
          {
            $unwind: {
              path: "$buyVolume",
            },
          },
          {
            $project: {
              user_name: "$data.user_name",
              user_img_url: "$data.user_img_url",
              user_address: "$data.user_address",
              tokens_count: "$data.tokens_count",
              slugs_count: "$data.slugs_count",
              slugs: "$data.slugs",
              tokens: "$data.tokens",
              holding_value: "$data.holding_value",
              buyVolume: "$buyVolume.buyVolume",
              sellVolume: "$sellVolume.sellVolume",
            },
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: userProfile,
        }),
        1440
      );

      res.status(200).send({
        success: true,
        data: userProfile,
      });
    } catch (error) {
      return res.status(500).json({
        message: error.message,
      });
    }
  };

  public GetUserCollections = async (req: Request, res: Response) => {
    try {
      const { user } = req.params;
      const { pageKey, pageSize } = req.query;

      console.log("pageKey", pageKey);

      const baseURL = `https://eth-mainnet.g.alchemy.com/v2/BBqSK30IXTgfKP0pkLS8YRjQesXXfhev`;
      const url = `${baseURL}/getNFTs/?owner=${user}&pageKey=${pageKey}&pageSize=${pageSize}`;

      const result = await axios.get(url).then((response) => {
        return response.data;
      });

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
      console.log("error", error);
      return res.status(500).json({
        message: error.message,
      });
    }
  };
}
