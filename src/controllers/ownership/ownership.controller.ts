import { Request, Response } from "express";
import { db } from "../../utilities/mongo";
import { setCache, uniqueKey } from "../../utilities/redis";
import {
  getDateFormat,
  fixMissingDateRange,
  getSubtractedtime,
} from "../../helpers/formatter";

import { structure } from "../../helpers/stats";
import * as dayjs from "dayjs";
import axios from "axios";

export default class OwnershipController {
  public GetNFTsHoldingPeriod = async (req: Request, res: Response) => {
    let { slug } = req.params;
    try {
      let oneday = await getSubtractedtime(
        "24h",
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );
      let oneweek = await getSubtractedtime(
        "7d",
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );
      let onemonth = await getSubtractedtime(
        "30d",
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );
      let threemonth = await getSubtractedtime(
        "3m",
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );
      let oneyear = await getSubtractedtime(
        "1y",
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );
      let pipeline = [
        {
          $match: {
            slug: slug,
          },
        },
        {
          $project: {
            _id: 1,
            timestamp: {
              $toDate: "$block_timestamp",
            },
            from_address: 1,
            to_address: 1,
          },
        },
        {
          $facet: {
            one_day: [
              {
                $match: {
                  timestamp: {
                    $gte: new Date(oneday.toISOString()),
                  },
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            seven_day: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(oneweek.toISOString()) } },
                    { timestamp: { $lte: new Date(oneday.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            thirty_day: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(onemonth.toISOString()) } },
                    { timestamp: { $lte: new Date(oneweek.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            three_months: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(threemonth.toISOString()) } },
                    { timestamp: { $lte: new Date(onemonth.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            one_year: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(oneyear.toISOString()) } },
                    { timestamp: { $lte: new Date(threemonth.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            all_time: [
              {
                $match: {
                  timestamp: {
                    $lte: new Date(oneyear.toISOString()),
                  },
                },
              },
              {
                $project: {
                  _id: 1,
                  timestamp: 1,
                  from_address: 1,
                  to_address: 1,
                },
              },
              {
                $group: {
                  _id: null,
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  count: {
                    $size: "$wallet",
                  },
                },
              },
            ],
            graph: [
              {
                $project: {
                  _id: 1,
                  timestamp: 1,
                  from_address: 1,
                  to_address: 1,
                },
              },
              {
                $group: {
                  _id: {
                    year: {
                      $year: "$timestamp",
                    },
                    month: {
                      $month: "$timestamp",
                    },
                    day: {
                      $dayOfMonth: "$timestamp",
                    },
                  },
                  wallet: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(),
                  count: {
                    $size: "$wallet",
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
            graph: 1,
            one_day: {
              $first: "$one_day.count",
            },
            seven_day: {
              $first: "$seven_day.count",
            },
            thirty_day: {
              $first: "$thirty_day.count",
            },
            three_months: {
              $first: "$three_months.count",
            },
            one_year: {
              $first: "$one_year.count",
            },
            all_time: {
              $first: "$all_time.count",
            },
          },
        },
      ];
      const data = await db
        .collection("transfers")
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
        data: data,
      });
    } catch (e) {
      res.status(500).send({
        success: false,
        error: e,
      });
    }
  };

  public GetNFTsOwnedByDiamondHands = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }
      let pipeline = [
        {
          $match: {
            slug: slug,
            event_type: "successful",
          },
        },
        {
          $project: {
            slug: 1,
            token_id: 1,
            from_address: "$seller.address",
            to_address: "$winner_account.address",
          },
        },
        {
          $group: {
            _id: null,
            sellers: {
              $addToSet: "$from_address",
            },
            buyers: {
              $addToSet: "$to_address",
            },
          },
        },
        {
          $project: {
            diamond_hands: {
              $setDifference: ["$buyers", "$sellers"],
            },
          },
        },
        {
          $unwind: {
            path: "$diamond_hands",
          },
        },
        {
          $lookup: {
            from: "rarible_events",
            localField: "diamond_hands",
            foreignField: "winner_account.address",
            pipeline: [
              {
                $match: {
                  event_type: "successful",
                  slug: slug,
                },
              },
            ],
            as: "result",
          },
        },
        {
          $project: {
            diamond_hands: 1,
            result: 1,
            count: {
              $size: "$result",
            },
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $replaceRoot: {
            newRoot: "$result",
          },
        },
        {
          $sort: {
            token_id: 1,
          },
        },
        {
          $project: {
            token_id: 1,
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
                  slug: slug,
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
          $replaceRoot: {
            newRoot: "$results",
          },
        },
        {
          $project: {
            slug: 1,
            token_id: 1,
            name: 1,
            description: 1,
            image_url: 1,
            rarity_type: 1,
            rarity_rank: 1,
          },
        },
        {
          $facet: {
            tokens: [
              {
                $skip: (page - 1) * pageSize,
              },
              {
                $limit: pageSize,
              },
            ],
            count: [
              {
                $count: "count",
              },
            ],
          },
        },
        {
          $project: {
            tokens: 1,
            pagination: {
              total_pages: {
                $ceil: {
                  $divide: [
                    {
                      $first: "$count.count",
                    },
                    20,
                  ],
                },
              },
              current_page: String(page),
            },
          },
        },
      ];

      const result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      let data = result[0];

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

  public GetDiamondHands = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;
      let { time } = req.query;
      let subtractedTimeGreater;
      if (time) {
        subtractedTimeGreater = await getSubtractedtime(
          time,
          ["rarible_events"],
          ["created_date"],
          { slug: slug }
        );
      }
      let oneday = await getSubtractedtime(
        "24h",
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );
      let oneweek = await getSubtractedtime(
        "7d",
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );
      let onemonth = await getSubtractedtime(
        "30d",
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );
      let threemonth = await getSubtractedtime(
        "3m",
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );
      let oneyear = await getSubtractedtime(
        "1y",
        ["rarible_events"],
        ["created_date"],
        { slug: slug }
      );

      let pipeline = [
        {
          $match: {
            slug: slug,
            event_type: "successful",
          },
        },
        {
          $project: {
            slug: 1,
            token_id: 1,
            from_address: "$seller.address",
            to_address: "$winner_account.address",
            timestamp: {
              $toDate: "$created_date",
            },
          },
        },
        {
          $facet: {
            one_day: [
              {
                $match: {
                  timestamp: {
                    $gte: new Date(oneday.toISOString()),
                  },
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            seven_day: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(oneweek.toISOString()) } },
                    { timestamp: { $lte: new Date(oneday.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            thirty_day: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(onemonth.toISOString()) } },
                    { timestamp: { $lte: new Date(oneweek.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            three_months: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(threemonth.toISOString()) } },
                    { timestamp: { $lte: new Date(onemonth.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            one_year: [
              {
                $match: {
                  $and: [
                    { timestamp: { $gte: new Date(oneyear.toISOString()) } },
                    { timestamp: { $lte: new Date(threemonth.toISOString()) } },
                  ],
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            all_time: [
              {
                $match: {
                  timestamp: {
                    $lte: new Date(oneyear.toISOString()),
                  },
                },
              },
              {
                $project: {
                  _id: 1,
                  timestamp: 1,
                  from_address: 1,
                  to_address: 1,
                },
              },
              {
                $group: {
                  _id: null,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
                  },
                },
              },
            ],
            graph: [
              {
                $project: {
                  slug: 1,
                  token_id: 1,
                  from_address: 1,
                  to_address: 1,
                  created_date: "$timestamp",
                },
              },
              {
                $match: {
                  ...(time
                    ? {
                        created_date: {
                          $gte: new Date(subtractedTimeGreater.toISOString()),
                        },
                      }
                    : {}),
                },
              },
              {
                $group: {
                  _id: structure(time, slug).idFormat,
                  sellers: {
                    $addToSet: "$from_address",
                  },
                  buyers: {
                    $addToSet: "$to_address",
                  },
                },
              },
              {
                $project: {
                  _id: getDateFormat(time),
                  seller: {
                    $size: "$sellers",
                  },
                  buyers: {
                    $size: "$buyers",
                  },
                  diamond_hands: {
                    $setDifference: ["$buyers", "$sellers"],
                  },
                },
              },
              {
                $project: {
                  _id: 1,
                  buyers: 1,
                  diamond_hands: {
                    $size: "$diamond_hands",
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
            graph: 1,
            one_day: {
              $first: "$one_day.diamond_hands",
            },
            seven_day: {
              $first: "$seven_day.diamond_hands",
            },
            thirty_day: {
              $first: "$thirty_day.diamond_hands",
            },
            three_months: {
              $first: "$three_months.diamond_hands",
            },
            one_year: {
              $first: "$one_year.diamond_hands",
            },
            all_time: {
              $first: "$all_time.diamond_hands",
            },
          },
        },
      ];

      const data = await db
        .collection("rarible_events")
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

  public GetTopBalances = async (req: Request, res: Response) => {
    try {
      let { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }

      let pipeline = [
        {
          $match: {
            slug: slug,
            event_type: "successful",
          },
        },
        {
          $group: {
            _id: {
              token_id: "$token_id",
            },
            max_created_date: {
              $max: {
                $toDate: "$$ROOT.created_date",
              },
            },
            items: {
              $push: "$$CURRENT",
            },
          },
        },
        {
          $project: {
            _id: 1,
            recent_created_date: "$max_created_date",
            recent_ending_price: {
              $map: {
                input: {
                  $filter: {
                    input: "$items",
                    as: "i",
                    cond: {
                      $eq: [
                        {
                          $toDate: "$$i.created_date",
                        },
                        "$max_created_date",
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
            path: "$recent_ending_price",
          },
        },
        {
          $replaceRoot: {
            newRoot: "$recent_ending_price",
          },
        },
        {
          $group: {
            _id: "$winner_account.address",
            purchased_amount: {
              $sum: {
                $divide: [
                  {
                    $toDouble: "$total_price",
                  },
                  1000000000000000000,
                ],
              },
            },
            count: {
              $sum: 1,
            },
          },
        },
        {
          $sort: {
            purchased_amount: -1,
          },
        },
        {
          $limit: 2000,
        },
        {
          $facet: {
            users: [
              {
                $skip: (page - 1) * pageSize,
              },
              {
                $limit: pageSize,
              },
            ],
            count: [
              {
                $count: "count",
              },
            ],
          },
        },
        {
          $project: {
            users: 1,
            pagination: {
              total_pages: {
                $ceil: {
                  $divide: [
                    {
                      $first: "$count.count",
                    },
                    20,
                  ],
                },
              },
              current_page: String(page),
            },
          },
        },
      ];

      const result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: result[0],
        }),
        12 * 1440
      );

      res.status(200).send({
        success: true,
        data: result[0],
      });
    } catch (e) {
      res.status(500).send({
        success: false,
        error: e,
      });
    }
  };

  public GetTraders = async (req: Request, res: Response) => {
    let { slug } = req.params;
    let { time } = req.query;

    let subtractedTime: dayjs.Dayjs;
    if (time)
      subtractedTime = await getSubtractedtime(
        time,
        ["transfers"],
        ["block_timestamp"],
        { slug: slug }
      );

    let groupFormat: any = {
      year: {
        $year: "$time",
      },
      month: {
        $month: "$time",
      },
      day: {
        $dayOfMonth: "$time",
      },
    };

    if (time == "7d") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $multiply: [
            {
              $floor: {
                $divide: [
                  {
                    $hour: "$time",
                  },
                  2,
                ],
              },
            },
            2,
          ],
        },
      };
    } else if (time == "24h") {
      groupFormat = {
        ...groupFormat,
        hour: {
          $hour: "$time",
        },
      };
    }

    try {
      let buyersAndSellers = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug,
            },
          },
          {
            $project: {
              time: {
                $toDate: "$block_timestamp",
              },
              token_id: {
                $concat: ["$slug", "_", "$token_id"],
              },
              seller: "$from_address",
              buyer: "$to_address",
              value: 1,
            },
          },
          {
            $group: {
              _id: groupFormat,
              sellers: {
                $accumulator: {
                  init: "function() { return { sellers: []}}",
                  accumulate: `function(state, seller, value, time) { \n\
                     if (!state.sellers.includes(seller) && seller !== "0x0000000000000000000000000000000000000000" && value !== "0" ${
                       !time
                         ? ""
                         : `&& time >= new Date('${
                             structure(time, slug).matchFormat.created_date.$gte
                           }')`
                     }) { \n\
                        state.sellers.push(seller);\n\ 
                      }\n\
                      return state;  \n\
                    }`,
                  accumulateArgs: ["$seller", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { sellers: state1.sellers.concat(state2.sellers), }; }",
                  finalize: "function(state) { return state.sellers.length; }",
                  lang: "js",
                },
              },
              buyers: {
                $accumulator: {
                  init: "function() { return { buyers: []} }",
                  accumulate: `function(state, buyer, value,time) { \n\
                        if (!state.buyers.includes(buyer) && value !== "0" ${
                          !time
                            ? ""
                            : `&& time >= new Date('${
                                structure(time, slug).matchFormat.created_date
                                  .$gte
                              }')`
                        }) { \n\
                          state.buyers.push(buyer); \n\
                        }\n\
                        return state;  \n\
                      }`,
                  accumulateArgs: ["$buyer", "$value", "$time"],
                  merge:
                    "function(state1, state2) { return { buyers: state1.buyers.concat(state2.buyers),};}",
                  finalize: "function(state) { return state.buyers.length; }",
                  lang: "js",
                },
              },
              traders: {
                $accumulator: {
                  init: "function() { return { traders:[]} }",
                  accumulate: `function(state, seller, buyer, value, time){ \n\
                     if(value !== "0" ${
                       !time
                         ? ""
                         : `&& time >= new Date('${
                             structure(time, slug).matchFormat.created_date.$gte
                           }')`
                     }) {\n\
                        if(!state.traders.includes(seller)) \n\
                          state.traders.push(seller);\n\
                        if(!state.traders.includes(buyer)) \n\
                          state.traders.push(buyer);\n\
                      } \n\
                      return state;\n\
                    }`,
                  accumulateArgs: ["$seller", "$buyer", "$value", "$time"],
                  merge:
                    "function(state1,state2) {return {traders: state1.traders.concat(state2.traders),};}",
                  finalize: "function(state) { return state.traders.length; }",
                  lang: "js",
                },
              },
              token_transactions: {
                $accumulator: {
                  init: "function() { return { token_trans: {}}}",
                  accumulate:
                    "function(state, token_id, buyer, time) { if (!state.token_trans.hasOwnProperty(token_id) || state.token_trans[token_id].time.getTime() < time.getTime() ) {state.token_trans[token_id] = {buyer,time}; } return state; }",
                  accumulateArgs: ["$token_id", "$buyer", "$time"],
                  merge:
                    "function(state1, state2) { Object.assign(state1.token_trans, state2.token_trans); return state1; }",
                  finalize: "function(state) { return state.token_trans; }",
                  lang: "js",
                },
              },
            },
          },
          {
            $sort: {
              _id: 1,
            },
          },
          {
            $group: {
              _id: null,
              records: {
                $accumulator: {
                  init: "function() { return { records: [], tokens: {}}; }",
                  accumulate:
                    "function(state,token_transactions, _id, buyers, sellers, traders) { Object.assign(state.tokens, token_transactions);\
                     let set = new Set(); Object.keys(state.tokens).forEach(k => {set.add(state.tokens[k].buyer); });\
                     let res = { _id, buyers, sellers, traders, holders: set.size}; state.records.push(res);\
                     return {records: state.records,tokens: state.tokens};}",
                  accumulateArgs: [
                    "$token_transactions",
                    "$_id",
                    "$buyers",
                    "$sellers",
                    "$traders",
                  ],
                  merge:
                    "function(state1, state2) { return state1.records.concat(state2.records); }",
                  finalize: "function(state) { return state.records; }",
                  lang: "js",
                },
              },
            },
          },

          {
            $unwind: {
              path: "$records",
            },
          },
          {
            $project: {
              buyers: "$records.buyers",
              sellers: "$records.sellers",
              holders: "$records.holders",
              traders: "$records.traders",
              _id: {
                $dateFromParts: {
                  year: "$records._id.year",
                  month: "$records._id.month",
                  day: "$records._id.day",
                  ...(time == "24h" || time == "7d"
                    ? { hour: "$records._id.hour" }
                    : {}),
                },
              },
            },
          },
          ...(time
            ? [{ $match: { _id: { $gte: subtractedTime.toDate() } } }]
            : []),
        ])
        .toArray();

      // Data Formatting
      let data = [];
      var startFrom = !time
        ? buyersAndSellers.length
          ? dayjs(buyersAndSellers[0]._id)
          : dayjs()
        : subtractedTime;
      var prevHolders = buyersAndSellers.length
        ? buyersAndSellers[0].holders
        : 0;

      buyersAndSellers.forEach((day) => {
        const date = dayjs(day._id);

        const value = {
          buyers: 0,
          sellers: 0,
          holders: prevHolders,
        };

        // Fix sparse date ranges.
        fixMissingDateRange(data, !time ? "1y" : time, startFrom, date, value);

        data.push(day);
        startFrom = date;
        prevHolders = day.holders;
      });

      // const value = {
      //   buyers: 0,
      //   sellers: 0,
      //   holders: prevHolders,
      // }
      // fixMissingDateRange(data, !time ? "1y" : time, startFrom, dayjs(), value);

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: data,
        }),
        2 * 1440
      );

      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (error) {
      res.status(500).send(error);
    }
  };

  public GetOtherCollectionsOwnedByOwners = async (
    req: Request,
    res: Response
  ) => {
    try {
      const { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      const result = await db
        .collection("transfers")
        .aggregate([
          {
            $match: {
              slug,
            },
          },
          {
            $project: {
              slug: 1,
              to_address: 1,
            },
          },
          {
            $lookup: {
              from: "transfers",
              localField: "to_address",
              foreignField: "to_address",
              as: "result",
              pipeline: [
                {
                  $project: {
                    to_address: 1,
                    slug: 1,
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$result",
            },
          },
          {
            $group: {
              _id: "$to_address",
              collections: {
                $addToSet: "$result.slug",
              },
            },
          },
          {
            $unwind: {
              path: "$collections",
            },
          },
          {
            $group: {
              _id: "$collections",
              owners: {
                $addToSet: "$_id",
              },
            },
          },
          {
            $lookup: {
              from: "collections",
              localField: "_id",
              foreignField: "slug",
              as: "result",
              pipeline: [
                {
                  $project: {
                    image_url: 1,
                    name: 1,
                    slug: 1,
                  },
                },
              ],
            },
          },
          {
            $unwind: {
              path: "$result",
            },
          },
          {
            $unwind: {
              path: "$owners",
            },
          },
          {
            $group: {
              _id: "$owners",
              collections: {
                $addToSet: {
                  slug: "$collections",
                  name: "$result.name",
                  image_url: "$result.image_url",
                },
              },
            },
          },
          {
            $project: {
              address: "$_id",
              collections: 1,
              no_of_collections: {
                $size: "$collections",
              },
            },
          },
          {
            $facet: {
              data: [
                {
                  $sort: {
                    no_of_collections: -1,
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
        totalPages: Math.ceil(result[0].totalCount[0]?.count || 0 / pageSize),
      };

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          paginatedData,
          list: result[0].data,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        paginatedData,
        list: result[0].data,
      });
    } catch (error) {
      res.status(500).send;
    }
  };

  public GetTransactionsOfTopOwners = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      const pipeline = [
        {
          $match: {
            slug,
          },
        },
        {
          $group: {
            _id: "$to_address",
            count: {
              $sum: {
                $divide: [
                  {
                    $toDouble: "$value",
                  },
                  1000000000000000000,
                ],
              },
            },
          },
        },
        {
          $sort: {
            count: -1,
          },
        },
        {
          $limit: 100,
        },
        {
          $lookup: {
            from: "rarible_events",
            localField: "_id",
            foreignField: "winner_account.address",
            as: "buys",
            pipeline: [
              {
                $match: {
                  slug,
                  event_type: "successful",
                },
              },
              {
                $project: {
                  type: "buy",
                  _id: 0,
                  slug: 1,
                  event_type: 1,
                  marketplace_id: 1,
                  seller: "$seller.address",
                  buyer: "$winner_account.address",
                  transaction: 1,
                  total_price: 1,
                  starting_price: 1,
                  ending_price: 1,
                  rarible_price: 1,
                  token_id: 1,
                  created_date: 1,
                },
              },
              {
                $sort: {
                  created_date: -1,
                },
              },
            ],
          },
        },
        {
          $lookup: {
            from: "rarible_events",
            localField: "_id",
            foreignField: "seller.address",
            as: "sells",
            pipeline: [
              {
                $match: {
                  slug,
                  event_type: "successful",
                },
              },
              {
                $project: {
                  type: "sell",
                  _id: 0,
                  slug: 1,
                  event_type: 1,
                  marketplace_id: 1,
                  seller: "$seller.address",
                  buyer: "$winner_account.address",
                  transaction: 1,
                  total_price: 1,
                  starting_price: 1,
                  ending_price: 1,
                  rarible_price: 1,
                  token_id: 1,
                  created_date: 1,
                },
              },
              {
                $sort: {
                  created_date: -1,
                },
              },
            ],
          },
        },
        {
          $project: {
            volume: "$count",
            transaction: {
              $concatArrays: ["$buys", "$sells"],
            },
          },
        },
        {
          $unwind: {
            path: "$transaction",
          },
        },
        {
          $facet: {
            data: [
              {
                $sort: {
                  volume: -1,
                  "transaction.created_date": -1,
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
                    token_id: "$transaction.token_id",
                    slug,
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
                  _id: 1,
                  volume_by_wallet: "$volume",
                  slug: "$transaction.slug",
                  type: "$transaction.type",
                  seller: "$transaction.seller",
                  buyer: "$transaction.buyer",
                  amount_in_usd: {
                    $toDouble: "$transaction.rarible_price.usd_price",
                  },
                  amount_in_eth: "$transaction.total_price",
                  created_date: "$transaction.created_date",
                  token_id: "$transaction.token_id",
                  transaction_hash: "$transaction.transaction.transaction_hash",
                  marketplace_id: "$transaction.marketplace_id",
                  token_name: "$token.name",
                  token_img_url: "$token.image_url",
                },
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
      ];

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
          paginatedData,
          list: result[0].data,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        paginatedData,
        list: result[0].data,
      });
    } catch (error) {
      console.error(error);
      res.status(500).send(error);
    }
  };

  public GetWalletsWithOneNFT = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      const pipeline = [
        {
          $match: {
            slug,
          },
        },
        {
          $group: {
            _id: "$token_id",
            res: {
              $addToSet: {
                to_address: "$winner_account.address",
                token_id: "$token_id",
                slug: "$slug",
                block_timestamp: "$created_date",
                value: "$rarible_price.usd_price",
              },
            },
          },
        },
        {
          $unwind: {
            path: "$res",
          },
        },
        {
          $sort: {
            "res.block_timestamp": -1,
          },
        },
        {
          $group: {
            _id: "$_id",
            owner: {
              $last: "$res",
            },
          },
        },
        {
          $group: {
            _id: "$owner.to_address",
            count: {
              $sum: 1,
            },
            events: {
              $addToSet: "$owner",
            },
          },
        },
        {
          $match: {
            count: 1,
          },
        },
        {
          $unwind: {
            path: "$events",
          },
        },
        {
          $project: {
            _id: 0,
            wallet: "$_id",
            slug: "$events.slug",
            token_id: "$events.token_id",
            block_timestamp: "$events.block_timestamp",
            value: "$events.value",
          },
        },
        {
          $lookup: {
            from: "tokens",
            localField: "token_id",
            foreignField: "token_id",
            as: "result",
            pipeline: [
              {
                $match: {
                  slug,
                },
              },
            ],
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $project: {
            wallet: 1,
            token_id: 1,
            slug: 1,
            last_transaction: "$block_timestamp",
            value: 1,
            token_name: "$result.name",
            token_image_url: "$result.image_url",
          },
        },
        {
          $facet: {
            data: [
              {
                $sort: {
                  count: -1,
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
                $group: {
                  _id: null,
                  count: { $sum: 1 },
                },
              },
            ],
          },
        },
      ];

      const result = await db
        .collection("rarible_events")
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
          paginatedData,
          list: result[0].data,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        paginatedData,
        list: result[0].data,
      });
    } catch (e) {
      res.status(500).send(e);
    }
  };

  public GetCollectionsOwnedByFrequency = async (
    req: Request,
    res: Response
  ) => {
    try {
      const { slug } = req.params;

      let collection_data = await db.collection("collections").findOne(
        {
          slug,
        },
        {
          projection: {
            _id: 0,
            slug: 1,
            address: 1,
          },
        }
      );

      let collection_contract = collection_data.address;
      let current_timestamp = Date.now();

      let owners = await axios
        .get(
          `https://eth-mainnet.g.alchemy.com/nft/v2/BBqSK30IXTgfKP0pkLS8YRjQesXXfhev/getOwnersForCollection?contractAddress=${collection_contract}&withTokenBalances=false&block=%20${current_timestamp}`,
          {
            headers: {
              "Content-Type": "application/json",
            },
          }
        )
        .then((res) =>
          res.data.ownerAddresses.filter(
            (address: string) =>
              address !== "0x000000000000000000000000000000000000dead" &&
              address !== "0x0000000000000000000000000000000000000000"
          )
        )
        .catch((e) => {
          console.log(e);
          res.status(500).send(e);
        });

      await db
        .collection("crunching_collection")
        .drop()
        .catch((e) => {
          console.log(e);
        });
      await db.collection("crunching_collection").createIndex({ address: 1 });

      for (let i = 0; i < owners.length; i++) {
        process.stdout.write(` ${i} / ${owners.length}\r`);

        var pageKey: any = "";
        while (pageKey == "" || pageKey != "&pageKey=") {
          await axios
            .get(
              `https://eth-mainnet.g.alchemy.com/nft/v2/BBqSK30IXTgfKP0pkLS8YRjQesXXfhev/getContractsForOwner?&excludeFilters\[\]=SPAM&owner=${
                owners[i]
              }${pageKey ? pageKey : ""}`,
              {
                headers: {
                  "Content-Type": "application/json",
                },
              }
            )
            .then(async (res) => {
              let contracts = res.data.contracts.map((item) => ({
                address: item.address,
                name: item.name,
              }));

              await db
                .collection("crunching_collection")
                .insertMany(contracts, { ordered: false });

              pageKey = "&pageKey=" + (res.data.pageKey || "");
            })
            .catch((e) => {
              console.log(e);
            });
        }
      }

      let result = await db
        .collection("crunching_collection")
        .aggregate([
          {
            $group: {
              _id: {
                address: "$address",
                name: "$name",
              },
              count: {
                $sum: 1,
              },
            },
          },
          {
            $sort: {
              count: -1,
            },
          },
          {
            $limit: 100,
          },
        ])
        .toArray();

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          list: result,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        list: result,
      });
    } catch (e) {
      console.log(e);
      res.status(500).send(e);
    }
  };

  public GetBuysFromTop5Wallets = async (req: Request, res: Response) => {
    try {
      const { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      let subtractedTime = await getSubtractedtime(
        "7d",
        ["rarible_events"],
        ["created_date"],
        { slug }
      );

      const pipeline = [
        {
          $match: {
            slug,
            event_type: "successful",
            created_date: {
              $gte: subtractedTime.toISOString(),
            },
          },
        },
        {
          $group: {
            _id: "$winner_account.address",
            volume: {
              $sum: {
                $toDouble: "$rarible_price.usd_price",
              },
            },
          },
        },
        {
          $sort: {
            volume: -1,
          },
        },
        {
          $limit: 5,
        },
        {
          $lookup: {
            from: "rarible_events",
            localField: "_id",
            foreignField: "winner_account.address",
            as: "result",
            pipeline: [
              {
                $match: {
                  event_type: "successful",
                  created_date: {
                    $gte: subtractedTime.toISOString(),
                  },
                },
              },
              {
                $sort: {
                  created_date: -1,
                },
              },
            ],
          },
        },
        {
          $unwind: {
            path: "$result",
          },
        },
        {
          $project: {
            top_buyer_info: {
              wallet: "$_id",
              volume_traded_in_collection: "$volume",
            },
            marketplace_id: "$result.marketplace_id",
            buyer: "$result.winner_account.address",
            seller: "$result.seller.address",
            created_date: "$result.created_date",
            amount_in_usd: {
              $toDouble: "$result.rarible_price.usd_price",
            },
            transaction_hash: "$result.transaction.transaction_hash",
            token_id: "$result.token_id",
            slug: "$result.slug",
          },
        },
        {
          $lookup: {
            from: "tokens",
            let: {
              firstUser: "$slug",
              secondUser: "$token_id",
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      {
                        $eq: ["$slug", "$$firstUser"],
                      },
                      {
                        $eq: ["$token_id", "$$secondUser"],
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
          $replaceRoot: {
            newRoot: {
              $mergeObjects: [
                {
                  $arrayElemAt: ["$result", 0],
                },
                {
                  image_url: "$$ROOT.image_url",
                  top_buyer_info: "$$ROOT.top_buyer_info",
                  marketplace_id: "$$ROOT.marketplace_id",
                  buyer: "$$ROOT.buyer",
                  seller: "$$ROOT.seller",
                  created_date: "$$ROOT.created_date",
                  amount_in_usd: "$$ROOT.amount_in_usd",
                  transaction_hash: "$$ROOT.transaction_hash",
                },
              ],
            },
          },
        },
        {
          $project: {
            _id: 0,
            description: 0,
            token_uri: 0,
            traits: 0,
          },
        },
        {
          $facet: {
            data: [
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
      ];

      const result = await db
        .collection("rarible_events")
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
          paginatedData,
          list: result[0].data,
        }),
        15 * 1440
      );

      res.status(200).send({
        success: true,
        paginatedData,
        list: result[0].data,
      });
    } catch (e) {
      res.status(500).send(e);
    }
  };

  public GetTopProfitWallets = async (req: Request, res: Response) => {
    try {
      // TODO: Add the pipeline
      let { slug } = req.params;
      let pageSize = 20;
      let pageString = req.query.page;
      let page = Number(pageString) || 1;

      if (!page || page <= 0) {
        page = 1;
      }
      let pipeline = [
        {
          $match: {
            slug: slug,
            event_type: "successful",
          },
        },
        {
          $project: {
            slug: 1,
            token_id: 1,
            from_address: "$seller.address",
            to_address: "$winner_account.address",
            price: {
              $divide: [
                {
                  $toDouble: "$total_price",
                },
                1000000000000000000,
              ],
            },
          },
        },
        {
          $group: {
            _id: null,
            sellers: {
              $push: {
                wallet: "$from_address",
                price: {
                  $multiply: ["$price", 1],
                },
              },
            },
            buyers: {
              $addToSet: {
                wallet: "$to_address",
                price: {
                  $multiply: ["$price", -1],
                },
              },
            },
          },
        },
        {
          $project: {
            _id: 0,
            users: {
              $concatArrays: ["$buyers", "$sellers"],
            },
          },
        },
        {
          $unwind: {
            path: "$users",
          },
        },
        {
          $group: {
            _id: "$users.wallet",
            profit: {
              $sum: "$users.price",
            },
            nftsTransaction: {
              $sum: 1,
            },
          },
        },
        {
          $sort: {
            profit: -1,
          },
        },
        {
          $facet: {
            users: [
              {
                $skip: (page - 1) * pageSize,
              },
              {
                $limit: pageSize,
              },
            ],
            count: [
              {
                $count: "count",
              },
            ],
          },
        },
        {
          $project: {
            users: 1,
            pagination: {
              total_pages: {
                $ceil: {
                  $divide: [
                    {
                      $first: "$count.count",
                    },
                    20,
                  ],
                },
              },
              current_page: String(page),
            },
          },
        },
      ];

      const result = await db
        .collection("rarible_events")
        .aggregate(pipeline)
        .toArray();

      let data = result[0];

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
}
