import { Router } from "express";
import Controller from "./community.controller";

const community: Router = Router();
const controller = new Controller();

community.get("/:slug/reddit-engagement", controller.GetRedditEngagement);
community.get("/:slug/total-subreddits", controller.GetTotalSubReddits);

community.get("/:slug/total-tweets", controller.GetTotalTweets);
community.get("/:slug/discord-presence", controller.GetDiscordPresence);
community.get("/:slug/twitter-engagement", controller.GetTwitterEngagement);
community.get("/:slug/trends-engagement", controller.GetTrendsEngagement);
community.get("/:slug/trends-region", controller.GetTrendsByCountry);
community.get(
  "/:slug/engagement-vs-liquidity",
  controller.GetEngagementVsLiquidity
);
community.get("/:slug/engagement-vs-volume", controller.GetEngagementVsVolume);
community.get("/:slug/engagement-vs-sales", controller.GetEngagementVsSales);
community.get(
  "/:slug/engagement-vs-floorprice",
  controller.GetEngagementVsFloorPrice
);
community.get(
  "/:slug/engagement-vs-market-cap",
  controller.GetEngagementVsMarketCap
);

community.get("/:slug/recent-tweets", controller.GetRecentTweets);
community.get("/:slug/overall-sentiment", controller.GetOverallSentiment);
community.get("/:slug/reddit-sentiment", controller.GetRedditSentiment);
community.get("/:slug/twitter-sentiment", controller.GetTwitterSentiment);
community.get("/:slug/overall-engagement", controller.GetOverallEngagement);
community.get(
  "/:slug/reddit-sentiment-similar",
  controller.GetRedditSentimentSimilar
);
community.get(
  "/:slug/twitter-sentiment-similar",
  controller.GetTwitterSentimentSimilar
);
community.get(
  "/:slug/twitter-engagement-similar",
  controller.GetTwitterEngagementSimilar
);
community.get(
  "/:slug/reddit-engagement-similar",
  controller.GetRedditEngagementSimilar
);
community.get(
  "/:slug/trends-engagement-similar",
  controller.GetTrendsEngagementSimilar
);
community.get(
  "/:slug/overall-sentiment-similar",
  controller.GetOverallSentimentSimilar
);
community.get("/:slug/discord-engagement", controller.GetDiscordEngagement);
community.get("/:slug/discord-sentiment", controller.GetDiscordSentiment);
export default community;
