import { Router } from "express";

// Import middlewares
// import jwtAuthCheck from "../helpers/auth0jwtverify";

// Import Routers
import authRouter from "./auth/auth.route";
import formsRouter from "./forms/forms.route";
import watchListsRouter from "./watchlists/watchlists.route";
import newsRouter from "./news/news.route";
import collectionsRouter from "./collections/collections.route";
import marketRouter from "./market/market.route";
import financialsRouter from "./financials/financials.route";
import tokenomicScoreRouter from "./tokenomic-score/tokenomic-score.route";
import mintsRouter from "./mints/mints.route";
import activitiesRouter from "./activities/activities.route";
import rarityRouter from "./rarity/rarity.route";
import tokensRouter from "./tokens/tokens.route";
import communityRouter from "./community/community.route";
import whalesRouter from "./whales/whales.route";
import searchRouter from "./search/search.route";
import leaderboardRouter from "./leaderboard/leaderboard.route";
import ownershipRouter from "./ownership/ownership.route";
import usersRouter from "./users/users.route";

const router: Router = Router();

// Authenticated routes
// router.use("/auth", jwtAuthCheck, authRouter);
router.use("/auth", authRouter);

// Unauthenticated routes
router.use("/watchlists", watchListsRouter);
router.use("/collections", collectionsRouter);
router.use("/financials", financialsRouter);
router.use("tokenomic-score", tokenomicScoreRouter);
router.use("/mints", mintsRouter);
router.use("/forms", formsRouter);
router.use("/news", newsRouter);
router.use("/market", marketRouter);
router.use("/activities", activitiesRouter);
router.use("/rarity", rarityRouter);
router.use("/tokens", tokensRouter);
router.use("/community", communityRouter);
router.use("/whales", whalesRouter);
router.use("/search", searchRouter);
router.use("/leaderboard", leaderboardRouter);
router.use("/ownership", ownershipRouter);
router.use("/users", usersRouter);

export default router;
