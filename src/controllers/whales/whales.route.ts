import { Router } from "express";
import Controller from "./whales.controller";

const whales: Router = Router();
const controller = new Controller();

// TODO: Whales List - Implement No-recompute approach
whales.get("/list", controller.GetListing);
whales.get("/list-collection", controller.GetWhalesList);

whales.get("/:slug", controller.GetWhalesInCollection);

whales.get("/mint/most-minted", controller.GetMostMintedCollections);
whales.get("/mint/top-minters", controller.GetTopMinters);

whales.get("/activity/trend", controller.GetTrends);
whales.get("/activity/:type", controller.GetActivities);

whales.get("/trade/whales-involved", controller.GetMostWhalesInvolved);
whales.get("/trade/whales-bought", controller.GetWhalesBought);
whales.get("/trade/top-buyers", controller.GetTopBuyers);
whales.get("/trade/top-sellers", controller.GetTopSellers);

export default whales;
