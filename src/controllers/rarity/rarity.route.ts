import { Router } from "express";
import Controller from "./rarity.controller";

const rarity: Router = Router();
const controller = new Controller();

rarity.get("/:slug/distribution", controller.GetDistribution);
rarity.get("/:slug/volume", controller.GetRarityVsVolume);
rarity.get("/:slug/market-cap", controller.GetRarityVsMarketCap);
rarity.get("/:slug/floor-price", controller.GetRarityVsFloor);
rarity.get("/:slug/sales", controller.GetRarityVsSales);

export default rarity;
