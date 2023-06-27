import { Router } from "express";
import Controller from "./market.controller";

const market: Router = Router();
const controller = new Controller();

market.get("/sentiment", controller.GetMarketSentiment);

market.get("/buyers-sellers", controller.GetBuyersAndSellers);
market.get("/holders-traders", controller.GetHoldersTraders);

market.get("/category-stats", controller.GetCategoryStats);
market.get("/price", controller.GetPrice);
market.get("/volume", controller.GetVolume);
market.get("/sales-liquidity", controller.GetSalesAndLiquidity);
market.get("/floor-price", controller.GetFloorPrice);
market.get("/transfers", controller.GetTransfers);
market.get("/market-cap", controller.GetMarketCap);

market.get("/top-collections", controller.GetTopCollectionsByMCorVolume);
market.get("/collection-distribution", controller.GetCollectionDistribution);

market.get("/top-sales", controller.GetTopSales);
market.get("/traders-count", controller.GetTradersCount);
market.get("/blue-chip-index", controller.GetBlueChipIndex);

market.get("/platform-sales-volume", controller.GetPlatformVolumeAndSales);
market.get("/currency-sales-volume", controller.GetCurrencyVolumeAndSales);
export default market;
