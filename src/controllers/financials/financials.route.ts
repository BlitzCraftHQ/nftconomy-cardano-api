import { Router } from "express";
import Controller from "./financials.controller";

const financials: Router = Router();
const controller = new Controller();

financials.get("/volume/:slug", controller.GetVolume);
financials.get("/price/:slug", controller.GetPrice);
financials.get("/sales-liquidity/:slug", controller.GetSalesAndLiquidity);
financials.get("/floor-price/:slug", controller.GetFloorPrice);
financials.get("/market-cap/:slug", controller.GetMarketCap);
financials.get("/transfers/:slug", controller.GetTransfers);
financials.get("/no-of-listings/:slug", controller.GetNoOfListings);
financials.get("/top-sales/:slug", controller.GetTopSales);
financials.get(
  "/platform-sales-volume/:slug",
  controller.GetPlatformVolumeAndSales
);
financials.get(
  "/currency-sales-volume/:slug",
  controller.GetCurrencyVolumeAndSales
);

financials.get("/volume-similar/:slug", controller.GetVolumeSimilar);
financials.get("/market-cap-similar/:slug", controller.GetMarketCapSimilar);
financials.get("/sales-similar/:slug", controller.GetSalesSimilar);
financials.get("/liquidity-similar/:slug", controller.GetLiquiditySimilar);
financials.get("/floor-price-similar/:slug", controller.GetFloorPriceSimilar);
financials.get("/avg-price-similar/:slug", controller.GetAvgPriceSimilar);
financials.get("/min-price-similar/:slug", controller.GetMinPriceSimilar);
financials.get("/max-price-similar/:slug", controller.GetMaxPriceSimilar);

financials.get("/transfers/:slug", controller.GetTransfers);
financials.get("/no-of-listings/:slug", controller.GetNoOfListings);
financials.get("/top-sales/:slug", controller.GetTopSales);
financials.get(
  "/platform-sales-volume/:slug",
  controller.GetPlatformVolumeAndSales
);
financials.get(
  "/currency-sales-volume/:slug",
  controller.GetCurrencyVolumeAndSales
);

// TODO: Add pipeline
financials.get("/sales-currency/:slug", controller.GetSalesCurrency);

export default financials;
