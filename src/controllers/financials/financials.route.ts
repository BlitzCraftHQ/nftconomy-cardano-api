import { Router } from "express";
import Controller from "./financials.controller";

const financials: Router = Router();
const controller = new Controller();

financials.get("/volume/:name", controller.GetVolume);
financials.get("/price/:name", controller.GetPrice);
financials.get("/sales-liquidity/:name", controller.GetSalesAndLiquidity);
financials.get("/floor-price/:name", controller.GetFloorPrice);
financials.get("/market-cap/:name", controller.GetMarketCap);
financials.get("/transfers/:name", controller.GetTransfers);
financials.get("/no-of-listings/:name", controller.GetNoOfListings);
financials.get("/top-sales/:name", controller.GetTopSales);
financials.get(
  "/platform-sales-volume/:name",
  controller.GetPlatformVolumeAndSales
);
financials.get(
  "/currency-sales-volume/:name",
  controller.GetCurrencyVolumeAndSales
);

financials.get("/volume-similar/:name", controller.GetVolumeSimilar);
financials.get("/market-cap-similar/:name", controller.GetMarketCapSimilar);
financials.get("/sales-similar/:name", controller.GetSalesSimilar);
financials.get("/liquidity-similar/:name", controller.GetLiquiditySimilar);
financials.get("/floor-price-similar/:name", controller.GetFloorPriceSimilar);
financials.get("/avg-price-similar/:name", controller.GetAvgPriceSimilar);
financials.get("/min-price-similar/:name", controller.GetMinPriceSimilar);
financials.get("/max-price-similar/:name", controller.GetMaxPriceSimilar);

financials.get("/transfers/:name", controller.GetTransfers);
financials.get("/no-of-listings/:name", controller.GetNoOfListings);
financials.get("/top-sales/:name", controller.GetTopSales);
financials.get(
  "/platform-sales-volume/:name",
  controller.GetPlatformVolumeAndSales
);
financials.get(
  "/currency-sales-volume/:name",
  controller.GetCurrencyVolumeAndSales
);

// TODO: Add pipeline
financials.get("/sales-currency/:name", controller.GetSalesCurrency);

export default financials;
