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

financials.get("/transfers/:name", controller.GetTransfers);
financials.get("/no-of-listings/:name", controller.GetNoOfListings);
financials.get("/top-sales/:name", controller.GetTopSales);

export default financials;
