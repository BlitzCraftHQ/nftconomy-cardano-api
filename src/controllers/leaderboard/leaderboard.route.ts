import { Router } from "express";
import Controller from "./leaderboard.controller";

const leaderboard: Router = Router();
const controller = new Controller();

leaderboard.get("/profit-leaderboard", controller.GetProfitLeaderboard);
leaderboard.get("/top-buyers", controller.GetTopBuyers);
leaderboard.get("/top-sellers", controller.GetTopSellers);

export default leaderboard;
