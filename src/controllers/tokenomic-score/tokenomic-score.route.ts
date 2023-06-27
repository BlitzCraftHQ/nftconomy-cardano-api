import { Router } from "express";
import Controller from "./tokenomic-score.controller";

const tokenomicScore: Router = Router();
const controller = new Controller();

tokenomicScore.get("/:slug", controller.GetTokenomicScore);
tokenomicScore.get(
  "/:slug/distribution",
  controller.GetTokenomicScoreDistribution
);

export default tokenomicScore;
