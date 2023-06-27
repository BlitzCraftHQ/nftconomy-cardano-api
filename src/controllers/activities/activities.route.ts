import { Router } from "express";
import Controller from "./activities.controller";

const activities: Router = Router();
const controller = new Controller();

activities.route("/:slug").get(controller.GetActivities);
activities.route("/historical-mints/:slug").get(controller.GetHistoricalMints);
activities.route("/historical-burns/:slug").get(controller.GetHistoricalBurns);
activities.route("/:type/:slug").get(controller.GetCollectionActivity);

export default activities;
