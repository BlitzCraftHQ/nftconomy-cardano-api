import { Router } from "express";
import Controller from "./activities.controller";

const activities: Router = Router();
const controller = new Controller();

activities.route("/:name").get(controller.GetActivities);
activities.route("/historical-mints/:name").get(controller.GetHistoricalMints);
activities.route("/historical-burns/:name").get(controller.GetHistoricalBurns);
activities.route("/:type/:name").get(controller.GetCollectionActivity);

export default activities;
