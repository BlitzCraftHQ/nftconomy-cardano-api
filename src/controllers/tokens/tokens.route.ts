import { Router } from "express";
import Controller from "./tokens.controller";

const tokens: Router = Router();
const controller = new Controller();

tokens.route("/:name").get(controller.GetTokens);
tokens.route("/:slug/:token_id").get(controller.GetTokenOverview);
tokens.route("/:slug/:token_id/activity").get(controller.GetTokenActivity);
tokens.route("/:slug/:token_id/price").get(controller.GetTokenActivity);

export default tokens;
