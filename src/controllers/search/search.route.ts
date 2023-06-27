import { Router } from "express";
import Controller from "./search.controller";

const search: Router = Router();
const controller = new Controller();

search.get("/all", controller.SearchAll);
search.get("/collections", controller.SearchCollections);
search.get("/tokens", controller.SearchTokens);
search.get("/users", controller.SearchUsers);
search.get("/trends", controller.SearchTrends);

export default search;
