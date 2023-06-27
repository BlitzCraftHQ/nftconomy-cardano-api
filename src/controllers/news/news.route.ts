import { Router } from "express";
import Controller from "./news.controller";

const news: Router = Router();
const controller = new Controller();

news.get("/", controller.getNewsCatcher);
news.get("/new-york-times", controller.getNYTNews);
news.get("/google-news", controller.getGoogleNews);

export default news;
