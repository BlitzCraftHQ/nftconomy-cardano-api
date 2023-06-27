import { Router } from "express";
import Controller from "./watchlists.controller";

const watchlists: Router = Router();
const controller = new Controller();

watchlists.post("/new", controller.CreateWatchList);
watchlists.get("/all", controller.GetAllWatchListsUnderUser);
watchlists.get("/:watchListId", controller.GetWatchList);
watchlists.put("/:watchListId", controller.AddToWatchList);
watchlists.delete("/:watchListId", controller.RemoveFromWatchList);

export default watchlists;
