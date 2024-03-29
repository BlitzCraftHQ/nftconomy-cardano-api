import { Router } from "express";
import Controller from "./collections.controller";

const collections: Router = Router();
const controller = new Controller();

// Add Controllers here
collections.route("/").get(controller.GetAllCollections);
collections.route("/listing").get(controller.GetCollectionsListing);
collections.route("/:name").get(controller.GetCollection);
collections
  .route("/:name/number-of-nfts")
  .get(controller.GetNumberofNftsListed);
collections.route("/:name/current-listing").get(controller.GetCurrentListing);

export default collections;
