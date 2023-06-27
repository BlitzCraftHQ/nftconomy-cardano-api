import { Router } from "express";
import Controller from "./mints.controller";

const mints: Router = Router();
const controller = new Controller();

mints.get("/avg-mints-per-wallet", controller.GetAvgMintsPerWallet);

export default mints;
