import { Router } from "express";
import Controller from "./auth.controller";

const auth: Router = Router();
const controller = new Controller();

auth.post("/status", controller.GetAuthStatus);

export default auth;
