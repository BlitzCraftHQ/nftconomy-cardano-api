import { Router } from "express";
import Controller from "./users.controller";

const users: Router = Router();
const controller = new Controller();

users.get("/collections/:user", controller.GetUserCollections);
users.get("/", controller.GetUserProfile);

export default users;
