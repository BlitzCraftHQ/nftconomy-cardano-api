import { Router } from "express";
import Controller from "./forms.controller";

const forms: Router = Router();
const controller = new Controller();

forms.post("/", controller.PostForm);
forms.get("/", controller.GetAllForms);

export default forms;
