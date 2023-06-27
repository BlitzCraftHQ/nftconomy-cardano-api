import { Request, Response } from "express";
import { db } from "../../utilities/mongo";

export default class FormsController {
  public PostForm = async (req: Request, res: Response) => {
    try {
      const data = await db.collection("forms").insertOne(req.body);
      res.status(200).send({
        success: true,
        data,
      });
    } catch (e) {
      res.status(500).send({
        success: false,
        error: e,
      });
    }
  };

  public GetAllForms = async (req: Request, res: Response) => {
    try {
      // Project the fields you want to return
      const data = await db.collection("forms").find().toArray();
      res.status(200).send(data);
    } catch (e) {
      res.status(500).send({
        success: false,
        error: e,
      });
    }
  };
}
