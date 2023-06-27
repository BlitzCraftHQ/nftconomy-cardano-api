import { Request, Response } from "express";
import { db } from "../../utilities/mongo";

export default class AuthController {
  public GetAuthStatus = async (req: Request, res: Response) => {
    try {
      const uid = req["user"]["sub"];
      try {
        const data = await db.collection("users").insertOne({ uid });
      } catch (e) {
        if (e.code === 11000) {
          // duplicate key error
          const user = await db.collection("users").findOne({ uid });

          // Get user's collections if any
          if (user?.slugs) {
            for (let i = 0; i < user.slugs.length; i++) {
              let collection_data = await db
                .collection("collections")
                .findOne({ slug: user.slugs[i] });

              user.slugs[i] = collection_data;
            }
          }

          res.status(200).send({
            success: true,
            data: user,
          });
        } else {
          res.status(500).send({
            success: false,
            error: e,
          });
        }
      }
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };
}
