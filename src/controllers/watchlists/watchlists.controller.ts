import { Request, Response } from "express";
import { db } from "../../utilities/mongo";

export default class WatchListsController {
  public CreateWatchList = async (req: Request, res: Response) => {
    try {
      const uid = req["user"]["sub"];
      const name = req.body.name;
      const slugs = req.body.slugs;

      const watchListId = name.replaceAll(" ", "-").toLowerCase();

      try {
        const data = await db.collection("watchlists").insertOne({
          uid,
          name,
          slugs,
          watchListId,
        });

        const watchList = await db
          .collection("watchlists")
          .findOne({ watchListId });

        res.status(200).send({
          success: true,
          data: watchList,
        });
      } catch (e) {
        if (e.code === 11000) {
          // duplicate key error
          res.status(200).send({
            success: false,
            reason: "Watchlist already exists",
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

  public GetAllWatchListsUnderUser = async (req: Request, res: Response) => {
    try {
      const uid = req.query.uid;
      const data = await db.collection("watchlists").find({ uid }).toArray();
      res.status(200).send({
        success: true,
        data: data,
      });
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };

  public GetWatchList = async (req: Request, res: Response) => {
    try {
      const uid = req["user"]["sub"];
      const watchListId = req.params.watchListId;
      const watchList = await db
        .collection("watchlists")
        .findOne({ uid, watchListId });

      if (watchList) {
        res.status(200).send({
          success: true,
          data: watchList,
        });
      } else {
        res.status(404).send({
          success: false,
          reason: "Watchlist not found",
        });
      }
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };

  public AddToWatchList = async (req: Request, res: Response) => {
    try {
      const uid = req["user"]["sub"];
      const watchListId = req.params.watchListId;
      const slug = req.query.slug;

      const data = await db
        .collection("watchlists")
        .updateOne({ uid, watchListId }, { $addToSet: { slugs: slug } });

      const watchList = await db
        .collection("watchlists")
        .findOne({ uid, watchListId });

      res.status(200).send({
        success: true,
        data: watchList,
      });
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };

  public RemoveFromWatchList = async (req: Request, res: Response) => {
    try {
      const uid = req["user"]["sub"];
      const watchListId = req.params.watchListId;
      const slug = req.query.slug;

      const data = await db
        .collection("watchlists")
        .updateOne({ uid, watchListId }, { $pull: { slugs: slug } });

      const watchList = await db
        .collection("watchlists")
        .findOne({ uid, watchListId });

      res.status(200).send({
        success: true,
        data: watchList,
      });
    } catch (err) {
      console.log(err);

      res.status(500).json({
        message: err.message,
      });
    }
  };
}
