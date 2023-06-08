import { Request, Response, NextFunction } from "express";
import { db } from "../utilities/mongo";

export const rbac = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<any> => {
  let uid: any = "google-oauth2|103393718237131367287";
  const user = await db.collection("users").findOne({ uid });

  // let planId = user?.planId;
  let planId = user?.planId;
  let route = req;
  console.log(route);

  if (plans[planId].includes(route)) {
    next();
  } else {
    res.status(403).send({
      success: false,
      message: "You are not allowed to access this route",
    });
  }
};

// Define plan id and allowed routes
const plans = {
  "1": ["collections", "collections/:slug", "collections/:slug/entries"],
  "2": [
    "collections",
    "collections/:slug",
    "collections/:slug/entries",
    "collections/:slug/entries/:entryId",
  ],
  "3": [
    "collections",
    "collections/:slug",
    "collections/:slug/entries",
    "collections/:slug/entries/:entryId",
    "collections/:slug/entries/:entryId/fields",
  ],
  "4": [
    "collections",
    "collections/:slug",
    "collections/:slug/entries",
    "collections/:slug/entries/:entryId",
    "collections/:slug/entries/:entryId/fields",
    "collections/:slug/entries/:entryId/fields/:fieldId",
  ],
  "5": [
    "collections",
    "collections/:slug",
    "collections/:slug/entries",
    "collections/:slug/entries/:entryId",
    "collections/:slug/entries/:entryId/fields",
    "collections/:slug/entries/:entryId/fields/:fieldId",
    "collections/:slug/entries/:entryId/fields/:fieldId/entries",
  ],
};
