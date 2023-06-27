import { Router } from "express";

import Controller from "./ownership.controller";

const ownership: Router = Router();
const controller = new Controller();

ownership.get("/traders/:slug", controller.GetTraders);
ownership.get("/diamond-hands/:slug", controller.GetDiamondHands);
ownership.get(
  "/diamond-hands-owned/:slug",
  controller.GetNFTsOwnedByDiamondHands
);
ownership.get("/holding-period/:slug", controller.GetNFTsHoldingPeriod);
ownership.get("/top-balances/:slug", controller.GetTopBalances);
ownership.get(
  "/other-collections-owned/:slug",
  controller.GetOtherCollectionsOwnedByOwners
);
ownership
  .route("/top-owners-transactions/:slug")
  .get(controller.GetTransactionsOfTopOwners);
ownership
  .route("/wallets-with-one-nft/:slug")
  .get(controller.GetWalletsWithOneNFT);
ownership
  .route("/buys-from-top-5/:slug")
  .get(controller.GetBuysFromTop5Wallets);
ownership.get("/profit-made/:slug", controller.GetTopProfitWallets);
ownership.get(
  "/other-collections-owned-frequency/:slug",
  controller.GetCollectionsOwnedByFrequency
);

export default ownership;
