import { Router } from "express";

import Controller from "./ownership.controller";

const ownership: Router = Router();
const controller = new Controller();

ownership.get("/traders/:name", controller.GetTraders);
ownership.get("/diamond-hands/:name", controller.GetDiamondHands);
ownership.get(
  "/diamond-hands-owned/:name",
  controller.GetNFTsOwnedByDiamondHands
);
ownership.get("/holding-period/:name", controller.GetNFTsHoldingPeriod);
ownership.get("/top-balances/:name", controller.GetTopBalances);
ownership.get(
  "/other-collections-owned/:name",
  controller.GetOtherCollectionsOwnedByOwners
);
ownership
  .route("/top-owners-transactions/:name")
  .get(controller.GetTransactionsOfTopOwners);
ownership
  .route("/wallets-with-one-nft/:name")
  .get(controller.GetWalletsWithOneNFT);
ownership
  .route("/buys-from-top-5/:name")
  .get(controller.GetBuysFromTop5Wallets);
ownership.get("/profit-made/:name", controller.GetTopProfitWallets);
ownership.get(
  "/other-collections-owned-frequency/:name",
  controller.GetCollectionsOwnedByFrequency
);

export default ownership;
