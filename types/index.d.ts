import { ObjectId } from "mongodb";

export type CardanoJamOnBreadPolicy = {
  _id?: ObjectId;
  nftsInCirculation: number;
  floorPrice: number;
  volume: number;
  owners: number;
  name: string;
  displayName: string;
  imageFormat: string;
  exampleNfts: never[];
  featuredImages: string[];
};

export type CardanoJamOnBreadAsset = {
  _id?: ObjectId;
  policyId: string;
  assetNameHex: string;
  displayName: string;
  image: string;
  imageFormat: string;
  properties: {
    name: string;
    value: string;
    commonness: string;
  }[];
  fullMetadata: any;
  assetFingerprint: string;
  collection: {
    name: string;
    displayName: string;
    description: string;
    nftsInCirculation: number;
    floorPrice: number;
    volume: number;
    imageFormat: string;
    category: string;
    royaltiesAddress: string;
    royaltiesRate: string;
    articleLink: any;
    introVideoLink: any;
  };
  sellOrder: {
    listedByAddress: string;
    listedByStakeKey: string;
    price: number;
    listedOn: string;
    source: string;
    royalties: any;
    scriptPaymentCredentials: any;
    payouts: {
      address: string;
      amount: number;
    }[];
  };
  owner: any;
  rarity: {
    percentage: number;
    score: number;
    order: number;
  };
  likes: number;
  files: any;
};

export type CardanoJamOnBreadDelistAction = {
  _id?: ObjectId;
  action: "delist";
  timestamp: string;
  price: number;
  fromUser: {
    address: string;
    stakeKey: string;
    username: string;
    usernameType: "adahandle";
  };
  fromAddress: string;
  fromStakeKey: string;
  toUser: any;
  toAddress: any;
  toStakeKey: any;
  collection: "spacebudz";
  policyId: string;
  assetNameHex: string;
  image: string;
  displayName: string;
  txHash: string;
  source: "sell_order";
};

export type CardanoJamOnBreadSellAction = {
  _id?: ObjectId;
  action: "sell";
  timestamp: string;
  price: number;
  fromUser: {
    address: string;
    stakeKey: string;
    username: any;
    usernameType: "adahandle";
  };
  fromAddress: string;
  fromStakeKey: string;
  toUser: {
    address: string;
    stakeKey: string;
    username: any;
    usernameType: "adahandle";
  };
  toAddress: string;
  toStakeKey: string;
  collection: "spacebudz";
  policyId: string;
  assetNameHex: string;
  image: string;
  displayName: string;
  txHash: string;
  source: "sell_order";
};

export type CardanoJamOnBreadListAction = {
  _id?: ObjectId;
  action: "list";
  timestamp: string;
  price: number;
  fromUser: {
    address: string;
    stakeKey: string;
    username: string;
    usernameType: "adahandle";
  };
  fromAddress: string;
  fromStakeKey: string;
  toUser: any;
  toAddress: any;
  toStakeKey: any;
  collection: "spacebudz";
  policyId: string;
  assetNameHex: string;
  image: string;
  displayName: string;
  txHash: string;
  source: "sell_order";
};
