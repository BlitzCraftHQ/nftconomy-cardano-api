import { ObjectId } from "mongodb";

export type Collection = {
  _id?: ObjectId;
  slug: string;
  chain: string;
  categories?: string[];
  marketplaces: string[];
  address: string;
  asset_contract_type: string;
  nft_version?: string;
  opensea_version?: string;
  schema_name: string;
  symbol: string;
  total_supply: number;
  banner_image_url: string | null;
  chat_url: string | null;
  created_date: Date;
  description: string | null;
  discord_url: string | null;
  external_url: string | null;
  image_url: string | null;
  large_image_url: string | null;
  name: string;
  payout_address: string | null;
  telegram_url: string | null;
  twitter_username: string | null;
  instagram_username: string | null;
  wiki_url: string | null;
  is_nsfw: Boolean;
  short_description: string | null;
  editors?: string[];
  tokens?: Token[] | null;
};

export type Token = {
  id: string;
  address?: string;
  escrow_address?: string;
  amount?: string;
  block_number_minted?: string;
  metadata_address?: string;
  attrib_count?: number;
  name: string;
  for_sale?: number | boolean;
  description?: string;
  image_url: string | null;
  large_image_url?: string | null;
  featured_image_url?: string | null;
  perma_url?: string;
  metadata_url?: string | null;
  rarity_percentage?: number;
  tokenomic_score?: number;
  traits: Trait[] | null;
};

export type Trait = {
  trait_type: string;
  value: string;
};
