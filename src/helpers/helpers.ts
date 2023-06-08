import axios from "axios";

export const all_categories = {
  Gaming: [
    "entertainment",
    "games",
    "game",
    "play",
    "pc",
    "console",
    "gaming",
    "fun",
    "multiplayer",
    "gamer",
    "online gaming",
    "video game",
  ],
  Memes: ["funny", "entertainment", "memes"],
  Music: [
    "vibe",
    "entertainment",
    "sound",
    "music",
    "genre",
    "track",
    "soundtrack",
    "sound track",
    "volume",
    "album",
    "singer",
    "band",
    "song",
    "playlist",
  ],
  "Digital Artwork": [
    "art",
    "artist",
    "artwork",
    "art work",
    "appreciate",
    "appreciation",
    "painting",
    "visual",
    "creative",
  ],
  Sports: ["energy", "enthusiasm", "sport", "sports", "score"],
  Fashion: [
    "beauty",
    "design",
    "fashion",
    "designer",
    "designing",
    "accesories",
    "lifestyle",
  ],
  Collectibles: [
    "rare",
    "unique",
    "different",
    "collections",
    "collection",
    "collectibles",
    "rarity",
    "popular",
    "popularity",
  ],
  Cinema: ["entertainment", "cinema", "movie", "film", "excitement", "reviews"],
  Metaverse: [
    "meta",
    "metaverse",
    "virtual",
    "avatars",
    "avatar",
    "virtual world",
    "virtualworld",
    "ar",
    "augmented reality",
    "augmented",
    "vr",
    "virtual reality",
    "reality",
  ],
  Miscellaneous: [
    "miscellaneous",
    "misc",
    "items",
    "item",
    "diverse",
    "mixed",
    "heterogeneous",
  ],
  "Domain Name": ["dns", "ens", "domainname", "domain"],
  Avatars: ["meta", "metaverse", "virtual", "avatars", "avatar"],
  Photographs: [
    "memories",
    "photo",
    "photos",
    "photographs",
    "capture",
    "camera",
    "photographer",
    "photoframe",
  ],
  "Digital Real Estates": [
    "realestates",
    "real estates",
    "real estate",
    "estate",
    "estates",
    "land",
  ],
};

export function categorize(description: any) {
  let categories = [];
  for (const [key, val] of Object.entries(all_categories)) {
    for (let j of val) {
      let pattern = new RegExp(`${j}`, "gi");
      let data = pattern.exec(description);
      if (data != null) {
        if (categories.includes(key) == false) {
          categories.push(key);
        }
      }
    }
  }

  return categories;
}
