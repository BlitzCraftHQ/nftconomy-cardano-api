export function rarityScoreToName(score: any) {
  if (score >= 90) {
    return "legendary";
  } else if (score >= 70 && score < 90) {
    return "rare";
  } else if (score >= 40 && score < 70) {
    return "classic";
  } else {
    return "common";
  }
}

export function rarityNameToScore(name: any) {
  if (name == "legendary") {
    return 90;
  } else if (name == "rare") {
    return 70;
  } else if (name == "classic") {
    return 40;
  } else {
    return 0;
  }
}
