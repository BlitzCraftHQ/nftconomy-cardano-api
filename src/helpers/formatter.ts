import * as dayjs from "dayjs";
import { db } from "../utilities/mongo";

// helper function to fix missing date range.
export const fixMissingDateRange = (
  arr,
  time,
  from: dayjs.Dayjs,
  to: dayjs.Dayjs,
  data
) => {
  const unit = time == "24h" || time == "7d" ? "hour" : "day";
  var difference = to.diff(from, unit);
  var timeDiff = 1;

  if (time == "7d") {
    timeDiff = 2;
  }
  difference = Math.floor(difference / timeDiff);

  for (var i = 1; i < difference; i++) {
    from = from.add(timeDiff, unit);
    arr.push({ _id: from.toISOString(), ...data });
  }
};

export const getDateFormat = (time = null, datekey = "$_id") => ({
  $toDate: {
    $concat: [
      {
        $toString: `${datekey}.year`,
      },
      "-",
      {
        $toString: `${datekey}.month`,
      },
      "-",
      {
        $toString: `${datekey}.day`,
      },
      " ",
      ...[
        time === "24h" || time === "7d"
          ? {
              $toString: `${datekey}.hour`,
            }
          : "00",
      ],
      ":00:00",
    ],
  },
});

export const getSubtractedtime = async (
  time,
  collections,
  fields,
  query = {}
) => {
  const getDayjsObject = (collectionName, dateval) => {
    if (
      collectionName === "reddit_posts" ||
      collectionName === "google_trends" ||
      collectionName === "discord_messages"
    ) {
      var d = new Date(0);
      d.setUTCSeconds(dateval);
      // console.log(d.toISOString())
      return dayjs(d);
    }
    return dayjs(dateval);
  };

  let lastFoundDate: dayjs.Dayjs;

  // Get the recent date in db
  for (var index = 0; index < collections.length; index++) {
    let sort = {};
    sort[fields[index]] = -1;
    let result = (
      await db
        .collection(collections[index])
        .find(query)
        .sort(sort)
        .limit(1)
        .toArray()
    )[0];
    if (typeof result === "undefined") {
      console.log("no data");
      continue;
    }
    if (!lastFoundDate)
      lastFoundDate = getDayjsObject(collections[index], result[fields[index]]);
    else {
      const date = getDayjsObject(collections[index], result[fields[index]]);
      if (date.isBefore(lastFoundDate)) {
        lastFoundDate = date;
      }
    }
  }
  if (typeof lastFoundDate === "undefined") lastFoundDate = dayjs();
  // console.log(lastFoundDate.toISOString())

  let subtractedTime: dayjs.Dayjs;

  if (time == "5m") {
    subtractedTime = lastFoundDate.subtract(5, "minute");
  }
  if (time == "10m") {
    subtractedTime = lastFoundDate.subtract(10, "minute");
  }
  if (time == "30m") {
    subtractedTime = lastFoundDate.subtract(30, "minute");
  }
  if (time == "1h") {
    subtractedTime = lastFoundDate.subtract(1, "hour");
  }
  if (time == "12h") {
    subtractedTime = lastFoundDate.subtract(12, "hour");
  }
  if (time == "24h") {
    subtractedTime = lastFoundDate.subtract(1, "day");
  }
  if (time == "7d") {
    subtractedTime = lastFoundDate.subtract(7, "day");
  }
  if (time == "30d") {
    subtractedTime = lastFoundDate.subtract(30, "day");
  }
  if (time == "3m") {
    subtractedTime = lastFoundDate.subtract(3, "month");
  }
  if (time == "1y") {
    subtractedTime = lastFoundDate.subtract(1, "year");
  }
  // console.log(subtractedTime.toISOString())

  return subtractedTime;
};
