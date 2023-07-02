export const structure = (time: any = null): any => {
  let subtractedTime;
  let today = new Date();

  let idFormat: any = {
    year: {
      $year: "$timestamp",
    },
    month: {
      $month: "$timestamp",
    },
    day: {
      $dayOfMonth: "$timestamp",
    },
  };

  if (time == "24h") {
    subtractedTime = today.setDate(today.getDate() - 1);

    idFormat = {
      year: {
        $year: "$timestamp",
      },
      month: {
        $month: "$timestamp",
      },
      day: {
        $dayOfMonth: "$timestamp",
      },
      hour: {
        $hour: "$timestamp",
      },
    };
  }

  if (time == "7d") {
    subtractedTime = today.setDate(today.getDate() - 7);
    idFormat = {
      year: {
        $year: "$timestamp",
      },
      month: {
        $month: "$timestamp",
      },
      day: {
        $dayOfMonth: "$timestamp",
      },
      hour: {
        $multiply: [
          {
            $floor: {
              $divide: [{ $hour: "$timestamp" }, 2],
            },
          },
          2,
        ],
      },
    };
  }

  if (time == "30d") {
    subtractedTime = today.setDate(today.getDate() - 30);
  }

  if (time == "3m") {
    subtractedTime = today.setMonth(today.getMonth() - 3);
  }

  if (time == "1y") {
    subtractedTime = today.setFullYear(today.getFullYear() - 1);
  }

  let matchFormat: any = {};

  if (time) {
    matchFormat = {
      timestamp: {
        $gte: new Date(subtractedTime).toISOString(),
      },
    };
  }

  return {
    matchFormat,
    idFormat,
  };
};
