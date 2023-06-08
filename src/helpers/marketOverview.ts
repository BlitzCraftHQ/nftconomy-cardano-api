export const structure = (time: any = null): any => {
  let subtractedTime;
  let today = new Date();

  let idFormat: any = {
    year: {
      $year: "$created_date",
    },
    month: {
      $month: "$created_date",
    },
    day: {
      $dayOfMonth: "$created_date",
    },
  };

  if (time == "24h") {
    subtractedTime = today.setDate(today.getDate() - 1);

    idFormat = {
      year: {
        $year: "$created_date",
      },
      month: {
        $month: "$created_date",
      },
      day: {
        $dayOfMonth: "$created_date",
      },
      hour: {
        $hour: "$created_date",
      },
    };
  }

  if (time == "7d") {
    subtractedTime = today.setDate(today.getDate() - 7);
    idFormat = {
      year: {
        $year: "$created_date",
      },
      month: {
        $month: "$created_date",
      },
      day: {
        $dayOfMonth: "$created_date",
      },
      hour: {
        $multiply: [
          {
            $floor: {
              $divide: [{ $hour: "$created_date" }, 2],
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

  let matchFormat: any = {
    event_type: "successful",
    total_price: {
      $nin: [null, "0", 0],
    },
  };

  if (time) {
    matchFormat = {
      event_type: "successful",
      created_date: {
        $gte: new Date(subtractedTime).toISOString(),
      },
      total_price: {
        $nin: [null, "0", 0],
      },
    };
  }

  return {
    matchFormat,
    idFormat,
  };
};
