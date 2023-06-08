export const structure = (
  time: any,
  slug: string,
  event_type: string = "successful",
  date_key: any = "$created_date"
): any => {
  let subtractedTime;
  let today = new Date();

  let idFormat: any = {
    year: {
      $year: date_key,
    },
    month: {
      $month: date_key,
    },
    day: {
      $dayOfMonth: date_key,
    },
  };

  if (time == "24h") {
    subtractedTime = today.setDate(today.getDate() - 1);

    idFormat = {
      year: {
        $year: date_key,
      },
      month: {
        $month: date_key,
      },
      day: {
        $dayOfMonth: date_key,
      },
      hour: {
        $hour: date_key,
      },
    };
  }

  if (time == "7d") {
    subtractedTime = today.setDate(today.getDate() - 7);
    idFormat = {
      year: {
        $year: date_key,
      },
      month: {
        $month: date_key,
      },
      day: {
        $dayOfMonth: date_key,
      },
      hour: {
        $multiply: [
          {
            $floor: {
              $divide: [{ $hour: date_key }, 2],
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

  let matchFormat: any =
    event_type == "successful"
      ? {
          event_type: event_type,
          slug: slug,
          total_price: {
            $nin: [null, "0", 0],
          },
        }
      : {
          event_type: event_type,
          slug: slug,
        };
  if (time) {
    matchFormat =
      event_type == "successful"
        ? {
            event_type: event_type,
            slug: slug,
            created_date: {
              $gte: new Date(subtractedTime).toISOString(),
            },
            total_price: {
              $nin: [null, "0", 0],
            },
          }
        : {
            event_type: event_type,
            slug: slug,
            created_date: {
              $gte: new Date(subtractedTime).toISOString(),
            },
          };
  }

  return {
    matchFormat,
    idFormat,
  };
};
