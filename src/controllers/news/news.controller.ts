import { Request, Response } from "express";
const ogs = require("open-graph-scraper");
import axios from "axios";
import { setCache, uniqueKey } from "../../utilities/redis";
var convert = require("xml-js");

require("dotenv").config();

export default class NewsController {
  public getNewsCatcher = async (req: Request, res: Response) => {
    try {
      let keyword = req.query.keyword;
      let page = req.query.page;

      const news = await axios.get(
        `https://api.newscatcherapi.com/v2/search?q=${keyword}&lang=en${
          page ? `&page_size=10&page=${page}` : ""
        }`,
        {
          headers: {
            "x-api-key": process.env.NEWS_CATCHER_API_KEY || "",
          },
        }
      );

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: news.data,
        }),
        1440
      );

      res.status(200).json({
        success: true,
        data: news.data,
      });
    } catch (err) {
      res.status(500).json({ message: err.message });
    }
  };

  public getNYTNews = async (req: Request, res: Response) => {
    try {
      let keyword = req.query.keyword;
      const news = await axios.get(
        `https://api.nytimes.com/svc/search/v2/articlesearch.json?q=${keyword}&api-key=mYMA9YFOOy03jFmFzFtYpBYwB6QEmsAe`
      );

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          data: news.data,
        }),
        1440
      );

      res.status(200).json({
        success: true,
        data: news.data,
      });
    } catch (err) {
      res.status(500).json({ message: err.message });
    }
  };

  public getGoogleNews = async (req: Request, res: Response) => {
    try {
      let keyword = req.query.keyword;
      let with_images = req.query.with_images || 0;
      let page = Number(req.query.page || 0);

      const newsRSS: any = await axios
        .get(
          `https://news.google.com/rss/search?q=${keyword}&hl=en-IN&gl=IN&ceid=IN:en`
        )
        .catch((err) => {
          console.log(err);
        });

      var news = JSON.parse(
        convert.xml2json(newsRSS?.data, {
          compact: true,
          spaces: 4,
        })
      ).rss.channel.item;

      news = news ? news : [];

      if (with_images == 1 && news.length > 0) {
        for (let i = 0; i < news.length; i++) {
          let link = news[i].link._text;

          let image_url = await ogs({ url: link })
            .then((response: any) => {
              return response.result.ogImage?.url;
            })
            .catch((err: any) => {
              console.log(err.message);
              return null;
            });

          news[i].image = image_url;
        }
      }

      let totalCount = news.length;
      let pageSize = 10;

      if (page !== 0) {
        news = news.slice(page * 10 - 9, page * 10 + 1);
      }

      let pagination =
        page == 0
          ? {}
          : {
              pagination: {
                pageSize: pageSize,
                currentPage: page,
                totalPages: Math.ceil(totalCount / pageSize),
              },
            };

      if (with_images == 1) {
        news = news.filter((item: any) => {
          return item?.image;
        });
      }

      setCache(
        uniqueKey(req),
        JSON.stringify({
          success: true,
          totalCount,
          ...pagination,
          data: news,
        }),
        1440
      );

      res.status(200).json({
        success: true,
        data: news,
        ...pagination,
        totalCount,
      });
    } catch (err) {
      console.log(err);
      res.status(500).json({ message: err.message });
    }
  };
}
