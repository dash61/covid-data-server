import express, { Request, Response } from 'express';
import cors from "cors";
import { Database } from "./database/db";
import { detectEmptyObject } from "./utils";

class Server {
  protected app = express();
  protected port = 4000;
  protected db: Database = undefined;

  public constructor(db: Database) {
    this.db = db;
    const options: cors.CorsOptions = {
      origin: "*"
    };
    this.app.use(cors(options));
    this.app.use(express.json());

    // Wire up routes.
    this.app.get("/ping", this.handlePingRequest);
    this.app.post("/getDataPoints", this.handleBaselineRequest);
    this.app.get("/getCountryData", this.handleCountryDataRequest);
    this.app.get("/getContinents", this.handleContinentsRequest);
    this.app.get("/getMetricNames", this.handleMetricNamesRequest);
    this.app.post("/getOneMetric", this.handleGetOneMetricRequest);

    
    this.app.listen(this.port, () => {
      console.log(`The server is running on port ${this.port}.`);
    });

  }
  
  // Handle ping to see if this server is up.
  handlePingRequest(_0: Request, res: Response): void {
    res.sendStatus(200);
  }
  
  // Handle /getDataPoints endpoint.
  handleBaselineRequest = async (req: Request, res: Response) : Promise<void> => {
    const bodyOk = !detectEmptyObject(req.body);
    if (bodyOk) {
      // TODO - For future enhancement: have a button somewhere that refreshes
      // the data file by pulling down a new datafile and dropping the db
      // collection then refilling the db with this new data.
      // freshenData is the indication from the front-end to do this.
      // const getFreshData = req.body.freshenData;

      // console.log("Handling baseline req, req.body=", req.body);

      let resultantData = {};

      // Convert start/stop to strings.
      const start = new Date(req.body.start * 1000).toISOString();
      const stop = new Date(req.body.stop * 1000).toISOString();

      if (req.body.metric) {
        const results = await this.db.getDataPoints(req.body.metric, start, stop, req.body.location);
        resultantData = results;
      }
      res.status(200).send(JSON.stringify(resultantData));
      res.end();
    } else {
      res.sendStatus(500);
    }
  }

  handleCountryDataRequest = async (_0: Request, res: Response) : Promise<void> => {
    const results = await this.db.getAllCountryData();
    res.status(200).send(JSON.stringify(results));
    res.end();
  }

  handleContinentsRequest = async (_0: Request, res: Response) : Promise<void> => {
    const results = await this.db.getAllContinents();
    res.status(200).send(JSON.stringify(results));
    res.end();
  }

  handleMetricNamesRequest = async (_0: Request, res: Response) : Promise<void> => {
    const results = await this.db.getAllMetricNames();
    res.status(200).send(JSON.stringify(results));
    res.end();
  }

  handleGetOneMetricRequest = async (req: Request, res: Response) : Promise<void> => {
    const bodyOk = !detectEmptyObject(req.body);
    if (bodyOk) {
      const results = await this.db.getOneMetricPerCountry(req.body.metric, req.body.date);
      res.status(200).send(JSON.stringify(results));
      res.end();
    } else {
      res.sendStatus(500);
    }
  }

}

export default Server;
