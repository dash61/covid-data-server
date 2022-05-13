import { Database } from "./database/db";
import Server from "./server";

// Start the server.
export async function start(): Promise<number> {

  const database = new Database("mongofiles_db");
  if (database) {
    try {
      await database.connectToDatabase();
      const numberOfRecords = await database.checkForDataInDatabase();
      console.log("The num records in mongo=", numberOfRecords);

      // If there is no data in the database, read in the csv file.
      if (numberOfRecords === 0) {
        database.readCsvIntoDatabase();
      }

      // See if we can get some basic data thru aggregation:
      // const deaths = await database.getNewDeaths("2020-04-01", "2022-04-01", "USA");
      // console.log("deaths=", deaths);
    } catch (err) {
      console.log("Exception connecting to database, err=", err);
    }
  }
  
  new Server(database);

  return 0;
}

start().catch((err) => {
  console.log("start failed:", err?.message || err);
  process.exit(1);
});
