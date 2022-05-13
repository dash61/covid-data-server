import * as fs from "fs-extra";
import * as path from "path";
import * as mongodb from "mongodb";
import { parse } from '@fast-csv/parse';
import * as dotenv from "dotenv";
import { ICovidData } from "../interfaces";
import { ISODate } from "../utils";

export const collections: { covid_collection?: mongodb.Collection } = {}

export class Database {
  protected client?: mongodb.MongoClient;
  private collection: mongodb.Collection;

  public constructor(dataDir: string) {
    fs.ensureDirSync(dataDir); // make sure database dir exists
  }

  public async connectToDatabase() {
    try {
      dotenv.config();

      // Connect using MongoClient:
      this.client = new mongodb.MongoClient(process.env.DB_CONN_STRING);
      await this.client.connect();
      const db: mongodb.Db = this.client.db(process.env.DB_NAME);
      this.collection = db.collection(process.env.COLLECTION_NAME);
      console.log(`Successfully connected to database: ${db.databaseName} and collection: ${this.collection.collectionName}`);

    } catch (err) {
      console.log("Exception connecting to mongo, err=", err);
    }
  }

  // TODO - get updated csv file periodically
  public readCsvIntoDatabase() {
    const csvPath = path.resolve(__dirname, "./owid-covid-data.csv"); // all the data
    let csvData: ICovidData[] = [];
    fs.createReadStream(csvPath)
    .pipe(parse({ headers: true }))

    // Transform from all strings to the appropriate type.
    // TODO - Refactor this later;
    // Most types are numbers, a few are strings, one is a Date.
    .transform((obj: ICovidData) => {
      return {
        "iso_code": obj["iso_code"],   // string
        "continent": obj["continent"], // string
        "location": obj["location"],   // string
        "date": new Date(obj["date"]), // date
        "total_cases": +obj["total_cases"],
        "new_cases": +obj["new_cases"],
        "new_cases_smoothed": +obj["new_cases_smoothed"],
        "total_deaths": +obj["total_deaths"],
        "new_deaths": +obj["new_deaths"],
        "new_deaths_smoothed": +obj["new_deaths_smoothed"],
        "total_cases_per_million": +obj["total_cases_per_million"],
        "new_cases_per_million": +obj["new_cases_per_million"],
        "new_cases_smoothed_per_million": +obj["new_cases_smoothed_per_million"],
        "total_deaths_per_million": +obj["total_deaths_per_million"],
        "new_deaths_per_million": +obj["new_deaths_per_million"],
        "new_deaths_smoothed_per_million": +obj["new_deaths_smoothed_per_million"],
        "reproduction_rate": +obj["reproduction_rate"],
        "icu_patients": +obj["icu_patients"],
        "icu_patients_per_million": +obj["icu_patients_per_million"],
        "hosp_patients": +obj["hosp_patients"],
        "hosp_patients_per_million": +obj["hosp_patients_per_million"],
        "weekly_icu_admissions": +obj["weekly_icu_admissions"],
        "weekly_icu_admissions_per_million": +obj["weekly_icu_admissions_per_million"],
        "weekly_hosp_admissions": +obj["weekly_hosp_admissions"],
        "weekly_hosp_admissions_per_million": +obj["weekly_hosp_admissions_per_million"],
        "total_tests": +obj["total_tests"],
        "new_tests": +obj["new_tests"],
        "total_tests_per_thousand": +obj["total_tests_per_thousand"],
        "new_tests_per_thousand": +obj["new_tests_per_thousand"],
        "new_tests_smoothed": +obj["new_tests_smoothed"],
        "new_tests_smoothed_per_thousand": +obj["new_tests_smoothed_per_thousand"],
        "positive_rate": +obj["positive_rate"],
        "tests_per_case": +obj["tests_per_case"],
        "tests_units": obj["tests_units"], // string
        "total_vaccinations": +obj["total_vaccinations"],
        "people_vaccinated": +obj["people_vaccinated"],
        "people_fully_vaccinated": +obj["people_fully_vaccinated"],
        "total_boosters": +obj["total_boosters"],
        "new_vaccinations": +obj["new_vaccinations"],
        "new_vaccinations_smoothed": +obj["new_vaccinations_smoothed"],
        "total_vaccinations_per_hundred": +obj["total_vaccinations_per_hundred"],
        "people_vaccinated_per_hundred": +obj["people_vaccinated_per_hundred"],
        "people_fully_vaccinated_per_hundred": +obj["people_fully_vaccinated_per_hundred"],
        "total_boosters_per_hundred": +obj["total_boosters_per_hundred"],
        "new_vaccinations_smoothed_per_million": +obj["new_vaccinations_smoothed_per_million"],
        "new_people_vaccinated_smoothed": +obj["new_people_vaccinated_smoothed"],
        "new_people_vaccinated_smoothed_per_hundred": +obj["new_people_vaccinated_smoothed_per_hundred"],
        "stringency_index": +obj["stringency_index"],
        "population": +obj["population"],
        "population_density": +obj["population_density"],
        "median_age": +obj["median_age"],
        "aged_65_older": +obj["aged_65_older"],
        "aged_70_older": +obj["aged_70_older"],
        "gdp_per_capita": +obj["gdp_per_capita"],
        "extreme_poverty": +obj["extreme_poverty"],
        "cardiovasc_death_rate": +obj["cardiovasc_death_rate"],
        "diabetes_prevalence": +obj["diabetes_prevalence"],
        "female_smokers": +obj["female_smokers"],
        "male_smokers": +obj["male_smokers"],
        "handwashing_facilities": +obj["handwashing_facilities"],
        "hospital_beds_per_thousand": +obj["hospital_beds_per_thousand"],
        "life_expectancy": +obj["life_expectancy"],
        "human_development_index": +obj["human_development_index"],
        "excess_mortality_cumulative_absolute": +obj["excess_mortality_cumulative_absolute"],
        "excess_mortality_cumulative": +obj["excess_mortality_cumulative"],
        "excess_mortality": +obj["excess_mortality"],
        "excess_mortality_cumulative_per_million": +obj["excess_mortality_cumulative_per_million"],
      };
    })
    .on('error', error => console.error(error))
    .on('data', row => csvData.push(row))
    .on('end', async (rowCount: number) => {
      csvData.shift();
      await this.writeToTable(csvData);
    });
  }

  public async writeToTable(data: ICovidData[]) {
    const recs = await this.collection.insertMany(data);
    console.log(`Inserted ${recs.insertedCount} csv records`);
  }

  public async checkForDataInDatabase() {
    try {
      return await this.collection.countDocuments();
    } catch (err) {
      console.log("Count of all docs in collection gave err=", err);
    }
    return 0;
  }

  // Get data for given metric and country and time range.
  public async getDataPoints(metric: string, dateStart: string, dateEnd: string, countryCode: string) {
    try {
      console.log("getDataPoints - metric=", metric, " start=", dateStart,
        " end=", dateEnd, " country=", countryCode);
      
      let metricString = "";
      switch (metric) {
        case "cases": metricString = "new_cases_smoothed"; break;
        case "deaths": metricString = "new_deaths_smoothed"; break;
        case "vacc": metricString = "new_vaccinations_smoothed"; break;
        case "hospital": metricString = "hosp_patients"; break;
        default: break;
      }

      const cursor = this.collection.aggregate([
        { $match: { iso_code: countryCode, date: { $gte: ISODate(dateStart), $lte: ISODate(dateEnd) } } },
        { $project: { [metricString]: 1, date: 1 } }
      ]);

      const result = await cursor.toArray();
      console.log("getDataPoints, number found=", result.length);
      if (result.length) {
        return result;
      }
      return 0;
    } catch (err) {
      console.log("getDataPoints - EXCEPTION=", err);
    }
  }

  // Get total deaths for a time span for a country.
  public async getNewDeaths(dateStart: string, dateEnd: string, countryCode: string) {
    try {
      console.log("getNewDeaths - start=", dateStart, " end=", dateEnd, " country=", countryCode);
      const cursor = this.collection.aggregate([
        { $match: { iso_code: countryCode, date: { $gte: ISODate(dateStart), $lte: ISODate(dateEnd) } } },
        { $group: { "_id": "$iso_code", total: { $sum: "$new_deaths" } } }
      ]);

      const result = await cursor.toArray();
      console.log("getNewDeaths, number found=", result);
      if (result.length) {
        return result[0].total;
      }
      return 0;
    } catch (err) {
      console.log("getNewDeaths - EXCEPTION=", err);
    }
  }

  // each country has iso_code, continent, location (name of country), date
  //
  // if iso_code starts with OWID, it is not a country; might be a continent?
  //
  // need fns like getDeathsForCountry, getDeathsTotal, getDeathsForContinent, etc.
  // pass in a date range - yyyy-mm-dd
  // also: newCases, newDeaths, totalCasesPerMillion, newCasesPerMillion, totalDeathsPer...
  // also: reproductionRate, icuPatients, hospPatients, weeklyICUAdmissions, weeklyHospAdmissions
  // also: totalTests, newTests, .. perThousand, positiveRate
  // also: totalVaccs, peopleVacced, peopleFullyVacced, totalBoosters, newVaccs, ..perHundred
  // also: population, populationDensity, medianAge, 65Older, 70Older, gdpPerCapita,
  // also: extremePoverty, cardioDeathRate, diabetesPrev, femaleSmokers, male...,
  // also: mortality stats, lifeExpec, hospBedsPerThousand, ...

}
