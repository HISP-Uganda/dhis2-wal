const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const df = require("date-fns");
const {
  processAndInsert2,
  batchSize,
  createBacklogQuery,
} = require("./common.js");
const dotenv = require("dotenv");
const { sortBy, orderBy } = require("lodash");
dotenv.config();

const args = process.argv.slice(2);

function getDatesInRange(startDate, endDate) {
  const date = new Date(startDate.getTime());
  const dates = [];
  while (date <= endDate) {
    dates.push(new Date(date));
    date.setDate(date.getDate() + 1);
  }
  return dates;
}
const pool = new Pool({
  user: process.env.PG_USER_LIVE,
  password: process.env.PG_PASSWORD_LIVE,
  host: process.env.PG_HOST_LIVE,
  port: process.env.PG_PORT_LIVE,
  database: process.env.PG_DATABASE_LIVE,
});

const processData = async () => {
  const dates = getDatesInRange(new Date(args[0]), new Date(args[1]))
    .sort(df.compareDesc)
    .map((d) => [
      df.format(d, "yyyy-MM-dd"),
      df.format(df.addDays(d, 1), "yyyy-MM-dd"),
    ]);
  const client = await pool.connect();
  try {
    for (const [start, end] of orderBy(
      dates,
      (o) => {
        return o[0];
      },
      ["desc"]
    )) {
      console.log(`Working on ${start}`);
      const cursor = client.query(new Cursor(createBacklogQuery(start, end)));
      let rows = await cursor.read(batchSize);
      if (rows.length > 0) {
        await processAndInsert2("epivac", rows);
      }
      while (rows.length > 0) {
        rows = await cursor.read(batchSize);
        if (rows.length > 0) {
          await processAndInsert2("epivac", rows);
        }
      }
      console.log(`Finished working on ${start}`);
    }
  } catch (error) {
    console.log(error.message);
  } finally {
    client.release();
  }
};
processData().then(() => console.log("Done"));
