const { Pool } = require("pg");
const Cursor = require("pg-cursor");

const {
  processAndInsert2,
  batchSize,
  monthlyBacklogQuery,
} = require("./common.js");
const dotenv = require("dotenv");
dotenv.config();

const args = process.argv.slice(2);

const pool = new Pool({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

const processData = async () => {
  const client = await pool.connect();
  try {
    for (const date of args) {
      console.log(`Working on ${date}`);
      const cursor = client.query(new Cursor(monthlyBacklogQuery(date)));
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
      console.log(`Finished working on ${date}`);
    }
  } catch (error) {
    console.log(error.message);
  } finally {
    client.release();
  }
};
processData().then(() => console.log("Done"));
