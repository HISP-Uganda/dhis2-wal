const { Pool } = require("pg");
const { updateQuery } = require("./common.js");
const dotenv = require("dotenv");
const cron = require("node-cron");
dotenv.config();

const pool = new Pool({
  user: process.env.PG_USER_LIVE,
  password: process.env.PG_PASSWORD_LIVE,
  host: process.env.PG_HOST_LIVE,
  port: process.env.PG_PORT_LIVE,
  database: process.env.PG_DATABASE_LIVE,
});

cron.schedule("45 20 * * *", async () => {
  const client = await pool.connect();
  try {
    const response = await client.query(updateQuery);
    console.log(response);
  } catch (error) {
    console.log(error.message);
  } finally {
    client.release();
  }
});
