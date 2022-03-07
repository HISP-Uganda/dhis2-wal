import pgwire from "pgwire";
import { api } from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();

export const client = await pgwire.connect({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});
client.on("notification", ({ channel, payload }) => {
  try {
    api.post(`wal/index?index=${channel}`, JSON.parse(payload));
  } catch (error) {
    console.log(error.message);
  }
});
await client.query(`LISTEN foo`);
