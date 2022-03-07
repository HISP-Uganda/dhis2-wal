import pgwire from "pgwire";
import _ from "lodash";
import common from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();

const args = process.argv.slice(2);

export const client = await pgwire.connect({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

const response = client.query({
  statement: args[0],
});
try {
  for await (const chunk of response) {
    if (chunk.boundary) {
      continue;
    }
    const columns = String(args[1]).split(",");
    const channel = args[2];
    const data = _.fromPairs(
      columns.map((column, index) => {
        return [column, chunk[index]];
      })
    );
    await common.api.post(`wal/index?index=${channel}`, data);
  }
} catch (error) {
} finally {
  client.end();
}
