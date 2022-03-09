import * as pgwire from "pgwire";
import _ from "lodash";
import * as common from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();

const args = process.argv.slice(2);
export const client = await pgwire.pgconnect({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});
const limit = 10000;
try {
  for (const i = 0; i <= 683; i++) {
    console.log(`Working on ${i + 1}`);
    const { results } = await client.query(
      `${args[0]} limit ${limit} offset ${i * limit}`
    );
    for (const { rows, columns } of results) {
      const data = rows.map((r) => {
        return _.fromPairs(
          columns.map(({ name }, index) => {
            return [name, r[index]];
          })
        );
      });
      await common.api.post(
        `wal/index?index=${args[1]}`,
        {
          data,
        }
      );
    }
    console.log(`Finished working on ${i + 1}`);
    i = i + 1;
  }
} catch (error) {
  console.log(error.message);
} finally {
  client.end();
}
