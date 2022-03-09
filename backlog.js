import * as pgwire from "pgwire";
import _ from "lodash";
import * as common from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();
const hirarchy = {
  0: "national",
  1: "region",
  2: "district",
  3: "subcounty",
  4: "facility",
};
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
  for (let i = 0; i <= 683; i++) {
    console.log(`Working on ${i}`);
    const { results } = await client.query(
      `${args[0]} limit ${limit} offset ${i * limit};`
    );
    for (const { rows, columns } of results) {
      const data = rows.map((r) => {
        return _.fromPairs(
          columns.map(({ name }, index) => {
            if (name === "path") {
              return [
                name,
                _.fromPairs(
                  String(r[index])
                    .split("/")
                    .slice(1)
                    .map((x, i) => {
                      return [hirarchy[i] || "other", x];
                    })
                ),
              ];
            }
            return [name, r[index]];
          })
        );
      });
      await common.api.post(`wal/index?index=${args[1]}`, {
        data,
      });
    }
    console.log(`Finished working on ${i}`);
  }
} catch (error) {
  console.log(error.message);
} finally {
  client.end();
}
