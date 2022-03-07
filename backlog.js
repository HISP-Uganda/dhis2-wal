import pgwire from "pgwire";
import { fromPairs } from "lodash";
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
    const data = fromPairs(
      columns.map((column, index) => {
        return [column, chunk[index]];
      })
    );
    console.log(data);
  }
} catch (error) {
} finally {
  client.end();
}
