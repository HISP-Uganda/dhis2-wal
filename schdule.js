import * as pgwire from "pgwire";
import _ from "lodash";
import * as common from "./common.js";
import * as dotenv from "dotenv";
import * as df from "date-fns";
import cron from "node-cron";

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

cron.schedule("*/5 * * * *", async () => {
  try {
    const current = new Date();
    const hours = df.subMinutes(current, 5);
    const end = df.format(current, "yyyy-MM-dd HH:mm:ss.SSS");
    const start = df.format(hours, "yyyy-MM-dd HH:mm:ss.SSS");
    const { results } = await client.query(
      `select o.uid ou,o.name,o.path,psi.programstageinstanceid::text,psi.uid,to_char(psi.created,'YYYY-MM-DD') created,to_char(psi.created,'MM') m,to_char(psi.lastupdated,'YYYY-MM-DD') lastupdated,programinstanceid::text,programstageid::text,attributeoptioncomboid::text,psi.deleted,psi.storedby,to_char(duedate,'YYYY-MM-DD') duedate,to_char(executiondate,'YYYY-MM-DD') executiondate,psi.organisationunitid::text,status,completedby,to_char(completeddate,'YYYY-MM-DD') completeddate,eventdatavalues->'bbnyNYD1wgS'->>'value' as vaccine,eventdatavalues->'LUIsbsm3okG'->>'value' as dose,assigneduserid::text,psi.createdbyuserinfo,psi.lastupdatedbyuserinfo from programstageinstance psi inner join organisationunit o using(organisationunitid) where psi.created >= '${start}' and psi.created < '${end}' and programstageid = 12715`
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
      console.log(`Found ${data.length} records`);

      const all = _.chunk(data, 10000).map((chunk) => {
        return common.api.post(`wal/index?index=${args[0]}`, {
          data: chunk,
        });
      });
      await Promise.all(all);
    }
    console.log(`Finished working on ${start} and ${end}`);
  } catch (error) {
    console.log(error.message);
  } finally {
    client.end();
  }
});