import * as pgwire from "pgwire";
import _ from "lodash";
import * as common from "./common.js";
import * as dotenv from "dotenv";
import * as df from "date-fns";

dotenv.config();
const hirarchy = {
  0: "national",
  1: "region",
  2: "district",
  3: "subcounty",
  4: "facility",
};
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
export const client = await pgwire.pgconnect({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});
const dates = getDatesInRange(new Date(args[0]), new Date(args[1]))
  .sort(df.compareAsc)
  .map((d) => [
    df.format(d, "yyyy-MM-dd"),
    df.format(df.addDays(d, 1), "yyyy-MM-dd"),
  ]);
try {
  for (const [start, end] of dates) {
    console.log(`Working on ${start}`);
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
        return common.api.post(`wal/index?index=${args[2]}`, {
          data: chunk,
        });
      });
      const response = await Promise.all(all);
      console.log(response);
    }
    console.log(`Finished working on ${start}`);
  }
} catch (error) {
  console.log(error.message);
} finally {
  client.end();
}
