const _ = require("lodash");
const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const df = require("date-fns");
const { processAndInsert } = require("./common.js");
const dotenv = require("dotenv");
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
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD || "",
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

const processData = async () => {
  const dates = getDatesInRange(new Date(args[0]), new Date(args[1]))
    .sort(df.compareAsc)
    .map((d) => [
      df.format(d, "yyyy-MM-dd"),
      df.format(df.addDays(d, 1), "yyyy-MM-dd"),
    ]);
  try {
    for (const [start, end] of dates) {
      const client = await pool.connect();
      console.log(`Working on ${start} to ${end}`);
      const cursor = client.query(
        new Cursor(
          `select o.uid ou,o.name,o.path,psi.programstageinstanceid::text,psi.uid,to_char(psi.created,'YYYY-MM-DD') created,to_char(psi.created,'MM') m,to_char(psi.lastupdated,'YYYY-MM-DD') lastupdated,programinstanceid::text,programstageid::text,attributeoptioncomboid::text,psi.deleted,psi.storedby,to_char(duedate,'YYYY-MM-DD') duedate,to_char(executiondate,'YYYY-MM-DD') executiondate,psi.organisationunitid::text,status,completedby,to_char(completeddate,'YYYY-MM-DD') completeddate,eventdatavalues->'bbnyNYD1wgS'->>'value' as vaccine,eventdatavalues->'LUIsbsm3okG'->>'value' as dose,assigneduserid::text,psi.createdbyuserinfo,psi.lastupdatedbyuserinfo from programstageinstance psi inner join organisationunit o using(organisationunitid) where psi.created >= '${start}' and psi.created < '${end}' and programstageid = 12715`
        )
      );

      let rows = await cursor.read(batchSize);
      processAndInsert("programinstance", rows);
      while (rows.length) {
        rows = await cursor.read(batchSize);
        if (rows.length > 0) {
          processAndInsert("programinstance", rows);
        }
      }
      console.log(`Finished working on ${start} to ${finish}`);
      client.release();
    }
  } catch (error) {
    console.log(error.message);
  }
};
processData().then(() => console.log("Done"));
