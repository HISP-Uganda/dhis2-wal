const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const { processAndInsert } = require("./common.js");
const dotenv = require("dotenv");
const cron = require("node-cron");
dotenv.config();

const pool = new Pool({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD || "",
  hostname: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});
const batchSize = 1000;

cron.schedule("*/5 * * * *", async () => {
  const client = await pool.connect();
  try {
    const cursor = client.query(
      new Cursor(
        `select o.uid ou,o.name,o.path,psi.programstageinstanceid::text,psi.uid,to_char(psi.created,'YYYY-MM-DD') created,to_char(psi.created,'MM') m,to_char(psi.lastupdated,'YYYY-MM-DD') lastupdated,programinstanceid::text,programstageid::text,attributeoptioncomboid::text,psi.deleted,psi.storedby,to_char(duedate,'YYYY-MM-DD') duedate,to_char(executiondate,'YYYY-MM-DD') executiondate,psi.organisationunitid::text,status,completedby,to_char(completeddate,'YYYY-MM-DD') completeddate,eventdatavalues->'bbnyNYD1wgS'->>'value' as vaccine,eventdatavalues->'LUIsbsm3okG'->>'value' as dose,assigneduserid::text,psi.createdbyuserinfo,psi.lastupdatedbyuserinfo from programstageinstance psi inner join organisationunit o using(organisationunitid) where psi.created >= LOCALTIMESTAMP - INTERVAL '5 minutes' and programstageid = 12715`
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
  } catch (error) {
    console.log(error.message);
  } finally {
    client.release();
  }
});
