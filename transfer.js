const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const _ = require("lodash");
const { query, api } = require("./common");

const pool = new Pool({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

const batchSize = 1000;

const processAndInsert = async (index, rows) => {
  const all = rows.map(({ id, dt, orgunit, path, regorgunit, regpath }) => {
    const eventOrgUnit = _.fromPairs(
      String(path)
        .split("/")
        .slice(1)
        .map((x, i) => {
          return [`level${i + 1}`, x];
        })
    );
    const registrationOrgUnit = _.fromPairs(
      String(regpath)
        .split("/")
        .slice(1)
        .map((x, i) => {
          return [`level${i + 1}`, x];
        })
    );
    return {
      ...dt,
      id,
      event: { ...dt.event, ...eventOrgUnit, orgunit },
      tei: { ...dt.tei, ...registrationOrgUnit, regorgunit },
    };
  });
  const { data } = await api.post(`wal/index?index=${index}`, {
    data: all,
  });
  console.log(data);
};

const transferAll = async (index, q) => {
  const client = await pool.connect();
  const cursor = client.query(new Cursor(q));
  let rows = await cursor.read(batchSize);
  processAndInsert(index, rows);
  while (rows.length) {
    rows = await cursor.read(batchSize);
    if (rows.length > 0) {
      processAndInsert(index, rows);
    }
  }
  cursor.close(() => client.release());
};

transferAll("epivac", query).then(() => console.log("Done"));
