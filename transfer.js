const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const _ = require("lodash");
const { query, api } = require("./common");

// const pool = new Pool({
//   user: "carapai",
//   host: "localhost",
//   database: "cbs",
//   password: "Baby77@Baby771",
//   port: 5432,
// });

const pool = new Pool({
  user: "postgres",
  host: "172.27.1.57",
  database: "epivac",
  password: "",
  port: 5432,
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
