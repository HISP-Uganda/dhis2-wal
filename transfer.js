const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const _ = require("lodash");
const { queryByProgram, batchSize, processAndInsert2 } = require("./common");

const dotenv = require("dotenv");
dotenv.config();

const pool = new Pool({
    user: process.env.PG_USER_LIVE,
    password: process.env.PG_PASSWORD_LIVE,
    host: process.env.PG_HOST_LIVE,
    port: process.env.PG_PORT_LIVE,
    database: process.env.PG_DATABASE_LIVE,
});

const transferAll = async (index) => {
    const client = await pool.connect();
    const cursor = client.query(new Cursor(queryByProgram("yDuAzyqYABS")));
    let rows = await cursor.read(batchSize);
    if (rows.length > 0) {
        await processAndInsert2(index, rows);
    }
    while (rows.length > 0) {
        rows = await cursor.read(batchSize);
        if (rows.length > 0) {
            await processAndInsert2(index, rows);
        }
    }
    cursor.close(() => client.release());
};

transferAll("epivac").then(() => console.log("Done"));
