const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const {
    processAndInsert2,
    batchSize,
    createBacklogQuery,
    createBacklogQuery2,
} = require("./common.js");
const dayjs = require("dayjs");
const dotenv = require("dotenv");
const cron = require("node-cron");
dotenv.config();

const pool = new Pool({
    user: process.env.PG_USER_LIVE,
    password: process.env.PG_PASSWORD_LIVE,
    host: process.env.PG_HOST_LIVE,
    port: process.env.PG_PORT_LIVE,
    database: process.env.PG_DATABASE_LIVE,
});

cron.schedule("*/30 * * * *", async () => {
    const client = await pool.connect();
    try {
        console.log("Updating by program tracked entity instance");
        const cursor1 = client.query(
            new Cursor(
                createBacklogQuery2(
                    dayjs().format("YYYY-MM-DD"),
                    dayjs().add(1, "days").format("YYYY-MM-DD")
                )
            )
        );
        let rows1 = await cursor1.read(batchSize);
        if (rows1.length > 0) {
            await processAndInsert2("epivac", rows1);
        }
        while (rows1.length > 0) {
            rows1 = await cursor1.read(batchSize);
            if (rows1.length > 0) {
                await processAndInsert2("epivac", rows1);
            }
        }

        console.log("Updating by program stage instance");
        const cursor = client.query(
            new Cursor(
                createBacklogQuery(
                    dayjs().format("YYYY-MM-DD"),
                    dayjs().add(1, "days").format("YYYY-MM-DD")
                )
            )
        );
        let rows = await cursor.read(batchSize);
        if (rows.length > 0) {
            await processAndInsert2("epivac", rows);
        }
        while (rows.length > 0) {
            rows = await cursor.read(batchSize);
            if (rows.length > 0) {
                await processAndInsert2("epivac", rows);
            }
        }
    } catch (error) {
        console.log(error.message);
    } finally {
        client.release();
    }
    console.log("Done");
});
