import pgwire from "pgwire";
import * as common from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();

const client = await pgwire.connect(
  {
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    hostname: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
  },
  {
    replication: "database",
  }
);

try {
  const replicationStream = await client.logicalReplication({
    slot: "elasticsearch_slot",
    startLsn: "0/0",
    options: {
      proto_version: 1,
      publication_names: "elasticsearch_pub",
    },
  });
  process.on("SIGINT", (_) => replicationStream.destroy());
  for await (const pgoMessage of replicationStream.pgoutput()) {
    switch (pgoMessage.tag) {
      case "insert":
      case "update":
        const {
          relation: { name },
          after,
        } = pgoMessage;
        try {
          // await api.post(`wal/index?index=${name}`, after);
          console.log(after);
          // replicationStream.ack(pgoMessage.lsn);
        } catch (error) {
          console.log(error.message);
        }
    }
  }
} finally {
  client.end();
}
