const PGPubsub = require("pg-pubsub");

const pubsubInstance = new PGPubsub(
    "postgres://carapai:Baby77@Baby771@localhost/dhis40"
);

async function main() {
    await pubsubInstance.addChannel(
        "new_data_value",
        function (channelPayload) {
            console.log(channelPayload);
        }
    );
}

main();
