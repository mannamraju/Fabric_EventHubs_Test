const { EventHubProducerClient } = require("@azure/event-hubs");
var moment = require('moment');

const connectionString = "Endpoint=sb://eventstream-f07241c9-fe3b-43d0-8b78-684d2ff9f64d.servicebus.windows.net/;SharedAccessKeyName=key_96b95324-0b63-5385-8090-a6125f9be11c;SharedAccessKey=GuAv0GvqzFrsnNO+k/toKDt147HmfAntA+AEhP+J8Eg=";
const entityName = "es_a2b541a2-121a-4d7b-adac-0d4d8be1beaa";
const msgId = 0;

//Generate event data
function getRowData(id) {
    const time = moment().toISOString();
    const deviceID = id + 100;
    const humidity = Math.round(Math.random()*(65-35) + 35);
    const temperature = Math.round(Math.random()*(37-20) + 20);

    return {"entryTime":time, "messageId":id, "temperature":temperature, "humidity":humidity, "deviceID":deviceID};
  }

function sleep(ms) {  
    return new Promise(resolve => setTimeout(resolve, ms));  
  } 

async function main() {
    // Create a producer client to send messages to the eventstream.
    const producer = new EventHubProducerClient(connectionString, entityName);

    // There are 10 devices. They are sending events every second nearly. So, there are 10 events within one batch.
    // The event counts per batch. For this case, it is the sensor device count.
    const batchSize = 10;
    // The event batch count. If you want to send events indefinitely, you can increase this number to any desired value.
    const batchCount = 5;
    // Generating and sending events...
    for (let j = 0; j < batchCount; ++j) {
        const eventDataBatch = await producer.createBatch();
        for (let k = 0; k < batchSize; ++k) {
            eventDataBatch.tryAdd({ body: getRowData(k) });
        }  
        // Send the batch to the eventstream.
        await producer.sendBatch(eventDataBatch);
        console.log(moment().format('YYYY/MM/DD HH:mm:ss'), `[Send events to Fabric Eventstream]: batch#${j} (${batchSize} events) has been sent to eventstream`);
        // sleep for 1 second.
        await sleep(1000); 
    }
    // Close the producer client.
    await producer.close();
    console.log(moment().format('YYYY/MM/DD HH:mm:ss'), `[Send events to Fabric Eventstream]: All ${batchCount} batches have been sent to eventstream`);
}

main().catch((err) => {
    console.log("Error occurred: ", err);
});