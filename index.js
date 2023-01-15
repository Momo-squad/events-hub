import express from 'express';
import cors from 'cors'
import * as dotenv from 'dotenv'
dotenv.config()

import { EventHubConsumerClient, earliestEventPosition  } from "@azure/event-hubs";
import { ContainerClient } from "@azure/storage-blob";    
import { BlobCheckpointStore } from "@azure/eventhubs-checkpointstore-blob";

const app = express();
const PORT = process.env.PORT || 8080
app.use(cors())

const connectionString = process.env.CONNECTION_STRING;    
const eventHubName = process.env.EVENTS_HUB_NAME;
const consumerGroup = process.env.CONSUMER_GROUP; // name of the default consumer group
const storageConnectionString = process.env.STORAGE_CONNECTION_STRING;
const containerName = process.env.CONTAINER_NAME;

app.get('/', async (req, res) => {
  // Create a blob container client and a blob checkpoint store using the client.
  const containerClient = new ContainerClient(storageConnectionString, containerName);
  const checkpointStore = new BlobCheckpointStore(containerClient);

  // Create a consumer client for the event hub by specifying the checkpoint store.
  const consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName, checkpointStore);

  // Subscribe to the events, and specify handlers for processing the events and errors.
  const subscription = consumerClient.subscribe({
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(`No events received within wait time. Waiting for next interval`);
          return;
        }

        for (const event of events) {
            var utcDate = new Date(event.enqueuedTimeUtc);
            var offset = utcDate.getTimezoneOffset();
            var localDate = new Date(utcDate.getTime() + offset * 60000);
            const eventData = {
              sequenceNumber: event.sequenceNumber,
              telemetry: event.body.telemetry,
              enqueuedTimeUtc: localDate.toString(),
              messageId: event.messageId
            }
            console.log(eventData);
            res.write(JSON.stringify(eventData))
        }
        // Update the checkpoint.
        await context.updateCheckpoint(events[events.length - 1]);
      },

      processError: async (err, context) => {
        console.log(`Error : ${err}`);
      }
    },
    { startPosition: earliestEventPosition }
  );
})

app.listen(PORT, () => {
  console.log(`Listening at PORT: ${PORT}`)
})