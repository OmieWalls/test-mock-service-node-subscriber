var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
    // Imports the Google Cloud client library
    const PubSub = require(`@google-cloud/pubsub`);

    // Imports the Google Cloud client library
    const Datastore = require('@google-cloud/datastore');

    // Your Google Cloud Platform project ID
    const projectId = 'test-mock-service';

// Initiating a client
    const pubsub = new PubSub({
        projectId: projectId,
        keyFilename: process.env.HOME+'/Projects/Secrets/test-mock-service-new.json'
    });

// Creates a client
    const datastore = new Datastore({
        projectId: projectId,
    });

const subscriptionName = 'projects/test-mock-service/subscriptions/Node-Subscriber';
const timeout = 60;

// References an existing subscription
    const subscription = pubsub.subscription(subscriptionName);

// Create an event handler to handle messages
    let messageCount = 0;
    const messageHandler = message => {
        console.log(`Received message ${message.id}:`);
        console.log(`\tData: ${message.data.toString('UTF8')}`);
        console.log(`\tAttributes: ${JSON.stringify(message.attributes)}`);
        messageCount += 1;
        //TODO: Store data
        const data = message.data.toString('UTF8');
        const json = JSON.parse(data);

//      The kind for the new entity
        const kind = req.params['schema'];
//      The name/ID for the new entity
        const name = json[req.params['name']];
//      The Cloud Datastore key for the new entity
        const taskKey = datastore.key([kind, name]);

        const task = {
            key: taskKey,
            data: json,
        };

// Saves the entity
        datastore
            .upsert(task)
            .then(() => {
                console.log(`Saved ${task.key.name}: ${task.data}`);
            })
            .catch(err => {
                console.error('ERROR:', err);
            });

        // Acknowledge receipt of the message
        message.ack();
    };

// Listen for new messages until timeout is hit
    subscription.on(`message`, messageHandler);
    setTimeout(() => {
        subscription.removeListener('message', messageHandler);
        console.log(`${messageCount} message(s) received.`);
    }, timeout * 1000);
});

module.exports = router;
