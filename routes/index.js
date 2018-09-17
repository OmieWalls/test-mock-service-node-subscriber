var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
    // Imports the Google Cloud client library
    const PubSub = require(`@google-cloud/pubsub`);

    // Your Google Cloud Platform project ID
    const projectId = 'test-mock-service';

// Initiating a client
    const pubsub = new PubSub({
        projectId: projectId,
        keyFilename: process.env.HOME+'/Projects/Secrets/test-mock-service-new.json'
    });

    /**
     * TODO(developer): Uncomment the following lines to run the sample.
     */
const subscriptionName = 'projects/test-mock-service/subscriptions/Node-Subscriber';
const timeout = 60;

// References an existing subscription
    const subscription = pubsub.subscription(subscriptionName);

// Create an event handler to handle messages
    let messageCount = 0;
    const messageHandler = message => {
        console.log(`Received message ${message.id}:`);
        console.log(`\tData: ${message.data}`);
        console.log(`\tAttributes: ${message.attributes}`);
        messageCount += 1;

        // "Ack" (acknowledge receipt of) the message
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
