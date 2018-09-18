const express = require('express');
const router = express.Router();

// Initializes global variables
let topic, subscriber, kind, entity, projectId, is_upsert,      // Query Parameters
    existingTopic, existingSubscriber,                          // Pub/Sub Flags
    pubsub, datastore, subscription, timeout;                   // Client-Related Variables

//  Imports the Google Cloud client library
    const PubSub = require(`@google-cloud/pubsub`);

//  Imports the Google Cloud client library
    const Datastore = require('@google-cloud/datastore');

/* GET home page. */
router.get('/', function(req, res, next) {

//  Sets request parameters:
//  Pub/Sub variables
    projectId = req.query['project'];
    topic = req.query['topic'];
    subscriber = req.query['subscriber'];
    timeout = req.query['timeout'];
//  Datastore variables
    kind = req.query['kind'];
    entity = req.query['entity'];
    is_upsert = req.query['is_upsert'];

//  Declare flags
    existingSubscriber = false;
    existingTopic = false;

//  Initiating a Pub/Sub client
    pubsub = new PubSub({
        projectId: projectId,
        keyFilename: process.env.GOOGLE_APPLICATIO_CREDENTIALS
    });

//  Creates a Datastore client
    datastore = new Datastore({
        projectId: projectId,
    });

//  Checks for existing topics and subscriptions and creates them if they are missing.
    checkTopics();

//  Create an event handler to handle messages
    let messageCount = 0;

    const messageHandler = message => {
        logMessage();

//      Decode the message ByteString and parse into an JSON object
        const data = message.data.toString('utf8');
        const json = JSON.parse(data);

//      The Cloud Datastore key for the new entity
        const taskKey = datastore.key([kind, json[entity]]);

//      Prepare task
        const task = {
            key: taskKey,
            data: json,
        };

//      Handles upsert or insert based upon indication
        is_upsert ? upsert(task) : insert(task);
    };

//  Logs incoming messages
    function logMessage() {
        console.log(`Received message ${message.id}:`);
        console.log(`\tData: ${message.data.toString('utf8')}`);
        console.log(`\tAttributes: ${JSON.stringify(message.attributes)}`);
        messageCount += 1;
    }

// Checks topics for the topic given
    function checkTopics() {
        pubsub
            .getTopics()
            .then(results => {
                const topics = results[0];

                topics.forEach(resultTopic => {
//              If topic exists then the flag is set to true
                    if (resultTopic.name.indexOf(topic) > -1) {
                        existingTopic = true;
                    }
                });
//              Creates topic if there is no existing topic. If there is, proceed to check subscriptions
                if (!existingTopic) {
                    createTopic();
                } else {
                    checkSubscriptions();
                }
            })
            .catch(err => {
                console.error('ERROR while checking topics:', err);
            });
    }

// Checks subscriptions for the subscriber given
    function checkSubscriptions() {
        pubsub
            .getSubscriptions()
            .then(results => {
                    const subscriptions = results[0];

                    subscriptions.forEach(resultSubscription => {

//                      If subscription exists then the flag is set to true
                        if (resultSubscription.name.indexOf(subscriber) > -1) {
                            existingSubscriber = true;
                        }
                    });
//                  If there are existing subscriptions, listen for new messages. If none exist, create one
                    if (!existingSubscriber) {
                        createSubscription();
                    } else {
                        onSubscription();
                    }
                }
            )
            .catch(err => {
                console.error('GET SUBSCRIPTIONS ERROR:', err);
            });
    }

//  Creates a new topic if there is no existing topic given
    function createTopic() {
//  Creates a new topic
        pubsub
            .createTopic(topic)
            .then(results => {
                const topicResult = results[0];
                console.log(`Topic ${topicResult} created.`);

//              Once the topic is created, check for subscriptions
                checkSubscriptions();
            })
            .catch(err => {
                console.error('ERROR while creating topic:', err);
            });
    }

//  Creates a new subscription if there is no existing subscriber given
    function createSubscription() {
        pubsub
            .topic(topic)
            .createSubscription(subscriber)
            .then(results => {
                const subscriptionResult = results[0];
                console.log(`Subscription ${subscriptionResult} created.`);
//              Once the subscription is created, listen for new messages
                onSubscription();
            })
            .catch(err => {
                console.error('ERROR while creating subscription:', err);
            });
    }

//  Listen for new messages until timeout is hit
    function onSubscription() {
//      Sets up Subscriber
        const subscriptionName = 'projects/' + projectId + '/subscriptions/' + subscriber;
        subscription = pubsub.subscription(subscriptionName);

//      Handles message reception with a callback function
        subscription.on(`message`, messageHandler);
        setTimeout(() => {
            subscription.removeListener('message', messageHandler);
            console.log(`${messageCount} message(s) received.`);
        }, timeout * 1000);
    }

    function upsert(task) {
        datastore
            .upsert(task)
            .then(() => {
                console.log(`Saved ${task.key.name}: ${task.data}`);
            })
            .catch(err => {
                console.error('ERROR:', err);
            });
    }

    function insert(task) {
        datastore
            .insert(task)
            .then(() => {
                console.log(`Saved ${task.key.name}: ${task.data}`);
            })
            .catch(err => {
                console.error('ERROR:', err);
            });

        // Acknowledge receipt of the message
        message.ack();

    }

});
    module.exports = router;