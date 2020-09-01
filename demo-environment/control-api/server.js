const WebSocket = require('ws');
const kafka = require('kafka-node');
const kafkaConsumer = require('./KafkaConsumer');
const kafkaProducer = require('./KafkaProducer');

const wss = new WebSocket.Server({ port: 3030 });
const kafkaHost = 'kafka1:19092';

const consumerTopics = ['alerts', 'control-output-stream']
const eventStream = 'events'
const controlStream = 'control-stream'

function sendToKafka(type, ruleId, ruleVersion, ruleContent) {
  let payload = {
    timestamp: new Date().toISOString(),
    type: type,
    ruleId: ruleId,
    ruleVersion: ruleVersion,
    content: ruleContent,
    controlSchemaVersion: "1",
  }

  kafkaProducer.produceJob(controlStream, payload, (err, data) => {
    if (err) {
      console.log('[kafka-producer]: broker update failed')
    }
  })
}

wss.on('connection', function connection(ws) {
  try {
    startKafkaConsumer()
  } catch (error) {
    console.log(error)
  }

  ws.on('message', function incoming(data) {
    var message = JSON.parse(data)

    switch (message.request) {
      case 'list-rules':
        sendToKafka("QUERY_STATUS")
        break;
      case 'add-rule':
        sendToKafka("ADD_RULE", message.ruleId, message.ruleVersion, message.ruleContent)
        break;
      case 'remove-rule':
        sendToKafka("REMOVE_RULE", message.ruleId, message.ruleVersion)
        break;
      case 'update-rule':
        sendToKafka("ADD_RULE", message.ruleId, message.ruleVersion, message.ruleContent)
        break;

      default:
        console.log(`Unexpected request: ${message.request}`)
        break;
    }
  });
});

function broadcastMessage(stream, data) {
  wss.clients.forEach(function each(client) {
    if (client.readyState === WebSocket.OPEN) {
      var obj = { type: stream, data: data };
      client.send(JSON.stringify(obj));
    }
  });
}

let isConnected = false

function startKafkaConsumer() {
  if (isConnected) return
  isConnected = true

  try {
    const client = new kafka.KafkaClient({ kafkaHost });

    client.loadMetadataForTopics(consumerTopics, (err, resp) => {
      //console.log(JSON.stringify(resp))
    });

    kafkaConsumer.initiateKafkaConsumerGroup(
      'control-api',
      consumerTopics,
      message => {
        broadcastMessage(message.topic, message.value)
      }
    )
  } catch (error) {
    console.log(err)
    isConnected = false
  }
}

function sendEvent(colour) {
  let payload = {
    '@timestamp': new Date().toISOString(),
    colour: colour,
  }

  kafkaProducer.produceJob(eventStream, payload, (err, data) => {
    if (err) {
      console.log('[kafka-producer]: broker update failed')
    }
  })
  broadcastMessage(eventStream, JSON.stringify(payload))
}

setTimeout(function() {
  setInterval(eventGenerator, 5000)
}, 20000)
function eventGenerator() {
  sendEvent('Blue')
  sendEvent('Red')
}