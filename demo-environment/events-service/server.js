const WebSocket = require('ws')
const kafka = require('kafka-node')
const kafkaConsumer = require('./KafkaConsumer')
const kafkaProducer = require('./KafkaProducer')

const wss = new WebSocket.Server({ port: 3030 })

const kafkaHost = 'kafka1:19092'
const alertStream = 'alerts'
const eventStream = 'events'

wss.on('connection', function connection(ws) {
  try {
    startKafkaConsumer()
  } catch (error) {
    console.log(error)
  }
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

    client.loadMetadataForTopics([alertStream], (err, resp) => {
      //console.log(JSON.stringify(resp))
    });

    kafkaConsumer.initiateKafkaConsumerGroup(
      'event-service',
      [alertStream],
      message => {
        broadcastMessage(message.topic, message.value)
      }
    )
  } catch (error) {
    console.log(error)
    isConnected = false
  }
}

const locations = ['Earth', 'Mars', 'Jupiter', 'Saturn', 'Neptune']

function sendEvent(pilot, eventType, location) {
  let payload = {
    '@timestamp': new Date().toISOString(),
    location: locations[location],
    type: eventType,
    pilot: pilot.name,
    vessel: pilot.ship,
    customer: "PlanEx"
  }

  kafkaProducer.produceJob(eventStream, payload, (err, data) => {
    if (err) {
      console.log('[kafka-producer]: broker update failed')
    }
  })
  broadcastMessage(eventStream, JSON.stringify(payload))
}

[
  { name: 'Kwame Whiteley', ship: 'Decoy' },
  { name: 'Siraj Fields', ship: 'The Primrose' },
  { name: 'Inez Holland', ship: 'Butterfly' },
  { name: 'Yanis Bannister', ship: 'Black Swan' },
  { name: 'Yasser Rojas', ship: 'The Bastion' },
  { name: 'Franklyn Kirk', ship: 'Firebrand' },
  { name: 'Trevor Short', ship: 'Dervish' },
  { name: 'Faizah Knox', ship: 'The Young Prince' },
  { name: 'Chloe-Louise Sheridan', ship: 'Voltaire' },
  { name: 'Vishal Arias', ship: 'Golden Lion' }
].forEach ( pilot => {
  setTimeout(startFlight.bind(null, pilot, 0), 20 * 1000)
});

function startFlight(pilot, location) {
  sendEvent(pilot, 'FLIGHT_START', location)
  setTimeout(endFlight.bind(null, pilot, location), getRandomInt(20, 60) * 1000)
}

function endFlight(pilot, location) {
  var newLocation = 0
  do {
    newLocation = getRandomInt(0,4)
  } while (newLocation == location)
  location = newLocation

  sendEvent(pilot, 'FLIGHT_END', location)
  setTimeout(startFlight.bind(null, pilot, location), getRandomInt(5, 20) * 1000)
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive).
 */
function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}