
var kafka = require('kafka-node');

const kafkaHost = 'kafka1:19092';

var producer = null;
var readyFlag = false;

var BaseModel = function () {
};

BaseModel.prototype.produceJob = function (topic, payload, callback) {
    module.exports.getProducer();
    function send() {
        if (!readyFlag) {
            console.log("Waiting to send...")
            setTimeout(send, 2000);
        } else {
            producer.send([
                { topic: topic, messages: JSON.stringify(payload) }
            ], function (err, data) {
                if (err) callback(err);
                else callback();
            });
        }
    }

    send();
};

BaseModel.prototype.getProducer = function () {
    if (producer) {
        return producer;
    } else {
        module.exports.connect();

        producer.on('ready', function () {
            readyFlag = true;
        });
    }
}

BaseModel.prototype.connect = function () {
    const client = new kafka.KafkaClient({ kafkaHost });

    console.log("Connecting to kafka")
    producer = new kafka.Producer(client);

    producer.on('error', function (err) {
        console.log('error', err);
    });

    return producer;
};

module.exports = new BaseModel();