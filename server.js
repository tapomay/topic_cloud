'use strict';

const express = require('express');
var app = express();
var server = require('http').Server(app);

const kafka = require('kafka-node');
const HighLevelConsumer = kafka.HighLevelConsumer;
const Offset = kafka.Offset;
const Client = kafka.Client;


// PARSE ARGS
var argv = require('yargs')
           .boolean('from_beginning')
           .boolean('verbose').argv;

if(argv.help){
  console.log("--zk=cs185:5181 --dataTopic data --metaTopic meta --port=8080");
  process.exit(code=0)
}

var port = argv.port || 9080;
var zk = argv.zk || 'cs185:5181';
var topiclist = (argv.topic || 'weather_display').split(",");
var fromBeginning = argv.from_beginning || true;
var verbose = argv.verbose || true;
var dataTopic = argv.dataTopic || 'data';
var metaTopic = argv.metaTopic || 'meta';
var topics = [{topic: dataTopic}, {topic: metaTopic}];

// INIT KAFKA
var client = new Client(zk);
var options = { autoCommit: true, fromBeginning: fromBeginning, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
var consumer = new HighLevelConsumer(client, topics, options);
var offset = new Offset(client);

const path = require('path');
const INDEX = path.join(__dirname, 'index.html');

// var server = express()
app.use("/", express.static('public'));
app.use((req, res) => res.sendFile(INDEX) )
server.listen(port, () => console.log(`Listening on ${ port }`));

const io = require('socket.io')(server);
// KAFKA CONSUMER
consumer.on('message', function (message) {
    if(verbose){
      console.log(this.id, message);
    }else{
      //console.log(message.topic);
    }
    try{
      var msg = message.value;
      // try{
      //   msg = JSON.parse(message.value);
      // }catch(e){
      //    //ok it's not json
      // }
      io.emit(message.topic, msg);
    }catch(err){
      console.log('error', err);
    }
});

consumer.on('error', function (err) {
    console.log('error', err);
});

consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});

io.on('connection', (socket) => {
  console.log('Client connected');
  socket.on('disconnect', () => console.log('Client disconnected'));
});

// setInterval(() => io.emit('time', new Date().toTimeString()), 1000);
