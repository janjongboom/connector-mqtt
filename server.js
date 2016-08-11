'use strict';

var TOKEN = 'YOUR_ACCESS_TOKEN';

var konekuta = require('../konekuta');
var mosca = require('mosca');
var promisify = require('es6-promisify');
var co = require('co');

var ascoltatore = {
  type: 'redis',
  redis: require('redis'),
  db: 12,
  host: 'localhost',
  port: 6379,
  return_buffers: true, // to handle binary payloads
};

var moscaSettings = {
  port: 1883,
  backend: ascoltatore,
  persistence: {
    factory: mosca.persistence.Redis
  }
};

if (!process.env.TOKEN && TOKEN === 'YOUR_ACCESS_TOKEN') {
  console.error('First set your mbed Device Connector access token in server.js');
  process.exit(1);
}

console.log('Connecting to mbed Device Connector...');

konekuta({
  ignoreEndpointType: true,
  token: process.env.TOKEN,
  deviceModel: {},  // see above
  mapToView: device => device,
  subscribeToAllResources: true,
  verbose: process.argv.indexOf('-v') > -1
}, (err, devices, ee, connector) => {
  if (err) {
    console.error('Error while loading Konekuta', err);
    process.exit(1);
  }

  console.log('Loaded device model... %d devices registered', devices.length);

  var server = new mosca.Server(moscaSettings);

  server.on('ready', () => {
    console.log('MQTT server is up and running at port 1883');
  });

  server.on('error', err => {
    console.error('Error while setting up MQTT server', err);
    process.exit(1);
  });

  server.on('published', co.wrap(function*(packet, client) {
    if (!client) return;
    if (packet.topic.indexOf('$SYS') === 0) return;

    let t = packet.topic.split('/', 2);
    let type = t[0], endpoint = t[1];
    let path = packet.topic.split('/').splice(2).join('/');
    let method = 'putResourceValue';

    if (path.indexOf('/post') === path.length - 5) {
      path = path.substr(0, path.length - 5);
      method = 'postResource';
    }
    else if (path.indexOf('/put') === path.length - 4) {
      path = path.substr(0, path.length - 4);
      method = 'putResourceValue';
    }

    console.log(method, endpoint, path, packet.payload.toString('utf8'));

    try {
      yield promisify(connector[method].bind(connector))(endpoint, path, packet.payload.toString('utf8'));
      console.log(method, 'succeeded', endpoint, path, packet.payload.toString('utf8'));
    }
    catch (ex) {
      console.log(method, 'failed...', ex.toString().trim());
    }
  }));

  ee.on('notification-unmapped-route', (device, path, newValue) => {
    var topic = device.endpointType + '/' + device.endpoint + path;
    var msg = {
      topic: topic,
      payload: newValue,
      qos: 0,
      retain: false
    };

    console.log(`publishing ${topic} ${newValue}`);
    server.publish(msg, function() {
      console.log(`published ${topic} ${newValue}`);
    });
  });
});
