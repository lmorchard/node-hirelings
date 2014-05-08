var util = require('util'),
    _ = require('underscore'),
    hirelings = require(__dirname + '/../../lib/hirelings');

var self = new hirelings.Worker();

var someAPI = {};

self.on('job', function (job) {
    var value = someAPI.thisMethodDoesNotExist();
    self.success(value);
});
