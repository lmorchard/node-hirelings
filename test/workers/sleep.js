var util = require('util'),
    _ = require('underscore'),
    hirelings = require(__dirname + '/../../lib/hirelings');

var self = new hirelings.Worker();

self.on('job', function (job) {
    setTimeout(function () {
        self.success("Done!");
    }, job.delay);
});
