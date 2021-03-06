var util = require('util'),
    _ = require('underscore'),
    hirelings = require(__dirname + '/../../lib/hirelings');

var self = new hirelings.Worker();

self.on('init', function (config) {
    self.ready();
});

self.on('job', function (job) {
    self.start();
    if (job.cause_error) {
        throw "THIS IS AN ERROR";
    } else if (job.cause_failure) {
        self.failure("THIS IS A FAILURE");
    } else {
        self.progress(1);
        self.progress(2);
        self.progress(3);
        self.success({
            options: self.options,
            job: job
        });
    }
    self.ready();
});
