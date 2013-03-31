var util = require('util'),
    vows = require('vows'),
    assert = require('assert'),
    path = require('path'),
    _ = require('underscore'),
    hirelings = require('../lib/hirelings');

var CONCURRENCY = 4;

var suite = vows.describe('Basic hirelings tests');

var job_result_topic = function (job) {
    var self = this;
    var status = {
        started: false,
        progress: [],
        succeeded: false,
        failed: false,
        errored: false,
        result: null
    };
    job.on('start', function () {
        status.started = true;
    });
    job.on('progress', function (data) {
        status.progress.push(data);
    });
    job.on('success', function (data) {
        status.succeeded = true;
        status.result = data;
        self.callback(null, status);
    });
    job.on('error', function (data) {
        status.errored = true;
        self.callback(null, status);
    });
    job.on('failure', function (data) {
        status.failed = true;
        self.callback(null, status);
    });
};

suite.addBatch({
    'a Leader': {
        topic: function () {
            var leader = new hirelings.Leader({
                concurrency: CONCURRENCY,
                module: __dirname + '/workers/basic.js',
                options: { thing: 'ohai' }
            });
            setTimeout(function () { leader.exit(); }, 10000);
            return leader;
        },
        'can be instantiated': function (leader) {
            assert.ok(leader);
        },
        'can enqueue a Job': {
            '(with success) that': {
                topic: function (leader) {
                    return leader.enqueue({
                        message: 'orly',
                        cause_error: false,
                        cause_failure: false
                    });
                },
                'causes a HirelingProcess': {
                    topic: function (job) {
                        var pids = _.keys(job.leader.hirelings);
                        assert.ok(pids.length > 0);
                        return job.leader.hirelings[pids[0]];
                    },
                    'to be created': function (hp) {
                        assert.ok(hp);
                        assert.ok(hp.process.pid);
                    }
                },
                'can be watched for': {
                    topic: job_result_topic,
                    'start': function (err, status) {
                        assert.ok(status.started);
                    },
                    'progress': function (err, status) {
                        assert.deepEqual([1,2,3], status.progress);
                    },
                    'success': function (err, status) {
                        assert.ok(status.succeeded);
                        var r = status.result;
                        assert.equal('ohai', r.options.thing);
                        assert.equal('orly', r.job.message);
                    }
                }
            },
            '(with error) that': {
                topic: function (leader) {
                    return leader.enqueue({
                        message: 'ohai',
                        cause_error: true,
                        cause_failure: false
                    });
                },
                'can be watched for': {
                    topic: job_result_topic,
                    'error': function (err, status) {
                        assert.ok(status.errored);
                    }
                }
            },
            '(with failure) that': {
                topic: function (leader) {
                    return leader.enqueue({
                        message: 'ohai',
                        cause_error: false,
                        cause_failure: true
                    });
                },
                'can be watched for': {
                    topic: job_result_topic,
                    'failure': function (err, status) {
                        assert.ok(status.failed);
                    }
                }
            }
        }
    }
});

suite.addBatch({
    'a Leader': {
        topic: function () {
            var leader = new hirelings.Leader({
                concurrency: CONCURRENCY,
                module: __dirname + '/workers/sleep.js',
                options: { }
            });
            setTimeout(function () { leader.exit(); }, 1000);
            return leader;
        },
        'can enqueue many Jobs': {
            topic: function (leader) {
                var jobs = [];
                for (var i=0; i<(CONCURRENCY*2); i++) {
                    jobs.push(leader.enqueue({delay: 5000}));
                }
                return jobs;
            },
            'and more Jobs than concurrent Hirelings should result in a backlog': function (jobs) {
                assert.ok(jobs[0].leader.backlog.length > 0);
            },
            'and aborting some jobs': {
                topic: function (jobs) {
                    for (var i=0; i<CONCURRENCY; i++) {
                        jobs.shift().abort();
                    }
                    return jobs;
                },
                'should result in an empty backlog': function (jobs) {
                    assert.equal(jobs[0].leader.backlog.length, 0);
                }
            }
        }
    }
});

// run or export the suite.
if (process.argv[1] === __filename) suite.run();
else suite.export(module);
