// TODO: Test new Leader events
// TODO: Test backlog vs hireling pool
// TODO: Optionally enforce a max backlog size with immediate job errors
// TODO: Enforce max job execution time
// TODO: Enforce max memory usage, use process.memoryUsage() in hireling to
//       report memory usage to parent?
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
    var events = [];

    var all_events = ['start', 'progress', 'success', 'error', 'failure'];
    var final_events = ['success', 'failure'];

    all_events.forEach(function (name) {
        job.on(name, function (data) {
            events.push([name, data]);
            if (-1 != final_events.indexOf(name)) {
                self.callback(null, events);
            }
        });
    });
};

suite.addBatch({
    'a Leader running an echo worker': {
        topic: function () {
            var leader = new hirelings.Leader({
                concurrency: CONCURRENCY,
                module: __dirname + '/workers/echo.js',
                options: { thing: 'ohai' }
            });
            setTimeout(function () { leader.exit(); }, 20000);
            return leader;
        },
        'can be instantiated': function (leader) {
            assert.isObject(leader);
        },
        'should start with no HirelingProcess instances': function (leader) {
            var pids = _.keys(leader.hirelings);
            assert.equal(pids.length, 0);
        },
        'that enqueues': {
            'a succesful Job': {
                topic: function (leader) {
                    return leader.enqueue({whatsit: 'orly'});
                },
                'should result in at least one HirelingProcess': function (job) {
                    var leader = job.leader;
                    var pids = _.keys(leader.hirelings);
                    assert.ok(pids.length > 0);
                    var hp = leader.hirelings[pids[0]];
                    assert.ok(hp);
                    assert.ok(hp.process.pid);
                },
                'to which event handlers are attached': {
                    topic: job_result_topic,
                    'should result in successful events': function (err, result) {
                        assert.deepEqual(result, [
                            [ 'start', undefined ],
                            [ 'progress', 1 ],
                            [ 'progress', 2 ],
                            [ 'progress', 3 ],
                            [ 'success', {
                                options: { thing: 'ohai' },
                                job: {whatsit: 'orly'}
                            }]
                        ]);
                    }
                }
            },
            'a failing Job': {
                topic: function (leader) {
                    return leader.enqueue({message: 'ohai', cause_failure: true});
                },
                'to which event handlers are attached': {
                    topic: job_result_topic,
                    'should result in a failure event': function (err, result) {
                        assert.deepEqual(result.pop(), 
                            ['failure', 'THIS IS A FAILURE']);
                    }
                }
            }
        }
    }
});

suite.addBatch({
    'a Leader running a sleep worker': {
        topic: function () {
            var leader = new hirelings.Leader({
                concurrency: CONCURRENCY,
                module: __dirname + '/workers/sleep.js',
                options: { }
            });
            setTimeout(function () { leader.exit(); }, 1000);
            return leader;
        },
        'with a Job enqueued and a HirelingProcess later killed': {
            topic: function (leader) {
                var job = leader.enqueue({delay: 500});
                setTimeout(function () {
                    job.hireling.process.kill();
                }, 200);
                return job_result_topic.call(this, job);
            },
            'should gracefully result in a failure': function (result) {
                assert.deepEqual(result.pop(), ['failure', 'exit']);
            }
        },
        'should start with an empty backlog': function (leader) {
            assert.equal(leader.backlog.length, 0);
        },
        'that enqueues more Jobs than available HirelingProcesses': {
            topic: function (leader) {
                var jobs = [];
                for (var i = 0; i < (CONCURRENCY * 2); i++) {
                    jobs.push(leader.enqueue({delay: 5000}));
                }
                return jobs;
            },
            'should result in a backlog': function (jobs) {
                var leader = jobs[0].leader;
                assert.ok(leader.backlog.length > 0);
            },
            'and aborts some Jobs': {
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
