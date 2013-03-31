var util = require('util'),
    child_process = require('child_process'),
    events = require('events'),
    _ = require('underscore');

function Leader (options) {
    var self = this;
    events.EventEmitter.call(this);
    this.options = _.defaults(options, {
        concurrency: 8 || Math.ceil(require('os').cpus().length * 1.25)
    });
    this.backlog = [];
    this.hirelings = {};
    process.on('SIGINT', function () { self.exit(); });
    process.on('SIGTERM', function () { self.exit(); });
}

util.inherits(Leader, events.EventEmitter);

_.extend(Leader.prototype, {
    
    enqueue: function (options) {
        var self = this;
        var job = new Job(self, options);
        process.nextTick(function () {
            self._taskHireling(job);
        });
        return job;
    },

    exit: function () {
        for (pid in this.hirelings) {
            this.hirelings[pid].exit();
        }
    },

    _taskHireling: function (job) {
        var self = this;
        var hireling = self._findFreeHireling();
        if (!hireling) {
            this.backlog.push(job);
        } else {
            hireling.acceptJob(job);
        }
    },

    _findFreeHireling: function () {
        for (pid in this.hirelings) {
            var hireling = this.hirelings[pid];
            if (!hireling.busy) { return hireling; }
        }
        return this._spawnHireling();
    },

    _spawnHireling: function () {
        var pids = _.keys(this.hirelings);
        if (pids.length >= this.options.concurrency) {
            return null;
        }
        var hp = new HirelingProcess(this, this.options.options);
        this.hirelings[hp.process.pid] = hp;
        return hp;
    },

    _onHirelingExit: function (hireling) {
        if (hireling.job) {
            hireling.job.emit('exit');
        }
        delete this.hirelings[hireling.process.pid];
    },

    _onJobDone: function (job) {
        if (job.hireling) {
            job.hireling.busy = false;
        }
        if (this.backlog.length) {
            this._taskHireling(this.backlog.shift());
        }
    }

});

function HirelingProcess (leader, options) {
    var self = this;
    events.EventEmitter.call(this);
    this.leader = leader;
    this.options = options;
    this.busy = false;
    this.job = null;
    this.process = child_process.fork(
        this.leader.options.module,
        [],
        { env: this._getEnvForWorker() }
    );
    this.process.on('exit', function () {
        self.leader._onHirelingExit(self);
    });
    this.process.on('message', function (msg) {
        // Proxy all process messages to the current Job
        if (self.job) { self.job.emit(msg.op, msg.data); }
    });
    this.send({op: 'init', data: this.options});
}

util.inherits(HirelingProcess, events.EventEmitter);

_.extend(HirelingProcess.prototype, {

    exit: function () {
        this.process.kill();
        this.leader._onHirelingExit(this);
    },

    send: function (data) {
        this.process.send(data);
    },

    acceptJob: function (job) {
        if (this.busy) { throw "Hireling already busy working"; }
        this.busy = true;
        this.job = job;
        job.hireling = this;
        this.process.send({op: 'job', data: job.options});
    },

    _getEnvForWorker: function() {
        var env = {};
        for (var i in process.env) {
            env[i] = process.env[i];
        }
        delete env.NODE_WORKER_ID; //Node.js cluster worker marker for v0.6
        delete env.NODE_UNIQUE_ID; //Node.js cluster worker marker for v0.7
        return env;
    }

});

var job_id = 0;

function Job (leader, options) {
    var self = this;
    self.id = (job_id++);
    events.EventEmitter.call(this);
    this.leader = leader;
    this.options = options;
    this.done = false;
    
    var _done = function () {
        if (self.done) { return; }
        self.done = true;
        self.leader._onJobDone(self);
    };

    this.on('success', _done);
    this.on('failure', _done);
    this.on('error', _done);
    this.on('exit', _done);
    this.on('abort', _done);
}

util.inherits(Job, events.EventEmitter);

_.extend(Job.prototype, {
    abort: function () {
        this.emit('abort');
        if (this.hireling) {
            this.hireling.exit();
        }
    }
});

// ## Hireling
function Hireling (options) {
    var self = this;
    this.options = options;
    events.EventEmitter.call(this);
    process.on('message', function (msg) {
        self['_handle_'+msg.op](msg.data);
    });
    process.on('uncaughtException', function (err) {
        self.error(err);
        process.exit();
    });
}

util.inherits(Hireling, events.EventEmitter);

_.extend(Hireling.prototype, {
    _handle_init: function (data) {
        this.options = _.defaults(this.options || {}, data || {});
        this.emit('init', this.options);
    },
    _handle_job: function (data) {
        process.send({op: 'start'});
        this.emit('job', data);
    },
    progress: function (data) {
        process.send({op: 'progress', data: data});
    },
    success: function (data) {
        process.send({op: 'success', data: data});
    },
    failure: function (data) {
        process.send({op: 'failure', data: data});
    },
    error: function (data) {
        process.send({op: 'error', data: data});
    }
});

module.exports = {
    Leader: Leader,
    Hireling: Hireling,
    Job: Job
};
