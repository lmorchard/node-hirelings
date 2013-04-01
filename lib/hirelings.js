// node-hirelings
// ==============
//
//
var util = require('util'),
    child_process = require('child_process'),
    events = require('events'),
    _ = require('underscore');

// Leader
// ------
//
// A Leader manages a pool of Hirelings. Jobs can be enqueued and tasked to
// Hirelings. When all the Hirelings in the pool are busy, Jobs are kept in a
// FIFO backlog.
function Leader (options) {
    var self = this;
    events.EventEmitter.call(this);
    this.options = _.defaults(options, {
        concurrency: 8 || Math.ceil(require('os').cpus().length * 1.25)
    });
    this.backlog = [];
    this.hirelings = {};
    var _exit = function () {
        self.exit();
    }
    process.on('SIGINT', _exit);
    process.on('SIGTERM', _exit);
}

util.inherits(Leader, events.EventEmitter);

_.extend(Leader.prototype, {
    
    // ### enqueue
    //
    // Create and enqueue a new Job
    enqueue: function (options) {
        var self = this;
        var job = new Job(self, options);
        process.nextTick(function () {
            self._taskHireling(job);
        });
        return job;
    },

    // ### exit
    //
    // Cause all HirelingProcesses to exit (if any). Call this when you're done
    // with the Leader.
    exit: function () {
        for (pid in this.hirelings) {
            this.hirelings[pid].exit();
        }
    },

    // ### _taskHireling
    //
    // Given a Job, task a HirelingProcess with its execution. If no
    // HirelingProcess is free, push it onto the backlog.
    _taskHireling: function (job) {
        var self = this;
        var hireling = self._findFreeHireling();
        if (!hireling) {
            this.backlog.push(job);
        } else {
            hireling.acceptJob(job);
        }
    },

    // ### _findFreeHireling
    //
    // Find an idle HirelingProcess available for a new Job. Spawn a new
    // process if the pool is not yet full. Return null, if the pool is full
    // and completely busy.
    _findFreeHireling: function () {
        for (pid in this.hirelings) {
            var hireling = this.hirelings[pid];
            if (!hireling.job) { return hireling; }
        }
        return this._spawnHireling();
    },

    // ### _spawnHireling
    //
    // Spawn a new HirelingProcess, if the pool is not yet full.
    _spawnHireling: function () {
        var pids = _.keys(this.hirelings);
        if (pids.length >= this.options.concurrency) {
            return null;
        }
        var hp = new HirelingProcess(this, this.options.options);
        this.hirelings[hp.process.pid] = hp;
        return hp;
    },

    // ### _onHirelingExit
    //
    // React to the exit of a HirelingProcess. If the process had a current
    // Job, report the exit as a failure. Either way, drop the HirelingProcess
    // from the pool.
    _onHirelingExit: function (hireling) {
        if (hireling.job) {
            hireling.job.emit('failure', 'exit');
        }
        delete this.hirelings[hireling.process.pid];
        // TODO: Spawn a replacement? Currently wait until a new Job needs one.
    },

    // ### _onJobDone
    //
    // React to the exit of a finished Job. Disassociate the Job from its
    // HirelingProcess to mark it idle, and task a hireling with the next Job
    // in the backlog (if any).
    _onJobDone: function (job) {
        if (job.hireling) {
            job.hireling.job = null;
        }
        if (this.backlog.length) {
            this._taskHireling(this.backlog.shift());
        }
    }

});

// Job
// ===
//
function Job (leader, options) {
    var self = this;
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

// HirelingProcess
// ===============
//
function HirelingProcess (leader, options) {
    var self = this;
    events.EventEmitter.call(this);
    
    this.leader = leader;
    this.options = options;
    this.job = null;

    this.process = child_process.fork(
        leader.options.module,
        [],
        { env: this._getEnvForWorker() }
    );
    this.process.on('exit', function () {
        self.leader._onHirelingExit(self);
    });
    this.process.on('message', function (msg) {
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
        if (this.job) { throw "Hireling already has a job"; }
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

// Hireling
// ========
//
function Hireling (options) {
    var self = this;
    this.options = options;

    events.EventEmitter.call(this);
    
    process.on('message', function (msg) {
        self['_handle_'+msg.op](msg.data);
    });
    process.on('uncaughtException', function (err) {
        self.failure(err);
        process.exit();
    });
}

util.inherits(Hireling, events.EventEmitter);

_.extend(Hireling.prototype, {
    _handle_init: function (data) {
        this.options = _.defaults(
            this.options || {},
            data || {}
        );
        this.emit('init', this.options);
    },
    _handle_job: function (data) {
        process.send({op: 'start'});
        this.emit('job', data);
    }
});

['progress', 'success', 'failure'].forEach(function (name) {
    Hireling.prototype[name] = function (data) {
        process.send({op: name, data: data});
    };
});

module.exports = {
    Leader: Leader,
    Hireling: Hireling
};
