# Python StatsD Client

[StatsD](https://github.com/etsy/statsd) is a stats server that plays with [Graphite](http://graphite.wikidot.com/).  Together,
they collect, aggregate, and show stats. If you don't know what either of those are, well, why are
you still reading this? If you write software or know someone that does, I bet collecting stats will
make your software better, or at the very least give you something to look at and think about.
StatsD makes it really easy to send stats within your code. This client maks it even easier to get
stats out of your python code.

## Install
Annoyed with managing external packages?  There are plenty of statsd clients that come up
under 'pip search statsd'.  Who has time to keep track of tiny dependencies for small projects? Just
copy statsd.py into your project if you're into that kind of thing. No need to depend on some
multi-file package for what should be a simple client. If you're a stickler for dependies, you
probably don't need to know how to install this, but here you go any how:

Clone and install:

    git clone git@github.com:gaelenh/python-statsd-client.git
    cd python-statsd-client
    python setup.py install

Install with pip:

    pip install python-statsd-client

Or like I said above, just copy statsd.py into your code base.

## Usage

### Basic
Setup is easy. By default, the client will connect to localhost on the default statsd port (8125).

    import statsd
    statsd.incr('processed') # Increment processed bucket by 1
    statsd.incr('processed', 5) # This time by 5
    statsd.incr('processed', sample_rate=0.9) # Increment with a sample rate of .9
    statsd.timing('pipeline', 2468.34) # Pipeline took 2468.34 ms to execute

Want to connect to a non-local statsd? Use statsd.init_statsd(settings). Settings is a dict with
any of these keys:

    STATSD_HOST (Default 'localhost'): String host name.
    STATSD_PORT (Default 8125): Integer port number.
    STATSD_SAMPLE_RATE (Default None (same as 1.0)): Integer/Float between 0 and 1.
    STATSD_BUCKET_PREFIX (Default None): String prefix added to all buckets. The code will handle dotting them together.

If you do not want to use init_statsd, you can always pass in your settings when you create the
clients, timers or counters:

    from statsd import StatsdClient
    client = StatsdClient(host='127.0.0.1', port=9999, prefix='app', sample_rate=0.9)

### Counters
Want to count things? Use StatsdCounter:

    import statsd
    statsd.init_statsd({'STATSD_BUCKET_PREFIX': 'photos'})
    counter = statsd.StatsdCounter('processed')
    # calls on counter will send updates to bucket named 'photos.processed'
    counter += 1 # equivalent to counter.incr() or counter.incr(1)
    counter += 5 # equivalent to counter.incr(5)
    counter -= 10 # equivalent to counter.decr(10)

### Timing
Interested in timing? Check out all the ways you can time things:

    import statsd
    statsd.init_statsd({'STATSD_BUCKET_PREFIX': 'photos'})
    timer = statsd.StatsdTimer('pipeline')
    timer.start()
    # Do stuff
    timer.split('stage1') # Sends timing data for bucket 'photos.pipeline.stage1'
    # Do more stuff
    timer.split('stage2') # Sends timing data for bucket 'photos.pipeline.stage2'
    # Do even more stuff
    timer.stop() # Sends timing data for bucket 'photos.pipeline.total'

Timers can be used as decorators too:

    from statsd import StatsdTimer
    @StatsdTimer.wrap('pipeline')
    def process():
        pass
    process() # Sends timing data for bucket 'pipeline.total'

Fancy with statement usage!

    from statsd import StatsdTimer
    with StatsdTimer('photos'):
        pass # Do stuff

Even fancier:

    from statsd import StatsdTimer
    with StatsdTimer('photos') as t:
        # Do stuff
        t.split('stage1')
        # Do more stuff
        t.split('stage2')
        # Finish up

Using timers with decorators or the with statement will still sends stats if an exception is raised
in the code block:

    from statsd import StatsdTimer
    class Foo(object):
        @StatsdTimer('photos')
        def proc(self):
                # Do stuff
                raise ValueError('Whoops')
    f = Foo()
    f.proc() # Raises exception, but sends timing data for bucket 'photos.total-except'


