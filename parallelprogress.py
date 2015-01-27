import os
import sys
import errno
import multiprocessing as mp
from multiprocessing.managers import BaseManager

try:
    from progressbar import (ProgressBar, Percentage,
                             Bar, ETA, SimpleProgress)
except ImportError:
    from conda.progressbar import (ProgressBar, Percentage,
                                   Bar, ETA, SimpleProgress)


class FlushWriter(object):
    """A printer that flushes after write."""
    def write(self, *args, **kwargs):
        sys.stdout.write(*args, **kwargs)
        sys.stdout.flush()


class ProgressUpdater(object):
    """Wrapper around the ProgressBar class to use as a shared
    progress bar.

    Notably, has an update() method that does not require any
    argument so that it can be called by functions that don't
    know where they are in the order of execution. Useful for
    multiprocessing.
    """
    def __init__(self, maxval=None, writer=FlushWriter()):
        widgets = [Percentage(), ' ', Bar(), ' ', ETA(), ' ', SimpleProgress()]
        self.maxval = maxval
        self.pbar = ProgressBar(maxval=maxval, widgets=widgets, fd=writer)

    def start(self):
        self.pbar.start()

    def finish(self):
        self.pbar.finish()

    def update(self, i=None):
        if not i:
            i = self.pbar.currval + 1
        self.pbar.update(i)

    def currval(self):
        return self.pbar.currval


class ProgressManager(BaseManager):
    """Create custom manager"""
    pass


# register the progressbar with the new manager
ProgressManager.register(typeid='ProgressUpdater',
                         callable=ProgressUpdater,
                         exposed=['start', 'finish', 'update', 'currval'])


def parallel_process(function, kwarglist, processors=None):
    """Parallelise execution of a function over a list of arguments.

    Inputs: function - function to apply
            kwarglist - iterator of keyword arguments to apply
                        function to
            processors - number of processors to use, default None
                         is to use 4 times the number of processors

    Returns: an *unordered* list of the return values of the
             function.

    The list of arguments must be formatted as keyword arguments,
    i.e. be a list of dictionaries.
    TODO: can it actually be a generator?
    No because take the len() of it.

    Explicitly splits a list of inputs into chunks and then
    operates on these chunks one at a time.

    I have tried using Pool for this, but it doesn't seem to release
    memory sensibly, despite setting maxtasksperchild. Thus, I've
    used Process and explicitly start and end the jobs.

    This does what it was supposed to which is keep the memory usage
    limited to that needed by the number of calls that can fit into
    the number of processes.  However, it is pretty slow if you have
    processors equal to the number available. If you set it high things
    happen quicker, but the load average can go a bit mad :).
    """
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

    # set processors equal to 4 times the number of physical
    # processors - typically good trade between memory use and
    # waiting for processes to get going
    if not processors:
        processors = mp.cpu_count() * 4

    # shared progressbar
    progress_manager = ProgressManager()
    progress_manager.start()
    N = len(kwarglist)
    pbar = progress_manager.ProgressUpdater(maxval=N)
    pbar.start()

    # The queue for storing the results
    # manager = mp.Manager()
    queue = mp.Queue()
    # TODO: allow arglist as well as kwarglist
    # TODO: allow args and kwargs, where these are lists
    kwargs_list = [dict(a, queue=queue, pbar=pbar) for a in kwarglist]

    outputs = []
    for job in chunker(kwargs_list, processors):
        processes = [mp.Process(target=function, kwargs=kwargs) for kwargs in job]
        # start them all going
        for p in processes:
            p.start()
        # populate the queue, if it was given
        if queue:
            for p in processes:
                outputs.append(queue.get())
        else:
            outputs = None
        # now wait for all the processes in this job to finish
        for p in processes:
            p.join()

    pbar.finish()
    return outputs


def parallel_stub(stub):
    """Decorator to use on functions that are fed to
    parallel_process. Calls the function and appends any output to
    the queue, then updates the progressbar.
    """
    def f(**kwargs):
        pbar = kwargs.pop('pbar')
        queue = kwargs.pop('queue')
        ret = stub(**kwargs)
        queue.put(ret)
        pbar.update()
    return f
