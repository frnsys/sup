import inspect
import numpy as np
import multiprocessing as mp
from itertools import islice
from functools import partial
from sup.progress import Progress


def apply_func(func, queue, args_chunk):
    # `func` could be a class if you want to maintain
    # some internal state per process.
    # Whatever class you use, it must implement the `__call__` method.
    if inspect.isclass(func):
        func = func()

    # Apply each group of arguments in a list of arg groups to a func.
    results = []
    for args in args_chunk:
        result = func(*args)
        results.append(result)

        # For progress.
        queue.put(1)

    return results


def parallelize(func, args_set, cpus=-1, timeout=10):
    """
    Run a function in parallel, with a list of tuples as arguments to pass in each call.

    Example:

    func:

        def func(a):
            return a

    args_set:

        [(1,),(2,),(3,),(4,)]

    Note: If this times out, it may be because you are out of memory.
    """
    if cpus < 1:
        cpus = mp.cpu_count() + cpus
    pool = mp.Pool(processes=cpus)
    print('Running on {0} cores.'.format(cpus))

    # Split args set into roughly equal-sized chunks, one for each core.
    args_chunks = np.array_split(args_set, cpus)

    # Create a queue so we can log everything to a single file.
    manager = mp.Manager()
    queue = manager.Queue()

    # A callback on completion.
    def done(results):
        queue.put(None)

    def error(exception):
        raise exception

    results = pool.map_async(partial(apply_func, func, queue),
                             args_chunks,
                             callback=done,
                             error_callback=error)

    # Print progress.
    p = Progress()
    comp = 0
    n_args = len(args_set)
    p.print_progress(comp/n_args)
    while True:
        msg = queue.get(timeout=timeout)
        p.print_progress(comp/n_args)
        if msg is None:
            break
        comp += msg

    pool.close()
    pool.join()

    # Flatten results.
    return [i for sub in results.get() for i in sub]


def parallelize_stream(func, stream, cpus=-1, timeout=10, expand_args=False):
    """
    If the `stream` iterator returns tuples which should be expanded as arguments
    to `func`, specify `expand_args=True`.
    """
    if cpus < 1:
        cpus = mp.cpu_count() + cpus
    pool = mp.Pool(processes=cpus)
    print('Running on {0} cores.'.format(cpus))

    if expand_args:
        results = pool.starmap(func, stream)
    else:
        results = pool.map(func, stream)

    # Alternatively...
    #for result in pool.imap(func, stream):
        #yield result

    pool.close()
    pool.join()
    return results
