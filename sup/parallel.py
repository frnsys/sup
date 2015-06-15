import numpy as np
import multiprocessing as mp
from functools import partial
from sup.progress import Progress


def apply_func(func, queue, args_chunk):
    # Apply each group of arguments in a list of arg groups to a func.
    results = []
    for args in args_chunk:
        result = func(*args)
        results.append(result)

        # For progress.
        queue.put(1)

    return results


def parallelize(func, args_set, cpus=0, timeout=10):
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
        cpus = mp.cpu_count() - 1
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
