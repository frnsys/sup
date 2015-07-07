import bz2
import json
import random
from itertools import chain


def sample_stream(stream, n):
    """
    Samples n random elements from an iterator or generator.
    from: <http://stackoverflow.com/a/12583436>
    """
    results = []

    # Fill in the first n elements:
    for _ in range(n):
        results.append(stream.__next__())

    # Randomize their positions
    random.shuffle(results)
    for i, v in enumerate(stream, n):
        r = random.randint(0, i)

        # At a decreasing rate, replace random items
        if r < n:
            results[r] = v

    if len(results) < n:
        raise ValueError('Sample larger than population.')
    return results


def bz2_stream(file, encoding='utf-8'):
    """
    A decompressed stream of a bz2 file.
    There's no guarantee that each yielded item is "complete",
    you'll have to handle that on your own.
    """
    dec = bz2.BZ2Decompressor()
    with open(file, 'rb') as f:
        for data in f:
            d = dec.decompress(data)
            if d:
                yield d.decode(encoding)


def json_stream(file, delimiter='\n', encoding='utf-8'):
    """
    Stream JSON objects from a file.
    Each object is demarcated by the specified delimiter (newline by default).
    Can stream directly from bzipped files.
    """
    if file.endswith('.bz2'):
        stream = bz2_stream(file, encoding=encoding)
    else:
        stream = open(file, 'r', encoding=encoding)

    # When streaming from a bz2 file,
    # we may get incomplete items from the stream.
    # So we keep the incomplete part and try to reassemble it with the next piece.
    trailing = ''
    for d in stream:
        for r in d.split(delimiter):
            if trailing:
                r = trailing + r
                trailing = ''
            try:
                yield json.loads(r)
            except ValueError:
                trailing = r


def merge_streams(streams):
    return chain(*streams)
