import bz2
import json
import random
from itertools import islice, chain


def sample_stream(stream, k):
    """
    Reservoir sampling.
    Samples n random elements from an iterator or generator.
    Adapted from: <http://stackoverflow.com/a/12583436>
    """
    # Fill in the first n elements:
    sample = list(islice(stream, k))
    n = k

    if len(sample) < k:
        raise ValueError('Sample larger than population.')

    for item in stream:
        n += 1
        s = int(random.random() * n)
        if s < k:
            sample[s] = item

    return sample


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


def json_stream(file, delimiter='\n', encoding='utf-8', progress=False):
    """
    Stream JSON objects from a file.
    Each object is demarcated by the specified delimiter (newline by default).
    Can stream directly from bzipped files.
    """
    if file.endswith('.bz2'):
        stream = bz2_stream(file, encoding=encoding)
    else:
        stream = open(file, 'r', encoding=encoding).readlines()

    # When streaming from a bz2 file,
    # we may get incomplete items from the stream.
    # So we keep the incomplete part and try to reassemble it with the next piece.
    i = 1
    trailing = ''
    for d in stream:
        for r in d.split(delimiter):
            if trailing:
                r = trailing + r
                trailing = ''
            if not r.strip():
                continue
            try:
                yield json.loads(r)
                if progress and i % 10000 == 0:
                    print(i)
                i += 1
            except ValueError:
                trailing = r


def merge_streams(streams):
    return chain(*streams)
