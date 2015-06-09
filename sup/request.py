"""
Request with retries
"""

from urllib import request, error, parse
from time import sleep

MAX_RETRIES = 6

class MaxRetriesReached(Exception):
    pass

def make_request(url, max_retries=MAX_RETRIES, open_func=None, headers={}):
    """
    Get the response for a given url.

    Make Unicode-safe requests
    which retry on certain errors:

    * 503 Service Unavailable
    * Connection Reset
    * URLError

    Args:
        | url (str)     -- the url to open.
        | max_retries (int)     -- the maximum number of times to retry (optional)
        | open_func (function)  -- the function to use to open the url. Defaults to `urllib.request.urlopen`, but you can pass in a custom opener as well.
    """
    retries = 0

    if open_func is None:
        open_func = request.urlopen

    errs = []
    while retries < max_retries:
        try:
            quoted_url = parse.quote(url, safe="%/:=&?~#+!$,;'@()*[]")
            req = request.Request(quoted_url, headers=headers)
            return open_func(req)

        except error.HTTPError as e:
            if e.code == 503 and retries < max_retries:
                sleep(1*retries)
                retries += 1
                errs.append(e)
            else:
                raise

        except (ConnectionResetError, error.URLError) as e:
            if retries < max_retries:
                sleep(1*retries)
                retries += 1
                errs.append(e)
            else:
                raise

    raise MaxRetriesReached("Maximum retries hit for url {0} (quoted={1}). Errors: {2}".format(url, quoted_url, errs))
