# sup

A package which includes tools I reuse a lot.

So far, there are:

- `sup.parallel` - for quickly parallelizing functions
- `sup.progress` - a progress bar with estimated completion time
- `sup.request`  - requests with retries
- `sup.logging`  - create loggers to easily log to console, file, or email (on errors)
- `sup.mailer`   - send emails (for notifications mostly)
- `sup.color`    - conveniently print color output to the terminal
- `sup.service`  - easily create multithreaded services (e.g. to run memory-intensive processes separately during parallel processing)


## Installation

Install with:

    $ pip install sup