"""One module per system under test.

Every runner is a command-line program with the same three subcommands, so that
`run.py` drives five different queues through one code path and a sixth system
is one new file:

    <runner> worker  --concurrency N        run until SIGTERM
    <runner> enqueue --jobs N ... --mode M  submit, print {"seconds": ...}
    <runner> versions                       print the libraries it just used

Each runs as its own process. That is not ceremony: importing Celery,
Dramatiq, RQ and FlexiQ into one interpreter would have them share a Redis
connection pool, a logging config and a signal disposition, and the first
number measured would be an artefact of the import order.
"""
