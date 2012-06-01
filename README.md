pg_query_statsd
===============

pg_query_statsd is a small plugin for PostgreSQL that sends query statistics to a statsd compatible interface. Right now it sends query runtime and total tuples returned.

Installation
------------

    make USE_PGXS=1
    make USE_PGXS=1 install

Configuration
-------------

Add the following to your postgresql.conf

    shared_preload_libraries = 'pg_query_statsd'
    custom_variable_classes = 'pg_query_statsd'
    
    pg_query_statsd.hostname = 'localhost' # The hostname of your statsd server
    #pg_query_statsd.port = 8125           # Default port

You can change the hostname and then reload the config for the changes to take effect.

Output
------

The current output sent to statsd looks something like this:

    query:38.643|ms
    tuples:2|c

Todo
----

 * Backport to PG 9.0 and 8.4
 * Add sampling capability
 * Add GUC's for custom bucket names
 * Add buffer usage stats
