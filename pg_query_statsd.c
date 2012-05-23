/* -------------------------------------------------------------------------
 *
 * pg_query_statsd.c
 *
 * Copyright (C) 2011, OmniTI Labs
 *
 * IDENTIFICATION
 *  contrib/pg_query_statsd/pg_query_statsd.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "executor/instrument.h"

#include <unistd.h>
#include <errno.h>

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static void initialize_socket(const char *host, const int port);
static void new_executor_start(QueryDesc *queryDesc, int eflags);
static void new_executor_end(QueryDesc *queryDesc);
static void assign_statsd_port(int newval, void *extra);
static void assign_statsd_host(char *newval, void *extra);

/* GUC Variables */
static char *statsd_host;
static int statsd_port = 8125;

/* Globals */
static int sockfd = -1;
static struct addrinfo *serverinfo_list = NULL, *serverinfo = NULL;

#define pg_query_statsd_enabled() \
    (sockfd != -1 && \
    serverinfo != NULL)

#define BUF_SIZE 256

/*
 * Placeholder for any pre-existing hooks
 */

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
    /* Define custom GUC variables */
    DefineCustomStringVariable(
        "pg_query_statsd.hostname",
        gettext_noop("Host to send statsd messages to"),
        NULL,
        &statsd_host,
        "",
        PGC_SIGHUP,
        0,
        NULL,
        &assign_statsd_host,
        NULL);

    DefineCustomIntVariable(
        "pg_query_statsd.port",                             // name
        gettext_noop("Port to send statsd messages to"),    // shor desc
        NULL,                                               // long desc
        &statsd_port,                                       // var addr
        8125,                                               // boot val
        0,                                                  // min val
        16384,                                              // mav val
        PGC_SIGHUP,                                         // context
        0,                                                  // flags
        NULL,                                               // check_hook
        &assign_statsd_port,                                // assign_hook
        NULL);                                              // show_hook

    EmitWarningsOnPlaceholders("pg_query_statsd");

    initialize_socket(statsd_host, statsd_port);

    /* Install Hooks */
    if (ExecutorEnd_hook)
        prev_ExecutorEnd = ExecutorEnd_hook;
    else
        prev_ExecutorEnd = standard_ExecutorEnd;

    ExecutorEnd_hook = new_executor_end;

    if (ExecutorStart_hook)
        prev_ExecutorStart = ExecutorStart_hook;
    else
        prev_ExecutorStart = standard_ExecutorStart;

    ExecutorStart_hook = new_executor_start;
}

/*
 * Module Unload Callback
 */
void
_PG_fini(void)
{
    if (prev_ExecutorStart != standard_ExecutorStart)
        ExecutorStart_hook = prev_ExecutorStart;
    else
        ExecutorStart_hook = NULL;

    if (prev_ExecutorEnd != standard_ExecutorEnd)
        ExecutorEnd_hook = prev_ExecutorEnd;
    else
        ExecutorEnd_hook = NULL;

    if (sockfd != -1)
    {
        close(sockfd);
        sockfd = -1;
    }

    if (serverinfo_list)
    {
        freeaddrinfo(serverinfo_list);
        serverinfo_list = NULL;
    }
    serverinfo = NULL;
}

static void
initialize_socket(const char *host, const int port)
{
    struct addrinfo hints;
    int rv;
    char service[6];

    if (sockfd != -1)
    {
        close(sockfd);
        sockfd = -1;
    }

    if (serverinfo_list)
    {
        freeaddrinfo(serverinfo_list);
        serverinfo_list = NULL;
        serverinfo = NULL;
    }

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;

    snprintf(service, 6, "%d", port); // this is dumb

    if ((rv = getaddrinfo(host, service, &hints, &serverinfo_list)) == 0)
    {
        // loop through all the results and make a socket
        for (serverinfo = serverinfo_list; serverinfo != NULL; serverinfo = serverinfo->ai_next)
        {
            if ((sockfd = socket(serverinfo->ai_family, serverinfo->ai_socktype, serverinfo->ai_protocol)) != -1)
                break;
        }

        if (serverinfo == NULL)
        {
            freeaddrinfo(serverinfo_list);
            serverinfo_list = NULL;
        }
    }
}

static void
new_executor_start(QueryDesc *queryDesc, int eflags)
{
    //Assert(queryDesc != NULL);

    if (pg_query_statsd_enabled() && queryDesc->totaltime == NULL)
        queryDesc->totaltime = InstrAlloc(1,1);

    prev_ExecutorStart(queryDesc,eflags);
}

static void
new_executor_end(QueryDesc *queryDesc)
{
    double query_duration;
    int tuple_count;
    char buffer[BUF_SIZE];

    //Assert(queryDesc != NULL);

    if (pg_query_statsd_enabled() && queryDesc->totaltime)
    {
        InstrEndLoop(queryDesc->totaltime);
        query_duration = queryDesc->totaltime->total * 1000;
        tuple_count = queryDesc->totaltime->ntuples;
        snprintf(buffer, BUF_SIZE, "query:%.3f|ms\ntuples:%d|c", query_duration, tuple_count);
        sendto(sockfd, buffer, strlen(buffer), 0, serverinfo->ai_addr, serverinfo->ai_addrlen);
    }

    prev_ExecutorEnd(queryDesc);
}

static void
assign_statsd_port(int newval, void *extra)
{
    // XXX not sure this is needed
    //statsd_port = newval;
    initialize_socket(statsd_host, statsd_port);
}

static void
assign_statsd_host(char *newval, void *extra)
{
    // XXX not sure this is needed
    //statsd_host = newval;
    initialize_socket(statsd_host, statsd_port);
}
