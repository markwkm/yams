#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <syslog.h>

#include <hiredis/hiredis.h>

#include <json/json.h>

#include <libpq-fe.h>

#define HOSTNAME_LEN 65
#define KEY_LEN 255

#define SQL_LEN 2047

/* Max length a table name can be in PostgreSQL. */
#define TABLENAME_LEN 255

#define CONNINFO_LEN 255

#define INSERT_STATEMENT \
		"INSERT INTO %s\n" \
		"            (time, interval, host, plugin, plugin_instance,\n" \
		"             type, type_instance, dsnames, dstypes, values)\n" \
		"VALUES (TIMESTAMP WITH TIME ZONE 'EPOCH' + " \
		"            %d * INTERVAL '1 SECOND',\n" \
		"        %d, '%s', '%s', '%s', '%s', '%s',\n" \
		"        (SELECT array_agg(trim(BOTH '\"' FROM a::TEXT))\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z),\n" \
		"        (SELECT array_agg(trim(BOTH '\"' FROM a::TEXT))\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z),\n" \
		"        (SELECT array_agg(a::TEXT::NUMERIC)\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z));"

/*
 * Note that nested JSON might not translate to HSTORE but not expecting
 * collectd to nest json.  Also some hoops for convert JSON arrays into
 * Postgres arrays.
 */
#define INSERT_STATEMENT_META \
		"INSERT INTO %s\n" \
		"            (time, interval, host, plugin, plugin_instance,\n" \
		"             type, type_instance, dsnames, dstypes, values, meta)\n" \
		"VALUES (TIMESTAMP WITH TIME ZONE 'EPOCH' + " \
		"        %d * INTERVAL '1 SECOND',\n" \
		"        %d, '%s', '%s', '%s', '%s', '%s',\n" \
		"        (SELECT array_agg(trim(BOTH '\"' FROM a::TEXT))\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z),\n" \
		"        (SELECT array_agg(trim(BOTH '\"' FROM a::TEXT))\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z),\n" \
		"        (SELECT array_agg(a::TEXT::NUMERIC)\n" \
		"         FROM (SELECT json_array_elements('%s'::json) AS a) AS z),\n" \
		"        (SELECT hstore(coalesce(string_agg(a::TEXT, ','), ''))\n" \
		"         FROM (SELECT hstore(ARRAY[key::TEXT, value::TEXT]) AS a\n" \
		"               FROM json_each_text('%s'::JSON)) AS z));"

#define SELECT_DAY0 "SELECT ((TIMESTAMP WITH TIME ZONE 'EPOCH' + %d * " \
		"INTERVAL '1 SECOND') AT TIME ZONE 'UTC')::DATE;"
#define SELECT_DAY1 "SELECT ('%s'::DATE + INTERVAL '1 DAY')::DATE;"

/*
 * Postgres constraint_exclusion needs to be 'on' or 'partition' in order to
 * take advantage of planner optimizations on inherited tables.
 */
#define CREATE_PARTITION_TABLE \
		"CREATE TABLE %s (\n" \
		"    CHECK (time >= '%s'::TIMESTAMP AT TIME ZONE 'UTC'\n" \
		"       AND time < '%s'::TIMESTAMP AT TIME ZONE 'UTC'),\n" \
		"    CHECK (plugin = '%s')\n" \
		") INHERITS(value_list);"

static int loop = 0;

static int verbose_flag = 0;
static int stats_flag = 0;

struct opts
{
	char *redis_server;
	int redis_port;
	char *redis_key;

	/* PostgreSQL connection string. */
	char conninfo[CONNINFO_LEN + 1];

	/* Hold some thread wide stats here too. */
	int pcount;
	int rcount;
};

static inline int create_partition_indexes(PGconn *, const char *, char *);
int create_partition_table(PGconn *, char *, const char *, const char *,
		time_t);
static inline int do_command(PGconn *, char *);
int do_insert(PGconn *, char *);
int load(PGconn *, json_object *);
void *thread_main(void *data);
void usage();
static inline int work(struct opts *);

static inline int create_partition_indexes(PGconn *conn, const char *plugin,
		char *tablename)
{
	char sql[SQL_LEN + 1];

	if (strcmp(plugin, "cpu") == 0) {
		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s (time, host, type_instance, " \
				"plugin_instance);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;
	} else if (strcmp(plugin, "postgresql") == 0) {
		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s (time, host);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;

		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s ((meta->'database')) " \
				"WHERE ((meta -> 'database') IS NOT NULL);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;

		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s ((meta->'schema')) " \
				"WHERE ((meta -> 'schema') IS NOT NULL);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;

		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s ((meta->'table')) " \
				"WHERE ((meta -> 'table') IS NOT NULL);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;

		snprintf(sql, SQL_LEN,
				"CREATE INDEX ON %s ((meta->'index')) " \
				"WHERE ((meta -> 'index') IS NOT NULL);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;
	} else if (strcmp(plugin, "memory") == 0 || strcmp(plugin, "vmem") == 0) {
		snprintf(sql, SQL_LEN, "CREATE INDEX ON %s (time, host);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;
	} else {
		snprintf(sql, SQL_LEN, "CREATE INDEX ON %s (time, host);", tablename);
		if (do_command(conn, sql) != 0)
			return 1;
	}

	snprintf(sql, SQL_LEN,
			"CREATE INDEX ON %s (host);", tablename);
	if (do_command(conn, sql) != 0)
		return 1;

	return 0;
}

int create_partition_table(PGconn *conn, char *tablename, const char *plugin,
		const char *type, time_t timet)
{
	PGresult *res;
	char sql[SQL_LEN + 1];
	char day0_str[11];
	char day1_str[11];

	if (do_command(conn, "BEGIN;") != 0)
		exit(1);

	snprintf(sql, SQL_LEN, SELECT_DAY0, (int) timet);
	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		syslog(LOG_ERR, "SELECT_DAY0 command failed: %s %s",
				PQresultErrorField(res, PG_DIAG_SQLSTATE),
				PQerrorMessage(conn));
		return 1;
	}
	strcpy(day0_str,  PQgetvalue(res, 0, 0));
	PQclear(res);

	snprintf(sql, SQL_LEN, SELECT_DAY1, day0_str);
	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		syslog(LOG_ERR, "SELECT_DAY1 command failed: %s %s",
				PQresultErrorField(res, PG_DIAG_SQLSTATE),
				PQerrorMessage(conn));
		return 1;
	}
	strcpy(day1_str,  PQgetvalue(res, 0, 0));
	PQclear(res);

	snprintf(sql, SQL_LEN, CREATE_PARTITION_TABLE, tablename, day0_str,
			day1_str, plugin);

	if (do_command(conn, sql) != 0)
		return 1;

	/* Addition CHECK constraints for constraint_exclusion by plugin. */
	if (strcmp(plugin, "postgresql") == 0) {
		snprintf(sql, SQL_LEN, "ALTER TABLE %s ADD CHECK (type = '%s');",
				tablename, type);
		if (do_command(conn, sql) != 0)
			return 1;
	}

	create_partition_indexes(conn, plugin, tablename);

	if (do_command(conn, "COMMIT;") != 0)
		return 1;

	return 0;
}

static inline int do_command(PGconn *conn, char *sql)
{
	PGresult *res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		syslog(LOG_ERR, "command failed: %s %s",
				PQresultErrorField(res, PG_DIAG_SQLSTATE),
				PQerrorMessage(conn));
		return 1;
	}
	PQclear(res);
	return 0;
}

int do_insert(PGconn *conn, char *sql)
{
	PGresult *res;

	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		if (strcmp(PQresultErrorField(res, PG_DIAG_SQLSTATE), "42P01") == 0) {
			PQclear(res);
			return 1;
		} else {
			syslog(LOG_WARNING, "INSERT command failed: %s %s",
					PQresultErrorField(res, PG_DIAG_SQLSTATE),
					PQerrorMessage(conn));
		}
	}
	PQclear(res);
	return 0;
}

int load(PGconn *conn, json_object *jsono)
{
	int i;

	struct json_object *jo_t ;

	const char *plugin = NULL;
	const char *plugin_instance = NULL;
	const char *type = NULL;
	const char *type_instance = NULL;
	const char *dsnames = NULL;
	const char *dstypes = NULL;
	const char *values = NULL;
	const char *host = NULL;
	const char *meta = NULL;
	time_t timet;
	int interval;
	struct tm gmtm;

	char sql[SQL_LEN + 1];
	char partition_table[TABLENAME_LEN + 1];

	jo_t = json_object_object_get(jsono, "plugin");
	plugin = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "plugin_instance");
	plugin_instance = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "type");
	type = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "type_instance");
	type_instance = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "dsnames");
	dsnames = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "dstypes");
	dstypes = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "values");
	values = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "time");
	timet = (time_t) json_object_get_int(jo_t);
	gmtime_r(&timet, &gmtm);

	jo_t = json_object_object_get(jsono, "interval");
	interval = json_object_get_int(jo_t);

	jo_t = json_object_object_get(jsono, "host");
	host = json_object_get_string(jo_t);

	jo_t = json_object_object_get(jsono, "meta");
	meta = json_object_get_string(jo_t);

	if (strcmp(plugin, "postgresql") == 0)
		/* Partition postgresql plugin data further by type. */
		snprintf(partition_table, TABLENAME_LEN, "vl_%s_%d%02d%02d_%s", plugin,
				gmtm.tm_year + 1900, gmtm.tm_mon + 1, gmtm.tm_mday, type);
	else
		snprintf(partition_table, TABLENAME_LEN, "vl_%s_%d%02d%02d", plugin,
				gmtm.tm_year + 1900, gmtm.tm_mon + 1, gmtm.tm_mday);

	if (meta == NULL)
		snprintf(sql, SQL_LEN, INSERT_STATEMENT, partition_table, (int) timet,
				interval, host, plugin, plugin_instance, type, type_instance,
				dsnames, dstypes, values);
	else
		snprintf(sql, SQL_LEN, INSERT_STATEMENT_META, partition_table,
				(int) timet, interval, host, plugin, plugin_instance, type,
				type_instance, dsnames, dstypes, values, meta);

	i = do_insert(conn, sql);
	if (i != 0) {
		/* The partition table does not exist, create it. */
		i = create_partition_table(conn, partition_table, plugin, type, timet);
		if (i == 0) {
			i = do_insert(conn, sql);
			if (i != 0) {
				syslog(LOG_ERR, "second insert attempt failed");
				return 1;
			}
		} else {
			do_command(conn, "ROLLBACK;");
			/*
			 * Assume the CREATE TABLE failed because another threads is in the
			 * middle of creating that table.  Wait a few seconds and try the
			 * insert again.
			 */
			sleep(3);
			if (do_insert(conn, sql) != 0)
				syslog(LOG_ERR, "unexpected second insert failure");
			return 1;
		}
	}

	return 0;
}

static void sig_int_handler(int __attribute__((unused)) signal)
{
	loop++;
}

static void sig_term_handler(int __attribute__((unused)) signal)
{
	loop++;
}

void *thread_main(void *data)
{
	work((struct opts *) data);
	return NULL;
}

void usage()
{
	printf("usage: yams-etl --help|-?\n");
	printf("       yams-etl [-f (Don't fork to the background.)]\n");
	printf("                [--pgdatabase <PGDATABASE>]\n");
	printf("                [--pghost <PGHOST>]\n");
	printf("                [--pgport <PGPORT>]\n");
	printf("                [--pgusername <PGUSER>]\n");
	printf("                [--redis-key <key>] (default: yamsetl)\n");
	printf("                [--redis-port <port>] (default: 6379)\n");
	printf("                [--redis-server <host>] (default: localhost)\n");
	printf("                [--verbose]\n");
	printf("                [--stats]\n");
	printf("                [--workers <threads>|-w <threads>]\n");
}

static inline int work(struct opts *options)
{
	redisContext *redis;
	redisReply *reply;

	PGconn *conn;

	int i, count = 0;

	/*
	 * Open a connection to Redis once the FastCGI service starts for
	 * the life of the service.
	 */
	redis = redisConnect(options->redis_server, options->redis_port);
	if (redis->err) {
		printf("yams-etl error: %s\n", redis->errstr);
		exit(1);
	}

	/*
	 * Open a connection to the PostgreSQL data warehouse.
	 */
	conn = PQconnectdb(options->conninfo);
	if (PQstatus(conn) != CONNECTION_OK) {
		syslog(LOG_ERR, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit(1);
	}

	while (loop == 0) {
		struct json_object *jsono;

		/* Pop the POST data from Redis. */
		/* Why doesn't BLPOP return any data? */
		reply = redisCommand(redis, "BLPOP %s 0", options->redis_key);
		if (stats_flag)
			++options->rcount;
		if (reply->elements != 2 || reply->element[1]->str == NULL)
			continue;

		if (verbose_flag)
			syslog(LOG_DEBUG, "processing: %s", reply->element[1]->str);

		/*
		 * json-c doesn't like the extra double quotes added by redis.  Cheat
		 * by skipping the first double quote.
		 */
		jsono = json_tokener_parse(reply->element[1]->str + 1);
		count = json_object_array_length(jsono);
		if (verbose_flag)
			syslog(LOG_DEBUG, "items to convert: %d", count);
		for (i = 0; i < count; i++) {
			if (verbose_flag)
				syslog(LOG_DEBUG, "converting [%d]: %s", i,
						json_object_get_string(
								json_object_array_get_idx(jsono, i)));
			load(conn, json_object_array_get_idx(jsono, i));
			if (stats_flag)
				++options->pcount;
		}
		/* release memory */
		freeReplyObject(reply);
		json_object_put(jsono);
	}

	PQfinish(conn);

	return 0;
}

int main(int argc, char *argv[])
{
	int c;

	pthread_t *threads;
	int workers = 1;

	/* Default values for Redis connection information. */
	const char redis_server[HOSTNAME_LEN + 1] = "localhost";
	const int redis_port = 6379;
	const char redis_key[KEY_LEN + 1] = "yamsetl";

	struct opts options;

	options.rcount = 0;
	options.pcount = 0;
	options.conninfo[0] = '\0';
	options.redis_server = (char *) redis_server;
	options.redis_port = redis_port;
	options.redis_key = (char *) redis_key;

	time_t thistime, lasttime;

	int daemonize = 1;
	pid_t pid;

	struct sigaction sig_int_action;
	struct sigaction sig_term_action;

	while (1) {
		int option_index = 0;
		static struct option long_options[] = {
			{"help", no_argument, NULL, '?'},
			{"pgdatabase", required_argument, NULL, 'D'},
			{"pghost", required_argument, NULL, 'H'},
			{"pgport", required_argument, NULL, 'P'},
			{"pgusername", required_argument, NULL, 'U'},
			{"redis-key", required_argument, NULL, 'd'},
			{"redis-port", required_argument, NULL, 'p'},
			{"redis-server", required_argument, NULL, 's'},
			{"verbose", no_argument, &verbose_flag, 1},
			{"stats", no_argument, &stats_flag, 'v'},
			{"workers", required_argument, NULL, 'w'},
			{0, 0, 0, 0}
		};
		c = getopt_long(argc, argv, "?D:d:fH:P:p:s:U:vw:", long_options,
				&option_index);
		if (c == -1)
			break;

		switch (c) {
		case '?':
			usage();
			exit(0);
		case 'D':
			if (strlen(options.conninfo) > 0)
				strncat(options.conninfo, " ",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, "dbname=",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, optarg,
						CONNINFO_LEN - strlen(options.conninfo));
			break;
		case 'H':
			if (strlen(options.conninfo) > 0)
				strncat(options.conninfo, " ",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, "host=",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, optarg,
						CONNINFO_LEN - strlen(options.conninfo));
			break;
		case 'P':
			if (strlen(options.conninfo) > 0)
				strncat(options.conninfo, " ",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, "port=",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, optarg,
						CONNINFO_LEN - strlen(options.conninfo));
			break;
		case 'U':
			if (strlen(options.conninfo) > 0)
				strncat(options.conninfo, " ",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, "user=",
						CONNINFO_LEN - strlen(options.conninfo));
			strncat(options.conninfo, optarg,
						CONNINFO_LEN - strlen(options.conninfo));
			break;
		case 'd':
			options.redis_key = optarg;
			break;
		case 'f':
			daemonize = 0;
			break;
		case 'p':
			options.redis_port = atoi(optarg);
			break;
		case 's':
			options.redis_server = optarg;
			break;
		case 'v':
			verbose_flag = 1;
			break;
		case 'w':
			workers = atoi(optarg);;
			break;
		}
	}

	if (daemonize) {
		FILE *fh;

		if ((pid = fork()) == -1) {
			fprintf(stderr, "fork: %d\n", errno);
			return 1;
		} else if (pid != 0) {
			return 0;
		}

		setsid();

		if ((fh = fopen("/tmp/yams-etl.pid", "w")) == NULL) {
			fprintf(stderr, "fopen: %d\n", errno);
			return 1;
		}
		fprintf(fh, "%i\n", (int) getpid());
		fclose(fh);

		close(2);
		close(1);
		close(0);
	}

	openlog("yams-etl", LOG_PID | LOG_CONS | LOG_PERROR, LOG_DAEMON);
	syslog(LOG_NOTICE, "starting");

	memset(&sig_int_action, '\0', sizeof (sig_int_action));
	sig_int_action.sa_handler = sig_int_handler;
	if (sigaction(SIGINT, &sig_int_action, NULL) != 0) {
		fprintf(stderr, "INT handler: %d\n", errno);
		return 1;
	}

	memset(&sig_term_action, '\0', sizeof (sig_term_action));
	sig_term_action.sa_handler = sig_term_handler;
	if (sigaction(SIGTERM, &sig_term_action, NULL) != 0) {
		fprintf(stderr, "TERM handler: %d\n", errno);
		return 1;
	}

	threads = (pthread_t *) malloc(sizeof(pthread_t) * workers);

	for (c = 0; c < workers; c++) {
		int ret = pthread_create(&threads[c], NULL, &thread_main,
				(void *) &options);
		if (ret != 0) {
			perror("pthread_create");
			exit(1);
		}
	}

	lasttime = time(NULL);
	while (loop == 0) {
		thistime = time(NULL);

		if (stats_flag && thistime - lasttime >= 60) {
			syslog(LOG_INFO, "blpops:%d inserts:%d", options.rcount,
					options.pcount);
			options.rcount = 0;
			options.pcount = 0;
			lasttime = thistime;
		}
		sleep(60);
	}

	if (daemonize)
		unlink("/tmp/yams-etl.pid");

	syslog(LOG_NOTICE, "stopping");
	closelog();

	return 0;
}
