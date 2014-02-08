#!/usr/bin/env python

import re
import commands
import psycopg2

PLUGIN_NAME = 'Postgres Statistics with replication'
PLUGIN_VERSION = '0.9'

CONFIG_PARAMS = [
    ('pg_dbname', True),
    ('pg_username', True),
    ('pg_password', True),
    ('pg_host', True),
    ('pg_port', False),
]

PLUGIN_STATS = [
    'pgVersion',
    'pgIsSlave',
    'pgSecondsBehindMaster',
    'pgMaxConnections',
    'pgCurrentConnections',
    'pgDetailedConnections',
]

class PostgresStatistics:
    def __init__(self, agentConfig, checksLogger, rawConfig):
        self.agentConfig = agentConfig
        self.checksLogger = checksLogger
        self.rawConfig = rawConfig

        # get config options
        if self.rawConfig.get('Main', False):
            for key, required in CONFIG_PARAMS:
                self.agentConfig[key] = self.rawConfig['Main'].get(key, None)
        else:
            self.checksLogger.error('Missing configuration section (Main) for %s' % PLUGIN_NAME)

        # reset all plugin parameters
        for param in PLUGIN_STATS:
            setattr(self, param, None)

    #---------------------------------------------------------------------------
    def run(self):

        # check for our config variables
        for key, required in CONFIG_PARAMS:
            if required and not self.agentConfig.get(key, False):
                self.checksLogger.error('%s: missing PostgresStatistics variable: %s' % (PLUGIN_NAME, key))
                return False

        # set default port if we need to!
        if not self.agentConfig.get('pg_port'):
            self.agentConfig['pg_port'] = 5432

        # connect
        try:
            db = psycopg2.connect(
                database=self.agentConfig.get('pg_dbname'),
                user=self.agentConfig.get('pg_username'),
                password=self.agentConfig.get('pg_password'),
                port=self.agentConfig.get('pg_port'),
                host=self.agentConfig.get('pg_host')
            )
        except psycopg2.OperationalError, e:
            self.checksLogger.error('%s: Postgres connection error: %s' % (PLUGIN_NAME, e))
            return 2

        # determine if we're a slave
        try:
            cursor = db.cursor()
            cursor.execute("SELECT pg_is_in_recovery();")
            self.pgIsSlave = (cursor.fetchone()[0] == 'f')
        except psycopg2.OperationalError, e:
            self.checksLogger.error('%s: Error when fetching if server is a slave: %s' % (PLUGIN_NAME, e))

        # determine how far behind the master we are
        self.pgSecondsBehindMaster = -1
        if self.pgIsSlave:
            try:
                cursor = db.cursor()
                cursor.execute("select extract(epoch from(now()::timestamp - pg_last_xact_replay_timestamp()::timestamp));")
                self.pgSecondsBehindMaster = cursor.fetchone()[0]
            except psycopg2.OperationalError, e:
                self.checksLogger.error('%s: Error when determining how far behind the master we are: %s' % (PLUGIN_NAME, e))


        # only get our version once
        if self.pgVersion == None:
            try:
                cursor = db.cursor()
                cursor.execute('SELECT VERSION();')
                result = cursor.fetchone()
                self.pgVersion = result[0].split(' ')[1] #PostgreSQL 9.2.6 on x86_64-unknown-linux-gnu, compiled by gcc (Ubuntu/Linaro 4.6.3-1ubuntu5) 4.6.3, 64-bit
            except psycopg2.OperationalError, e:
                self.checksLogger.error('%s: SQL query error when gettin version: %s' % (PLUGIN_NAME, e))

        # get max connections
        try:
            cursor = db.cursor()
            cursor.execute(
                "SHOW max_connections;"
            )
            self.pgMaxConnections = cursor.fetchone()[0]
        except psycopg2.OperationalError, e:
            self.checksLogger.error(
                '%s: Error when fetching max connections: %s' % (PLUGIN_NAME, e)
            )

        # get current connections
        try:
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM pg_stat_activity;")
            self.pgCurrentConnections = cursor.fetchone()[0]
        except psycopg2.OperationalError, e:
            self.checksLogger.error('%s: Error when finding current connections: %s' % (PLUGIN_NAME, e))

        # get current connection usernames
        try:
            cursor = db.cursor()
            cursor.execute("SELECT usename as username, COUNT(*) FROM pg_stat_activity GROUP BY username;")
            self.pgDetailedConnections = cursor.fetchall()
        except psycopg2.OperationalError, e:
            self.checksLogger.error('%s: Error when fetching usernames of current connections: %s' % (PLUGIN_NAME, e))

        stats = {}
        for param in PLUGIN_STATS:
            stats[param] = getattr(self, param, None)
        return stats
