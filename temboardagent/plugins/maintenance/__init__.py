from datetime import datetime
from pickle import loads as unpickle

from temboardagent.errors import UserError
from temboardagent.routing import RouteSet
from temboardagent.toolkit import taskmanager
from temboardagent.tools import validate_parameters
from temboardagent.types import T_OBJECTNAME

from . import functions


routes = RouteSet(prefix=b'/maintenance')


@routes.get(b'')
def get_instance(http_context, app):
    with app.postgres.connect() as conn:
        rows = functions.get_databases(conn)

    databases = []
    for database in rows:
        # we need to connect with a different database
        dbname = database['datname']
        with functions.get_postgres(app.config, dbname).connect() \
                as conn:
            database.update(**functions.get_database(conn))
        databases.append(database)

    return {'databases': databases}


T_DATABASE_NAME = T_OBJECTNAME
T_SCHEMA_NAME = T_OBJECTNAME
T_TABLE_NAME = T_OBJECTNAME
T_VACUUM_MODE = b'(^(standard|full|freeze|analyze)$)'
T_TIMESTAMP_UTC = b'(^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$)'
T_VACUUM_ID = b'(^[0-9a-f]{8}$)'


@routes.get(b'/%s' % (T_DATABASE_NAME))
def get_database(http_context, app):
    dbname = http_context['urlvars'][0]
    with functions.get_postgres(app.config, dbname).connect() as conn:
        database = functions.get_database_size(conn)
        schemas = functions.get_schemas(conn)
    return dict(database, **{'schemas': schemas})


@routes.get(b'/%s/schema/%s' % (T_DATABASE_NAME, T_SCHEMA_NAME))
def get_schema(http_context, app):
    dbname = http_context['urlvars'][0]
    schema = http_context['urlvars'][1]
    with functions.get_postgres(app.config, dbname).connect() \
            as conn:
        tables = functions.get_tables(conn, schema)
        indexes = functions.get_schema_indexes(conn, schema)
        schema = functions.get_schema(conn, schema)
    return dict(dict(tables, **indexes), **schema)


@routes.get(b'/%s/schema/%s/table/%s' % (T_DATABASE_NAME, T_SCHEMA_NAME,
                                         T_TABLE_NAME))
def get_table(http_context, app):
    dbname = http_context['urlvars'][0]
    schema = http_context['urlvars'][1]
    table = http_context['urlvars'][2]

    with functions.get_postgres(app.config, dbname).connect() \
            as conn:
        ret = functions.get_table(conn, schema, table)
        ret.update(**functions.get_table_indexes(conn, schema, table))
        return ret


@routes.post(b'/vacuum')
def post_vacuum(http_context, app):
    # Parameters format validation
    post = http_context['post']
    validate_parameters(post, [
        ('dbname', T_OBJECTNAME, False),
        ('schema', T_OBJECTNAME, False),
        ('table', T_OBJECTNAME, False),
    ])
    dbname = post['dbname']
    schema = post['schema']
    table = post['table']
    if 'datetime' in post:
        validate_parameters(post, [
            ('datetime', T_TIMESTAMP_UTC, False),
        ])
    dt = post.get('datetime',
                  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    if 'mode' in post:
        validate_parameters(post, [
            ('mode', T_VACUUM_MODE, False),
        ])
    mode = post.get('mode', 'standard')

    with functions.get_postgres(app.config, dbname).connect() as conn:
        return functions.schedule_vacuum(conn, dbname, schema, table, mode,
                                         dt, app)


@routes.get(b'/vacuum/scheduled')
def scheduled_vacuum(http_context, app):
    return functions.list_scheduled_vacuum(app)


@routes.delete(b'/vacuum/' + T_VACUUM_ID)
def delete_vacuum(http_context, app):
    vacuum_id = http_context['urlvars'][0]
    return functions.cancel_scheduled_vacuum(vacuum_id, app)


@taskmanager.worker(pool_size=10)
def vacuum_worker(config, dbname, schema, table, mode):
    config = unpickle(config)

    with functions.get_postgres(config, dbname).connect() \
            as conn:
        return functions.vacuum(conn, dbname, schema, table, mode)


class MaintenancePlugin(object):
    PG_MIN_VERSION = 90400

    def __init__(self, app, **kw):
        self.app = app

    def load(self):
        pg_version = self.app.postgres.fetch_version()
        if pg_version < self.PG_MIN_VERSION:
            msg = "%s is incompatible with Postgres below 9.4" % (
                self.__class__.__name__)
            raise UserError(msg)

        self.app.router.add(routes)
        for route in routes:
            print(route)

    def unload(self):
        self.app.router.remove(routes)
