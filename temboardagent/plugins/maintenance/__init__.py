from temboardagent.errors import UserError
from temboardagent.routing import RouteSet

from . import functions as maintenance_functions


routes = RouteSet(prefix=b'/maintenance')


@routes.get(b'')
def get_instance(http_context, app):
    with app.postgres.connect() as conn:
        rows = maintenance_functions.get_databases(conn)

    databases = []
    for database in rows:
        # we need to connect with a different database
        dbname = database['datname']
        with maintenance_functions.get_postgres(app, dbname).connect() as conn:
            database.update(**maintenance_functions.get_database(conn))
        databases.append(database)

    return {'databases': databases}


T_DATABASE_NAME = b'(.*)'
T_SCHEMA_NAME = T_DATABASE_NAME
T_TABLE_NAME = T_DATABASE_NAME


@routes.get(b'/%s' % (T_DATABASE_NAME))
def get_database(http_context, app):
    dbname = http_context['urlvars'][0]
    with maintenance_functions.get_postgres(app, dbname).connect() as conn:
        database = maintenance_functions.get_database_size(conn)
        schemas = maintenance_functions.get_schemas(conn)
    return dict(database, **{'schemas': schemas})


@routes.get(b'/%s/schema/%s' % (T_DATABASE_NAME, T_SCHEMA_NAME))
def get_schema(http_context, app):
    dbname = http_context['urlvars'][0]
    schema = http_context['urlvars'][1]
    with maintenance_functions.get_postgres(app, dbname).connect() as conn:
        tables = maintenance_functions.get_tables(conn, schema)
        indexes = maintenance_functions.get_schema_indexes(conn, schema)
        schema = maintenance_functions.get_schema(conn, schema)
    return dict(dict(tables, **indexes), **schema)


@routes.get(b'/%s/schema/%s/table/%s' % (T_DATABASE_NAME, T_SCHEMA_NAME,
                                         T_TABLE_NAME))
def get_table(http_context, app):
    dbname = http_context['urlvars'][0]
    schema = http_context['urlvars'][1]
    table = http_context['urlvars'][2]

    with maintenance_functions.get_postgres(app, dbname).connect() as conn:
        ret = maintenance_functions.get_table(conn, schema, table)
        ret.update(**maintenance_functions.get_table_indexes(conn, schema,
                                                             table))
        return ret


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
