from temboardagent.postgres import Postgres


# Taken from https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql  # noqa
TABLE_BLOAT_SQL = """
SELECT current_database(), schemaname, tblname, bs*tblpages AS real_size,
  (tblpages-est_tblpages)*bs AS extra_size,
  CASE WHEN tblpages - est_tblpages > 0
    THEN 100 * (tblpages - est_tblpages)/tblpages::float
    ELSE 0
  END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size,
  CASE WHEN tblpages - est_tblpages_ff > 0
    THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
    ELSE 0
  END AS bloat_ratio, is_na
  -- , (pst).free_percent + (pst).dead_tuple_percent AS real_frag
FROM (
  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
    -- , stattuple.pgstattuple(tblid) AS pst
  FROM (
    SELECT
      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
        - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
        - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
    FROM (
      SELECT
        tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
        tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
        coalesce(toast.reltuples, 0) AS toasttuples,
        coalesce(substring(
          array_to_string(tbl.reloptions, ' ')
          FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
          + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
        bool_or(att.atttypid = 'pg_catalog.name'::regtype)
          OR count(att.attname) <> count(s.attname) AS is_na
      FROM pg_attribute AS att
        JOIN pg_class AS tbl ON att.attrelid = tbl.oid
        JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
        LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
          AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
        LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
      WHERE att.attnum > 0 AND NOT att.attisdropped
        AND tbl.relkind = 'r'
      GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
      ORDER BY 2,3
    ) AS s
  ) AS s2
) AS s3
"""  # noqa


INDEX_BTREE_BLOAT_SQL = """
-- WARNING: executed with a non-superuser role, the query inspect only index on tables you are granted to read.
-- WARNING: rows with is_na = 't' are known to have bad statistics ("name" type is not supported).
-- This query is compatible with PostgreSQL 8.2 and after
SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
  bs*(relpages-est_pages)::bigint AS extra_size,
  100 * (relpages-est_pages)::float / relpages AS extra_ratio,
  fillfactor, bs*(relpages-est_pages_ff) AS bloat_size,
  100 * (relpages-est_pages_ff)::float / relpages AS bloat_ratio,
  is_na
  -- , 100-(sub.pst).avg_leaf_density, est_pages, index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, sub.reltuples, sub.relpages -- (DEBUG INFO)
FROM (
  SELECT coalesce(1 +
       ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0 -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
    ) AS est_pages,
    coalesce(1 +
       ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0
    ) AS est_pages_ff,
    bs, nspname, table_oid, tblname, idxname, relpages, fillfactor, is_na
    -- , stattuple.pgstatindex(quote_ident(nspname)||'.'||quote_ident(idxname)) AS pst, index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, reltuples -- (DEBUG INFO)
  FROM (
    SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid, fillfactor,
      ( index_tuple_hdr_bm +
          maxalign - CASE -- Add padding to the index tuple header to align on MAXALIGN
            WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
            ELSE index_tuple_hdr_bm%maxalign
          END
        + nulldatawidth + maxalign - CASE -- Add padding to the data to align on MAXALIGN
            WHEN nulldatawidth = 0 THEN 0
            WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
            ELSE nulldatawidth::integer%maxalign
          END
      )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
      -- , index_tuple_hdr_bm, nulldatawidth -- (DEBUG INFO)
    FROM (
      SELECT
        i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,
        current_setting('block_size')::numeric AS bs, fillfactor,
        CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
          WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
          ELSE 4
        END AS maxalign,
        /* per page header, fixed size: 20 for 7.X, 24 for others */
        24 AS pagehdr,
        /* per page btree opaque data */
        16 AS pageopqdata,
        /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
        CASE WHEN max(coalesce(s.null_frac,0)) = 0
          THEN 2 -- IndexTupleData size
          ELSE 2 + (( 32 + 8 - 1 ) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
        END AS index_tuple_hdr_bm,
        /* data len: we remove null values save space using it fractionnal part from stats */
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
        max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
      FROM pg_attribute AS a
        JOIN (
          SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,
            indrelid, indexrelid, indkey::smallint[] AS attnum,
            coalesce(substring(
              array_to_string(idx.reloptions, ' ')
               from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor
          FROM pg_index
            JOIN pg_class idx ON idx.oid=pg_index.indexrelid
            JOIN pg_class tbl ON tbl.oid=pg_index.indrelid
            JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
          WHERE pg_index.indisvalid AND tbl.relkind = 'r' AND idx.relpages > 0
        ) AS i ON a.attrelid = i.indexrelid
        JOIN pg_stats AS s ON s.schemaname = i.nspname
          AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
          OR   (s.tablename = i.idxname AND s.attname = a.attname))-- stats from functionnal cols
        JOIN pg_type AS t ON a.atttypid = t.oid
      WHERE a.attnum > 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ) AS s1
  ) AS s2
    JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = 'btree'
) AS sub
-- WHERE NOT is_na
ORDER BY 2,3,4
"""  # noqa


SCHEMAS_SQL = """
SELECT n.nspname AS "name",
       pg_size_pretty(schema_size) AS size,
       COALESCE(n_tables, 0) AS n_tables,
       tables_bytes,
       pg_size_pretty(tables_bytes) AS tables_size,
       COALESCE(n_indexes, 0) AS n_indexes,
       indexes_bytes,
       pg_size_pretty(indexes_bytes) AS index_size,
       tbloat.bloat_size AS tables_bloat_bytes,
       pg_size_pretty(tbloat.bloat_size::bigint) AS tables_bloat_size,
       ibloat.bloat_size AS indexes_bloat_bytes,
       pg_size_pretty(ibloat.bloat_size::bigint) AS indexes_bloat_size
FROM pg_catalog.pg_namespace n
-- schema size + tables for the schema (count, size)
-- See https://wiki.postgresql.org/wiki/Schema_Size
LEFT JOIN (
  SELECT schemaname,
         SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT AS schema_size,
         count(*) as n_tables,
         SUM(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT AS tables_bytes
  FROM pg_tables
  GROUP BY schemaname
) AS a
ON a.schemaname = n.nspname
-- indexes for the schema (count, size)
LEFT JOIN (
  SELECT count(*) as n_indexes,
         schemaname,
         SUM(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(indexname)))::BIGINT AS indexes_bytes
  FROM pg_catalog.pg_indexes
  GROUP BY schemaname
) AS indexes
ON indexes.schemaname = n.nspname
LEFT JOIN (
  SELECT SUM(bloat_size) AS bloat_size,
         schemaname
  FROM (
    %s
  ) AS a
  GROUP BY schemaname
) AS tbloat
ON tbloat.schemaname = n.nspname
LEFT JOIN (
  SELECT SUM(bloat_size) AS bloat_size,
         schemaname
  FROM (
    %s
  ) AS a
  GROUP BY schemaname
) AS ibloat
ON ibloat.schemaname = n.nspname
WHERE n.nspname !~ '^pg_'
AND n.nspname <> 'information_schema'
""" % (TABLE_BLOAT_SQL, INDEX_BTREE_BLOAT_SQL)  # noqa


INDEXES_SQL = """
SELECT i.tablename AS tablename,
       i.indexname AS name,
       tablespace,
       x.indnatts AS number_of_columns,
       idx_scan AS scans,
       idx_tup_read,
       idx_tup_fetch,
       indexrelname,
       indisunique,
       i.indexdef AS def,
       total_bytes,
       pg_size_pretty(total_bytes) AS total_size,
       am.amname AS type,
       ibloat.bloat_size AS bloat_bytes,
       pg_size_pretty(ibloat.bloat_size::bigint) AS bloat_size
FROM pg_index x
JOIN (
    SELECT *, oid, pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
) AS c
ON c.oid = x.indexrelid
JOIN pg_catalog.pg_indexes i
ON c.relname = i.indexname
JOIN pg_stat_all_indexes psai
ON x.indexrelid = psai.indexrelid
JOIN pg_am am
ON am.oid = c.relam
JOIN (
    """ + INDEX_BTREE_BLOAT_SQL + """
) AS ibloat
ON ibloat.schemaname = i.schemaname AND ibloat.tblname = i.tablename AND ibloat.idxname = i.indexname
WHERE i.schemaname = '{schema}'
{table_filter}
ORDER BY 1,2
"""  # noqa


def get_postgres(app, database):
    '''
    Same as `app.postgres` but with specific database not the default one.
    '''
    config = dict(**app.config.postgresql)
    config.update(dbname=database)
    return Postgres(**config)


def get_databases(conn):
    query = """
SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
WHERE NOT datistemplate;
    """
    conn.execute(query)
    return conn.get_rows()


def get_database_size(conn):
    query = """
SELECT pg_size_pretty(pg_database_size(current_database())) AS size"""
    conn.execute(query)
    return next(conn.get_rows())


def get_database(conn):
    query = """
SELECT SUM(n_tables) AS n_tables,
       SUM(tables_bytes) as tables_bytes,
       pg_size_pretty(SUM(tables_bytes)) AS tables_size,
       SUM(n_indexes) AS n_indexes,
       SUM(indexes_bytes) AS indexes_bytes,
       pg_size_pretty(SUM(indexes_bytes)) AS index_size,
       SUM(tables_bloat_bytes) AS tables_bloat_bytes,
       pg_size_pretty(SUM(tables_bloat_bytes)::bigint) AS tables_bloat_size,
       SUM(indexes_bloat_bytes) AS indexes_bloat_bytes,
       pg_size_pretty(SUM(indexes_bloat_bytes)::bigint) AS indexes_bloat_size
FROM (%s) a""" % SCHEMAS_SQL
    conn.execute(query)
    return next(conn.get_rows())


def get_schemas(conn):
    query = SCHEMAS_SQL
    conn.execute(query)
    ret = []
    for row in conn.get_rows():
        ret.append(row)
    return ret


def get_schema(conn, schema):
    query = """
SELECT pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT) AS size
FROM pg_tables
WHERE schemaname = '{schema}'
GROUP BY schemaname
"""  # noqa
    conn.execute(query.format(schema=schema))
    try:
        return next(conn.get_rows())
    except StopIteration:
        return {}


def get_tables(conn, schema):
    # taken from https://wiki.postgresql.org/wiki/Disk_Usage
    query = """
SELECT table_name AS name,
       total_bytes,
       index_bytes,
       toast_bytes,
       table_bytes,
       COALESCE(n_indexes, 0) AS n_indexes,
       pg_size_pretty(total_bytes) AS total_size,
       pg_size_pretty(index_bytes) AS index_size,
       pg_size_pretty(toast_bytes) AS toast_size,
       pg_size_pretty(table_bytes) AS table_size,
       tbloat.bloat_size AS bloat_bytes,
       pg_size_pretty(tbloat.bloat_size::bigint) AS bloat_size,
       ibloat.bloat_size AS index_bloat_bytes,
       pg_size_pretty(ibloat.bloat_size::bigint) AS index_bloat_size
FROM (
  SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes,0) AS table_bytes
  FROM (
    SELECT c.oid,nspname AS table_schema,
           relname AS TABLE_NAME,
           c.reltuples AS row_estimate,
           pg_total_relation_size(c.oid) AS total_bytes,
           pg_indexes_size(c.oid) AS index_bytes,
           pg_total_relation_size(reltoastrelid) AS toast_bytes
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE relkind = 'r'
  ) a
) a
LEFT JOIN (
  SELECT count(*) as n_indexes, tablename
  FROM pg_catalog.pg_indexes
  GROUP BY tablename
) AS indexes
ON indexes.tablename = table_name
JOIN (
  """ + TABLE_BLOAT_SQL + """
) AS tbloat
ON tbloat.schemaname = table_schema AND tbloat.tblname = table_name
LEFT JOIN (
  SELECT SUM(bloat_size) AS bloat_size,
         schemaname,
         tblname
  FROM (
    """ + INDEX_BTREE_BLOAT_SQL + """
  ) AS a
  GROUP BY schemaname, tblname
) AS ibloat
ON ibloat.schemaname = table_schema AND ibloat.tblname = table_name
WHERE table_schema = '{}';
    """ # noqa
    ret = {'tables': []}
    conn.execute(query.format(schema))
    for row in conn.get_rows():
        ret['tables'].append(row)
    return ret


def get_schema_indexes(conn, schema):
    ret = {'indexes': []}
    query = INDEXES_SQL
    conn.execute(query.format(schema=schema, table_filter=''))
    for row in conn.get_rows():
        ret['indexes'].append(row)
    return ret


def get_table_indexes(conn, schema, table):
    ret = {'indexes': []}
    query = INDEXES_SQL
    conn.execute(query.format(schema=schema,
                              table_filter="AND i.tablename = '%s'" % table))
    for row in conn.get_rows():
        ret['indexes'].append(row)
    return ret


def get_table(conn, schema, table):
    query = """
SELECT table_name AS name,
       total_bytes,
       index_bytes,
       toast_bytes,
       table_bytes,
       pg_size_pretty(total_bytes) AS total_size,
       pg_size_pretty(index_bytes) AS index_size,
       pg_size_pretty(toast_bytes) AS toast_size,
       pg_size_pretty(table_bytes) AS table_size,
       last_vacuum,
       last_autovacuum,
       last_analyze,
       last_autoanalyze,
       tbloat.bloat_size AS bloat_bytes,
       pg_size_pretty(tbloat.bloat_size::bigint) AS bloat_size,
       ibloat.bloat_size AS index_bloat_bytes,
       pg_size_pretty(ibloat.bloat_size::bigint) AS index_bloat_size
FROM (
  SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes,0) AS table_bytes
  FROM (
    SELECT c.oid,nspname AS table_schema,
           relname AS TABLE_NAME,
           c.reltuples AS row_estimate,
           pg_total_relation_size(c.oid) AS total_bytes,
           pg_indexes_size(c.oid) AS index_bytes,
           pg_total_relation_size(reltoastrelid) AS toast_bytes
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE relkind = 'r'
  ) a
) a
JOIN (
  """ + TABLE_BLOAT_SQL + """
) AS tbloat
ON tbloat.schemaname = table_schema AND tbloat.tblname = table_name
LEFT JOIN (
  SELECT SUM(bloat_size) AS bloat_size,
         schemaname,
         tblname
  FROM (
    """ + INDEX_BTREE_BLOAT_SQL + """
  ) AS a
  GROUP BY schemaname, tblname
) AS ibloat
ON ibloat.schemaname = table_schema AND ibloat.tblname = table_name
JOIN pg_stat_all_tables
ON relname = table_name
WHERE table_schema = '{}'
AND table_name = '{}';
    """
    conn.execute(query.format(schema, table))
    return dict(**next(conn.get_rows()))
