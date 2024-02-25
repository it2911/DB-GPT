#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Any, Iterable, Optional
from urllib.parse import quote
from urllib.parse import quote_plus as urlquote
from dbgpt.datasource.rdbms.base import RDBMSDatabase

from sqlalchemy import text

class TiDBConnect(RDBMSDatabase):
    """Connect TiDB Database fetch MetaData
    Args:
    Usage:
    """

    db_type: str = "tidb"
    db_dialect: str = "mysql"
    driver: str = "mysql+pymysql"

    @classmethod
    def from_uri_db(
        cls,
        host: str,
        port: int,
        user: str,
        pwd: str,
        db_name: str,
        engine_args: Optional[dict] = None,
        **kwargs: Any,
    ) -> RDBMSDatabase:
        db_url: str = (
            f"{cls.driver}://{quote(user)}:{urlquote(pwd)}@{host}:{str(port)}/{db_name}"
            f"?ssl_ca=/etc/ssl/certs/ca-certificates.crt&ssl_verify_cert=true&ssl_verify_identity=true"
        )
        return cls.from_uri(db_url, engine_args, **kwargs)

    def _sync_tables_from_db(self) -> Iterable[str]:
        table_results = self.session.execute(
            text(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = database()"
            )
        )
        view_results = self.session.execute(
            text(
                "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = database()"
            )
        )
        table_results = set(row[0] for row in table_results)
        view_results = set(row[0] for row in view_results)
        self._all_tables = table_results.union(view_results)
        self._metadata.reflect(bind=self._engine)
        return self._all_tables

    def get_grants(self):
        """TODO."""
        session = self._db_sessions()
        cursor = session.execute(
            text(
                f"""
                SELECT GRANTEE, PRIVILEGE_TYPE
                FROM information_schema.TABLE_PRIVILEGES
                WHERE GRANTEE = CONCAT('''', CURRENT_USER(), '''');"""
            )
        )
        grants = cursor.fetchall()
        return grants

    def get_collation(self):
        """Get collation."""
        try:
            session = self._db_sessions()
            cursor = session.execute(
                text(
                    "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = current_database();"
                )
            )
            collation = cursor.fetchone()[0]
            return collation
        except Exception as e:
            print("TiDB get collation error: ", e)
            return None

    def get_users(self):
        """Get user info."""
        try:
            cursor = self.session.execute(
                text("SELECT * FROM mysql.user;")
            )
            users = cursor.fetchall()
            return [user[0] for user in users]
        except Exception as e:
            print("TiDB get users error: ", e)
            return []

    def get_fields(self, table_name):
        """Get column fields about specified table."""
        session = self._db_sessions()
        cursor = session.execute(
            text(
                f"SELECT  COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE as column_comment \
                FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND table_name = :table_name",
            ),
            {"table_name": table_name},
        )
        fields = cursor.fetchall()
        return [(field[0], field[1], field[2], field[3], field[4]) for field in fields]

    def get_charset(self):
        """Get character_set."""
        session = self._db_sessions()
        cursor = session.execute(
            text(
                "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = database();"
            )
        )
        character_set = cursor.fetchone()[0]
        return character_set

    def get_show_create_table(self, table_name):
        cur = self.session.execute(
            text(
                f"""
            SELECT a.attname as column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
            FROM pg_catalog.pg_attribute a
            WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attnum <= (
                SELECT max(a.attnum)
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{table_name}')
            ) AND a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{table_name}')
                """
            )
        )
        rows = cur.fetchall()

        create_table_query = f"CREATE TABLE {table_name} (\n"
        for row in rows:
            create_table_query += f"    {row[0]} {row[1]},\n"
        create_table_query = create_table_query.rstrip(",\n") + "\n)"

        return create_table_query

    def get_table_comments(self, db_name=None):
        tablses = self.table_simple_info()
        comments = []
        for table in tablses:
            table_name = table[0]
            table_comment = self.get_show_create_table(table_name)
            comments.append((table_name, table_comment))
        return comments

    def get_database_list(self):
        session = self._db_sessions()
        cursor = session.execute(text("SHOW databases;"))
        results = cursor.fetchall()
        return [
            d[0] for d in results if d[0] not in ["template0", "template1", "postgres"]
        ]

    def get_database_names(self):
        session = self._db_sessions()
        cursor = session.execute(text("SHOW databases;"))
        results = cursor.fetchall()
        return [
            d[0] for d in results if d[0] not in ["template0", "template1", "postgres"]
        ]

    def get_current_db_name(self) -> str:
        return self.session.execute(text("SELECT current_database()")).scalar()

    def table_simple_info(self):
        _sql = f"""
            SELECT TABLE_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ') AS schema_info
            FROM (
                SELECT TABLES.TABLE_NAME, COLUMNS.COLUMN_NAME, COLUMNS.ORDINAL_POSITION
                FROM information_schema.TABLES
                JOIN information_schema.COLUMNS ON TABLES.TABLE_SCHEMA = COLUMNS.TABLE_SCHEMA AND TABLES.TABLE_NAME = COLUMNS.TABLE_NAME
                WHERE TABLES.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'metrics_schema', 'inspection_schema')
                ORDER BY TABLES.TABLE_NAME, COLUMNS.ORDINAL_POSITION
            ) AS sub
            GROUP BY TABLE_NAME;
            """
        cursor = self.session.execute(text(_sql))
        results = cursor.fetchall()
        return results

    def get_fields(self, table_name, schema_name="public"):
        """Get column fields about specified table."""
        session = self._db_sessions()
        cursor = session.execute(
            text(
                f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = current_database() AND TABLE_NAME = '{table_name}';
                """
            )
        )
        fields = cursor.fetchall()
        return [(field[0], field[1], field[2], field[3], field[4]) for field in fields]

    def get_indexes(self, table_name):
        """Get table indexes about specified table."""
        session = self._db_sessions()
        cursor = session.execute(
            text(
                f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{table_name}'"
            )
        )
        indexes = cursor.fetchall()
        return [(index[0], index[1]) for index in indexes]