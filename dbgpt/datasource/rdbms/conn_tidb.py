#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Any, Iterable, Optional
from urllib.parse import quote
from urllib.parse import quote_plus as urlquote
from dbgpt.datasource.rdbms.base import RDBMSDatabase


class TiDBConnect(RDBMSDatabase):
    """Connect TiDB Database fetch MetaData
    Args:
    Usage:
    """

    db_type: str = "tidb"
    db_dialect: str = "tidb"
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