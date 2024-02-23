#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dbgpt.datasource.rdbms.base import RDBMSDatabase


class TiDBConnect(RDBMSDatabase):
    """Connect TiDB Database fetch MetaData
    Args:
    Usage:
    """

    db_type: str = "tidb"
    db_dialect: str = "tidb"
    driver: str = "mysql+pymysql"

    default_db = ["information_schema", "performance_schema", "sys", "tidb"]
