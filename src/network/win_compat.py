"""
OpenClaw Mesh - Windows 兼容层
使用同步 sqlite3 + ThreadPoolExecutor 替代 aiosqlite
"""

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Any, List, Tuple, Union

_executor: Optional[ThreadPoolExecutor] = None

def _get_executor() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite_")
    return _executor

class Row:
    def __init__(self, row: sqlite3.Row):
        self._row = row
    def __getitem__(self, key: Union[str, int]) -> Any:
        return self._row[key]
    def __iter__(self):
        return iter(self._row)
    def __len__(self) -> int:
        return len(self._row)
    def keys(self) -> List[str]:
        return [key for key in self._row.keys()]
    def get(self, key: str, default: Any = None) -> Any:
        """支持 dict.get() 风格的访问"""
        try:
            return self._row[key]
        except (KeyError, IndexError):
            return default

class Cursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor
    async def execute(self, sql: str, parameters: Tuple = ()) -> 'Cursor':
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(_get_executor(), lambda: self._cursor.execute(sql, parameters))
        return self
    async def fetchone(self) -> Optional[Row]:
        loop = asyncio.get_event_loop()
        row = await loop.run_in_executor(_get_executor(), self._cursor.fetchone)
        return Row(row) if row else None
    async def fetchall(self) -> List[Row]:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(_get_executor(), self._cursor.fetchall)
        return [Row(row) for row in rows]
    @property
    def lastrowid(self) -> Optional[int]:
        return self._cursor.lastrowid
    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

class Connection:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
    async def _connect(self) -> 'Connection':
        loop = asyncio.get_event_loop()
        def _create():
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        self._conn = await loop.run_in_executor(_get_executor(), _create)
        return self
    async def execute(self, sql: str, parameters: Tuple = ()) -> Cursor:
        loop = asyncio.get_event_loop()
        def _exec():
            return self._conn.execute(sql, parameters)
        cursor = await loop.run_in_executor(_get_executor(), _exec)
        return Cursor(cursor)
    async def commit(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(_get_executor(), self._conn.commit)
    async def close(self):
        if self._conn:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(_get_executor(), self._conn.close)
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

async def connect(db_path: str) -> Connection:
    conn = Connection(db_path)
    await conn._connect()
    return conn
