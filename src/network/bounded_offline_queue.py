"""
OpenClaw Mesh - Bounded Offline Queue
离线队列模块：有界队列 + 消息重试机制
"""

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum

# Windows 兼容：使用 win_compat 替代 aiosqlite
if sys.platform == "win32":
    from .win_compat import connect as aiosqlite_connect
    import sqlite3 as aiosqlite  # 用于捕获 IntegrityError
else:
    import aiosqlite
    aiosqlite_connect = aiosqlite.connect

logger = logging.getLogger(__name__)


class QueueStatus(Enum):
    """队列消息状态"""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class QueuedMessage:
    """队列消息数据类"""
    id: int
    message_id: str
    to_node: str
    message_type: str
    payload: dict
    created_at: datetime
    retry_count: int
    next_retry_at: datetime
    status: QueueStatus
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "message_id": self.message_id,
            "to_node": self.to_node,
            "message_type": self.message_type,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "retry_count": self.retry_count,
            "next_retry_at": self.next_retry_at.isoformat(),
            "status": self.status.value,
            "last_error": self.last_error,
        }

    @classmethod
    def from_row(cls, row) -> "QueuedMessage":
        """从数据库行创建"""
        try:
            payload = json.loads(row["payload"]) if row["payload"] else {}
        except (json.JSONDecodeError, ValueError) as e:
            logging.getLogger(__name__).warning(f"Failed to parse payload: {e}, using empty dict")
            payload = {}
        return cls(
            id=row["id"],
            message_id=row["message_id"],
            to_node=row["to_node"],
            message_type=row["message_type"],
            payload=payload,
            created_at=datetime.fromisoformat(row["created_at"]),
            retry_count=row["retry_count"],
            next_retry_at=datetime.fromisoformat(row["next_retry_at"]),
            status=QueueStatus(row["status"]),
            last_error=row["last_error"],
        )


class BoundedOfflineQueue:
    """
    有界离线队列模块

    职责:
    1. 存储待发送的离线消息（有界队列，max_size=10000）
    2. 定时检查目标节点是否上线
    3. 上线后自动同步消息
    4. 指数退避重试策略

    特性:
    - 有界队列（每个目标节点最多 10000 条消息）
    - 指数退避重试
    - 自动清理已送达/失败消息
    """

    # 默认配置
    DEFAULT_MAX_SIZE = 10000  # 每个节点的最大队列大小
    DEFAULT_MAX_RETRIES = 10
    DEFAULT_BASE_DELAY = 30  # 基础延迟（秒）
    DEFAULT_PROCESS_INTERVAL = 30  # 处理间隔（秒）
    DEFAULT_CLEANUP_INTERVAL = 3600  # 清理间隔（秒）

    def __init__(
        self,
        node_id: str,
        db_path: str,
        node_discovery: Any,  # NodeDiscovery 实例
        message_transport: Any,  # MessageTransport 实例
        max_size: int = DEFAULT_MAX_SIZE,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: int = DEFAULT_BASE_DELAY,
        process_interval: int = DEFAULT_PROCESS_INTERVAL,
        cleanup_interval: int = DEFAULT_CLEANUP_INTERVAL,
        on_queue_full: Optional[callable] = None,  # 队列满回调函数
    ):
        """
        初始化离线队列模块

        Args:
            node_id: 本节点 ID
            db_path: SQLite 数据库路径
            node_discovery: 节点发现模块实例
            message_transport: 消息传输模块实例
            max_size: 每个节点的最大队列大小
            max_retries: 最大重试次数
            base_delay: 基础延迟（秒）
            process_interval: 处理间隔（秒）
            cleanup_interval: 清理间隔（秒）
            on_queue_full: 队列满时的回调函数，接收 (to_node, dropped_message) 参数
        """
        self.node_id = node_id
        self.db_path = db_path
        self.node_discovery = node_discovery
        self.message_transport = message_transport
        self.max_size = max_size
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.process_interval = process_interval
        self.cleanup_interval = cleanup_interval
        self.on_queue_full = on_queue_full

        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._db_connection = None
        self._initialized = False
        self._init_lock = asyncio.Lock()

        logger.info(
            f"BoundedOfflineQueue initialized: max_size={max_size}, "
            f"max_retries={max_retries}, base_delay={base_delay}s"
        )

    async def _get_db(self):
        """获取数据库连接（带连接池管理）"""
        if self._db_connection is None:
            self._db_connection = await aiosqlite_connect(self.db_path)
        return self._db_connection

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db_connection:
            await self._db_connection.close()
            self._db_connection = None
        self._initialized = False
        logger.debug("BoundedOfflineQueue database connection closed")

    async def init_database(self):
        """初始化数据库表"""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            db = await self._get_db()
            await db.execute("""
                CREATE TABLE IF NOT EXISTS message_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE,
                    to_node TEXT,
                    message_type TEXT,
                    payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    next_retry_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    last_error TEXT,
                    delivered_at TIMESTAMP
                )
            """)

            # 索引
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_to_node
                ON message_queue(to_node, status)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_retry
                ON message_queue(next_retry_at, status)
            """)
            await db.commit()
            self._initialized = True
            logger.info("Message queue database initialized")

    async def enqueue(self, to_node: str, message: Any) -> bool:
        """
        消息入队

        Args:
            to_node: 目标节点 ID
            message: 消息对象（需要 to_dict 方法）

        Returns:
            是否成功
        """
        db = await self._get_db()
        # 检查队列大小
        cursor = await db.execute(
            "SELECT COUNT(*) FROM message_queue WHERE to_node = ?",
            (to_node,)
        )
        count = (await cursor.fetchone())[0]

        dropped_message = None

        # 如果超过限制，删除最旧的消息
        if count >= self.max_size:
            # 获取将被删除的最旧消息
            cursor = await db.execute("""
                SELECT message_id, payload FROM message_queue
                WHERE to_node = ?
                ORDER BY id ASC LIMIT 1
            """, (to_node,))
            row = await cursor.fetchone()
            if row:
                try:
                    payload = json.loads(row["payload"]) if row["payload"] else {}
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse dropped message payload: {e}, using empty dict")
                    payload = {}
                dropped_message = {
                    "message_id": row["message_id"],
                    "payload": payload
                }

            await db.execute("""
                DELETE FROM message_queue
                WHERE to_node = ? AND id = (
                    SELECT MIN(id) FROM message_queue WHERE to_node = ?
                )
            """, (to_node, to_node))

            # 触发队列满回调
            await self._trigger_queue_full_callback(to_node, dropped_message)

        # 插入新消息
        message_dict = message.to_dict() if hasattr(message, 'to_dict') else message

        try:
            await db.execute("""
                INSERT INTO message_queue
                (message_id, to_node, message_type, payload, next_retry_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                message_dict.get("id", str(hash(str(message_dict)))),
                to_node,
                message_dict.get("type", "unknown"),
                json.dumps(message_dict),
                datetime.now(timezone.utc).isoformat(),
            ))
            await db.commit()

            logger.debug(f"Message queued for {to_node}")
            return True

        except aiosqlite.IntegrityError:
            # 消息 ID 重复
            logger.warning(f"Duplicate message ID, skipping")
            return False

    async def _trigger_queue_full_callback(self, to_node: str, dropped_message: Optional[dict]):
        """
        触发队列满回调

        Args:
            to_node: 目标节点 ID
            dropped_message: 被丢弃的消息信息
        """
        if self.on_queue_full:
            try:
                # 支持同步和异步回调
                if asyncio.iscoroutinefunction(self.on_queue_full):
                    await self.on_queue_full(to_node, dropped_message)
                else:
                    self.on_queue_full(to_node, dropped_message)
            except (TypeError, ValueError, RuntimeError) as e:
                logger.error(f"Queue full callback failed: {e}")
        else:
            # 默认回调：记录 warning 日志
            logger.warning(
                f"Queue full for {to_node}, dropped oldest message: "
                f"{dropped_message['message_id'] if dropped_message else 'unknown'}"
            )

    async def dequeue(self, to_node: str, limit: int = 10) -> List[QueuedMessage]:
        """
        获取待处理的消息

        Args:
            to_node: 目标节点 ID
            limit: 最大数量

        Returns:
            消息列表
        """
        db = await self._get_db()
        cursor = await db.execute("""
            SELECT * FROM message_queue
            WHERE to_node = ? AND status = 'pending'
            AND next_retry_at <= ?
            ORDER BY created_at ASC
            LIMIT ?
        """, (to_node, datetime.now(timezone.utc).isoformat(), limit))

        rows = await cursor.fetchall()
        return [QueuedMessage.from_row(row) for row in rows]

    async def mark_delivered(self, message_id: str):
        """
        标记消息已送达

        Args:
            message_id: 消息 ID
        """
        db = await self._get_db()
        await db.execute("""
            UPDATE message_queue
            SET status = 'delivered', delivered_at = ?
            WHERE message_id = ?
        """, (datetime.now(timezone.utc).isoformat(), message_id))
        await db.commit()

        logger.debug(f"Message marked as delivered: {message_id}")

    async def mark_failed(self, message_id: str, error: str):
        """
        标记消息失败

        Args:
            message_id: 消息 ID
            error: 错误信息
        """
        db = await self._get_db()
        await db.execute("""
            UPDATE message_queue
            SET status = 'failed', last_error = ?
            WHERE message_id = ?
        """, (error, message_id))
        await db.commit()

        logger.warning(f"Message marked as failed: {message_id}, error: {error}")

    async def schedule_retry(self, message_id: str, retry_count: int):
        """
        安排重试（指数退避）

        Args:
            message_id: 消息 ID
            retry_count: 当前重试次数
        """
        if retry_count >= self.max_retries:
            await self.mark_failed(message_id, f"Max retries ({self.max_retries}) exceeded")
            return

        # 指数退避: delay = base_delay * 2^(retry_count-1)
        delay = self.base_delay * (2 ** retry_count)
        next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)

        db = await self._get_db()
        await db.execute("""
            UPDATE message_queue
            SET retry_count = ?, next_retry_at = ?, status = 'pending'
            WHERE message_id = ?
        """, (retry_count, next_retry.isoformat(), message_id))
        await db.commit()

        logger.debug(f"Message {message_id} scheduled for retry {retry_count} at {next_retry}")

    async def get_pending_messages(self, limit: int = 100) -> List[QueuedMessage]:
        """
        获取所有待处理的消息

        Args:
            limit: 最大数量

        Returns:
            消息列表
        """
        db = await self._get_db()
        cursor = await db.execute("""
            SELECT * FROM message_queue
            WHERE status = 'pending' AND next_retry_at <= ?
            ORDER BY next_retry_at ASC
            LIMIT ?
        """, (datetime.now(timezone.utc).isoformat(), limit))

        rows = await cursor.fetchall()
        return [QueuedMessage.from_row(row) for row in rows]

    async def _try_deliver(self, message: QueuedMessage) -> bool:
        """
        尝试投递消息

        Args:
            message: 队列消息

        Returns:
            是否成功
        """
        try:
            # 检查目标节点是否在线
            node = await self.node_discovery.get_node(message.to_node)
            if not node or node.status != "online":
                logger.debug(f"Node {message.to_node} not online, will retry later")
                await self.schedule_retry(message.message_id, message.retry_count + 1)
                return False

            # 检测是否为简单消息（通过 payload 中的 content 字段）
            payload = message.payload
            if isinstance(payload, dict) and "content" in payload:
                # 简单消息：使用 /msg 端点
                content = payload["content"]
                target_ip = payload.get("target_ip", node.ip)
                target_port = payload.get("target_port", node.port)

                # 使用消息传输的简单发送方法
                await self.message_transport._send_simple_message_async(
                    target_ip,
                    target_port,
                    message.message_id,
                    content
                )
            else:
                # 普通消息：使用 /api/messages 端点
                await self.message_transport._send_to_address(
                    node.ip,
                    node.port,
                    payload
                )

            # 标记已送达
            await self.mark_delivered(message.message_id)
            return True

        except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
            logger.warning(f"Delivery failed for {message.message_id}: {e}")
            await self.schedule_retry(message.message_id, message.retry_count + 1)
            return False

    async def _process_queue(self):
        """处理队列循环"""
        while self._running:
            try:
                # 获取待处理的消息
                messages = await self.get_pending_messages(limit=10)

                if messages:
                    logger.debug(f"Processing {len(messages)} queued messages")

                    for message in messages:
                        try:
                            await self._try_deliver(message)
                        except (ConnectionError, OSError, RuntimeError) as e:
                            logger.error(f"Error processing message {message.message_id}: {e}")

            except (OSError, RuntimeError) as e:
                logger.error(f"Queue processing error: {e}")

            await asyncio.sleep(self.process_interval)

    async def _cleanup_queue(self):
        """清理队列循环"""
        while self._running:
            try:
                db = await self._get_db()
                # 删除已送达超过 7 天的消息
                await db.execute("""
                    DELETE FROM message_queue
                    WHERE status = 'delivered'
                    AND delivered_at < datetime('now', '-7 days')
                """)

                # 删除失败超过 30 天的消息
                await db.execute("""
                    DELETE FROM message_queue
                    WHERE status = 'failed'
                    AND created_at < datetime('now', '-30 days')
                """)

                await db.commit()

                # 获取统计信息
                stats = await self.get_stats()
                logger.info(f"Queue cleanup completed: {stats}")

            except (OSError, RuntimeError) as e:
                logger.error(f"Queue cleanup error: {e}")

            await asyncio.sleep(self.cleanup_interval)

    async def get_stats(self) -> dict:
        """
        获取队列统计信息

        Returns:
            统计信息字典
        """
        db = await self._get_db()
        # 总消息数
        cursor = await db.execute("SELECT COUNT(*) FROM message_queue")
        total = (await cursor.fetchone())[0]

        # 各状态数量
        cursor = await db.execute("""
            SELECT status, COUNT(*) FROM message_queue GROUP BY status
        """)
        status_counts = {row[0]: row[1] for row in await cursor.fetchall()}

        # 各节点数量
        cursor = await db.execute("""
            SELECT to_node, COUNT(*) FROM message_queue
            WHERE status = 'pending' GROUP BY to_node
        """)
        node_counts = {row[0]: row[1] for row in await cursor.fetchall()}

        return {
            "total_messages": total,
            "pending": status_counts.get('pending', 0),
            "delivered": status_counts.get('delivered', 0),
            "failed": status_counts.get('failed', 0),
            "pending_by_node": node_counts,
        }

    async def get_queue_size(self, to_node: Optional[str] = None) -> int:
        """
        获取队列大小

        Args:
            to_node: 目标节点 ID（可选）

        Returns:
            队列大小
        """
        db = await self._get_db()
        if to_node:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM message_queue WHERE to_node = ? AND status = 'pending'",
                (to_node,)
            )
        else:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM message_queue WHERE status = 'pending'"
            )

        return (await cursor.fetchone())[0]

    async def clear_queue(self, to_node: Optional[str] = None, status: Optional[str] = None):
        """
        清空队列

        Args:
            to_node: 目标节点 ID（可选）
            status: 状态过滤（可选）
        """
        db = await self._get_db()
        if to_node and status:
            await db.execute(
                "DELETE FROM message_queue WHERE to_node = ? AND status = ?",
                (to_node, status)
            )
        elif to_node:
            await db.execute(
                "DELETE FROM message_queue WHERE to_node = ?",
                (to_node,)
            )
        elif status:
            await db.execute(
                "DELETE FROM message_queue WHERE status = ?",
                (status,)
            )
        else:
            await db.execute("DELETE FROM message_queue")

        await db.commit()

        logger.info(f"Queue cleared: node={to_node}, status={status}")

    async def start(self):
        """启动离线队列"""
        if self._running:
            return

        self._running = True

        # 初始化数据库
        await self.init_database()

        # 启动处理循环
        self._process_task = asyncio.create_task(self._process_queue())

        # 启动清理循环
        self._cleanup_task = asyncio.create_task(self._cleanup_queue())

        logger.info("BoundedOfflineQueue started")

    async def stop(self):
        """停止离线队列"""
        self._running = False

        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        logger.info("BoundedOfflineQueue stopped")


class QueueError(Exception):
    """队列错误"""
    pass
