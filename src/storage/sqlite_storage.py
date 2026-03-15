"""
OpenClaw Mesh - SQLite Storage Layer
完全去中心化 P2P 网络存储层（WAL 模式）
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


# SQLite datetime 适配器（Python 3.12+ 兼容性）
def _adapt_datetime(dt: datetime) -> str:
    """将 datetime 转换为 ISO 格式字符串存储"""
    return dt.isoformat()


def _convert_datetime(s: bytes) -> datetime:
    """将 ISO 格式字符串转换回 datetime"""
    return datetime.fromisoformat(s.decode())


# 注册适配器
sqlite3.register_adapter(datetime, _adapt_datetime)
sqlite3.register_converter("TIMESTAMP", _convert_datetime)


@dataclass
class Node:
    """节点信息数据类"""
    node_id: str
    name: str
    ip: str
    tailscale_hostname: Optional[str] = None
    capabilities: List[str] = None
    public_key: Optional[str] = None
    last_seen: Optional[datetime] = None
    status: str = "offline"  # online/suspect/offline
    version: Optional[str] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.capabilities is None:
            self.capabilities = []
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # 转换 datetime 为 ISO 格式字符串
        for key in ['last_seen', 'created_at']:
            if data[key]:
                data[key] = data[key].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Node':
        # 转换 ISO 格式字符串为 datetime
        for key in ['last_seen', 'created_at']:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key].replace('Z', '+00:00'))
        if data.get('capabilities') and isinstance(data['capabilities'], str):
            try:
                data['capabilities'] = json.loads(data['capabilities'])
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Failed to parse capabilities: {e}, using empty list")
                data['capabilities'] = []
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class QueuedMessage:
    """队列消息数据类"""
    id: Optional[int] = None
    message_id: str = ""
    to_node: str = ""
    message_type: str = ""
    payload: Dict[str, Any] = None
    created_at: Optional[datetime] = None
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None
    status: str = "pending"  # pending/delivered/failed
    
    def __post_init__(self):
        if self.payload is None:
            self.payload = {}
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        for key in ['created_at', 'next_retry_at']:
            if data[key]:
                data[key] = data[key].isoformat()
        if data['payload']:
            data['payload'] = json.dumps(data['payload'])
        return data


@dataclass
class FileTransfer:
    """文件传输记录数据类"""
    transfer_id: str
    filename: str
    size: int
    checksum: str
    from_node: str
    to_node: str
    status: str = "pending"  # pending/transferring/completed/failed
    total_chunks: int = 0
    received_chunks: int = 0
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class Discussion:
    """讨论数据类"""
    id: str
    topic: str
    context: str
    participants: List[str]
    coordinator: str
    status: str = "inviting"  # inviting/discussing/completed
    round: int = 0
    consensus_reached: bool = False
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        for key in ['created_at']:
            if data[key]:
                data[key] = data[key].isoformat()
        data['participants'] = json.dumps(data['participants'])
        return data


class SQLiteStorage:
    """
    SQLite 存储层（WAL 模式）
    
    职责:
    1. 节点信息持久化
    2. 离线消息队列管理
    3. 文件传输记录
    4. 讨论状态管理
    5. 已处理消息去重
    
    特性:
    - WAL 模式支持高并发读写
    - 自动连接管理
    - 事务支持
    """
    
    # 默认配置
    DEFAULT_WAL_MODE = True
    DEFAULT_SYNCHRONOUS = "NORMAL"
    DEFAULT_CACHE_SIZE = 10000
    DEFAULT_BUSY_TIMEOUT = 5000  # 毫秒
    
    # 队列大小限制
    MAX_QUEUE_SIZE_PER_NODE = 1000
    
    def __init__(self, db_path: str, wal_mode: bool = True):
        """
        初始化 SQLite 存储
        
        Args:
            db_path: 数据库文件路径
            wal_mode: 是否启用 WAL 模式（默认启用）
        """
        self.db_path = Path(db_path)
        self.wal_mode = wal_mode
        self._initialized = False
        
        # 确保目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 初始化数据库
        self._init_database()
    
    def _init_database(self):
        """初始化数据库连接和配置"""
        # 使用 detect_types 启用转换器
        conn = sqlite3.connect(str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES)
        try:
            # 启用 WAL 模式
            if self.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
                # 使用安全的参数化方式（PRAGMA 不支持参数绑定，使用硬编码）
                synchronous_val = self.DEFAULT_SYNCHRONOUS if isinstance(self.DEFAULT_SYNCHRONOUS, str) else "NORMAL"
                cache_size_val = int(self.DEFAULT_CACHE_SIZE) if isinstance(self.DEFAULT_CACHE_SIZE, int) else 10000
                conn.execute(f"PRAGMA synchronous={synchronous_val}")
                conn.execute(f"PRAGMA cache_size={cache_size_val}")
            
            # 设置忙等待超时
            busy_timeout_val = int(self.DEFAULT_BUSY_TIMEOUT) if isinstance(self.DEFAULT_BUSY_TIMEOUT, int) else 5000
            conn.execute(f"PRAGMA busy_timeout={busy_timeout_val}")
            
            # 外键约束
            conn.execute("PRAGMA foreign_keys=ON")
        finally:
            conn.close()
        
        # 创建表结构
        self._create_tables()
        self._initialized = True
        logger.info(f"SQLiteStorage initialized: {self.db_path}")
    
    def _create_tables(self):
        """创建数据库表结构"""
        schema = """
        -- 节点信息表
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            name TEXT,
            ip TEXT UNIQUE,
            tailscale_hostname TEXT,
            capabilities TEXT,  -- JSON array
            public_key TEXT,
            last_seen TIMESTAMP,  -- 使用 detect_types 自动转换
            status TEXT DEFAULT 'offline',
            version TEXT,
            port INTEGER DEFAULT 8443,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 离线消息队列
        CREATE TABLE IF NOT EXISTS message_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            to_node TEXT,
            message_type TEXT,
            payload TEXT,  -- JSON
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 0,
            next_retry_at TIMESTAMP,
            status TEXT DEFAULT 'pending'  -- pending/delivered/failed
        );

        -- 已处理消息（去重）
        CREATE TABLE IF NOT EXISTS processed_messages (
            message_id TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 心跳失败计数
        CREATE TABLE IF NOT EXISTS heartbeat_fails (
            node_id TEXT PRIMARY KEY,
            fail_count INTEGER DEFAULT 0,
            last_fail_at TIMESTAMP,
            FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
        );

        -- 讨论表
        CREATE TABLE IF NOT EXISTS discussions (
            id TEXT PRIMARY KEY,
            topic TEXT,
            context TEXT,
            participants TEXT,  -- JSON array
            coordinator TEXT,
            status TEXT DEFAULT 'inviting',
            round INTEGER DEFAULT 0,
            consensus_reached BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 讨论消息
        CREATE TABLE IF NOT EXISTS discussion_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discussion_id TEXT,
            from_node TEXT,
            content TEXT,
            round INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discussion_id) REFERENCES discussions(id) ON DELETE CASCADE
        );

        -- 文件传输记录
        CREATE TABLE IF NOT EXISTS file_transfers (
            transfer_id TEXT PRIMARY KEY,
            filename TEXT,
            size INTEGER,
            checksum TEXT,
            from_node TEXT,
            to_node TEXT,
            status TEXT DEFAULT 'pending',
            total_chunks INTEGER DEFAULT 0,
            received_chunks INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 索引
        CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
        CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
        CREATE INDEX IF NOT EXISTS idx_nodes_ip ON nodes(ip);
        CREATE INDEX IF NOT EXISTS idx_queue_to_node ON message_queue(to_node, status);
        CREATE INDEX IF NOT EXISTS idx_queue_retry ON message_queue(next_retry_at, status);
        CREATE INDEX IF NOT EXISTS idx_queue_status ON message_queue(status);
        CREATE INDEX IF NOT EXISTS idx_processed_time ON processed_messages(processed_at);
        CREATE INDEX IF NOT EXISTS idx_discussions_status ON discussions(status);
        CREATE INDEX IF NOT EXISTS idx_discussion_messages_discussion ON discussion_messages(discussion_id);
        CREATE INDEX IF NOT EXISTS idx_file_transfers_status ON file_transfers(status);
        """
        
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.executescript(schema)
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    # ==================== 节点管理 ====================
    
    def upsert_node(self, node: Node) -> bool:
        """
        插入或更新节点信息
        
        Args:
            node: 节点信息
            
        Returns:
            是否成功
        """
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO nodes (node_id, name, ip, tailscale_hostname, 
                                     capabilities, public_key, last_seen, status, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        name = excluded.name,
                        ip = excluded.ip,
                        tailscale_hostname = excluded.tailscale_hostname,
                        capabilities = excluded.capabilities,
                        public_key = excluded.public_key,
                        last_seen = excluded.last_seen,
                        status = excluded.status,
                        version = excluded.version
                """, (
                    node.node_id, node.name, node.ip, node.tailscale_hostname,
                    json.dumps(node.capabilities) if node.capabilities else '[]',
                    node.public_key, node.last_seen, node.status, node.version
                ))
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to upsert node {node.node_id}: {e}")
            return False
    
    def get_node(self, node_id: str) -> Optional[Node]:
        """根据 ID 获取节点信息"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
            ).fetchone()
            return Node.from_dict(dict(row)) if row else None
    
    def get_node_by_ip(self, ip: str) -> Optional[Node]:
        """根据 IP 获取节点信息"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE ip = ?", (ip,)
            ).fetchone()
            return Node.from_dict(dict(row)) if row else None
    
    def get_all_nodes(self) -> List[Node]:
        """获取所有节点"""
        with self._get_connection() as conn:
            rows = conn.execute("SELECT * FROM nodes").fetchall()
            return [Node.from_dict(dict(row)) for row in rows]
    
    def get_online_nodes(self) -> List[Node]:
        """获取在线节点"""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE status = 'online'"
            ).fetchall()
            return [Node.from_dict(dict(row)) for row in rows]
    
    def update_node_status(self, node_id: str, status: str) -> bool:
        """更新节点状态"""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "UPDATE nodes SET status = ?, last_seen = ? WHERE node_id = ?",
                    (status, datetime.now(timezone.utc), node_id)
                )
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to update node status {node_id}: {e}")
            return False
    
    def delete_node(self, node_id: str) -> bool:
        """删除节点"""
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to delete node {node_id}: {e}")
            return False
    
    # ==================== 消息队列管理 ====================
    
    def enqueue_message(self, message: QueuedMessage) -> bool:
        """
        消息入队（有界队列）
        
        如果队列超过限制，删除最旧的消息
        """
        try:
            with self._get_connection() as conn:
                # 检查队列大小
                count = conn.execute(
                    "SELECT COUNT(*) FROM message_queue WHERE to_node = ?",
                    (message.to_node,)
                ).fetchone()[0]
                
                # 如果超过限制，删除最旧的消息
                if count >= self.MAX_QUEUE_SIZE_PER_NODE:
                    conn.execute("""
                        DELETE FROM message_queue 
                        WHERE to_node = ? AND id = (
                            SELECT MIN(id) FROM message_queue WHERE to_node = ?
                        )
                    """, (message.to_node, message.to_node))
                
                # 插入新消息
                conn.execute("""
                    INSERT INTO message_queue 
                    (message_id, to_node, message_type, payload, next_retry_at, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    message.message_id,
                    message.to_node,
                    message.message_type,
                    json.dumps(message.payload),
                    message.next_retry_at or datetime.now(timezone.utc),
                    message.status
                ))
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to enqueue message: {e}")
            return False
    
    def get_pending_messages(self, to_node: str, limit: int = 100) -> List[QueuedMessage]:
        """获取指定节点的待处理消息"""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM message_queue 
                WHERE to_node = ? AND status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
            """, (to_node, limit)).fetchall()
            
            messages = []
            for row in rows:
                data = dict(row)
                try:
                    data['payload'] = json.loads(data['payload']) if data['payload'] else {}
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse message payload: {e}, using empty dict")
                    data['payload'] = {}
                messages.append(QueuedMessage(**data))
            return messages
    
    def get_retryable_messages(self, limit: int = 10) -> List[QueuedMessage]:
        """获取可重试的消息（next_retry_at <= now）"""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM message_queue 
                WHERE status = 'pending' AND next_retry_at <= ?
                ORDER BY next_retry_at ASC
                LIMIT ?
            """, (datetime.now(timezone.utc), limit)).fetchall()
            
            messages = []
            for row in rows:
                data = dict(row)
                try:
                    data['payload'] = json.loads(data['payload']) if data['payload'] else {}
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse message payload: {e}, using empty dict")
                    data['payload'] = {}
                messages.append(QueuedMessage(**data))
            return messages
    
    def update_message_status(self, message_id: str, status: str, 
                             retry_count: Optional[int] = None,
                             next_retry_at: Optional[datetime] = None) -> bool:
        """更新消息状态"""
        try:
            with self._get_connection() as conn:
                if retry_count is not None and next_retry_at is not None:
                    conn.execute("""
                        UPDATE message_queue 
                        SET status = ?, retry_count = ?, next_retry_at = ?
                        WHERE message_id = ?
                    """, (status, retry_count, next_retry_at, message_id))
                else:
                    conn.execute(
                        "UPDATE message_queue SET status = ? WHERE message_id = ?",
                        (status, message_id)
                    )
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to update message status: {e}")
            return False
    
    def delete_message(self, message_id: str) -> bool:
        """删除消息"""
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM message_queue WHERE message_id = ?", (message_id,))
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to delete message: {e}")
            return False
    
    def get_queue_stats(self, to_node: Optional[str] = None) -> Dict[str, int]:
        """获取队列统计信息"""
        with self._get_connection() as conn:
            if to_node:
                row = conn.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) as delivered,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                    FROM message_queue WHERE to_node = ?
                """, (to_node,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) as delivered,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                    FROM message_queue
                """).fetchone()
            
            return {
                'total': row[0] or 0,
                'pending': row[1] or 0,
                'delivered': row[2] or 0,
                'failed': row[3] or 0
            }
    
    # ==================== 已处理消息（去重） ====================
    
    def is_message_processed(self, message_id: str) -> bool:
        """检查消息是否已处理"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_messages WHERE message_id = ?",
                (message_id,)
            ).fetchone()
            return row is not None
    
    def mark_message_processed(self, message_id: str) -> bool:
        """标记消息已处理"""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_messages (message_id) VALUES (?)",
                    (message_id,)
                )
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to mark message processed: {e}")
            return False
    
    def cleanup_processed_messages(self, retention_hours: int = 168) -> int:
        """
        清理过期的已处理消息记录
        
        Args:
            retention_hours: 保留时间（小时），默认 7 天
            
        Returns:
            删除的记录数
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM processed_messages WHERE processed_at < datetime('now', '-{} hours')".format(retention_hours)
                )
                conn.commit()
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Failed to cleanup processed messages: {e}")
            return 0
    
    # ==================== 心跳失败计数 ====================
    
    def increment_heartbeat_fail(self, node_id: str) -> int:
        """增加心跳失败计数，返回当前计数"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO heartbeat_fails (node_id, fail_count, last_fail_at)
                    VALUES (?, 1, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        fail_count = fail_count + 1,
                        last_fail_at = excluded.last_fail_at
                """, (node_id, datetime.now(timezone.utc)))
                conn.commit()
                
                row = conn.execute(
                    "SELECT fail_count FROM heartbeat_fails WHERE node_id = ?",
                    (node_id,)
                ).fetchone()
                return row[0] if row else 0
        except sqlite3.Error as e:
            logger.error(f"Failed to increment heartbeat fail: {e}")
            return 0
    
    def reset_heartbeat_fail(self, node_id: str) -> bool:
        """重置心跳失败计数"""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "DELETE FROM heartbeat_fails WHERE node_id = ?",
                    (node_id,)
                )
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to reset heartbeat fail: {e}")
            return False
    
    def get_heartbeat_fail_count(self, node_id: str) -> int:
        """获取心跳失败计数"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT fail_count FROM heartbeat_fails WHERE node_id = ?",
                (node_id,)
            ).fetchone()
            return row[0] if row else 0
    
    # ==================== 文件传输 ====================
    
    def create_file_transfer(self, transfer: FileTransfer) -> bool:
        """创建文件传输记录"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO file_transfers 
                    (transfer_id, filename, size, checksum, from_node, to_node, 
                     status, total_chunks, received_chunks)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    transfer.transfer_id, transfer.filename, transfer.size,
                    transfer.checksum, transfer.from_node, transfer.to_node,
                    transfer.status, transfer.total_chunks, transfer.received_chunks
                ))
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to create file transfer: {e}")
            return False
    
    def update_file_transfer_status(self, transfer_id: str, status: str,
                                    received_chunks: Optional[int] = None) -> bool:
        """更新文件传输状态"""
        try:
            with self._get_connection() as conn:
                if received_chunks is not None:
                    conn.execute("""
                        UPDATE file_transfers 
                        SET status = ?, received_chunks = ?
                        WHERE transfer_id = ?
                    """, (status, received_chunks, transfer_id))
                else:
                    conn.execute(
                        "UPDATE file_transfers SET status = ? WHERE transfer_id = ?",
                        (status, transfer_id)
                    )
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to update file transfer: {e}")
            return False
    
    def get_file_transfer(self, transfer_id: str) -> Optional[FileTransfer]:
        """获取文件传输记录"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM file_transfers WHERE transfer_id = ?",
                (transfer_id,)
            ).fetchone()
            if row:
                data = dict(row)
                return FileTransfer(**data)
            return None
    
    # ==================== 讨论管理 ====================
    
    def create_discussion(self, discussion: Discussion) -> bool:
        """创建讨论"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO discussions 
                    (id, topic, context, participants, coordinator, status, round, consensus_reached)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    discussion.id, discussion.topic, discussion.context,
                    json.dumps(discussion.participants),
                    discussion.coordinator, discussion.status,
                    discussion.round, discussion.consensus_reached
                ))
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to create discussion: {e}")
            return False
    
    def get_discussion(self, discussion_id: str) -> Optional[Discussion]:
        """获取讨论"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM discussions WHERE id = ?",
                (discussion_id,)
            ).fetchone()
            if row:
                data = dict(row)
                try:
                    data['participants'] = json.loads(data['participants']) if data['participants'] else []
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse participants: {e}, using empty list")
                    data['participants'] = []
                return Discussion(**data)
            return None
    
    def update_discussion(self, discussion: Discussion) -> bool:
        """更新讨论"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    UPDATE discussions 
                    SET status = ?, round = ?, consensus_reached = ?
                    WHERE id = ?
                """, (discussion.status, discussion.round, 
                      discussion.consensus_reached, discussion.id))
                conn.commit()
                return conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Failed to update discussion: {e}")
            return False
    
    def add_discussion_message(self, discussion_id: str, from_node: str,
                               content: str, round_num: int) -> bool:
        """添加讨论消息"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO discussion_messages (discussion_id, from_node, content, round)
                    VALUES (?, ?, ?, ?)
                """, (discussion_id, from_node, content, round_num))
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to add discussion message: {e}")
            return False
    
    def get_discussion_messages(self, discussion_id: str) -> List[Dict[str, Any]]:
        """获取讨论的所有消息"""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT from_node, content, round, created_at
                FROM discussion_messages
                WHERE discussion_id = ?
                ORDER BY round, created_at
            """, (discussion_id,)).fetchall()
            return [dict(row) for row in rows]
    
    # ==================== 维护操作 ====================
    
    def vacuum(self) -> bool:
        """执行 VACUUM 优化数据库"""
        try:
            with self._get_connection() as conn:
                conn.execute("VACUUM")
                return True
        except sqlite3.Error as e:
            logger.error(f"Failed to vacuum database: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        with self._get_connection() as conn:
            stats = {}
            
            # 表记录数
            tables = ['nodes', 'message_queue', 'processed_messages', 
                     'heartbeat_fails', 'discussions', 'file_transfers']
            for table in tables:
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                stats[table] = row[0]
            
            # 数据库大小
            stats['db_size_bytes'] = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            # WAL 模式状态
            row = conn.execute("PRAGMA journal_mode").fetchone()
            stats['journal_mode'] = row[0] if row else 'unknown'
            
            return stats
    
    async def close(self):
        """关闭存储（清理资源）"""
        logger.info("SQLiteStorage closed")


# 便捷函数：创建存储实例
def create_storage(db_path: str, wal_mode: bool = True) -> SQLiteStorage:
    """创建 SQLiteStorage 实例"""
    return SQLiteStorage(db_path, wal_mode)
