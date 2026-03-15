"""
OpenClaw Mesh - Heartbeat Manager
心跳管理模块：自适应心跳 + 节点状态管理
"""

import asyncio
import json
import logging
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set

import aiohttp
from typing import Any

# Windows 兼容：使用 win_compat 替代 aiosqlite
if sys.platform == "win32":
    from .win_compat import connect as aiosqlite_connect
else:
    import aiosqlite
    aiosqlite_connect = aiosqlite.connect

from src.constants import VERSION

logger = logging.getLogger(__name__)


@dataclass
class HeartbeatInfo:
    """心跳信息数据类"""
    node_id: str
    timestamp: datetime
    status: str  # online/suspect/offline
    capabilities: List[str]
    version: str
    
    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status,
            "capabilities": self.capabilities,
            "version": self.version,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "HeartbeatInfo":
        return cls(
            node_id=data["node_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            status=data.get("status", "online"),
            capabilities=data.get("capabilities", []),
            version=data.get("version", VERSION),
        )


class HeartbeatManager:
    """
    心跳管理模块
    
    职责:
    1. 自适应心跳（优先 suspect 节点）
    2. 节点状态管理（online/suspect/offline）
    3. 心跳失败检测
    
    特性:
    - 自适应心跳间隔（suspect 节点优先）
    - 指数退避重试
    - 节点状态自动转换
    """
    
    # 默认配置
    DEFAULT_HEARTBEAT_INTERVAL = 30  # 秒
    DEFAULT_SUSPECT_THRESHOLD = 3    # 连续失败次数阈值
    DEFAULT_OFFLINE_THRESHOLD = 6    # 离线阈值
    DEFAULT_PROBE_COUNT = 3          # 每次探测节点数
    DEFAULT_HEALTHY_PROBE_RATIO = 0.1  # 健康节点探测比例
    
    def __init__(
        self,
        node_id: str,
        node_discovery: Any,  # NodeDiscovery 实例
        message_transport: Any,  # MessageTransport 实例
        db_path: str,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        suspect_threshold: int = DEFAULT_SUSPECT_THRESHOLD,
        offline_threshold: int = DEFAULT_OFFLINE_THRESHOLD,
        probe_count: int = DEFAULT_PROBE_COUNT,
        capabilities: Optional[List[str]] = None,
        version: str = VERSION,
    ):
        """
        初始化心跳管理模块
        
        Args:
            node_id: 本节点 ID
            node_discovery: 节点发现模块实例
            message_transport: 消息传输模块实例
            db_path: SQLite 数据库路径
            heartbeat_interval: 心跳间隔（秒）
            suspect_threshold: suspect 阈值（连续失败次数）
            offline_threshold: offline 阈值（连续失败次数）
            probe_count: 每次探测节点数
            capabilities: 本节点能力列表
            version: 版本号
        """
        self.node_id = node_id
        self.node_discovery = node_discovery
        self.message_transport = message_transport
        self.db_path = db_path
        self.heartbeat_interval = heartbeat_interval
        self.suspect_threshold = suspect_threshold
        self.offline_threshold = offline_threshold
        self.probe_count = probe_count
        self.capabilities = capabilities or []
        self.version = version
        
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._db_connection = None
        self._initialized = False
        self._init_lock = asyncio.Lock()
        
        logger.info(
            f"HeartbeatManager initialized: interval={heartbeat_interval}s, "
            f"suspect_threshold={suspect_threshold}, offline_threshold={offline_threshold}"
        )
    
    async def _get_db(self):
        """获取数据库连接（带连接池管理）"""
        if self._db_connection is None:
            self._db_connection = await aiosqlite_connect(self.db_path)
        return self._db_connection
    
    async def close(self):
        """关闭数据库连接"""
        if self._db_connection:
            await self._db_connection.close()
            self._db_connection = None
        self._initialized = False
        logger.debug("HeartbeatManager database connection closed")
    
    async def init_database(self):
        """初始化数据库表"""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            db = await self._get_db()
            # 心跳失败计数表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_fails (
                    node_id TEXT PRIMARY KEY,
                    fail_count INTEGER DEFAULT 0,
                    last_fail_at TIMESTAMP,
                    last_success_at TIMESTAMP,
                    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
                )
            """)
            
            # 心跳历史表（可选，用于分析）
            await db.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    latency_ms INTEGER,
                    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
                )
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_heartbeat_history_node_id 
                ON heartbeat_history(node_id)
            """)
            
            await db.commit()
            self._initialized = True
            logger.info("Heartbeat database initialized")
    
    async def _get_nodes_to_probe(self) -> List[Any]:
        """
        获取需要探测的节点（自适应选择）
        
        Returns:
            节点列表
        """
        # 获取所有已知节点
        all_nodes = await self.node_discovery.get_known_nodes(exclude_self=True)
        
        if not all_nodes:
            return []
        
        # 分类节点
        suspect_nodes = [n for n in all_nodes if n.status == "suspect"]
        online_nodes = [n for n in all_nodes if n.status == "online"]
        offline_nodes = [n for n in all_nodes if n.status == "offline"]
        
        # 优先探测 suspect 节点
        to_probe = suspect_nodes.copy()
        
        # 如果 suspect 节点不足，补充 online 节点
        remaining_slots = self.probe_count - len(to_probe)
        if remaining_slots > 0 and online_nodes:
            # 随机采样 healthy 节点
            sample_size = min(remaining_slots, len(online_nodes))
            to_probe.extend(random.sample(online_nodes, sample_size))
        
        # 偶尔探测 offline 节点（检查是否恢复）
        if offline_nodes and random.random() < 0.1:  # 10% 概率
            to_probe.extend(random.sample(offline_nodes, min(1, len(offline_nodes))))
        
        return to_probe
    
    async def _send_heartbeat(self, node: Any) -> bool:
        """
        发送心跳到指定节点
        
        Args:
            node: 目标节点
            
        Returns:
            是否成功
        """
        try:
            heartbeat = HeartbeatInfo(
                node_id=self.node_id,
                timestamp=datetime.now(timezone.utc),
                status="online",
                capabilities=self.capabilities,
                version=self.version,
            )
            
            # 通过消息传输发送心跳
            await self.message_transport.send_message(
                to_node=node.node_id,
                msg_type="heartbeat",
                payload=heartbeat.to_dict(),
                timeout=5.0
            )
            
            # 记录成功
            await self._record_success(node.node_id)
            
            # 更新节点状态为 online
            if node.status != "online":
                await self.node_discovery.update_node_status(node.node_id, "online")
                await self._reset_fail_count(node.node_id)
            
            return True
            
        except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
            logger.debug(f"Heartbeat to {node.node_id} failed: {e}")
            await self._record_failure(node.node_id)
            return False
    
    async def _record_success(self, node_id: str):
        """记录心跳成功"""
        db = await self._get_db()
        await db.execute("""
            INSERT INTO heartbeat_fails (node_id, fail_count, last_success_at)
            VALUES (?, 0, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                fail_count = 0,
                last_success_at = excluded.last_success_at
        """, (node_id, datetime.now(timezone.utc).isoformat()))
        await db.commit()
    
    async def _record_failure(self, node_id: str):
        """记录心跳失败"""
        db = await self._get_db()
        await db.execute("""
            INSERT INTO heartbeat_fails (node_id, fail_count, last_fail_at)
            VALUES (?, 1, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                fail_count = heartbeat_fails.fail_count + 1,
                last_fail_at = excluded.last_fail_at
        """, (node_id, datetime.now(timezone.utc).isoformat()))
        await db.commit()
        
        # 检查是否需要更新状态
        await self._check_and_update_status(node_id)
    
    async def _reset_fail_count(self, node_id: str):
        """重置失败计数"""
        db = await self._get_db()
        await db.execute(
            "UPDATE heartbeat_fails SET fail_count = 0 WHERE node_id = ?",
            (node_id,)
        )
        await db.commit()
    
    async def _get_fail_count(self, node_id: str) -> int:
        """获取失败计数"""
        db = await self._get_db()
        cursor = await db.execute(
            "SELECT fail_count FROM heartbeat_fails WHERE node_id = ?",
            (node_id,)
        )
        row = await cursor.fetchone()
        return row["fail_count"] if row else 0
    
    async def _check_and_update_status(self, node_id: str):
        """
        检查并更新节点状态
        
        Args:
            node_id: 节点 ID
        """
        fail_count = await self._get_fail_count(node_id)
        node = await self.node_discovery.get_node(node_id)
        
        if not node:
            return
        
        current_status = node.status
        new_status = current_status
        
        # 状态转换逻辑
        if fail_count >= self.offline_threshold:
            new_status = "offline"
        elif fail_count >= self.suspect_threshold:
            new_status = "suspect"
        
        if new_status != current_status:
            await self.node_discovery.update_node_status(node_id, new_status)
            logger.warning(f"Node {node_id} status changed: {current_status} -> {new_status} "
                          f"(fail_count={fail_count})")
            
            # 广播节点状态变更
            if new_status == "offline":
                await self._broadcast_offline(node_id)
    
    async def _broadcast_offline(self, node_id: str):
        """广播节点离线通知"""
        try:
            await self.message_transport.broadcast("node_offline", {
                "node_id": node_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "reported_by": self.node_id,
            })
            logger.info(f"Broadcast node_offline for {node_id}")
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Failed to broadcast node_offline: {e}")
    
    async def _heartbeat_sender(self):
        """心跳发送循环"""
        while self._running:
            try:
                # 获取需要探测的节点
                nodes_to_probe = await self._get_nodes_to_probe()
                
                if nodes_to_probe:
                    logger.debug(f"Sending heartbeats to {len(nodes_to_probe)} nodes")
                    
                    # 并发发送心跳
                    tasks = [self._send_heartbeat(node) for node in nodes_to_probe]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    success_count = sum(1 for r in results if r is True)
                    logger.debug(f"Heartbeat results: {success_count}/{len(nodes_to_probe)} succeeded")
                
            except (OSError, RuntimeError) as e:
                logger.error(f"Heartbeat sender error: {e}")
            
            await asyncio.sleep(self.heartbeat_interval)
    
    async def handle_heartbeat(self, heartbeat: HeartbeatInfo):
        """
        处理收到的心跳
        
        Args:
            heartbeat: 心跳信息
        """
        # 统一转为小写
        node_id = heartbeat.node_id.lower()

        # 更新节点信息
        node = await self.node_discovery.get_node(node_id)
        if node:
            # 更新最后seen时间和状态
            await self.node_discovery.update_node_status(node_id, "online")
            # 重置失败计数
            await self._reset_fail_count(node_id)
            
            logger.debug(f"Heartbeat received from {node_id}")
        else:
            # 未知节点，可能是通过 Gossip 发现的
            logger.warning(f"Heartbeat from unknown node: {node_id}")
    
    async def _cleanup_old_records(self):
        """清理旧的心跳记录"""
        while self._running:
            try:
                db = await self._get_db()
                # 清理超过 7 天的历史记录
                await db.execute("""
                    DELETE FROM heartbeat_history 
                    WHERE received_at < datetime('now', '-7 days')
                """)
                await db.commit()
                
                logger.debug("Cleaned up old heartbeat records")
                
            except (OSError, RuntimeError) as e:
                logger.error(f"Cleanup error: {e}")
            
            # 每天清理一次
            await asyncio.sleep(86400)
    
    async def get_node_health(self, node_id: str) -> Optional[dict]:
        """
        获取节点健康状态
        
        Args:
            node_id: 节点 ID
            
        Returns:
            健康状态信息
        """
        db = await self._get_db()
        cursor = await db.execute(
            """SELECT fail_count, last_fail_at, last_success_at 
               FROM heartbeat_fails WHERE node_id = ?""",
            (node_id,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return None
        
        node = await self.node_discovery.get_node(node_id)
        
        return {
            "node_id": node_id,
            "status": node.status if node else "unknown",
            "fail_count": row["fail_count"],
            "last_fail_at": row["last_fail_at"],
            "last_success_at": row["last_success_at"],
        }
    
    async def get_all_health(self) -> List[dict]:
        """
        获取所有节点健康状态
        
        Returns:
            健康状态列表
        """
        nodes = await self.node_discovery.get_all_nodes()
        health_list = []
        
        for node in nodes:
            health = await self.get_node_health(node.node_id)
            if health:
                health_list.append(health)
        
        return health_list
    
    async def start(self):
        """启动心跳管理"""
        if self._running:
            return
        
        self._running = True
        
        # 初始化数据库
        await self.init_database()
        
        # 注册心跳消息处理器
        async def heartbeat_handler(message):
            heartbeat = HeartbeatInfo.from_dict(message.payload)
            await self.handle_heartbeat(heartbeat)
        
        self.message_transport.register_handler("heartbeat", heartbeat_handler)
        
        # 启动心跳发送循环
        self._heartbeat_task = asyncio.create_task(self._heartbeat_sender())
        
        # 启动清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_old_records())
        
        logger.info("HeartbeatManager started")
    
    async def stop(self):
        """停止心跳管理"""
        self._running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 关闭数据库连接
        await self.close()
        
        logger.info("HeartbeatManager stopped")


class HeartbeatError(Exception):
    """心跳错误"""
    pass
