"""
OpenClaw Mesh - Node Discovery
节点发现模块：Tailscale API 集成 + Gossip 随机采样
"""

import asyncio
import json
import logging
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from pathlib import Path

import aiohttp
from typing import Any

# Windows 兼容：使用 win_compat 替代 aiosqlite
if sys.platform == "win32":
    from .win_compat import connect as aiosqlite_connect
else:
    import aiosqlite
    aiosqlite_connect = aiosqlite.connect

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """节点信息数据类"""
    node_id: str
    name: str
    ip: str
    public_key: Optional[str] = None
    last_seen: Optional[datetime] = None
    status: str = "offline"  # online/suspect/offline
    version: str = "0.6.1"
    port: int = 8443

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "node_id": self.node_id,
            "name": self.name,
            "ip": self.ip,
            "public_key": self.public_key,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "status": self.status,
            "version": self.version,
            "port": self.port,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NodeInfo":
        """从字典创建"""
        last_seen = None
        if data.get("last_seen"):
            try:
                last_seen = datetime.fromisoformat(data["last_seen"])
            except (ValueError, TypeError):
                pass

        return cls(
            node_id=data["node_id"],
            name=data.get("name", data["node_id"]),
            ip=data["ip"],
            public_key=data.get("public_key"),
            last_seen=last_seen,
            status=data.get("status", "offline"),
            version=data.get("version", "0.6.1"),
            port=data.get("port", 8443),
        )


class NodeDiscovery:
    """
    节点发现模块

    职责:
    1. 从 Tailscale API 获取 peers 列表
    2. 通过 HTTP /health 检测哪些 peers 运行 Mesh
    3. 与已知节点交换节点列表（Gossip 协议，fanout=3）
    4. 本地 SQLite 缓存节点信息

    特性:
    - Tailscale API 集成
    - Gossip 随机采样（fanout=3）
    - 节点信息本地持久化
    """

    # 默认配置
    DEFAULT_FANOUT = 3
    DEFAULT_GOSSIP_INTERVAL = 60  # 秒
    DEFAULT_HEALTH_TIMEOUT = 2.0  # 秒
    DEFAULT_MESH_PORT = 8443

    def __init__(
        self,
        node_id: str,
        db_path: str,
        tailscale_api_url: str = "http://localhost:8088",
        http_client: Optional[aiohttp.ClientSession] = None,
        fanout: int = DEFAULT_FANOUT,
        gossip_interval: int = DEFAULT_GOSSIP_INTERVAL,
        _protocol: str = "https",  # Actual protocol to use
    ):
        """
        初始化节点发现模块

        Args:
            node_id: 本节点 ID
            db_path: SQLite 数据库路径
            tailscale_api_url: Tailscale API 地址
            http_client: HTTP 客户端（可选）
            fanout: Gossip 采样数量
            gossip_interval: Gossip 间隔（秒）
        """
        self.node_id = node_id
        self.db_path = db_path
        self.tailscale_api_url = tailscale_api_url
        self._http_client = http_client
        self.fanout = fanout
        self.gossip_interval = gossip_interval
        self._protocol = _protocol

        self._running = False
        self._gossip_task: Optional[asyncio.Task] = None
        self._local_node_info: Optional[NodeInfo] = None
        self._db_connection = None
        self._initialized = False
        self._init_lock = asyncio.Lock()

        logger.info(f"NodeDiscovery initialized: node_id={node_id}, fanout={fanout}")

    async def _get_db(self):
        """获取数据库连接（带连接池管理）"""
        if self._db_connection is None:
            self._db_connection = await aiosqlite_connect(self.db_path)
            # 设置 row_factory 以支持字典访问
            self._db_connection.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
        return self._db_connection

    async def init_database(self):
        """初始化数据库表"""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            db = await self._get_db()
            # 节点信息表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    name TEXT,
                    ip TEXT UNIQUE,
                    public_key TEXT,
                    last_seen TIMESTAMP,
                    status TEXT DEFAULT 'offline',
                    version TEXT DEFAULT '0.6.1',
                    port INTEGER DEFAULT 8443
                )
            """)

            # 索引
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen)
            """)

            await db.commit()
            self._initialized = True
            logger.info("Node database initialized")

    @property
    async def http_client(self) -> aiohttp.ClientSession:
        """获取 HTTP 客户端"""
        if self._http_client is None:
            # 禁用 SSL 验证，用于连接 Tailscale 节点
            connector = aiohttp.TCPConnector(ssl=False)
            self._http_client = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=connector,
                trust_env=False,  # 禁用系统代理
            )
        return self._http_client

    def get_protocol(self, target_ip: str = None) -> str:
        """Get protocol: https or http"""
        # Return actual protocol set during startup
        return self._protocol


    async def set_local_node_info(self, node_info: NodeInfo):
        """设置本节点信息"""
        # 统一转为小写，避免大小写问题
        node_info.node_id = node_info.node_id.lower()
        self._local_node_info = node_info
        # Also save to database
        await self.upsert_node(node_info)
        logger.info(f"Local node info set: {node_info.node_id}")

    async def discover_from_tailscale(self) -> List[NodeInfo]:
        """
        从 Tailscale API 发现节点

        Returns:
            发现的节点列表
        """
        discovered = []

        try:
            # 跨平台获取 Tailscale peers
            if sys.platform == "win32":
                # Windows: 使用命令行获取状态
                data = await self._get_tailscale_status_via_cli()
            else:
                # Linux/macOS: 使用 HTTP API
                data = await self._get_tailscale_status_unix()

            if not data:
                return discovered

            peers = data.get("Peer", {})
            logger.info(f"Found {len(peers)} Tailscale peers")

            for peer_id, peer_info in peers.items():
                # 获取 peer IP
                tailscale_ips = peer_info.get("TailscaleIPs", [])
                if not tailscale_ips:
                    continue

                peer_ip = tailscale_ips[0]
                peer_hostname = peer_info.get("DNSName", peer_info.get("HostName", peer_id)).rstrip('.')

                # 检查是否为 Mesh 节点
                if await self._is_mesh_node(peer_ip):
                    # 使用 Tailscale 主机名作为发现的节点 node_id
                    node = NodeInfo(
                        node_id=peer_hostname,
                        name=peer_hostname,
                        ip=peer_ip,
                        status="online",
                        last_seen=datetime.now(timezone.utc),
                    )
                    discovered.append(node)
                    logger.debug(f"Discovered mesh node: {peer_hostname}@{peer_ip}")

        except (aiohttp.ClientError, OSError, RuntimeError) as e:
            logger.warning(f"Failed to discover from Tailscale: {e}")

        return discovered

    async def _get_tailscale_status_unix(self) -> Optional[dict]:
        """Linux/macOS: 使用 HTTP API 获取 Tailscale 状态，失败时回退到命令行"""
        # 首先尝试 HTTP API
        try:
            client = await self.http_client
            async with client.get(
                f"{self.tailscale_api_url}/localapi/v0/status",
                timeout=5.0
            ) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, OSError) as e:
            logger.debug(f"Tailscale HTTP API failed: {e}, falling back to CLI")
        
        # 回退到命令行方式
        return await self._get_tailscale_status_via_cli()
    
    async def _get_tailscale_status_via_cli(self, platform_hints: dict = None) -> Optional[dict]:
        """使用 tailscale status --json 命令获取状态（跨平台统一）
        
        Args:
            platform_hints: 可选的平台提示，如 {'windows_paths': [...]} 用于指定Windows搜索路径
            
        Returns:
            Tailscale 状态字典或 None
        """
        import asyncio
        import subprocess
        import json
        import shutil
        import os
        
        try:
            tailscale_path = shutil.which("tailscale")
            
            # Windows 平台尝试额外路径
            if not tailscale_path and sys.platform == "win32":
                windows_paths = platform_hints.get('windows_paths', []) if platform_hints else [
                    r"C:\Program Files\Tailscale\tailscale.exe",
                    r"C:\Program Files (x86)\Tailscale\tailscale.exe",
                ]
                for path in windows_paths:
                    if os.path.exists(path):
                        tailscale_path = path
                        break
            
            if not tailscale_path:
                logger.warning("tailscale executable not found in PATH")
                return None
            
            logger.debug(f"Using tailscale at: {tailscale_path}")
            
            proc = await asyncio.create_subprocess_exec(
                tailscale_path, "status", "--json",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=10.0
            )
            
            if proc.returncode != 0:
                logger.warning(f"tailscale status failed: {stderr.decode()}")
                return None
            
            data = json.loads(stdout.decode())
            # 转换为标准格式（与 HTTP API 兼容）
            self_info = data.get("Self", {})
            peers_dict = data.get("Peer", {})
            return {
                "Self": {
                    "TailscaleIPs": self_info.get("TailscaleIPs", []),
                    "HostName": self_info.get("HostName", ""),
                    "DNSName": self_info.get("DNSName", ""),
                },
                "Peer": {
                    peer_id: {
                        "TailscaleIPs": peer_info.get("TailscaleIPs", []),
                        "HostName": peer_info.get("HostName", peer_id),
                        "DNSName": peer_info.get("DNSName", ""),
                    }
                    for peer_id, peer_info in peers_dict.items()
                }
            }
        except (OSError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to get Tailscale status via CLI: {e}")
            return None
            logger.warning(f"Failed to query Tailscale API: {e}")
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Unexpected error discovering from Tailscale: {e}")

        return discovered

    async def _is_mesh_node(self, ip: str, port: int = None) -> bool:
        """
        检测 IP 是否运行 OpenClaw Mesh

        Args:
            ip: IP 地址
            port: 端口（默认尝试多个常用端口）

        Returns:
            是否为 Mesh 节点
        """
        # 尝试指定端口或默认端口
        ports_to_try = [port] if port else [self.DEFAULT_MESH_PORT]
        
        # Try both protocols for fallback
        protocols = [self._protocol]
        if self._protocol == "https":
            protocols.append("http")
        elif self._protocol == "http":
            protocols.append("https")

        for try_port in ports_to_try:
            for protocol in protocols:
                try:
                    client = await self.http_client
                    async with client.get(
                        f"{protocol}://{ip}:{try_port}/health",
                        timeout=self.DEFAULT_HEALTH_TIMEOUT,
                        allow_redirects=False
                    ) as response:

                        if response.status == 200:
                            data = await response.json()
                            if data.get("service") == "openclaw-mesh":
                                logger.debug(f"Found mesh node at {ip}:{try_port} ({protocol})")
                                return True

                except aiohttp.ClientConnectorError:
                    logger.debug(f"No mesh service at {ip}:{try_port} ({protocol})")
                except asyncio.TimeoutError:
                    logger.debug(f"Timeout checking {ip}:{try_port} ({protocol})")
                except (OSError, RuntimeError, ValueError, TypeError) as e:
                    logger.debug(f"Error checking {ip}:{try_port} ({protocol}): {e}")
                except Exception as e:
                    logger.debug(f"Unexpected error checking {ip}:{try_port} ({protocol}): {type(e).__name__}: {e}")

        return False

    async def upsert_node(self, node: NodeInfo):
        """
        插入或更新节点信息

        Args:
            node: 节点信息
        """
        # 统一转为小写
        node.node_id = node.node_id.lower()

        db = await self._get_db()
        try:
            await db.execute("""
                INSERT INTO nodes (node_id, name, ip, public_key, last_seen, status, version, port)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    name = excluded.name,
                    ip = excluded.ip,
                    public_key = excluded.public_key,
                    last_seen = excluded.last_seen,
                    status = excluded.status,
                    version = excluded.version,
                    port = excluded.port
            """, (
                node.node_id,
                node.name,
                node.ip,
                node.public_key,
                node.last_seen.isoformat() if node.last_seen else None,
                node.status,
                node.version,
                node.port,
            ))
            await db.commit()
        except Exception as e:
            logger.error(f"Failed to upsert node: {e}")
        logger.debug(f"Node upserted: {node.node_id}")

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """
        获取节点信息

        Args:
            node_id: 节点 ID

        Returns:
            节点信息或 None
        """
        # 统一转为小写
        node_id = node_id.lower()

        db = await self._get_db()
        cursor = await db.execute(
            "SELECT * FROM nodes WHERE node_id = ?",
            (node_id,)
        )
        row = await cursor.fetchone()

        if row:
            return self._row_to_node(row)
        return None

    async def get_node_by_ip(self, ip: str) -> Optional[NodeInfo]:
        """
        通过 IP 获取节点信息

        Args:
            ip: IP 地址

        Returns:
            节点信息或 None
        """
        db = await self._get_db()
        cursor = await db.execute(
            "SELECT * FROM nodes WHERE ip = ?",
            (ip,)
        )
        row = await cursor.fetchone()

        if row:
            return self._row_to_node(row)
        return None

    async def get_all_nodes(self) -> List[NodeInfo]:
        """
        获取所有已知节点

        Returns:
            节点列表
        """
        db = await self._get_db()
        cursor = await db.execute("SELECT * FROM nodes")
        rows = await cursor.fetchall()
        return [self._row_to_node(row) for row in rows]

    async def get_online_nodes(self) -> List[NodeInfo]:
        """
        获取在线节点

        Returns:
            在线节点列表
        """
        db = await self._get_db()
        cursor = await db.execute(
            "SELECT * FROM nodes WHERE status = 'online'"
        )
        rows = await cursor.fetchall()
        return [self._row_to_node(row) for row in rows]

    async def get_known_nodes(self, exclude_self: bool = True) -> List[NodeInfo]:
        """
        获取已知节点（用于 Gossip）

        Args:
            exclude_self: 是否排除本节点

        Returns:
            节点列表
        """
        nodes = await self.get_all_nodes()
        if exclude_self:
            nodes = [n for n in nodes if n.node_id != self.node_id]
        return nodes

    def _row_to_node(self, row) -> NodeInfo:
        """将数据库行转换为 NodeInfo"""
        last_seen = None
        if row["last_seen"]:
            try:
                last_seen = datetime.fromisoformat(row["last_seen"])
            except (ValueError, TypeError):
                pass

        return NodeInfo(
            node_id=row["node_id"],
            name=row["name"],
            ip=row["ip"],
            public_key=row.get("public_key"),
            last_seen=last_seen,
            status=row["status"],
            version=row["version"],
            port=row["port"],
        )

    async def update_node_status(self, node_id: str, status: str):
        """
        更新节点状态

        Args:
            node_id: 节点 ID
            status: 新状态 (online/suspect/offline)
        """
        db = await self._get_db()
        await db.execute(
            "UPDATE nodes SET status = ?, last_seen = ? WHERE node_id = ?",
            (status, datetime.now(timezone.utc).isoformat(), node_id)
        )
        await db.commit()

        logger.debug(f"Node {node_id} status updated to {status}")

    async def gossip_exchange(self, target_node: NodeInfo) -> List[NodeInfo]:
        """
        与指定节点交换节点列表（Gossip 协议）

        Args:
            target_node: 目标节点

        Returns:
            从目标节点获取的节点列表
        """
        try:
            # 获取本节点已知的节点列表
            my_nodes = await self.get_known_nodes(exclude_self=True)

            # 构建请求
            request_data = {
                "from_node": self.node_id,
                "nodes": [n.to_dict() for n in my_nodes],
            }

            client = await self.http_client
            async with client.post(
                f"{self.get_protocol()}://{target_node.ip}:{target_node.port}/api/nodes/exchange",
                json=request_data,
                timeout=10.0
            ) as response:
                response.raise_for_status()

                # 解析响应
                data = await response.json()
                their_nodes = data.get("nodes", [])

                # 转换为 NodeInfo 对象
                discovered = []
                for node_data in their_nodes:
                    try:
                        node = NodeInfo.from_dict(node_data)
                        if node.node_id != self.node_id:
                            await self.upsert_node(node)
                            discovered.append(node)
                    except (KeyError, ValueError) as e:
                        logger.warning(f"Invalid node data from {target_node.node_id}: {e}")

                logger.info(f"Gossip exchange with {target_node.node_id}: "
                           f"sent {len(my_nodes)}, received {len(discovered)}")

                return discovered

        except aiohttp.ClientError as e:
            logger.warning(f"Gossip exchange failed with {target_node.node_id}: {e}")
            return []
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Unexpected error in gossip exchange with {target_node.node_id}: {e}")
            return []

    async def _gossip_loop(self):
        """Gossip 后台循环"""
        while self._running:
            try:
                await self._do_gossip_round()
            except (OSError, RuntimeError) as e:
                logger.error(f"Gossip round failed: {e}")

            await asyncio.sleep(self.gossip_interval)

    async def _do_gossip_round(self):
        """执行一轮 Gossip"""
        # 获取已知节点
        nodes = await self.get_known_nodes(exclude_self=True)

        if len(nodes) == 0:
            logger.debug("No known nodes for gossip, triggering Tailscale discovery")
            await self._discover_and_save()
            return

        # 随机采样（fanout）
        sample_size = min(self.fanout, len(nodes))
        sampled_nodes = random.sample(nodes, sample_size)

        logger.debug(f"Gossip round: sampling {sample_size} nodes from {len(nodes)}")

        # 与采样节点交换信息
        for node in sampled_nodes:
            try:
                await self.gossip_exchange(node)
            except (ConnectionError, OSError, RuntimeError) as e:
                logger.warning(f"Gossip with {node.node_id} failed: {e}")

    async def handle_exchange_request(self, request_data: dict) -> List[NodeInfo]:
        """
        处理节点交换请求

        Args:
            request_data: 请求数据

        Returns:
            本节点已知的节点列表
        """
        from_node = request_data.get("from_node")
        their_nodes = request_data.get("nodes", [])

        logger.debug(f"Received exchange request from {from_node} with {len(their_nodes)} nodes")

        # 保存对方提供的节点信息
        for node_data in their_nodes:
            try:
                node = NodeInfo.from_dict(node_data)
                if node.node_id != self.node_id:
                    # 检查 IP 是否已被本节点占用（自己的 IP 不能被其他节点使用）
                    existing = await self.get_node(self.node_id)
                    if existing and existing.ip == node.ip:
                        logger.debug(f"Skipping {node.node_id}: IP {node.ip} is already used by self")
                        continue
                    await self.upsert_node(node)
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid node data from {from_node}: {e}")

        # 返回本节点已知的节点（排除本节点）
        return await self.get_known_nodes(exclude_self=True)

    async def _discover_and_save(self):
        """从 Tailscale 发现并保存节点"""
        logger.info("Starting discovery from Tailscale...")
        try:
            nodes = await self.discover_from_tailscale()
            logger.info(f"Found {len(nodes)} nodes from Tailscale")
            for node in nodes:
                try:
                    await self.upsert_node(node)
                    logger.info(f"Saved node: {node.node_id}")
                except Exception as e:
                    logger.error(f"Failed to save node {node.node_id}: {e}")
        except Exception as e:
            logger.error(f"Discovery failed: {e}")

    async def start(self):
        """启动节点发现服务"""
        if self._running:
            return

        self._running = True

        # 初始化数据库
        await self.init_database()

        # 获取本机 Tailscale 信息
        local_ip = "0.0.0.0"  # 默认值
        local_hostname = self.node_id  # 默认使用配置中的 node_id
        try:
            tailscale_status = await self._get_tailscale_status_via_cli()
            if tailscale_status and "Self" in tailscale_status:
                self_info = tailscale_status["Self"]
                tailscale_ips = self_info.get("TailscaleIPs", [])
                if tailscale_ips:
                    local_ip = tailscale_ips[0]  # 使用第一个 Tailscale IP
                    logger.info(f"Got local Tailscale IP: {local_ip}")
                # 获取 Tailscale 主机名
                local_hostname = self_info.get("HostName", self.node_id)
                if local_hostname != self.node_id:
                    logger.info(f"Got local Tailscale HostName: {local_hostname}")
        except Exception as e:
            logger.warning(f"Failed to get local Tailscale info: {e}, using defaults")

        # 保存本节点信息，防止 Gossip 时被覆盖
        local_info = NodeInfo(
            node_id=self.node_id,
            name=local_hostname,  # 使用 Tailscale 主机名
            ip=local_ip,  # 使用实际 Tailscale IP
            status="online",
            last_seen=datetime.now(timezone.utc)
        )
        await self.set_local_node_info(local_info)

        # 从 Tailscale 发现节点
        await self._discover_and_save()

        # 启动 Gossip 循环
        self._gossip_task = asyncio.create_task(self._gossip_loop())

        logger.info("NodeDiscovery started")

    async def stop(self):
        """停止节点发现服务"""
        self._running = False

        if self._gossip_task:
            self._gossip_task.cancel()
            try:
                await self._gossip_task
            except asyncio.CancelledError:
                pass

        if self._http_client:
            await self._http_client.close()

        # 关闭数据库连接
        await self.close()

        logger.info("NodeDiscovery stopped")

    async def close(self):
        """关闭所有数据库连接"""
        if self._db_connection:
            try:
                await self._db_connection.close()
            except Exception:
                pass
            self._db_connection = None
        self._initialized = False
        logger.debug("NodeDiscovery database connection closed")

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.stop()

    async def discover_once(self) -> List[NodeInfo]:
        """
        执行一次发现（用于手动触发）

        Returns:
            发现的节点列表
        """
        # 从 Tailscale 发现
        nodes = await self.discover_from_tailscale()

        # 保存到数据库
        for node in nodes:
            await self.upsert_node(node)

        # 执行一次 Gossip
        await self._do_gossip_round()

        return nodes



