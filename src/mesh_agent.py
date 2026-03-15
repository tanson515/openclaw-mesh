"""
OpenClaw Mesh - MeshAgent 主类
完全去中心化 P2P 网络主控制器

职责:
1. 初始化所有模块（存储、安全、网络、传输、代理、讨论）
2. 生命周期管理（启动、停止）
3. 配置管理（加载/保存配置）
4. 事件处理（消息、文件、命令）
"""

import asyncio
import json
import logging
import platform
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Awaitable, TypeVar, Set
import yaml

from src.constants import VERSION as MESH_VERSION

# 类型变量
T = TypeVar('T')

# 存储模块
from src.storage import SQLiteStorage, create_storage

# 安全模块
from src.security import (
    CertificateManager,
    KeyManager,
    MessageSigner,
    RateLimiter,
    HTTPSServer,
    HTTPSClient,
)

# 网络模块
from src.network import (
    NodeDiscovery,
    NodeInfo,
    MessageTransport,
    Message,
    HeartbeatManager,
    BoundedOfflineQueue,
)

# 传输模块
from src.transfer import FileTransfer, TransferCallback

# 代理模块
from src.proxy import AgentProxy, PermissionManager, ExecutionResult, ExecutionStatus

# 讨论模块
from src.discussion import DiscussionCoordinator

logger = logging.getLogger(__name__)


@dataclass
class MeshConfig:
    """Mesh 配置数据类"""
    # 节点信息
    node_id: str
    tailscale_auth_key: Optional[str] = None

    # 网络配置
    host: str = "0.0.0.0"
    port: int = 8443
    max_connections: int = 100

    # 安全配置
    cert_path: Optional[str] = None
    key_path: Optional[str] = None
    ca_cert_path: Optional[str] = None
    tls_mode: str = "tailscale"  # tailscale | self_signed | custom_ca

    # 传输配置
    chunk_size: int = 1024 * 1024  # 1MB
    max_concurrent_transfers: int = 5
    temp_dir: str = "./temp"

    # 心跳配置
    heartbeat_interval: int = 1800  # 30分钟
    suspect_threshold: int = 3
    offline_threshold: int = 6

    # 队列配置
    queue_max_size: int = 1000
    queue_retry_interval: int = 60

    # 代理配置
    allowed_commands: List[str] = field(default_factory=list)
    proxy_timeout: int = 300

    # 讨论配置
    max_participants: int = 10
    speak_timeout: float = 120.0

    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None

    # 数据目录
    data_dir: str = "./data"

    def __post_init__(self) -> None:
        """
        验证配置有效性

        支持 Windows 和 Linux 双平台的路径验证
        """
        # 验证 node_id 不为空
        if not self.node_id or not isinstance(self.node_id, str):
            raise ValueError("node_id must be a non-empty string")
        if not self.node_id.strip():
            raise ValueError("node_id cannot be empty or whitespace only")

        # 验证 port 在有效范围 (1-65535)
        if not isinstance(self.port, int):
            raise TypeError(f"port must be an integer, got {type(self.port).__name__}")
        if self.port < 1 or self.port > 65535:
            raise ValueError(f"port must be between 1 and 65535, got {self.port}")

        # 验证 host 不为空
        if not self.host or not isinstance(self.host, str):
            raise ValueError("host must be a non-empty string")

        # 验证 tls_mode 有效值
        valid_tls_modes = {"tailscale", "self_signed", "custom_ca", "none"}
        if self.tls_mode not in valid_tls_modes:
            raise ValueError(f"tls_mode must be one of {valid_tls_modes}, got '{self.tls_mode}'")

        # 验证 log_level 有效值
        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level not in valid_log_levels:
            raise ValueError(f"log_level must be one of {valid_log_levels}, got '{self.log_level}'")

        # 验证数值范围
        if self.chunk_size < 1024:
            raise ValueError(f"chunk_size must be at least 1024 bytes, got {self.chunk_size}")
        if self.max_connections < 1:
            raise ValueError(f"max_connections must be at least 1, got {self.max_connections}")
        if self.heartbeat_interval < 1:
            raise ValueError(f"heartbeat_interval must be at least 1 second, got {self.heartbeat_interval}")
        if self.suspect_threshold < 1:
            raise ValueError(f"suspect_threshold must be at least 1, got {self.suspect_threshold}")
        if self.offline_threshold < self.suspect_threshold:
            raise ValueError(f"offline_threshold ({self.offline_threshold}) must be >= suspect_threshold ({self.suspect_threshold})")

        # 验证路径格式和可写性（Windows/Linux 兼容）
        self._validate_path("data_dir", self.data_dir, must_be_writable=True)
        self._validate_path("temp_dir", self.temp_dir, must_be_writable=True)

        # 验证可选路径
        if self.cert_path:
            self._validate_path("cert_path", self.cert_path, must_exist=True)
        if self.key_path:
            self._validate_path("key_path", self.key_path, must_exist=True)
        if self.ca_cert_path:
            self._validate_path("ca_cert_path", self.ca_cert_path, must_exist=True)
        if self.log_file:
            self._validate_path("log_file", self.log_file, parent_must_be_writable=True)

    def _validate_path(
        self,
        name: str,
        path_str: str,
        must_exist: bool = False,
        must_be_writable: bool = False,
        parent_must_be_writable: bool = False
    ) -> None:
        """
        验证路径有效性（跨平台兼容）

        Args:
            name: 配置项名称（用于错误信息）
            path_str: 路径字符串
            must_exist: 是否必须已存在
            must_be_writable: 是否必须可写
            parent_must_be_writable: 父目录是否必须可写

        Raises:
            ValueError: 路径验证失败
            TypeError: 路径类型错误
        """
        if not isinstance(path_str, str):
            raise TypeError(f"{name} must be a string, got {type(path_str).__name__}")

        if not path_str:
            raise ValueError(f"{name} cannot be empty")

        try:
            path = Path(path_str)
        except (TypeError, ValueError) as e:
            raise ValueError(f"{name} contains invalid path characters: {e}")

        # 检查路径是否包含空字符（Windows 和 Linux 都非法）
        if '\x00' in path_str:
            raise ValueError(f"{name} contains null bytes")

        # Windows 特定检查
        if platform.system() == "Windows":
            # 检查 Windows 保留字符
            reserved_chars = '<>:"/\\|?*'
            for char in reserved_chars:
                if char in path.name:
                    raise ValueError(f"{name} contains reserved character '{char}' on Windows")
            # 检查 Windows 保留名称 (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
            reserved_names = {
                'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2',
                'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
            }
            if path.name.upper() in reserved_names:
                raise ValueError(f"{name} uses Windows reserved name '{path.name}'")

        if must_exist and not path.exists():
            raise ValueError(f"{name} path does not exist: {path_str}")

        if must_be_writable:
            # 检查目录可写性
            try:
                if path.exists():
                    if not path.is_dir():
                        raise ValueError(f"{name} must be a directory: {path_str}")
                    # 尝试写入测试文件
                    test_file = path / ".write_test"
                    test_file.write_text("")
                    test_file.unlink()
                else:
                    # 尝试创建目录
                    path.mkdir(parents=True, exist_ok=True)
            except PermissionError as e:
                raise ValueError(f"{name} is not writable: {path_str}") from e
            except OSError as e:
                raise ValueError(f"{name} path error: {e}") from e

        if parent_must_be_writable and path.parent != path:
            try:
                if not path.parent.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                # 尝试在父目录写入测试文件
                test_file = path.parent / ".write_test"
                test_file.write_text("")
                test_file.unlink()
            except PermissionError as e:
                raise ValueError(f"parent directory of {name} is not writable: {path.parent}") from e
            except OSError as e:
                raise ValueError(f"parent directory of {name} error: {e}") from e

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MeshConfig':
        """从字典创建配置"""
        return cls(
            node_id=data.get('node_id', ''),
            tailscale_auth_key=data.get('tailscale_auth_key'),
            host=data.get('host', '0.0.0.0'),
            port=data.get('port', 8443),
            max_connections=data.get('max_connections', 100),
            cert_path=data.get('cert_path'),
            key_path=data.get('key_path'),
            ca_cert_path=data.get('ca_cert_path'),
            tls_mode=data.get('tls_mode', 'tailscale'),
            chunk_size=data.get('chunk_size', 1024 * 1024),
            max_concurrent_transfers=data.get('max_concurrent_transfers', 5),
            temp_dir=data.get('temp_dir', './temp'),
            heartbeat_interval=data.get('heartbeat_interval', 1800),
            suspect_threshold=data.get('suspect_threshold', 3),
            offline_threshold=data.get('offline_threshold', 6),
            queue_max_size=data.get('queue_max_size', 1000),
            queue_retry_interval=data.get('queue_retry_interval', 60),
            allowed_commands=data.get('allowed_commands', []),
            proxy_timeout=data.get('proxy_timeout', 300),
            max_participants=data.get('max_participants', 10),
            speak_timeout=data.get('speak_timeout', 120.0),
            log_level=data.get('log_level', 'INFO'),
            log_file=data.get('log_file'),
            data_dir=data.get('data_dir', './data'),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'node_id': self.node_id,
            'tailscale_auth_key': self.tailscale_auth_key,
            'host': self.host,
            'port': self.port,
            'max_connections': self.max_connections,
            'cert_path': self.cert_path,
            'key_path': self.key_path,
            'ca_cert_path': self.ca_cert_path,
            'tls_mode': self.tls_mode,
            'chunk_size': self.chunk_size,
            'max_concurrent_transfers': self.max_concurrent_transfers,
            'temp_dir': self.temp_dir,
            'heartbeat_interval': self.heartbeat_interval,
            'suspect_threshold': self.suspect_threshold,
            'offline_threshold': self.offline_threshold,
            'queue_max_size': self.queue_max_size,
            'queue_retry_interval': self.queue_retry_interval,
            'allowed_commands': self.allowed_commands,
            'proxy_timeout': self.proxy_timeout,
            'max_participants': self.max_participants,
            'speak_timeout': self.speak_timeout,
            'log_level': self.log_level,
            'log_file': self.log_file,
            'data_dir': self.data_dir,
        }


class MeshAgent:
    """
    OpenClaw Mesh Agent 主类

    集成所有模块，提供统一的 P2P 网络节点管理能力
    """

    VERSION = MESH_VERSION

    def __init__(self, config: MeshConfig):
        """
        初始化 MeshAgent

        Args:
            config: 配置对象
        """
        self.config = config
        self.node_id = config.node_id
        self._running = False
        self._shutdown_event = asyncio.Event()

        # 模块引用（延迟初始化）
        self._storage: Optional[SQLiteStorage] = None
        self._cert_manager: Optional[CertificateManager] = None
        self._key_manager: Optional[KeyManager] = None
        self._message_signer: Optional[MessageSigner] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._https_client: Optional[HTTPSClient] = None
        self._https_server: Optional[HTTPSServer] = None
        self._offline_queue: Optional[BoundedOfflineQueue] = None
        self._node_discovery: Optional[NodeDiscovery] = None
        self._message_transport: Optional[MessageTransport] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None
        self._file_transfer: Optional[FileTransfer] = None
        self._permission_manager: Optional[PermissionManager] = None
        self._agent_proxy: Optional[AgentProxy] = None
        self._discussion_coordinator: Optional[DiscussionCoordinator] = None

        # 事件处理器
        self._message_handlers: Dict[str, List[Callable[[Message], Awaitable[None]]]] = {}
        self._file_handlers: List[Callable[[str, str, str], Awaitable[None]]] = []
        self._command_handlers: Dict[str, Callable[[Dict[str, Any]], Awaitable[Any]]] = {}

        # Event Loop 生命周期管理
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_lock = asyncio.Lock()
        self._tasks: Set[asyncio.Task] = set()

        logger.info(f"MeshAgent {self.VERSION} initialized: node_id={self.node_id}")

    # ==================== 属性访问 ====================

    @property
    def storage(self) -> Optional[SQLiteStorage]:
        """获取存储模块"""
        return self._storage

    @property
    def node_discovery(self) -> Optional[NodeDiscovery]:
        """获取节点发现模块"""
        return self._node_discovery

    @property
    def message_transport(self) -> Optional[MessageTransport]:
        """获取消息传输模块"""
        return self._message_transport

    @property
    def heartbeat_manager(self) -> Optional[HeartbeatManager]:
        """获取心跳管理模块"""
        return self._heartbeat_manager

    @property
    def file_transfer(self) -> Optional[FileTransfer]:
        """获取文件传输模块"""
        return self._file_transfer

    @property
    def agent_proxy(self) -> Optional[AgentProxy]:
        """获取代理模块"""
        return self._agent_proxy

    @property
    def discussion_coordinator(self) -> Optional[DiscussionCoordinator]:
        """获取讨论协调模块"""
        return self._discussion_coordinator

    @property
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._running

    # ==================== Event Loop 生命周期管理 ====================

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """
        获取当前线程的 Event Loop（线程安全）

        Windows 和 Linux 的 asyncio 实现略有不同：
        - Windows: 默认使用 ProactorEventLoop（基于 IOCP）
        - Linux: 默认使用 SelectorEventLoop（基于 epoll/select）

        Returns:
            当前 Event Loop

        Raises:
            RuntimeError: 无法获取 Event Loop
        """
        try:
            loop = asyncio.get_running_loop()
            return loop
        except RuntimeError:
            # 没有正在运行的 Event Loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    # 创建新的 Event Loop
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                return loop
            except RuntimeError as e:
                # 在多线程环境中，可能需要创建新的 Event Loop
                if platform.system() == "Windows":
                    # Windows: 使用 ProactorEventLoop（支持管道和套接字）
                    loop = asyncio.ProactorEventLoop()
                else:
                    # Linux/macOS: 使用默认的 SelectorEventLoop
                    loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop

    def _ensure_loop_running(self) -> asyncio.AbstractEventLoop:
        """
        确保 Event Loop 正在运行

        Returns:
            正在运行的 Event Loop
        """
        loop = self._get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
        return loop

    def _run_coroutine_threadsafe(self, coro: Awaitable[T]) -> asyncio.Future[T]:
        """
        线程安全地运行协程

        Args:
            coro: 要运行的协程

        Returns:
            Future 对象

        Raises:
            RuntimeError: Event Loop 已关闭
        """
        loop = self._ensure_loop_running()
        return asyncio.run_coroutine_threadsafe(coro, loop)

    def _create_task(self, coro: Awaitable[T]) -> asyncio.Task[T]:
        """
        创建并跟踪任务

        Args:
            coro: 要运行的协程

        Returns:
            Task 对象
        """
        loop = self._ensure_loop_running()
        task = loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._handle_task_done)
        return task
    
    def _handle_task_done(self, task: asyncio.Task) -> None:
        """处理任务完成，捕获异常防止崩溃"""
        self._tasks.discard(task)
        try:
            task.result()
        except Exception as e:
            logger.exception(f"Task failed with exception: {e}")

    async def _cancel_all_tasks(self) -> None:
        """取消所有正在运行的任务"""
        if not self._tasks:
            return

        # 获取所有未完成的任务
        pending_tasks = [t for t in self._tasks if not t.done()]
        if not pending_tasks:
            return

        # 取消所有任务
        for task in pending_tasks:
            task.cancel()

        # 等待所有任务完成（带超时）
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending_tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Some tasks did not cancel within timeout")

    # ==================== 模块初始化 ====================

    async def _ensure_certificate(self) -> str:
        """
        确保有有效的证书，返回使用的协议 ("https" 或 "http")
        """
        import subprocess
        data_dir = Path(self.config.data_dir)
        cert_file = data_dir / "certs" / "tailscale.crt"
        key_file = data_dir / "certs" / "tailscale.key"
        
        # 尝试获取 Tailscale 证书
        hostname = self.node_id if self.node_id.endswith(".ts.net") else f"{self.node_id}.taila3f2d9.ts.net"
        try:
            result = subprocess.run(
                ["tailscale", "cert", "--cert-file", str(cert_file),
                 "--key-file", str(key_file), hostname],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                logger.info(f"Successfully obtained Tailscale certificate for {hostname}")
                return "https"
            else:
                logger.warning(f"Failed to get Tailscale cert: {result.stderr}")
        except FileNotFoundError:
            logger.warning("tailscale command not found")
        except Exception as e:
            logger.warning(f"Error getting Tailscale certificate: {e}")
        
        # 获取失败，检查旧证书是否有效
        if cert_file.exists() and key_file.exists():
            logger.info("Using existing certificate")
            return "https"
        
        # 没有有效证书，使用 HTTP
        logger.info("No valid certificate, falling back to HTTP")
        return "http"

    async def _init_storage(self) -> None:
        """初始化存储模块"""
        data_dir = Path(self.config.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        db_path = data_dir / f"{self.node_id}.db"

        self._storage = create_storage(str(db_path))
        logger.info(f"Storage initialized: {db_path}")

    async def _init_security(self) -> None:
        """初始化安全模块"""
        data_dir = Path(self.config.data_dir)

        # 证书管理器
        self._cert_manager = CertificateManager(
            cert_dir=str(data_dir / "certs"),
            mode=self.config.tls_mode,
        )

        # 密钥管理器（Ed25519）
        key_dir = data_dir / "keys"
        key_dir.mkdir(parents=True, exist_ok=True)
        self._key_manager = KeyManager(key_dir=str(key_dir), node_id=self.node_id)
        self._key_manager.ensure_keypair(auto_generate=True)

        # 消息签名器
        self._message_signer = MessageSigner(self._key_manager)

        # 速率限制器（初始化使用默认值，后续会根据节点数动态调整）
        self._rate_limiter = RateLimiter(rate=10.0, burst=5)

        logger.info("Security modules initialized")

    async def _get_rate_limit_config(self) -> dict:
        """
        根据节点数动态调整速率限制配置

        Returns:
            速率限制配置 {"rate": float, "burst": int}
        """
        try:
            # 获取在线节点数
            if self._node_discovery:
                nodes = await self._node_discovery.get_all_nodes()
                node_count = len([n for n in nodes if n.status == "online"])
            else:
                node_count = 1
        except Exception:
            node_count = 1

        # 动态计算：每个节点预留 2 QPS，至少 10 QPS
        rate = max(10.0, node_count * 2.0)
        burst = max(5, node_count)

        return {"rate": rate, "burst": burst}

    async def _update_rate_limiter(self) -> None:
        """根据当前节点数更新速率限制器"""
        rate_config = await self._get_rate_limit_config()
        self._rate_limiter = RateLimiter(rate=rate_config["rate"], burst=rate_config["burst"])
        logger.info(f"Rate limiter updated: rate={rate_config['rate']}, burst={rate_config['burst']}")

    async def _init_network(self) -> None:
        """初始化网络模块"""
        data_dir = Path(self.config.data_dir)

        # 检测协议：尝试获取 Tailscale 证书
        protocol = await self._ensure_certificate()
        logger.info(f"Using protocol: {protocol}")

        # 节点发现
        self._node_discovery = NodeDiscovery(
            node_id=self.node_id,
            db_path=str(data_dir / f"{self.node_id}.db"),
            _protocol=protocol,  # Pass actual protocol
        )

        # HTTPS 客户端
        # 当 tls_mode="tailscale" 时，证书是 Tailscale 内部颁发的，SAN 是主机名但连接使用 IP
        # 需要跳过 SSL 验证（Tailscale 传输层已加密）
        verify_mode = "none" if self.config.tls_mode == "tailscale" else (
            "peer" if self.config.tls_mode != "none" else "none"
        )
        self._https_client = HTTPSClient(
            cert_manager=self._cert_manager,
            verify_mode=verify_mode,
            ca_cert_path=self.config.ca_cert_path,
        )
        await self._https_client.start()

        # 消息传输
        # 根据 TLS 模式获取证书路径
        if self.config.tls_mode == "none":
            # 无 TLS 模式
            cert_path = None
            key_path = None
        elif self.config.tls_mode == "tailscale":
            # Tailscale 模式：从 CertificateManager 获取证书
            try:
                cert_path, key_path = self._cert_manager.get_ssl_context_files()
                # 检查证书文件是否真实存在
                import os
                if not cert_path or not key_path or not os.path.exists(cert_path) or not os.path.exists(key_path):
                    logger.warning(f"Tailscale certificate not found, falling back to HTTP. cert={cert_path}, key={key_path}")
                    cert_path = None
                    key_path = None
                else:
                    logger.info(f"Using Tailscale certificate: {cert_path}")
            except Exception as e:
                logger.warning(f"Failed to get Tailscale certificate: {e}, falling back to HTTP")
                cert_path = None
                key_path = None
        elif self.config.tls_mode == "self_signed":
            # 自签名模式：从配置读取
            cert_path = self.config.cert_path
            key_path = self.config.key_path
        else:
            # custom_ca 模式
            cert_path = self.config.cert_path
            key_path = self.config.key_path

        # 先创建离线队列（暂时不传入依赖）
        self._offline_queue = BoundedOfflineQueue(
            db_path=str(data_dir / f"{self.node_id}.db"),
            max_size=self.config.queue_max_size,
            base_delay=self.config.queue_retry_interval,
            node_id=self.node_id,
            node_discovery=self._node_discovery,
            message_transport=None,  # 稍后更新
        )
        await self._offline_queue.init_database()

        self._message_transport = MessageTransport(
            node_id=self.node_id,
            node_discovery=self._node_discovery,
            offline_queue=self._offline_queue,
            message_signer=self._message_signer,
            rate_limiter=self._rate_limiter,
            cert_path=cert_path,
            key_path=key_path,
            ca_cert_path=self.config.ca_cert_path,
            port=self.config.port,
            host=self.config.host,
            file_transfer=self._file_transfer,
        )

        # 更新离线队列的 message_transport 依赖
        self._offline_queue.message_transport = self._message_transport

        # 心跳管理
        self._heartbeat_manager = HeartbeatManager(
            node_id=self.node_id,
            node_discovery=self._node_discovery,
            message_transport=self._message_transport,
            db_path=str(data_dir / f"{self.node_id}.db"),
            heartbeat_interval=self.config.heartbeat_interval,
            suspect_threshold=self.config.suspect_threshold,
            offline_threshold=self.config.offline_threshold,
            capabilities=["message", "file_transfer", "proxy", "discussion"],
            version=self.VERSION,
        )

        logger.info("Network modules initialized")

    async def _init_transfer(self) -> None:
        """初始化文件传输模块"""
        temp_dir = Path(self.config.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        self._file_transfer = FileTransfer(
            node_id=self.node_id,
            node_discovery=self._node_discovery,
            https_client=self._https_client,
            temp_dir=str(temp_dir),
            port=self.config.port,
        )

        logger.info("File transfer module initialized")

    async def _init_proxy(self) -> None:
        """初始化代理模块"""
        # 权限管理器
        self._permission_manager = PermissionManager()

        # 设置允许的命令
        for pattern in self.config.allowed_commands:
            self._permission_manager.allowed_patterns.append(pattern)

        # Agent 代理
        self._agent_proxy = AgentProxy(
            node_id=self.node_id,
            message_transport=self._message_transport,
            permission_manager=self._permission_manager,
        )

        logger.info("Proxy module initialized")

    async def _init_discussion(self) -> None:
        """初始化讨论协调模块"""
        max_rounds = getattr(self.config, 'discussion_max_rounds', 5)
        speech_timeout = getattr(self.config, 'discussion_speech_timeout', 120.0)
        self._discussion_coordinator = DiscussionCoordinator(
            node_id=self.node_id,
            message_transport=self._message_transport,
            max_rounds=max_rounds,
            speech_timeout=speech_timeout,
        )

        logger.info("Discussion coordinator initialized")

    # ==================== 生命周期管理 ====================

    async def start(self) -> None:
        """启动 MeshAgent"""
        if self._running:
            logger.warning("MeshAgent already running")
            return

        logger.info("Starting MeshAgent...")

        try:
            # 初始化所有模块
            await self._init_storage()
            await self._init_security()
            await self._init_network()
            await self._init_transfer()
            await self._init_proxy()
            await self._init_discussion()

            # 关联 file_transfer 到 message_transport
            if self._message_transport and self._file_transfer:
                self._message_transport._file_transfer = self._file_transfer

            # 注册自己的节点信息
            await self._register_self_node()

            # 启动各个模块
            logger.info("Starting modules...")
            try:
                if self._message_transport and hasattr(self._message_transport, 'start'):
                    await self._message_transport.start()
                logger.info("Message transport started")
            except Exception as e:
                logger.error("Failed to start message transport: %s", e, exc_info=True)
            try:
                if self._node_discovery and hasattr(self._node_discovery, 'start'):
                    await self._node_discovery.start()
                logger.info("Node discovery started")
            except Exception as e:
                logger.error("Failed to start node discovery: %s", e, exc_info=True)
            try:
                if self._heartbeat_manager and hasattr(self._heartbeat_manager, 'start'):
                    await self._heartbeat_manager.start()
                logger.info("Heartbeat manager started")
            except Exception as e:
                logger.error("Failed to start heartbeat manager: %s", e, exc_info=True)
            try:
                if self._file_transfer and hasattr(self._file_transfer, 'start'):
                    await self._file_transfer.start()
                logger.info("File transfer started")
            except Exception as e:
                logger.error("Failed to start file transfer: %s", e, exc_info=True)
            # AgentProxy 在 __init__ 中已初始化，不需要 start()

            # 根据节点数更新速率限制器
            await self._update_rate_limiter()

            # 注册消息处理器
            self._register_default_handlers()

            # 设置信号处理器
            self._setup_signal_handlers()

            self._running = True
            logger.info(f"MeshAgent started successfully on {self.config.host}:{self.config.port}")

        except (ValueError, TypeError, OSError, RuntimeError) as e:
            logger.error(f"Failed to start MeshAgent: {e}", exc_info=True)
            await self.stop()
            raise

    async def stop(self) -> None:
        """
        停止 MeshAgent（优雅关闭）

        关闭顺序：
        1. 取消所有正在运行的任务
        2. 停止各个模块
        3. 关闭存储
        4. 关闭 Event Loop（如果需要）
        """
        if not self._running:
            return

        logger.info("Stopping MeshAgent...")
        self._running = False
        self._shutdown_event.set()

        # 1. 取消所有正在运行的任务
        try:
            await self._cancel_all_tasks()
        except RuntimeError as e:
            logger.warning(f"Error canceling tasks: {e}")

        # 2. 停止各个模块（按依赖顺序反向停止）
        modules = [
            ("discussion", self._discussion_coordinator),
            ("agent_proxy", self._agent_proxy),
            ("file_transfer", self._file_transfer),
            ("heartbeat", self._heartbeat_manager),
            ("node_discovery", self._node_discovery),
            ("message_transport", self._message_transport),
            ("https_client", self._https_client),
            ("offline_queue", self._offline_queue),
        ]

        for name, module in modules:
            if module:
                try:
                    if hasattr(module, 'stop'):
                        await module.stop()
                    elif hasattr(module, 'close'):
                        await module.close()
                    logger.debug(f"Stopped {name}")
                except (RuntimeError, OSError, ConnectionError) as e:
                    logger.warning(f"Error stopping {name}: {e}")

        # 3. 关闭存储
        if self._storage:
            try:
                await self._storage.close()
            except (RuntimeError, OSError) as e:
                logger.warning(f"Error closing storage: {e}")

        # 4. 清理 Event Loop（Windows/Linux 兼容）
        try:
            loop = self._get_event_loop()
            if loop and not loop.is_closed():
                # 获取当前任务
                current_task = asyncio.current_task(loop)
                # 取消所有剩余任务（排除当前任务）
                pending = [t for t in asyncio.all_tasks(loop) if t is not current_task and not t.done()]
                if pending:
                    for task in pending:
                        task.cancel()
                    # 等待任务取消完成（带超时）
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*pending, return_exceptions=True),
                            timeout=2.0
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Some tasks did not complete within timeout during shutdown")
        except RuntimeError:
            # Event Loop 可能已经关闭
            pass

        logger.info("MeshAgent stopped")

    async def run(self) -> None:
        """运行 MeshAgent（阻塞直到停止）"""
        await self.start()
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("MeshAgent run cancelled")
        finally:
            await self.stop()

    def _setup_signal_handlers(self) -> None:
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        if platform.system() != "Windows":
            signal.signal(signal.SIGTERM, signal_handler)

    async def _register_self_node(self) -> None:
        """注册自己的节点信息到数据库"""
        if not self._storage or not self.node_id:
            return
        
        try:
            from src.storage.sqlite_storage import Node
            from datetime import datetime
            
            # 从 tailscale 命令获取本机信息
            tailscale_hostname = self.node_id
            local_ip = None
            
            try:
                import subprocess
                # 获取 hostname
                result = subprocess.run(
                    ["tailscale", "status", "--json"],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    import json
                    data = json.loads(result.stdout)
                    self_info = data.get("Self", {})
                    tailscale_hostname = (self_info.get("HostName") or self.node_id).lower()
                    # 获取 Tailscale IP
                    ips = self_info.get("TailscaleIPs", [])
                    if ips:
                        local_ip = ips[0]
            except Exception as e:
                logger.warning(f"Failed to get Tailscale info: {e}")
            
            # 如果获取失败，使用 node_id
            if not local_ip:
                local_ip = self.node_id
            
            # 创建节点对象
            node = Node(
                node_id=tailscale_hostname,  # 使用完整 hostname
                name=tailscale_hostname,
                ip=local_ip,
                tailscale_hostname=tailscale_hostname,
                status="online",
                version=self.VERSION,
                created_at=datetime.now(),
            )
            
            # 尝试注册
            if self._storage.upsert_node(node):
                logger.info(f"Registered self node: {tailscale_hostname} ({local_ip})")
            else:
                logger.debug(f"Node {tailscale_hostname} already exists")
        except Exception as e:
            logger.error(f"Failed to register self node: {e}")

    # ==================== 事件处理 ====================

    def _register_default_handlers(self) -> None:
        """注册默认消息处理器"""
        # 注册到消息传输模块
        if self._message_transport:
            self._message_transport.register_handler("chat", self._on_chat_message)
            self._message_transport.register_handler("command", self._on_command_message)
            self._message_transport.register_handler("file", self._on_file_message)

    async def _on_chat_message(self, message: Message) -> None:
        """处理聊天消息"""
        logger.debug(f"Received chat message from {message.from_node}")
        # 调用注册的处理器
        handlers = self._message_handlers.get("chat", [])
        for handler in handlers:
            try:
                await handler(message)
            except (TypeError, ValueError, RuntimeError) as e:
                logger.error(f"Error in chat handler: {e}")

    async def _on_command_message(self, message: Message) -> None:
        """处理命令消息"""
        logger.debug(f"Received command message from {message.from_node}")
        command = message.payload.get("command")
        if command and command in self._command_handlers:
            try:
                result = await self._command_handlers[command](message.payload)
                # 发送响应
                await self.send_message(
                    to_node=message.from_node,
                    message_type="command_response",
                    payload={
                        "command": command,
                        "result": result,
                        "in_reply_to": message.id,
                    }
                )
            except (TypeError, ValueError, KeyError, RuntimeError) as e:
                logger.error(f"Error handling command {command}: {e}")

    async def _on_file_message(self, message: Message) -> None:
        """处理文件消息"""
        logger.debug(f"Received file message from {message.from_node}")
        file_path = message.payload.get("file_path")
        file_name = message.payload.get("file_name")
        if file_path:
            for handler in self._file_handlers:
                try:
                    await handler(message.from_node, file_path, file_name or "")
                except (TypeError, ValueError, OSError, RuntimeError) as e:
                    logger.error(f"Error in file handler: {e}")

    def register_message_handler(
        self,
        message_type: str,
        handler: Callable[[Message], Awaitable[None]]
    ) -> None:
        """
        注册消息处理器

        Args:
            message_type: 消息类型
            handler: 处理器函数
        """
        if message_type not in self._message_handlers:
            self._message_handlers[message_type] = []
        self._message_handlers[message_type].append(handler)
        logger.debug(f"Registered handler for message type: {message_type}")

    def register_file_handler(
        self,
        handler: Callable[[str, str, str], Awaitable[None]]
    ) -> None:
        """
        注册文件处理器

        Args:
            handler: 处理器函数 (from_node, file_path, file_name)
        """
        self._file_handlers.append(handler)
        logger.debug("Registered file handler")

    def register_command_handler(
        self,
        command: str,
        handler: Callable[[Dict[str, Any]], Awaitable[Any]]
    ) -> None:
        """
        注册命令处理器

        Args:
            command: 命令名称
            handler: 处理器函数
        """
        self._command_handlers[command] = handler
        logger.debug(f"Registered handler for command: {command}")

    # ==================== 公共 API ====================

    async def send_message(
        self,
        to_node: str,
        message_type: str,
        payload: Dict[str, Any],
        reply_to: Optional[str] = None,
    ) -> bool:
        """
        发送消息到指定节点

        Args:
            to_node: 目标节点 ID
            message_type: 消息类型
            payload: 消息内容
            reply_to: 回复的消息 ID

        Returns:
            是否发送成功
        """
        if not self._message_transport:
            logger.error("Message transport not initialized")
            return False

        try:
            await self._message_transport.send_message(
                to_node=to_node,
                msg_type=message_type,
                payload=payload,
                reply_to=reply_to,
            )
            return True
        except (ConnectionError, OSError, RuntimeError, TimeoutError) as e:
            logger.error(f"Failed to send message to {to_node}: {e}")
            return False

    async def broadcast_message(
        self,
        message_type: str,
        payload: Dict[str, Any],
        exclude_nodes: Optional[List[str]] = None,
    ) -> Dict[str, bool]:
        """
        广播消息到所有在线节点

        Args:
            message_type: 消息类型
            payload: 消息内容
            exclude_nodes: 排除的节点列表

        Returns:
            各节点发送结果
        """
        if not self._node_discovery:
            logger.error("Node discovery not initialized")
            return {}

        results = {}
        exclude_nodes = exclude_nodes or []

        try:
            nodes = await self._node_discovery.get_online_nodes()
            for node in nodes:
                if node.node_id != self.node_id and node.node_id not in exclude_nodes:
                    success = await self.send_message(
                        to_node=node.node_id,
                        message_type=message_type,
                        payload=payload,
                    )
                    results[node.node_id] = success
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Error broadcasting message: {e}")

        return results

    async def send_file(
        self,
        to_node: str,
        file_path: str,
        remote_path: Optional[str] = None,
        callback: Optional[TransferCallback] = None,
    ) -> Optional[str]:
        """
        发送文件到指定节点

        Args:
            to_node: 目标节点 ID
            file_path: 本地文件路径
            remote_path: 远程保存路径
            callback: 传输回调

        Returns:
            传输 ID
        """
        if not self._file_transfer:
            logger.error("File transfer not initialized")
            return None

        try:
            transfer_id = await self._file_transfer.send_file(
                target_node=to_node,
                file_path=file_path,
                callback=callback,
            )
            return transfer_id
        except (ConnectionError, OSError, RuntimeError, PermissionError) as e:
            logger.error(f"Failed to send file to {to_node}: {e}")
            return None

    async def execute_command(
        self,
        target_node: str,
        command: str,
        timeout: Optional[int] = None,
    ) -> Optional[ExecutionResult]:
        """
        在远程节点执行命令

        Args:
            target_node: 目标节点 ID
            command: 要执行的命令
            timeout: 超时时间（秒）

        Returns:
            执行结果
        """
        if not self._agent_proxy:
            logger.error("Agent proxy not initialized")
            return None

        try:
            result = await self._agent_proxy.execute_command(
                node_id=target_node,
                command=command,
                timeout=timeout,
            )
            return result
        except (ConnectionError, OSError, RuntimeError, TimeoutError) as e:
            logger.error(f"Failed to execute command on {target_node}: {e}")
            return None

    async def start_discussion(
        self,
        topic: str,
        context: str,
        participants: List[str],
        user_id: str,
        max_rounds: int = 5,
    ) -> Optional[str]:
        """
        发起讨论

        Args:
            topic: 讨论主题
            context: 讨论上下文
            participants: 参与者节点列表
            user_id: 用户 ID
            max_rounds: 最大轮数

        Returns:
            讨论 ID
        """
        if not self._discussion_coordinator:
            logger.error("Discussion coordinator not initialized")
            return None

        try:
            discussion_id = await self._discussion_coordinator.create_discussion(
                topic=topic,
                context=context,
                participants=participants,
                user_id=user_id,
                max_rounds=max_rounds,
            )
            return discussion_id
        except (ValueError, TypeError, RuntimeError, ConnectionError) as e:
            logger.error(f"Failed to start discussion: {e}")
            return None

    async def get_nodes(self) -> List[NodeInfo]:
        """
        获取所有已知节点

        Returns:
            节点列表
        """
        if not self._node_discovery:
            return []

        try:
            return await self._node_discovery.get_all_nodes()
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Failed to get nodes: {e}")
            return []

    async def get_online_nodes(self) -> List[NodeInfo]:
        """
        获取在线节点

        Returns:
            在线节点列表
        """
        if not self._node_discovery:
            return []

        try:
            return await self._node_discovery.get_online_nodes()
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Failed to get online nodes: {e}")
            return []

    def get_status(self) -> Dict[str, Any]:
        """
        获取节点状态

        Returns:
            状态信息
        """
        return {
            "node_id": self.node_id,
            "version": self.VERSION,
            "running": self._running,
            "host": self.config.host,
            "port": self.config.port,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "modules": {
                "storage": self._storage is not None,
                "security": self._key_manager is not None,
                "network": self._node_discovery is not None,
                "transfer": self._file_transfer is not None,
                "proxy": self._agent_proxy is not None,
                "discussion": self._discussion_coordinator is not None,
            }
        }

    # ==================== 配置管理 ====================

    @classmethod
    def load_config(cls, config_path: str) -> MeshConfig:
        """
        从文件加载配置

        Args:
            config_path: 配置文件路径

        Returns:
            配置对象
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, 'r', encoding='utf-8') as f:
            if path.suffix in ['.yaml', '.yml']:
                data = yaml.safe_load(f)
            elif path.suffix == '.json':
                data = json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {path.suffix}")

        return MeshConfig.from_dict(data)

    def save_config(self, config_path: str) -> None:
        """
        保存配置到文件

        Args:
            config_path: 配置文件路径
        """
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = self.config.to_dict()

        with open(path, 'w', encoding='utf-8') as f:
            if path.suffix in ['.yaml', '.yml']:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
            elif path.suffix == '.json':
                json.dump(data, f, indent=2, ensure_ascii=False)
            else:
                raise ValueError(f"Unsupported config format: {path.suffix}")

        logger.info(f"Config saved to {config_path}")


# ==================== 便捷函数 ====================

def create_agent(
    node_id: str,
    config_path: Optional[str] = None,
    **kwargs
) -> MeshAgent:
    """
    创建 MeshAgent 实例

    Args:
        node_id: 节点 ID
        config_path: 配置文件路径（可选）
        **kwargs: 其他配置参数

    Returns:
        MeshAgent 实例
    """
    if config_path:
        config = MeshAgent.load_config(config_path)
        # 覆盖配置
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
    else:
        config = MeshConfig(node_id=node_id, **kwargs)

    return MeshAgent(config)


async def run_agent(
    node_id: str,
    config_path: Optional[str] = None,
    **kwargs
) -> None:
    """
    运行 MeshAgent（阻塞）

    Args:
        node_id: 节点 ID
        config_path: 配置文件路径（可选）
        **kwargs: 其他配置参数
    """
    agent = create_agent(node_id, config_path, **kwargs)
    await agent.run()
