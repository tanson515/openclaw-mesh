"""
OpenClaw Mesh - Message Transport
消息传输模块：HTTPS 客户端/服务器 + 消息签名验证
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any, Awaitable

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig

from src.security.message_signer import MessageSigner
from src.constants import VERSION
from src.security.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """消息数据类"""
    id: str
    type: str
    from_node: str
    to_node: str
    payload: dict
    timestamp: float
    signature: Optional[str] = None
    reply_to: Optional[str] = None

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "type": self.type,
            "from_node": self.from_node,
            "to_node": self.to_node,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "signature": self.signature,
            "reply_to": self.reply_to,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        """从字典创建"""
        import uuid
        return cls(
            id=data.get("id") or str(uuid.uuid4()),
            type=data.get("type") or data.get("message_type", "unknown"),
            from_node=data.get("from_node", ""),
            to_node=data.get("to_node", ""),
            payload=data.get("payload", {}) or {"content": data.get("content", "")},
            timestamp=data.get("timestamp", time.time()),
            signature=data.get("signature"),
            reply_to=data.get("reply_to"),
        )


class MessageTransport:
    """
    消息传输模块

    职责:
    1. P2P HTTPS 直接发送消息到目标节点
    2. 发送失败时存入本地离线队列
    3. 接收端通过 HTTPS API 接收消息
    4. 离线消息自动重试

    特性:
    - HTTPS 客户端（带证书验证）
    - 消息签名验证中间件
    - 速率限制
    - 消息去重
    """

    # 默认配置
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_PORT = 8443

    def __init__(
        self,
        node_id: str,
        node_discovery: Any,  # NodeDiscovery 实例
        offline_queue: Any,  # BoundedOfflineQueue 实例
        message_signer: Optional[MessageSigner] = None,
        rate_limiter: Optional[RateLimiter] = None,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        ca_cert_path: Optional[str] = None,
        port: int = DEFAULT_PORT,
        host: str = "0.0.0.0",
        file_transfer: Any = None,  # FileTransfer 实例（可选）
    ):
        """
        初始化消息传输模块

        Args:
            node_id: 本节点 ID
            node_discovery: 节点发现模块实例
            offline_queue: 离线队列实例
            message_signer: 消息签名器（可选）
            rate_limiter: 速率限制器（可选）
            cert_path: 证书路径
            key_path: 私钥路径
            ca_cert_path: CA 证书路径（用于验证对等节点）
            port: 监听端口
            host: 监听地址
        """
        self.node_id = node_id
        self.node_discovery = node_discovery
        self.offline_queue = offline_queue
        self.message_signer = message_signer
        self.rate_limiter = rate_limiter
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_cert_path = ca_cert_path
        self.port = port
        self.host = host
        self.file_transfer = file_transfer

        # HTTP 客户端
        self._client: Optional[aiohttp.ClientSession] = None

        # 消息处理器
        self._handlers: Dict[str, Callable[[Message], Awaitable[None]]] = {}

        # 已处理消息缓存（去重）
        self._processed_messages: set = set()
        self._max_processed_cache = 10000

        # 服务器
        self._server: Optional[uvicorn.Server] = None
        self._app: Optional[FastAPI] = None

        logger.info(f"MessageTransport initialized: node_id={node_id}, port={port}")

    def _create_ssl_context(self) -> aiohttp.ClientSession:
        """创建带 SSL 的 HTTP 客户端"""
        import ssl

        # 创建 SSL 上下文
        ssl_context = ssl.create_default_context()

        if self.ca_cert_path:
            ssl_context.load_verify_locations(self.ca_cert_path)

        if self.cert_path and self.key_path:
            ssl_context.load_cert_chain(self.cert_path, self.key_path)

        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
        ssl_context.check_hostname = False

        # 创建 aiohttp ClientSession
        connector = aiohttp.TCPConnector(
            ssl=ssl_context if self.ca_cert_path else False,
            limit=100,
            limit_per_host=20
        )

        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT),
            connector=connector,
            trust_env=False,  # 禁用系统代理
        )

    @property
    async def http_client(self) -> aiohttp.ClientSession:
        """获取 HTTP 客户端"""
        if self._client is None:
            self._client = self._create_ssl_context()
        return self._client

    def register_handler(self, msg_type: str, handler: Callable[[Message], Awaitable[None]]):
        """
        注册消息处理器

        Args:
            msg_type: 消息类型
            handler: 处理器函数
        """
        self._handlers[msg_type] = handler
        logger.info(f"Message handler registered for type: {msg_type}")

    def create_message(
        self,
        msg_type: str,
        to_node: str,
        payload: dict,
        reply_to: Optional[str] = None
    ) -> Message:
        """
        创建消息

        Args:
            msg_type: 消息类型
            to_node: 目标节点
            payload: 消息内容
            reply_to: 回复的消息 ID（可选）

        Returns:
            消息对象
        """
        import uuid

        message = Message(
            id=str(uuid.uuid4()),
            type=msg_type,
            from_node=self.node_id,
            to_node=to_node,
            payload=payload,
            timestamp=time.time(),
            reply_to=reply_to,
        )

        # 签名消息
        if self.message_signer:
            message.signature = self.message_signer.sign_message(
                message_id=message.id,
                from_node=message.from_node,
                payload=message.payload,
                timestamp=int(message.timestamp)
            )

        return message

    async def send_message(
        self,
        to_node: str,
        msg_type: str,
        payload: dict,
        reply_to: Optional[str] = None,
        timeout: Optional[float] = None
    ) -> str:
        """
        发送消息到指定节点

        Args:
            to_node: 目标节点 ID
            msg_type: 消息类型
            payload: 消息内容
            reply_to: 回复的消息 ID（可选）
            timeout: 超时（秒）

        Returns:
            消息 ID
        """
        # 查找节点地址
        node = await self.node_discovery.get_node(to_node)
        if not node:
            raise NodeNotFoundError(f"Node not found: {to_node}")

        logger.info(f"[SEND] 发送消息: type={msg_type}, to={to_node}, node_ip={node.ip}, node_port={node.port}")

        # 创建消息
        message = self.create_message(msg_type, to_node, payload, reply_to)

        # 尝试直接发送
        try:
            await self._send_to_address(node.ip, node.port, message, timeout)
            logger.debug(f"Message sent to {to_node}: {message.id}")
            return message.id

        except (ConnectionError, OSError, TimeoutError, ClientResponseError) as e:
            logger.warning(f"Direct send to {to_node} failed: {e}")

            # 存入离线队列
            if self.offline_queue:
                await self.offline_queue.enqueue(to_node, message)
                logger.info(f"Message queued for offline node {to_node}: {message.id}")
            else:
                raise MessageTransportError(f"Failed to send message: {e}")

            return message.id

    async def send_simple_message(
        self,
        to_node: str,
        content: Any,
        msg_id: Optional[str] = None
    ) -> str:
        """
        简化版消息发送 - 使用 /msg 端点，无签名，异步发送

        Args:
            to_node: 目标节点 ID
            content: 消息内容（字符串或字典）
            msg_id: 可选的消息 ID

        Returns:
            消息 ID
        """
        import uuid

        message_id = msg_id or f"msg_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        # 查找节点地址
        node = await self.node_discovery.get_node(to_node)
        if not node:
            # 节点未知，存入离线队列稍后重试
            if self.offline_queue:
                # 构造一个兼容的消息对象存入队列
                from dataclasses import dataclass

                @dataclass
                class SimpleMessage:
                    id: str
                    type: str = "simple"
                    from_node: str = ""
                    to_node: str = ""
                    payload: dict = None
                    timestamp: float = 0
                    signature: Optional[str] = None

                    def __post_init__(self):
                        if self.payload is None:
                            self.payload = {}

                    def to_dict(self):
                        return {
                            "id": self.id,
                            "type": self.type,
                            "from_node": self.from_node,
                            "to_node": self.to_node,
                            "payload": self.payload,
                            "timestamp": self.timestamp,
                            "signature": self.signature,
                        }

                msg = SimpleMessage(
                    id=message_id,
                    from_node=self.node_id,
                    to_node=to_node,
                    payload={"content": content},
                    timestamp=time.time(),
                )
                await self.offline_queue.enqueue(to_node, msg)
                logger.info(f"节点 {to_node} 未知，消息已入队: {message_id}")
            else:
                logger.warning(f"节点 {to_node} 未知且离线队列未配置")
            return message_id

        # 异步发送（不等待确认）
        asyncio.create_task(
            self._send_simple_message_async(node.ip, node.port, message_id, content)
        )

        return message_id

    async def _send_simple_message_async(
        self,
        ip: str,
        port: int,
        message_id: str,
        content: Any
    ):
        """
        异步发送简单消息到指定地址

        Args:
            ip: IP 地址
            port: 端口
            message_id: 消息 ID
            content: 消息内容
        """
        try:
            client = await self.http_client

            # 根据是否有证书决定使用 HTTP 还是 HTTPS
            protocol = "https" if (self.cert_path and self.key_path) else "http"

            data = {
                "id": message_id,
                "from": self.node_id,
                "to": "*",  # 广播或指定接收
                "content": content,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            async with client.post(
                f"{protocol}://{ip}:{port}/msg",
                json=data,
                timeout=10.0
            ) as response:
                if response.status == 200:
                    logger.debug(f"简单消息 [{message_id}] 已发送至 {ip}:{port}")
                else:
                    raise ConnectionError(f"HTTP {response.status}")

        except Exception as e:
            logger.warning(f"发送简单消息到 {ip}:{port} 失败: {e}")
            # 存入离线队列，稍后重试
            if self.offline_queue:
                from dataclasses import dataclass

                @dataclass
                class SimpleMessage:
                    id: str
                    type: str = "simple"
                    from_node: str = ""
                    to_node: str = ""
                    payload: dict = None
                    timestamp: float = 0
                    signature: Optional[str] = None

                    def __post_init__(self):
                        if self.payload is None:
                            self.payload = {}

                    def to_dict(self):
                        return {
                            "id": self.id,
                            "type": self.type,
                            "from_node": self.from_node,
                            "to_node": self.to_node,
                            "payload": self.payload,
                            "timestamp": self.timestamp,
                            "signature": self.signature,
                        }

                # 获取节点 ID
                try:
                    node = await self.node_discovery.get_node_by_ip(ip)
                    to_node = node.node_id if node else "unknown"
                except:
                    to_node = "unknown"

                msg = SimpleMessage(
                    id=message_id,
                    from_node=self.node_id,
                    to_node=to_node,
                    payload={"content": content, "target_ip": ip, "target_port": port},
                    timestamp=time.time(),
                )
                await self.offline_queue.enqueue(to_node, msg)
                logger.info(f"消息 [{message_id}] 已入队，稍后重试")

    async def _send_to_address(
        self,
        ip: str,
        port: int,
        message: Message,
        timeout: Optional[float] = None
    ):
        """
        发送消息到指定地址

        Args:
            ip: IP 地址
            port: 端口
            message: 消息对象
            timeout: 超时（秒）
        """
        client = await self.http_client

        # 根据是否有证书决定使用 HTTP 还是 HTTPS
        protocol = "https" if (self.cert_path and self.key_path) else "http"

        async with client.post(
            f"{protocol}://{ip}:{port}/api/messages",
            json=message.to_dict(),
            timeout=timeout or self.DEFAULT_TIMEOUT
        ) as response:
            response.raise_for_status()

    async def broadcast(
        self,
        msg_type: str,
        payload: dict,
        exclude: Optional[List[str]] = None
    ) -> List[str]:
        """
        广播消息到所有在线节点

        Args:
            msg_type: 消息类型
            payload: 消息内容
            exclude: 排除的节点 ID 列表（可选）

        Returns:
            成功发送的消息 ID 列表
        """
        nodes = await self.node_discovery.get_online_nodes()
        exclude_set = set(exclude or [])
        exclude_set.add(self.node_id)

        message_ids = []

        for node in nodes:
            if node.node_id in exclude_set:
                continue

            try:
                msg_id = await self.send_message(node.node_id, msg_type, payload)
                message_ids.append(msg_id)
            except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
                logger.warning(f"Broadcast to {node.node_id} failed: {e}")

        logger.info(f"Broadcast sent to {len(message_ids)} nodes")
        return message_ids

    async def broadcast_simple(
        self,
        content: Any,
        exclude: Optional[List[str]] = None,
        wait: bool = False
    ) -> str:
        """
        简化广播 - 使用 /msg 端点，无签名，异步发送

        Args:
            content: 广播内容（字符串或字典）
            exclude: 排除的节点 ID 列表（可选）
            wait: 是否等待所有发送完成（默认 False，立即返回）

        Returns:
            广播消息 ID
        """
        import uuid

        message_id = f"broadcast_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        nodes = await self.node_discovery.get_online_nodes()
        exclude_set = set(exclude or [])
        exclude_set.add(self.node_id)

        sent_count = 0
        failed_nodes = []

        async def send_to_node(node):
            """发送给单个节点"""
            nonlocal sent_count
            try:
                await self._send_simple_message_async(
                    node.ip, node.port, message_id, content
                )
                sent_count += 1
            except Exception as e:
                logger.debug(f"Broadcast to {node.node_id} failed: {e}")
                failed_nodes.append(node.node_id)

        # 收集所有发送任务
        tasks = []
        for node in nodes:
            if node.node_id in exclude_set:
                continue
            if wait:
                tasks.append(send_to_node(node))
            else:
                # 不等待，直接创建后台任务
                asyncio.create_task(send_to_node(node))

        if wait and tasks:
            # 等待所有发送完成
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Broadcast [{message_id}] sent to {sent_count}/{len(tasks)} nodes")
        else:
            # 立即返回，后台发送
            logger.info(f"Broadcast [{message_id}] initiated to {len(nodes) - len(exclude_set)} nodes")

        return message_id

    async def _verify_message(self, message: Message) -> bool:
        """
        验证消息签名

        Args:
            message: 消息对象

        Returns:
            是否验证通过
        """
        if not self.message_signer or not message.signature:
            # 未配置签名器，跳过验证
            return True

        # 获取发送者公钥
        sender = await self.node_discovery.get_node(message.from_node)
        if not sender or not sender.public_key:
            logger.warning(f"Cannot verify message from {message.from_node}: no public key, allowing anyway")
            # TODO: Add proper public key exchange
            return True

        # 验证签名
        return self.message_signer.verify_message_with_key_bytes(
            signature=message.signature,
            message_id=message.id,
            from_node=message.from_node,
            payload=message.payload,
            public_key_bytes=sender.public_key.encode(),
            timestamp=int(message.timestamp)
        )

    def _is_message_processed(self, message_id: str) -> bool:
        """检查消息是否已处理（去重）"""
        return message_id in self._processed_messages

    def _mark_message_processed(self, message_id: str):
        """标记消息已处理"""
        # 清理缓存
        if len(self._processed_messages) >= self._max_processed_cache:
            # 移除一半的旧记录
            self._processed_messages = set(list(self._processed_messages)[self._max_processed_cache // 2:])

        self._processed_messages.add(message_id)

    async def _save_received_message(
        self,
        message_id: str,
        from_node: str,
        to_node: str,
        content: Any,
        raw_data: dict
    ) -> bool:
        """
        保存收到的消息到本地数据库

        Args:
            message_id: 消息 ID
            from_node: 发送节点
            to_node: 目标节点
            content: 消息内容
            raw_data: 原始数据

        Returns:
            是否保存成功
        """
        try:
            import aiosqlite
            from pathlib import Path

            # 使用 node_discovery 的数据库路径
            db_path = self.node_discovery.db_path

            async with aiosqlite.connect(db_path) as db:
                # 创建接收消息表（如果不存在）
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS received_messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id TEXT UNIQUE,
                        from_node TEXT,
                        to_node TEXT,
                        content TEXT,
                        raw_data TEXT,
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        read_status TEXT DEFAULT 'unread'
                    )
                """)

                # 插入消息
                await db.execute("""
                    INSERT OR REPLACE INTO received_messages
                    (message_id, from_node, to_node, content, raw_data)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    message_id,
                    from_node,
                    to_node,
                    json.dumps(content) if isinstance(content, (dict, list)) else str(content),
                    json.dumps(raw_data)
                ))
                await db.commit()

            logger.debug(f"消息 [{message_id}] 已保存到本地数据库")
            return True
        except Exception as e:
            logger.error(f"保存消息失败: {e}")
            return False

    async def _handle_incoming_message(self, message: Message) -> dict:
        """
        处理收到的消息

        Args:
            message: 消息对象

        Returns:
            响应数据
        """
        # 去重检查
        if self._is_message_processed(message.id):
            logger.debug(f"Message already processed: {message.id}")
            return {"status": "already_processed", "message_id": message.id}

        # 签名验证
        if not await self._verify_message(message):
            raise HTTPException(status_code=403, detail="Invalid signature")

        # 标记已处理（内存缓存）
        self._mark_message_processed(message.id)

        # 保存到数据库
        try:
            await self._save_received_message(
                message_id=message.id,
                from_node=message.from_node,
                to_node=message.to_node,
                content=message.payload.get("content", "") if isinstance(message.payload, dict) else str(message.payload),
                raw_data=message.to_dict()
            )
        except Exception as e:
            logger.error(f"Failed to save message to database: {e}")

        # 查找处理器
        handler = self._handlers.get(message.type)
        if handler:
            try:
                await handler(message)
            except (TypeError, ValueError, RuntimeError, KeyError) as e:
                logger.error(f"Message handler failed for {message.type}: {e}")
                raise HTTPException(status_code=500, detail="Handler error")
        else:
            logger.warning(f"No handler for message type: {message.type}")

        return {
            "status": "received",
            "message_id": message.id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _create_app(self) -> FastAPI:
        """创建 FastAPI 应用"""
        app = FastAPI(
            title="OpenClaw Mesh",
            version=VERSION,
            docs_url=None,
            redoc_url=None,
        )

        # CORS 中间件
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # 速率限制中间件
        @app.middleware("http")
        async def rate_limit_middleware(request: Request, call_next):
            if self.rate_limiter:
                client_ip = request.client.host if request.client else "unknown"
                if not self.rate_limiter.allow(client_ip):
                    return JSONResponse(
                        status_code=429,
                        content={"error": "Rate limit exceeded"}
                    )
            return await call_next(request)

        # 健康检查端点
        @app.get("/health")
        async def health_check():
            return {
                "service": "openclaw-mesh",
                "version": VERSION,
                "node_id": self.node_id,
                "status": "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        # 消息接收端点
        @app.post("/api/messages")
        async def receive_message(request: Request):
            body = await request.json()
            message = Message.from_dict(body)

            # 统一转为小写
            message.to_node = message.to_node.lower()
            message.from_node = message.from_node.lower()

            # 提取纯 node_id（去除 Tailscale 域名后缀）
            my_node_id = self.node_id.split('.')[0] if '.' in self.node_id else self.node_id
            target_node_id = message.to_node.split('.')[0] if '.' in message.to_node else message.to_node
            
            logger.info(f"[RECV] 收到消息: type={message.type}, from={message.from_node}, to={message.to_node}, my_node={self.node_id}({my_node_id})")

            # 检查是否是发给本节点的
            if target_node_id != my_node_id and message.to_node != "*":
                logger.info(f"[RECV] 消息不匹配，转发: to={message.to_node}, my={self.node_id}")
                # 转发消息
                if message.to_node != "*":
                    try:
                        # 获取目标节点信息，检查是否是自己
                        node = await self.node_discovery.get_node(message.to_node)
                        # Check if message is for this node
                        if node and (node.ip == self.host or node.ip == "127.0.0.1" or node.ip.startswith("localhost")):
                            logger.debug(f"Skipping forward to self: {message.to_node}")
                            return {"status": "skipped", "reason": "self"}
                            logger.debug(f"Skipping forward to self: {message.to_node}")
                            return {"status": "skipped", "reason": "self"}

                        await self.send_message(
                            message.to_node,
                            message.type,
                            message.payload,
                            message.reply_to
                        )
                        return {"status": "forwarded", "message_id": message.id}
                    except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
                        logger.error(f"Message forwarding failed: {e}")
                        raise HTTPException(status_code=502, detail="Forwarding failed")

            result = await self._handle_incoming_message(message)
            return result

        # 节点交换端点（Gossip）
        @app.post("/api/nodes/exchange")
        async def exchange_nodes(request: Request):
            body = await request.json()
            nodes = await self.node_discovery.handle_exchange_request(body)
            return {
                "from_node": self.node_id,
                "nodes": [n.to_dict() for n in nodes]
            }

        # 极简消息接收端点（新增）
        @app.post("/msg")
        async def simple_receive(request: Request):
            '''极简消息接收，无验证，本地存储保障'''
            try:
                data = await request.json()
                from_node = data.get("from")
                to_node = data.get("to")
                content = data.get("content")
                msg_id = data.get("id") or f"msg_{int(time.time() * 1000)}"

                # 保存到本地数据库
                await self._save_received_message(
                    message_id=msg_id,
                    from_node=from_node,
                    to_node=to_node,
                    content=content,
                    raw_data=data
                )

                logger.info(f"收到消息 [{msg_id}] from {from_node}: {content[:100] if isinstance(content, str) else content}")

                return {"ok": True, "received": True, "id": msg_id}
            except Exception as e:
                logger.error(f"消息接收失败: {e}")
                return {"ok": False, "error": str(e)}

        # 错误处理
        @app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            logger.error(f"Unhandled exception: {exc}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"error": "Internal server error"}
            )

        return app

    async def start_server(self):
        """启动 HTTPS 服务器"""
        self._app = self._create_app()

        # 构建 hypercorn 配置
        config = HypercornConfig()
        config.bind = [f"{self.host}:{self.port}"]
        config.loglevel = "info"
        config.accesslog = logger
        config.errorlog = logger

        # SSL 配置
        if self.cert_path and self.key_path:
            config.certfile = self.cert_path
            config.keyfile = self.key_path
            config.ssl_handshake_timeout = 15.0  # 关键：握手超时
            logger.info(f"HTTPS server starting on https://{self.host}:{self.port}")
        else:
            logger.info(f"HTTP server starting on http://{self.host}:{self.port}")

        # 启动 hypercorn 服务器
        self._server = await serve(self._app, config)

    async def stop_server(self):
        """停止 HTTPS 服务器"""
        if self._server:
            self._server.should_exit = True
            # 等待服务器停止
            await asyncio.sleep(0.5)

        if self._client:
            await self._client.close()

        logger.info("HTTPS server stopped")

    async def start(self):
        """启动传输模块"""
        await self.start_server()

    async def stop(self):
        """停止传输模块"""
        await self.stop_server()


class NodeNotFoundError(Exception):
    """节点未找到错误"""
    pass


class MessageTransportError(Exception):
    """消息传输错误"""
    pass
