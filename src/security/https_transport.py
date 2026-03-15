"""
OpenClaw Mesh - HTTPS Transport
HTTPS 服务器和客户端：基于 FastAPI，支持双向 TLS
"""

import asyncio
import logging
import ssl
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, Callable, Awaitable

import aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from ..constants import VERSION
from .certificate_manager import CertificateManager
from .message_signer import MessageSigner

logger = logging.getLogger(__name__)


class HTTPSClient:
    """
    HTTPS 客户端
    
    职责:
    1. 发送 HTTPS 请求
    2. 支持客户端证书验证
    3. 支持双向 TLS
    4. 连接池管理
    
    特性:
    - 支持 mTLS（双向 TLS）
    - 连接复用
    - 自动重试
    """
    
    # 默认配置
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_MAX_RETRIES = 3
    
    def __init__(
        self,
        cert_manager: CertificateManager,
        verify_mode: str = "peer",  # none | peer | mutual
        ca_cert_path: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES
    ):
        """
        初始化 HTTPS 客户端
        
        Args:
            cert_manager: 证书管理器
            verify_mode: 验证模式 (none | peer | mutual)
            ca_cert_path: CA 证书路径（用于验证服务器）
            timeout: 请求超时（秒）
            max_retries: 最大重试次数
        """
        self.cert_manager = cert_manager
        self.verify_mode = verify_mode
        self.ca_cert_path = ca_cert_path
        self.timeout = timeout
        self.max_retries = max_retries
        
        self._client: Optional[aiohttp.ClientSession] = None
        self._ssl_context: Optional[ssl.SSLContext] = None
        
        logger.info(f"HTTPSClient initialized: verify_mode={verify_mode}")
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """
        创建 SSL 上下文
        
        Returns:
            SSL 上下文
        """
        if self.verify_mode == "none":
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            # 禁用 TLSv1/TLSv1.1，强制 TLS 1.2+
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
            # 禁用压缩（解决 EOF 的关键）
            context.options |= ssl.OP_NO_COMPRESSION
            # 设置 ciphers 兼容 aiohttp
            context.set_ciphers('DEFAULT:!aNULL:!eNULL:!LOW:!EXPORT:!DES:!3DES:!MD5:!PSK')
            return context
        
        elif self.verify_mode == "peer":
            context = ssl.create_default_context()
            if self.ca_cert_path:
                context.load_verify_locations(self.ca_cert_path)
            context.verify_mode = ssl.CERT_REQUIRED
            # 禁用 TLSv1/TLSv1.1，强制 TLS 1.2+
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
            # 禁用压缩（解决 EOF 的关键）
            context.options |= ssl.OP_NO_COMPRESSION
            # 设置 ciphers 兼容 aiohttp
            context.set_ciphers('DEFAULT:!aNULL:!eNULL:!LOW:!EXPORT:!DES:!3DES:!MD5:!PSK')
            return context
        
        elif self.verify_mode == "mutual":
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            
            # 加载客户端证书
            cert_file, key_file = self.cert_manager.get_ssl_context_files()
            context.load_cert_chain(cert_file, key_file)
            
            # 加载 CA 证书（如果需要验证服务器）
            if self.ca_cert_path:
                context.load_verify_locations(self.ca_cert_path)
            
            context.verify_mode = ssl.CERT_REQUIRED
            # 禁用 TLSv1/TLSv1.1，强制 TLS 1.2+
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
            # 禁用压缩（解决 EOF 的关键）
            context.options |= ssl.OP_NO_COMPRESSION
            # 设置 ciphers 兼容 aiohttp
            context.set_ciphers('DEFAULT:!aNULL:!eNULL:!LOW:!EXPORT:!DES:!3DES:!MD5:!PSK')
            return context
        
        else:
            raise ValueError(f"Invalid verify_mode: {self.verify_mode}")
    
    async def start(self):
        """启动客户端"""
        self._ssl_context = self._create_ssl_context()
        
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ssl=self._ssl_context if self.verify_mode != "none" else False
        )
        self._client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            connector=connector,
            trust_env=False  # 禁用系统代理，避免干扰节点间通信
        )
        
        logger.info("HTTPSClient started")
    
    async def stop(self):
        """停止客户端"""
        if self._client:
            await self._client.close()
            self._client = None
        logger.info("HTTPSClient stopped")
    
    async def request(
        self,
        method: str,
        url: str,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> aiohttp.ClientResponse:
        """
        发送 HTTP 请求
        
        Args:
            method: HTTP 方法
            url: 请求 URL
            json_data: JSON 数据
            data: 二进制数据
            headers: 请求头
            timeout: 超时（秒）
            
        Returns:
            HTTP 响应
        """
        if not self._client:
            raise RuntimeError("Client not started")
        
        last_error = None
        for attempt in range(self.max_retries):
            try:
                async with self._client.request(
                    method=method,
                    url=url,
                    json=json_data,
                    data=data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout or self.timeout)
                ) as response:
                    response.raise_for_status()
                    return response
                
            except aiohttp.ClientResponseError as e:
                # HTTP 错误不重试
                raise
            except (ConnectionError, OSError, TimeoutError) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    logger.warning(f"Request failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    break
        
        logger.error(f"Request failed after {self.max_retries} attempts: {last_error}")
        raise last_error
    
    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> aiohttp.ClientResponse:
        """发送 GET 请求"""
        return await self.request("GET", url, headers=headers, timeout=timeout)
    
    async def post(
        self,
        url: str,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> aiohttp.ClientResponse:
        """发送 POST 请求"""
        return await self.request("POST", url, json_data, data, headers, timeout)
    
    async def put(
        self,
        url: str,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> aiohttp.ClientResponse:
        """发送 PUT 请求"""
        return await self.request("PUT", url, json_data, data, headers, timeout)
    
    async def delete(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> aiohttp.ClientResponse:
        """发送 DELETE 请求"""
        return await self.request("DELETE", url, headers=headers, timeout=timeout)


class HTTPSServer:
    """
    HTTPS 服务器
    
    职责:
    1. 基于 FastAPI 的 HTTPS 服务器
    2. 客户端证书验证
    3. 双向 TLS
    
    特性:
    - 支持 mTLS（双向 TLS）
    - 客户端证书验证
    - 请求签名验证
    """
    
    # 默认配置
    DEFAULT_HOST = "0.0.0.0"
    DEFAULT_PORT = 8443
    
    def __init__(
        self,
        cert_manager: CertificateManager,
        message_signer: Optional[MessageSigner] = None,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        require_client_cert: bool = False,
        ca_cert_path: Optional[str] = None,
        allowed_nodes: Optional[list] = None
    ):
        """
        初始化 HTTPS 服务器
        
        Args:
            cert_manager: 证书管理器
            message_signer: 消息签名器（可选）
            host: 监听地址
            port: 监听端口
            require_client_cert: 是否要求客户端证书
            ca_cert_path: CA 证书路径（用于验证客户端）
            allowed_nodes: 允许的节点列表（可选）
        """
        self.cert_manager = cert_manager
        self.message_signer = message_signer
        self.host = host
        self.port = port
        self.require_client_cert = require_client_cert
        self.ca_cert_path = ca_cert_path
        self.allowed_nodes = set(allowed_nodes) if allowed_nodes else None
        
        self._app: Optional[FastAPI] = None
        self._server = None
        self._ssl_context: Optional[ssl.SSLContext] = None
        
        # 路由处理器
        self._handlers: Dict[str, Callable] = {}
        
        logger.info(f"HTTPSServer initialized: host={host}, port={port}, mtls={require_client_cert}")
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """
        创建 SSL 上下文
        
        Returns:
            SSL 上下文
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        
        # 加载服务器证书
        cert_file, key_file = self.cert_manager.get_ssl_context_files()
        context.load_cert_chain(cert_file, key_file)
        
        # 配置客户端证书验证
        if self.require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
            if self.ca_cert_path:
                context.load_verify_locations(self.ca_cert_path)
        else:
            context.verify_mode = ssl.CERT_OPTIONAL if self.ca_cert_path else ssl.CERT_NONE
        
        # 设置 TLS 版本
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        
        return context
    
    def _create_app(self) -> FastAPI:
        """
        创建 FastAPI 应用
        
        Returns:
            FastAPI 应用
        """
        app = FastAPI(
            title="OpenClaw Mesh",
            version=VERSION,
            docs_url=None,  # 禁用文档
            redoc_url=None,
        )
        
        # 添加中间件
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # 健康检查端点
        @app.get("/health")
        async def health_check():
            return {
                "service": "openclaw-mesh",
                "version": VERSION,
                "status": "healthy"
            }
        
        # 注册动态路由
        for path, handler in self._handlers.items():
            app.add_api_route(path, handler, methods=["GET", "POST", "PUT", "DELETE"])
        
        # 错误处理
        @app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            logger.error(f"Unhandled exception: {exc}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"error": "Internal server error"}
            )
        
        return app
    
    def add_route(
        self,
        path: str,
        handler: Callable,
        methods: list = None,
        require_signature: bool = False
    ):
        """
        添加路由
        
        Args:
            path: 路由路径
            handler: 处理器函数
            methods: HTTP 方法列表
            require_signature: 是否要求签名验证
        """
        if methods is None:
            methods = ["POST"]
        
        # 包装处理器（添加签名验证）
        if require_signature and self.message_signer:
            async def wrapped_handler(request: Request):
                # 验证签名
                body = await request.json()
                signature = body.get("signature")
                if not signature:
                    raise HTTPException(status_code=401, detail="Missing signature")
                
                # 验证签名
                from_node = body.get("from_node")
                payload = body.get("payload")
                timestamp = body.get("timestamp")
                
                if not from_node or payload is None:
                    raise HTTPException(status_code=400, detail="Missing required fields for signature verification")
                
                # 构造验证消息
                message_data = {
                    "from_node": from_node,
                    "payload": payload,
                    "timestamp": timestamp,
                }
                
                # 使用消息签名器验证
                is_valid = await self.message_signer.verify_message(
                    message_id=body.get("message_id", ""),
                    from_node=from_node,
                    payload=payload,
                    signature=signature,
                    timestamp=timestamp or 0,
                )
                
                if not is_valid:
                    raise HTTPException(status_code=403, detail="Invalid signature")
                
                return await handler(request)
            
            self._handlers[path] = wrapped_handler
        else:
            self._handlers[path] = handler
        
        logger.info(f"Route added: {path} {methods}")
    
    async def start(self):
        """启动服务器"""
        self._ssl_context = self._create_ssl_context()
        self._app = self._create_app()
        
        config = uvicorn.Config(
            app=self._app,
            host=self.host,
            port=self.port,
            ssl_keyfile=self.cert_manager.key_path,
            ssl_certfile=self.cert_manager.cert_path,
            ssl_cert_reqs=ssl.CERT_REQUIRED if self.require_client_cert else ssl.CERT_NONE,
            ssl_ca_certs=self.ca_cert_path,
            log_level="info",
        )
        
        self._server = uvicorn.Server(config)
        
        # 在后台运行服务器
        self._server_task = asyncio.create_task(self._server.serve())
        
        logger.info(f"HTTPS server started on https://{self.host}:{self.port}")
    
    async def stop(self):
        """停止服务器"""
        if self._server:
            self._server.should_exit = True
            if hasattr(self, '_server_task'):
                try:
                    await asyncio.wait_for(self._server_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("Server shutdown timed out")
        
        logger.info("HTTPS server stopped")
    
    @property
    def is_running(self) -> bool:
        """检查服务器是否运行中"""
        return self._server is not None and not self._server.should_exit


class HTTPSTransportError(Exception):
    """HTTPS 传输错误"""
    pass