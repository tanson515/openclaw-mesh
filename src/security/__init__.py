"""
OpenClaw Mesh - Security Module
安全模块：TLS/HTTPS + Ed25519 密钥管理
"""

from .certificate_manager import CertificateManager
from .key_manager import KeyManager
from .message_signer import MessageSigner
from .rate_limiter import RateLimiter

# 可选导入（依赖 httpx/fastapi）
try:
    from .https_transport import HTTPSServer, HTTPSClient
    __all__ = [
        'CertificateManager',
        'KeyManager',
        'MessageSigner',
        'HTTPSServer',
        'HTTPSClient',
        'RateLimiter',
    ]
except ImportError:
    __all__ = [
        'CertificateManager',
        'KeyManager',
        'MessageSigner',
        'RateLimiter',
    ]