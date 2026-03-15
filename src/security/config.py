"""
OpenClaw Mesh - Security Configuration
安全配置模块
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class TLSConfig:
    """TLS 配置"""
    enabled: bool = True
    mode: str = "self_signed"  # tailscale | self_signed | custom_ca
    
    # Tailscale 配置
    use_tailscale_cert: bool = False
    tailscale_hostname: Optional[str] = None
    
    # 自签名证书配置
    auto_generate_cert: bool = True
    cert_days: int = 365
    cert_country: str = "CN"
    cert_organization: str = "OpenClaw Mesh"
    
    # 通用配置
    min_version: str = "TLSv1.3"
    cert_dir: str = "./certs"
    
    # 客户端验证
    verify_client: bool = False
    ca_cert_path: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'enabled': self.enabled,
            'mode': self.mode,
            'use_tailscale_cert': self.use_tailscale_cert,
            'tailscale_hostname': self.tailscale_hostname,
            'auto_generate_cert': self.auto_generate_cert,
            'cert_days': self.cert_days,
            'cert_country': self.cert_country,
            'cert_organization': self.cert_organization,
            'min_version': self.min_version,
            'cert_dir': self.cert_dir,
            'verify_client': self.verify_client,
            'ca_cert_path': self.ca_cert_path,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TLSConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class SignatureConfig:
    """签名配置"""
    enabled: bool = True
    key_dir: str = "./keys"
    
    # 时间戳检查
    timestamp_tolerance: int = 300  # 5 分钟
    max_clock_skew: int = 60  # 1 分钟
    
    # 签名缓存
    max_signature_cache: int = 10000
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'enabled': self.enabled,
            'key_dir': self.key_dir,
            'timestamp_tolerance': self.timestamp_tolerance,
            'max_clock_skew': self.max_clock_skew,
            'max_signature_cache': self.max_signature_cache,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SignatureConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class AccessControlConfig:
    """访问控制配置"""
    enabled: bool = True
    
    # 白名单
    allowed_nodes: List[str] = field(default_factory=list)
    
    # 命令过滤
    allowed_commands: List[Dict[str, Any]] = field(default_factory=list)
    
    # 速率限制
    rate_limit_enabled: bool = True
    rate_limit_requests_per_second: float = 10.0
    rate_limit_burst: int = 5
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'enabled': self.enabled,
            'allowed_nodes': self.allowed_nodes,
            'allowed_commands': self.allowed_commands,
            'rate_limit_enabled': self.rate_limit_enabled,
            'rate_limit_requests_per_second': self.rate_limit_requests_per_second,
            'rate_limit_burst': self.rate_limit_burst,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccessControlConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class SecurityConfig:
    """安全配置总类"""
    tls: TLSConfig = field(default_factory=TLSConfig)
    signature: SignatureConfig = field(default_factory=SignatureConfig)
    access_control: AccessControlConfig = field(default_factory=AccessControlConfig)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tls': self.tls.to_dict(),
            'signature': self.signature.to_dict(),
            'access_control': self.access_control.to_dict(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SecurityConfig':
        return cls(
            tls=TLSConfig.from_dict(data.get('tls', {})),
            signature=SignatureConfig.from_dict(data.get('signature', {})),
            access_control=AccessControlConfig.from_dict(data.get('access_control', {})),
        )


# 默认安全配置
default_security_config = SecurityConfig(
    tls=TLSConfig(
        enabled=True,
        mode="self_signed",
        auto_generate_cert=True,
        cert_days=365,
        min_version="TLSv1.3",
    ),
    signature=SignatureConfig(
        enabled=True,
        timestamp_tolerance=300,
        max_clock_skew=60,
    ),
    access_control=AccessControlConfig(
        enabled=True,
        rate_limit_enabled=True,
        rate_limit_requests_per_second=10.0,
        rate_limit_burst=5,
    ),
)