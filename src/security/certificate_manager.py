"""
OpenClaw Mesh - Certificate Manager
证书管理器：支持自签名证书和 Tailscale 证书
"""

import logging
import platform
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ed25519

logger = logging.getLogger(__name__)


class CertificateManager:
    """
    证书管理器
    
    职责:
    1. 生成自签名证书
    2. 支持 Tailscale 证书
    3. 证书加载和验证
    
    特性:
    - 支持 RSA 和 Ed25519 密钥
    - 自动证书轮换
    - 证书链验证
    """
    
    # 默认配置
    DEFAULT_KEY_SIZE = 2048
    DEFAULT_CERT_DAYS = 365
    DEFAULT_COUNTRY = "CN"
    DEFAULT_ORGANIZATION = "OpenClaw Mesh"
    DEFAULT_ORG_UNIT = "P2P Network"
    
    def __init__(self, cert_dir: str, mode: str = "self_signed"):
        """
        初始化证书管理器
        
        Args:
            cert_dir: 证书存储目录
            mode: 证书模式 (tailscale | self_signed | custom_ca)
        """
        self.cert_dir = Path(cert_dir)
        self.mode = mode
        self.cert_dir.mkdir(parents=True, exist_ok=True)
        
        # 证书文件路径
        self.key_path = self.cert_dir / "node.key"
        self.cert_path = self.cert_dir / "node.crt"
        self.ca_cert_path = self.cert_dir / "ca.crt"
        
        self._private_key: Optional[rsa.RSAPrivateKey] = None
        self._certificate: Optional[x509.Certificate] = None
        self._ca_certificate: Optional[x509.Certificate] = None
        
        logger.info(f"CertificateManager initialized: mode={mode}, dir={cert_dir}")
    
    def generate_self_signed_cert(
        self,
        hostname: str,
        key_size: int = DEFAULT_KEY_SIZE,
        days: int = DEFAULT_CERT_DAYS,
        country: str = DEFAULT_COUNTRY,
        organization: str = DEFAULT_ORGANIZATION,
        org_unit: str = DEFAULT_ORG_UNIT,
        alt_names: Optional[list] = None
    ) -> Tuple[rsa.RSAPrivateKey, x509.Certificate]:
        """
        生成自签名证书
        
        Args:
            hostname: 主机名
            key_size: RSA 密钥大小
            days: 证书有效期（天）
            country: 国家代码
            organization: 组织名称
            org_unit: 组织单位
            alt_names: 备用名称列表 (DNS 名称或 IP 地址)
            
        Returns:
            (私钥, 证书) 元组
        """
        try:
            logger.info(f"Generating self-signed certificate for {hostname}")
            
            # 生成 RSA 私钥
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size
            )
            
            # 构建主题名称
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, country),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization),
                x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, org_unit),
                x509.NameAttribute(NameOID.COMMON_NAME, hostname),
            ])
            
            # 构建备用名称
            san_list = [x509.DNSName(hostname)]
            if alt_names:
                for name in alt_names:
                    if self._is_ip_address(name):
                        san_list.append(x509.IPAddress(self._parse_ip(name)))
                    else:
                        san_list.append(x509.DNSName(name))
            
            # 生成证书
            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                issuer
            ).public_key(
                private_key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.now(timezone.utc)
            ).not_valid_after(
                datetime.now(timezone.utc) + timedelta(days=days)
            ).add_extension(
                x509.SubjectAlternativeName(san_list),
                critical=False
            ).add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True
            ).add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    key_encipherment=True,
                    key_cert_sign=False,
                    crl_sign=False,
                    content_commitment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False
                ),
                critical=True
            ).add_extension(
                x509.ExtendedKeyUsage([
                    x509.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.ExtendedKeyUsageOID.CLIENT_AUTH
                ]),
                critical=False
            ).sign(private_key, hashes.SHA256())
            
            logger.info(f"Self-signed certificate generated successfully for {hostname}")
            return private_key, cert
            
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to generate self-signed certificate: {e}")
            raise CertificateError(f"Certificate generation failed: {e}") from e
    
    def save_certificate(
        self,
        private_key: rsa.RSAPrivateKey,
        certificate: x509.Certificate,
        password: Optional[bytes] = None
    ) -> bool:
        """
        保存证书到文件
        
        Args:
            private_key: 私钥
            certificate: 证书
            password: 私钥加密密码（可选）
            
        Returns:
            是否成功
        """
        try:
            # 序列化私钥
            if password:
                key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.BestAvailableEncryption(password)
                )
            else:
                key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            
            # 序列化证书
            cert_bytes = certificate.public_bytes(serialization.Encoding.PEM)
            
            # 写入文件（设置安全权限）
            self.key_path.write_bytes(key_bytes)
            if platform.system() != "Windows":
                self.key_path.chmod(0o600)  # 仅所有者可读写
            
            self.cert_path.write_bytes(cert_bytes)
            if platform.system() != "Windows":
                self.cert_path.chmod(0o644)  # 所有人可读，仅所有者可写
            
            logger.info(f"Certificate saved to {self.cert_dir}")
            return True
            
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.error(f"Failed to save certificate: {e}")
            return False
    
    def load_certificate(self, password: Optional[bytes] = None) -> bool:
        """
        从文件加载证书
        
        Args:
            password: 私钥解密密码（可选）
            
        Returns:
            是否成功
        """
        try:
            if not self.key_path.exists() or not self.cert_path.exists():
                logger.warning("Certificate files not found")
                return False
            
            # 加载私钥
            key_data = self.key_path.read_bytes()
            self._private_key = serialization.load_pem_private_key(key_data, password)
            
            # 加载证书
            cert_data = self.cert_path.read_bytes()
            self._certificate = x509.load_pem_x509_certificate(cert_data)
            
            logger.info("Certificate loaded successfully")
            return True
            
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.error(f"Failed to load certificate: {e}")
            return False
    
    def get_tailscale_cert(self, hostname: str) -> bool:
        """
        获取 Tailscale 证书
        
        Args:
            hostname: Tailscale 主机名
            
        Returns:
            是否成功
        """
        try:
            logger.info(f"Fetching Tailscale certificate for {hostname}")
            
            # 使用 tailscale 命令获取证书
            result = subprocess.run(
                ["tailscale", "cert", "--cert-file", str(self.cert_path), 
                 "--key-file", str(self.key_path), hostname],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"Tailscale cert failed: {result.stderr}")
                return False
            
            # 设置权限
            if platform.system() != "Windows":
                self.key_path.chmod(0o600)
                self.cert_path.chmod(0o644)
            
            # 加载证书
            return self.load_certificate()
            
        except FileNotFoundError:
            logger.error("tailscale command not found")
            return False
        except subprocess.TimeoutExpired:
            logger.error("Tailscale cert command timed out")
            return False
        except (subprocess.SubprocessError, OSError) as e:
            logger.error(f"Failed to get Tailscale certificate: {e}")
            return False
    
    def ensure_certificate(
        self,
        hostname: str,
        auto_generate: bool = True,
        use_tailscale: bool = False
    ) -> bool:
        """
        确保证书可用（自动获取或生成）
        
        Args:
            hostname: 主机名
            auto_generate: 是否自动生成自签名证书
            use_tailscale: 是否尝试使用 Tailscale 证书
            
        Returns:
            是否成功
        """
        # 尝试加载已有证书
        if self.load_certificate():
            if not self.is_certificate_expired(days_before=7):
                logger.info("Using existing certificate")
                return True
            logger.info("Certificate expires soon, renewing...")
        
        # 尝试获取 Tailscale 证书
        if use_tailscale and self.mode == "tailscale":
            if self.get_tailscale_cert(hostname):
                return True
            logger.warning("Failed to get Tailscale certificate, falling back to self-signed")
        
        # 生成自签名证书
        if auto_generate and self.mode in ("self_signed", "tailscale"):
            try:
                private_key, cert = self.generate_self_signed_cert(hostname)
                if self.save_certificate(private_key, cert):
                    # 更新内存中的证书
                    self._private_key = private_key
                    self._certificate = cert
                    return True
                return False
            except (ValueError, TypeError, OSError) as e:
                logger.error(f"Failed to generate certificate: {e}")
                return False
        
        logger.error("No certificate available")
        return False
    
    def is_certificate_expired(self, days_before: int = 0) -> bool:
        """
        检查证书是否过期（或即将过期）
        
        Args:
            days_before: 提前天数（用于检查即将过期）
            
        Returns:
            是否过期
        """
        if not self._certificate:
            return True
        
        now = datetime.now(timezone.utc)
        
        # 兼容不同版本的 cryptography 库
        try:
            expiry = self._certificate.not_valid_after_utc
        except AttributeError:
            expiry = self._certificate.not_valid_after.replace(tzinfo=timezone.utc)
        
        if days_before > 0:
            expiry = expiry - timedelta(days=days_before)
        
        return now >= expiry
    
    def get_certificate_info(self) -> Optional[dict]:
        """
        获取证书信息
        
        Returns:
            证书信息字典
        """
        if not self._certificate:
            return None
        
        try:
            subject = self._certificate.subject
            issuer = self._certificate.issuer
            
            # 获取备用名称
            san = []
            try:
                ext = self._certificate.extensions.get_extension_for_oid(
                    x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME
                )
                san = [str(name) for name in ext.value]
            except x509.ExtensionNotFound:
                pass
            
            # 兼容不同版本的 cryptography 库
            try:
                not_before = self._certificate.not_valid_before_utc.isoformat()
                not_after = self._certificate.not_valid_after_utc.isoformat()
            except AttributeError:
                not_before = self._certificate.not_valid_before.replace(tzinfo=timezone.utc).isoformat()
                not_after = self._certificate.not_valid_after.replace(tzinfo=timezone.utc).isoformat()
            
            return {
                "subject": {
                    "cn": subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value 
                          if subject.get_attributes_for_oid(NameOID.COMMON_NAME) else None,
                    "o": subject.get_attributes_for_oid(NameOID.ORGANIZATION_NAME)[0].value 
                         if subject.get_attributes_for_oid(NameOID.ORGANIZATION_NAME) else None,
                },
                "issuer": {
                    "cn": issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value 
                          if issuer.get_attributes_for_oid(NameOID.COMMON_NAME) else None,
                },
                "serial_number": str(self._certificate.serial_number),
                "not_before": not_before,
                "not_after": not_after,
                "san": san,
                "fingerprint": self._certificate.fingerprint(hashes.SHA256()).hex(),
            }
        except (OSError, IOError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Failed to get certificate info: {e}")
            return None
    
    def get_ssl_context_files(self) -> Tuple[str, str]:
        """
        获取 SSL 上下文所需的文件路径
        
        Returns:
            (cert_file, key_file) 元组
        """
        import os
        cert_dir = str(self.cert_dir)
        
        # 优先查找 tailscale.crt (Tailscale 生成的证书)
        tailscale_cert = os.path.join(cert_dir, "tailscale.crt")
        tailscale_key = os.path.join(cert_dir, "tailscale.key")
        if os.path.exists(tailscale_cert) and os.path.exists(tailscale_key):
            return tailscale_cert, tailscale_key
        
        # 其次查找 node.crt (默认名称)
        node_cert = os.path.join(cert_dir, "node.crt")
        node_key = os.path.join(cert_dir, "node.key")
        if os.path.exists(node_cert) and os.path.exists(node_key):
            return node_cert, node_key
        
        # 最后使用配置的文件名
        return str(self.cert_path), str(self.key_path)
    
    def _is_ip_address(self, name: str) -> bool:
        """检查名称是否为 IP 地址"""
        import ipaddress
        try:
            ipaddress.ip_address(name)
            return True
        except ValueError:
            return False
    
    def _parse_ip(self, name: str):
        """解析 IP 地址"""
        import ipaddress
        return ipaddress.ip_address(name)
    
    @property
    def private_key(self) -> Optional[rsa.RSAPrivateKey]:
        """获取私钥"""
        return self._private_key
    
    @property
    def certificate(self) -> Optional[x509.Certificate]:
        """获取证书"""
        return self._certificate


class CertificateError(Exception):
    """证书错误"""
    pass