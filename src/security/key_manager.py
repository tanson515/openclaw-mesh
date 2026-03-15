"""
OpenClaw Mesh - Key Manager
Ed25519 密钥管理器：密钥对生成、安全存储、公钥读取
"""

import base64
import hashlib
import logging
import os
import platform
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature

logger = logging.getLogger(__name__)


class KeyManager:
    """
    Ed25519 密钥管理器
    
    职责:
    1. Ed25519 密钥对生成
    2. 密钥安全存储（文件权限 600）
    3. 公钥读取
    
    特性:
    - 使用 Ed25519 算法（现代、安全、快速）
    - 私钥文件权限 600（仅所有者可读写）
    - 支持密码加密私钥
    - 公钥可导出为多种格式
    """
    
    # 文件权限
    PRIVATE_KEY_PERMS = 0o600  # 仅所有者可读写
    PUBLIC_KEY_PERMS = 0o644   # 所有人可读
    
    def __init__(self, key_dir: str, node_id: str):
        """
        初始化密钥管理器
        
        Args:
            key_dir: 密钥存储目录
            node_id: 节点 ID（用于生成文件名）
        """
        self.key_dir = Path(key_dir)
        self.node_id = node_id
        self.key_dir.mkdir(parents=True, exist_ok=True)
        
        # 密钥文件路径
        self.private_key_path = self.key_dir / f"{node_id}.key"
        self.public_key_path = self.key_dir / f"{node_id}.pub"
        
        self._private_key: Optional[Ed25519PrivateKey] = None
        self._public_key: Optional[Ed25519PublicKey] = None
        
        logger.info(f"KeyManager initialized: node_id={node_id}, dir={key_dir}")
    
    def generate_keypair(self) -> tuple[Ed25519PrivateKey, Ed25519PublicKey]:
        """
        生成新的 Ed25519 密钥对
        
        Returns:
            (私钥, 公钥) 元组
        """
        try:
            logger.info(f"Generating Ed25519 keypair for {self.node_id}")
            
            private_key = Ed25519PrivateKey.generate()
            public_key = private_key.public_key()
            
            logger.info(f"Ed25519 keypair generated successfully for {self.node_id}")
            return private_key, public_key
            
        except (ValueError, TypeError, OSError) as e:
            logger.error(f"Failed to generate Ed25519 keypair: {e}")
            raise KeyManagerError(f"Key generation failed: {e}") from e
    
    def save_keypair(
        self,
        private_key: Ed25519PrivateKey,
        password: Optional[bytes] = None
    ) -> bool:
        """
        保存密钥对到文件
        
        Args:
            private_key: 私钥
            password: 加密密码（可选）
            
        Returns:
            是否成功
        """
        try:
            public_key = private_key.public_key()
            
            # 序列化私钥
            if password:
                private_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.BestAvailableEncryption(password)
                )
            else:
                private_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            
            # 序列化公钥
            public_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            # 写入私钥文件（设置安全权限）
            self.private_key_path.write_bytes(private_bytes)
            if platform.system() != "Windows":
                self.private_key_path.chmod(self.PRIVATE_KEY_PERMS)
            
            # 写入公钥文件
            self.public_key_path.write_bytes(public_bytes)
            if platform.system() != "Windows":
                self.public_key_path.chmod(self.PUBLIC_KEY_PERMS)
            
            logger.info(f"Keypair saved for {self.node_id}")
            return True
            
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.error(f"Failed to save keypair: {e}")
            return False
    
    def load_keypair(self, password: Optional[bytes] = None) -> bool:
        """
        从文件加载密钥对
        
        Args:
            password: 解密密码（可选）
            
        Returns:
            是否成功
        """
        try:
            if not self.private_key_path.exists():
                logger.warning(f"Private key file not found: {self.private_key_path}")
                return False
            
            # 检查文件权限
            self._check_file_permissions()
            
            # 加载私钥
            private_bytes = self.private_key_path.read_bytes()
            self._private_key = serialization.load_pem_private_key(private_bytes, password)
            
            if not isinstance(self._private_key, Ed25519PrivateKey):
                raise KeyManagerError("Loaded key is not Ed25519")
            
            # 加载或派生公钥
            if self.public_key_path.exists():
                public_bytes = self.public_key_path.read_bytes()
                self._public_key = serialization.load_pem_public_key(public_bytes)
            else:
                self._public_key = self._private_key.public_key()
            
            logger.info(f"Keypair loaded for {self.node_id}")
            return True
            
        except (OSError, IOError, ValueError, TypeError, KeyManagerError) as e:
            logger.error(f"Failed to load keypair: {e}")
            return False
    
    def ensure_keypair(self, auto_generate: bool = True) -> bool:
        """
        确保密钥对可用（自动加载或生成）
        
        Args:
            auto_generate: 是否自动生成新密钥对
            
        Returns:
            是否成功
        """
        # 尝试加载已有密钥
        if self.load_keypair():
            logger.info(f"Using existing keypair for {self.node_id}")
            return True
        
        # 生成新密钥对
        if auto_generate:
            try:
                private_key, public_key = self.generate_keypair()
                if self.save_keypair(private_key):
                    # 更新内存中的密钥
                    self._private_key = private_key
                    self._public_key = public_key
                    return True
                return False
            except (ValueError, TypeError, OSError, KeyManagerError) as e:
                logger.error(f"Failed to generate keypair: {e}")
                return False
        
        logger.error(f"No keypair available for {self.node_id}")
        return False
    
    def _check_file_permissions(self):
        """检查私钥文件权限"""
        # Windows 上跳过权限检查
        if platform.system() == "Windows":
            return
        try:
            mode = self.private_key_path.stat().st_mode
            # 检查是否过于开放（非所有者权限）
            if mode & 0o077:
                logger.warning(
                    f"Private key file has insecure permissions: {oct(mode & 0o777)}. "
                    f"Expected {oct(self.PRIVATE_KEY_PERMS)}"
                )
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.warning(f"Failed to check file permissions: {e}")
    
    def get_public_key_bytes(self) -> bytes:
        """
        获取公钥原始字节
        
        Returns:
            32 字节公钥
        """
        if not self._public_key:
            raise KeyManagerError("Public key not loaded")
        return self._public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
    
    def get_public_key_pem(self) -> str:
        """
        获取公钥 PEM 格式字符串
        
        Returns:
            PEM 格式公钥
        """
        if not self._public_key:
            raise KeyManagerError("Public key not loaded")
        return self._public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
    
    def get_public_key_base64(self) -> str:
        """
        获取 Base64 编码的公钥
        
        Returns:
            Base64 编码公钥
        """
        return base64.b64encode(self.get_public_key_bytes()).decode('utf-8')
    
    def get_node_id_from_public_key(self) -> str:
        """
        从公钥派生节点 ID
        
        Returns:
            节点 ID（公钥的 SHA256 前 16 字节）
        """
        pub_key_bytes = self.get_public_key_bytes()
        return hashlib.sha256(pub_key_bytes).hexdigest()[:32]
    
    def sign(self, data: bytes) -> bytes:
        """
        使用私钥签名数据
        
        Args:
            data: 待签名数据
            
        Returns:
            64 字节签名
        """
        if not self._private_key:
            raise KeyManagerError("Private key not loaded")
        return self._private_key.sign(data)
    
    def verify(self, data: bytes, signature: bytes) -> bool:
        """
        使用公钥验证签名
        
        Args:
            data: 原始数据
            signature: 签名
            
        Returns:
            是否验证通过
        """
        if not self._public_key:
            raise KeyManagerError("Public key not loaded")
        try:
            self._public_key.verify(signature, data)
            return True
        except InvalidSignature:
            return False
    
    def load_public_key_from_bytes(self, key_bytes: bytes) -> Ed25519PublicKey:
        """
        从字节加载公钥（用于验证其他节点的签名）
        
        Args:
            key_bytes: 公钥字节（32 字节原始格式或 PEM 格式）
            
        Returns:
            公钥对象
        """
        try:
            # 尝试 PEM 格式
            if key_bytes.startswith(b'-----BEGIN'):
                return serialization.load_pem_public_key(key_bytes)
            
            # 尝试原始格式
            if len(key_bytes) == 32:
                return Ed25519PublicKey.from_public_bytes(key_bytes)
            
            # 尝试 Base64 编码
            try:
                decoded = base64.b64decode(key_bytes)
                if len(decoded) == 32:
                    return Ed25519PublicKey.from_public_bytes(decoded)
            except (ValueError, TypeError):
                pass
            
            raise KeyManagerError(f"Invalid public key format: {len(key_bytes)} bytes")
            
        except KeyManagerError:
            raise
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to load public key: {e}")
            raise KeyManagerError(f"Failed to load public key: {e}") from e
    
    def load_public_key_from_file(self, file_path: str) -> Optional[Ed25519PublicKey]:
        """
        从文件加载公钥
        
        Args:
            file_path: 公钥文件路径
            
        Returns:
            公钥对象或 None
        """
        try:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"Public key file not found: {file_path}")
                return None
            
            key_bytes = path.read_bytes()
            return self.load_public_key_from_bytes(key_bytes)
            
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.error(f"Failed to load public key from file: {e}")
            return None
    
    def delete_keypair(self) -> bool:
        """
        删除密钥对文件
        
        Returns:
            是否成功
        """
        try:
            deleted = False
            if self.private_key_path.exists():
                self.private_key_path.unlink()
                deleted = True
            if self.public_key_path.exists():
                self.public_key_path.unlink()
                deleted = True
            
            self._private_key = None
            self._public_key = None
            
            if deleted:
                logger.info(f"Keypair deleted for {self.node_id}")
            return deleted
            
        except (OSError, IOError, ValueError, TypeError) as e:
            logger.error(f"Failed to delete keypair: {e}")
            return False
    
    @property
    def has_keypair(self) -> bool:
        """检查是否已加载密钥对"""
        return self._private_key is not None and self._public_key is not None
    
    @property
    def private_key(self) -> Optional[Ed25519PrivateKey]:
        """获取私钥"""
        return self._private_key
    
    @property
    def public_key(self) -> Optional[Ed25519PublicKey]:
        """获取公钥"""
        return self._public_key


class KeyManagerError(Exception):
    """密钥管理器错误"""
    pass