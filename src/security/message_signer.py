"""
OpenClaw Mesh - Message Signer
消息签名器：Ed25519 消息签名与验证，防重放攻击
"""

import base64
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

from .key_manager import KeyManager

logger = logging.getLogger(__name__)


@dataclass
class SignaturePayload:
    """签名载荷数据结构"""
    message_id: str
    timestamp: int  # Unix 时间戳（秒）
    from_node: str
    content_hash: str  # SHA256 前 32 字符
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'message_id': self.message_id,
            'timestamp': self.timestamp,
            'from_node': self.from_node,
            'content_hash': self.content_hash,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SignaturePayload':
        """从字典创建"""
        return cls(
            message_id=data['message_id'],
            timestamp=data['timestamp'],
            from_node=data['from_node'],
            content_hash=data['content_hash'],
        )
    
    def to_bytes(self) -> bytes:
        """转换为签名字节"""
        # 使用规范化的 JSON 格式确保一致性
        return json.dumps(self.to_dict(), sort_keys=True, separators=(',', ':')).encode('utf-8')


class MessageSigner:
    """
    消息签名器
    
    职责:
    1. 消息签名（包含 message_id, timestamp, from_node, content_hash）
    2. 签名验证
    3. 防重放攻击（timestamp 检查）
    
    特性:
    - 使用 Ed25519 签名
    - 签名包含内容哈希，防止内容篡改
    - 时间戳检查防止重放攻击
    - 支持签名缓存去重
    """
    
    # 默认配置
    DEFAULT_TIMESTAMP_TOLERANCE = 300  # 5 分钟时间窗口
    DEFAULT_MAX_CLOCK_SKEW = 60  # 1 分钟最大时钟偏差
    
    def __init__(
        self,
        key_manager: KeyManager,
        timestamp_tolerance: int = DEFAULT_TIMESTAMP_TOLERANCE,
        max_clock_skew: int = DEFAULT_MAX_CLOCK_SKEW
    ):
        """
        初始化消息签名器
        
        Args:
            key_manager: 密钥管理器
            timestamp_tolerance: 时间戳容差（秒）
            max_clock_skew: 最大时钟偏差（秒）
        """
        self.key_manager = key_manager
        self.timestamp_tolerance = timestamp_tolerance
        self.max_clock_skew = max_clock_skew
        
        # 已处理签名缓存（防止重放）
        self._processed_signatures: set = set()
        self._max_cache_size = 10000
        
        logger.info(
            f"MessageSigner initialized: tolerance={timestamp_tolerance}s, "
            f"clock_skew={max_clock_skew}s"
        )
    
    def sign_message(
        self,
        message_id: str,
        from_node: str,
        payload: Dict[str, Any],
        timestamp: Optional[int] = None
    ) -> str:
        """
        签名消息
        
        Args:
            message_id: 消息 ID
            from_node: 发送节点 ID
            payload: 消息内容
            timestamp: 时间戳（秒，可选，默认当前时间）
            
        Returns:
            Base64 编码的签名
        """
        try:
            # 计算内容哈希
            content_hash = self._compute_content_hash(payload)
            
            # 构建签名载荷
            sig_payload = SignaturePayload(
                message_id=message_id,
                timestamp=timestamp or int(time.time()),
                from_node=from_node,
                content_hash=content_hash
            )
            
            # 签名
            signature = self.key_manager.sign(sig_payload.to_bytes())
            
            logger.debug(f"Message signed: {message_id}")
            return base64.b64encode(signature).decode('utf-8')
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Failed to sign message {message_id}: {e}")
            raise SignatureError(f"Signing failed: {e}") from e
    
    def verify_message(
        self,
        signature: str,
        message_id: str,
        from_node: str,
        payload: Dict[str, Any],
        public_key: Ed25519PublicKey,
        timestamp: Optional[int] = None,
        check_replay: bool = True
    ) -> bool:
        """
        验证消息签名
        
        Args:
            signature: Base64 编码的签名
            message_id: 消息 ID
            from_node: 发送节点 ID
            payload: 消息内容
            public_key: 发送者公钥
            timestamp: 消息时间戳（秒）
            check_replay: 是否检查重放攻击
            
        Returns:
            是否验证通过
        """
        try:
            # 解码签名
            try:
                sig_bytes = base64.b64decode(signature)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid signature encoding: {e}")
                return False
            
            # 计算内容哈希
            content_hash = self._compute_content_hash(payload)
            
            # 构建签名载荷
            sig_payload = SignaturePayload(
                message_id=message_id,
                timestamp=timestamp or int(time.time()),
                from_node=from_node,
                content_hash=content_hash
            )
            
            # 验证签名
            try:
                public_key.verify(sig_bytes, sig_payload.to_bytes())
            except InvalidSignature:
                logger.warning(f"Invalid signature for message {message_id}")
                return False
            
            # 检查重放攻击
            if check_replay:
                if not self._check_timestamp(sig_payload.timestamp):
                    logger.warning(f"Timestamp check failed for message {message_id}")
                    return False
                
                if self._is_signature_replayed(signature):
                    logger.warning(f"Replay attack detected for message {message_id}")
                    return False
                
                # 记录已处理签名
                self._record_signature(signature)
            
            logger.debug(f"Message signature verified: {message_id}")
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Failed to verify message {message_id}: {e}")
            return False
    
    def verify_message_with_key_bytes(
        self,
        signature: str,
        message_id: str,
        from_node: str,
        payload: Dict[str, Any],
        public_key_bytes: bytes,
        timestamp: Optional[int] = None,
        check_replay: bool = True
    ) -> bool:
        """
        使用公钥字节验证消息签名
        
        Args:
            signature: Base64 编码的签名
            message_id: 消息 ID
            from_node: 发送节点 ID
            payload: 消息内容
            public_key_bytes: 公钥字节
            timestamp: 消息时间戳（秒）
            check_replay: 是否检查重放攻击
            
        Returns:
            是否验证通过
        """
        try:
            public_key = self.key_manager.load_public_key_from_bytes(public_key_bytes)
            return self.verify_message(
                signature=signature,
                message_id=message_id,
                from_node=from_node,
                payload=payload,
                public_key=public_key,
                timestamp=timestamp,
                check_replay=check_replay
            )
        except (TypeError, ValueError, OSError, RuntimeError) as e:
            logger.error(f"Failed to verify message with key bytes: {e}")
            return False
    
    def _compute_content_hash(self, payload: Dict[str, Any]) -> str:
        """
        计算内容哈希
        
        Args:
            payload: 消息内容
            
        Returns:
            SHA256 哈希前 32 字符
        """
        # 使用规范化的 JSON 格式确保一致性
        content_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(content_bytes).hexdigest()[:32]
    
    def _check_timestamp(self, timestamp: int) -> bool:
        """
        检查时间戳是否有效
        
        Args:
            timestamp: Unix 时间戳（秒）
            
        Returns:
            是否有效
        """
        now = int(time.time())
        
        # 检查时间戳是否在未来（时钟偏差）
        if timestamp > now + self.max_clock_skew:
            logger.warning(f"Timestamp {timestamp} is in the future (now={now})")
            return False
        
        # 检查时间戳是否过期
        if timestamp < now - self.timestamp_tolerance:
            logger.warning(f"Timestamp {timestamp} is too old (now={now})")
            return False
        
        return True
    
    def _is_signature_replayed(self, signature: str) -> bool:
        """
        检查签名是否已处理过（重放攻击检查）
        
        Args:
            signature: Base64 编码的签名
            
        Returns:
            是否已处理过
        """
        return signature in self._processed_signatures
    
    def _record_signature(self, signature: str):
        """
        记录已处理的签名

        Args:
            signature: Base64 编码的签名
        """
        # 如果缓存过大，清空一部分
        if len(self._processed_signatures) >= self._max_cache_size:
            # 清空超过一半，确保清理后大小小于阈值
            self._processed_signatures = set(
                list(self._processed_signatures)[(self._max_cache_size // 2) + 1:]
            )

        self._processed_signatures.add(signature)
    
    def clear_signature_cache(self):
        """清空签名缓存"""
        self._processed_signatures.clear()
        logger.info("Signature cache cleared")
    
    def get_signature_cache_size(self) -> int:
        """获取签名缓存大小"""
        return len(self._processed_signatures)


class SignatureError(Exception):
    """签名错误"""
    pass