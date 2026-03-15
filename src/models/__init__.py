"""
OpenClaw Mesh - 数据模型
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import json

from ..constants import VERSION


@dataclass
class Message:
    """P2P 消息数据类"""
    id: str
    from_node: str
    to_node: str  # 可以是特定节点ID或 "broadcast"
    type: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reply_to: Optional[str] = None
    signature: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        return {
            'id': self.id,
            'from_node': self.from_node,
            'to_node': self.to_node,
            'type': self.type,
            'payload': self.payload,
            'timestamp': self.timestamp.isoformat(),
            'reply_to': self.reply_to,
            'signature': self.signature,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """从字典创建实例"""
        data = data.copy()
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class Heartbeat:
    """心跳消息数据类"""
    from_node: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "online"
    capabilities: List[str] = field(default_factory=list)
    version: str = VERSION
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'from_node': self.from_node,
            'timestamp': self.timestamp.isoformat(),
            'status': self.status,
            'capabilities': self.capabilities,
            'version': self.version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Heartbeat':
        data = data.copy()
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class FileChunk:
    """文件块数据类"""
    transfer_id: str
    chunk_index: int
    data: bytes
    checksum: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'transfer_id': self.transfer_id,
            'chunk_index': self.chunk_index,
            'checksum': self.checksum,
            # data 不序列化为 JSON
        }


@dataclass
class AgentProxyRequest:
    """代理执行请求"""
    original_message: str
    from_user: str
    reply_to: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'original_message': self.original_message,
            'from_user': self.from_user,
            'reply_to': self.reply_to,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentProxyRequest':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class AgentProxyResponse:
    """代理执行响应"""
    status: str  # success/failed
    original_message: str
    result: Optional[str] = None
    error: Optional[str] = None
    executed_by: str = ""
    completed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'original_message': self.original_message,
            'result': self.result,
            'error': self.error,
            'executed_by': self.executed_by,
            'completed_at': self.completed_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentProxyResponse':
        data = data.copy()
        if isinstance(data.get('completed_at'), str):
            data['completed_at'] = datetime.fromisoformat(data['completed_at'].replace('Z', '+00:00'))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DiscussionInvite:
    """讨论邀请"""
    discussion_id: str
    topic: str
    context: str
    coordinator: str
    participants: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'discussion_id': self.discussion_id,
            'topic': self.topic,
            'context': self.context,
            'coordinator': self.coordinator,
            'participants': self.participants,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DiscussionInvite':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DiscussionTurn:
    """讨论发言通知"""
    discussion_id: str
    topic: str
    context: str
    round: int
    previous_messages: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'discussion_id': self.discussion_id,
            'topic': self.topic,
            'context': self.context,
            'round': self.round,
            'previous_messages': self.previous_messages,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DiscussionTurn':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# 消息类型常量
class MessageType:
    """消息类型常量"""
    DIRECT = "direct"
    BROADCAST = "broadcast"
    AGENT_PROXY = "agent_proxy"
    AGENT_PROXY_RESPONSE = "agent_proxy_response"
    FILE_REQUEST = "file_request"
    FILE_ACCEPT = "file_accept"
    FILE_COMPLETE = "file_complete"
    HEARTBEAT = "heartbeat"
    REGISTER = "register"
    UNREGISTER = "unregister"
    NODE_OFFLINE = "node_offline"
    DISCUSSION_INVITE = "discussion_invite"
    DISCUSSION_JOIN = "discussion_join"
    DISCUSSION_TURN = "discussion_turn"
    DISCUSSION_SPEECH = "discussion_speech"
    DISCUSSION_RESULT = "discussion_result"
    IP_CONFLICT = "ip_conflict"
