"""
OpenClaw Mesh - 工具函数
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any


def generate_message_id() -> str:
    """生成唯一消息 ID"""
    return str(uuid.uuid4())


def generate_transfer_id() -> str:
    """生成唯一传输 ID"""
    return str(uuid.uuid4())


def generate_discussion_id() -> str:
    """生成唯一讨论 ID"""
    return str(uuid.uuid4())


def calculate_sha256(data: bytes) -> str:
    """计算 SHA256 哈希"""
    return hashlib.sha256(data).hexdigest()


def calculate_file_sha256(file_path: str, chunk_size: int = 8192) -> str:
    """计算文件的 SHA256 哈希"""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def calculate_content_hash(payload: Dict[str, Any]) -> str:
    """计算内容哈希（用于消息签名）"""
    content = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(content).hexdigest()[:32]


def get_timestamp() -> datetime:
    """获取当前 UTC 时间"""
    return datetime.now(timezone.utc)


def format_timestamp(dt: datetime) -> str:
    """格式化时间为 ISO 字符串"""
    return dt.isoformat()


def parse_timestamp(ts: str) -> datetime:
    """解析 ISO 时间字符串"""
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def exponential_backoff_delay(retry_count: int, base_delay: float = 30.0, max_delay: float = 3600.0) -> float:
    """
    计算指数退避延迟
    
    Args:
        retry_count: 重试次数
        base_delay: 基础延迟（秒）
        max_delay: 最大延迟（秒）
        
    Returns:
        延迟时间（秒）
    """
    import math
    delay = base_delay * (2 ** (retry_count - 1))
    return min(delay, max_delay)


class RateLimiter:
    """
    速率限制器（Token Bucket 算法）
    """
    
    def __init__(self, rate: float = 10.0, burst: int = 5):
        """
        初始化速率限制器
        
        Args:
            rate: 每秒允许的请求数
            burst: 突发容量
        """
        self.rate = rate
        self.burst = burst
        self.buckets = {}
    
    def allow(self, key: str) -> bool:
        """
        检查是否允许请求
        
        Args:
            key: 限制键（如 IP 或用户 ID）
            
        Returns:
            是否允许
        """
        import time
        
        now = time.time()
        
        if key not in self.buckets:
            self.buckets[key] = {"tokens": float(self.burst), "last_update": now}
        
        bucket = self.buckets[key]
        elapsed = now - bucket["last_update"]
        bucket["tokens"] = min(self.burst, bucket["tokens"] + elapsed * self.rate)
        bucket["last_update"] = now
        
        if bucket["tokens"] >= 1:
            bucket["tokens"] -= 1
            return True
        
        return False
    
    def reset(self, key: str):
        """重置指定键的限制"""
        if key in self.buckets:
            del self.buckets[key]


class CircuitBreaker:
    """
    熔断器
    
    用于防止连续失败的操作
    """
    
    STATE_CLOSED = "closed"      # 正常状态
    STATE_OPEN = "open"          # 熔断状态
    STATE_HALF_OPEN = "half_open"  # 半开状态
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        """
        初始化熔断器
        
        Args:
            failure_threshold: 失败阈值
            recovery_timeout: 恢复超时（秒）
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = self.STATE_CLOSED
    
    def can_execute(self) -> bool:
        """检查是否可以执行操作"""
        import time
        
        if self.state == self.STATE_CLOSED:
            return True
        
        if self.state == self.STATE_OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = self.STATE_HALF_OPEN
                return True
            return False
        
        # HALF_OPEN
        return True
    
    def record_success(self):
        """记录成功"""
        self.failure_count = 0
        self.state = self.STATE_CLOSED
    
    def record_failure(self):
        """记录失败"""
        import time
        
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = self.STATE_OPEN
    
    def __enter__(self):
        if not self.can_execute():
            raise CircuitBreakerOpen("Circuit breaker is open")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.record_success()
        else:
            self.record_failure()
        return False


class CircuitBreakerOpen(Exception):
    """熔断器打开异常"""
    pass
