"""
OpenClaw Mesh - Rate Limiter
速率限制器：Token Bucket 算法
"""

import logging
import time
from collections import defaultdict
from typing import Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TokenBucket:
    """Token Bucket 数据结构"""
    tokens: float
    last_update: float
    rate: float
    capacity: float


class RateLimiter:
    """
    速率限制器（Token Bucket 算法）
    
    职责:
    1. 限制请求速率
    2. 防止资源耗尽
    3. 支持按 key 限流
    
    特性:
    - Token Bucket 算法
    - 支持突发流量
    - 自动令牌补充
    """
    
    def __init__(
        self,
        rate: float = 10.0,      # 每秒令牌数
        burst: int = 5,          # 桶容量（突发请求数）
        cleanup_interval: int = 300  # 清理间隔（秒）
    ):
        """
        初始化速率限制器
        
        Args:
            rate: 令牌生成速率（每秒）
            burst: 桶容量（最大突发请求数）
            cleanup_interval: 清理过期桶的间隔（秒）
        """
        self.rate = rate
        self.capacity = float(burst)
        self.cleanup_interval = cleanup_interval
        
        self._buckets: Dict[str, TokenBucket] = {}
        self._last_cleanup = time.time()
        
        logger.info(f"RateLimiter initialized: rate={rate}/s, burst={burst}")
    
    def allow(self, key: str, tokens: int = 1) -> bool:
        """
        检查是否允许请求
        
        Args:
            key: 限流 key（如 IP、用户 ID）
            tokens: 消耗的令牌数
            
        Returns:
            是否允许
        """
        now = time.time()
        
        # 定期清理过期桶
        if now - self._last_cleanup > self.cleanup_interval:
            self._cleanup_buckets(now)
        
        # 获取或创建 bucket
        if key not in self._buckets:
            self._buckets[key] = TokenBucket(
                tokens=self.capacity,
                last_update=now,
                rate=self.rate,
                capacity=self.capacity
            )
        
        bucket = self._buckets[key]
        
        # 计算新令牌数
        elapsed = now - bucket.last_update
        bucket.tokens = min(self.capacity, bucket.tokens + elapsed * self.rate)
        bucket.last_update = now
        
        # 检查是否有足够令牌
        if bucket.tokens >= tokens:
            bucket.tokens -= tokens
            return True
        
        return False
    
    def check(self, key: str, tokens: int = 1) -> bool:
        """
        仅检查是否允许，不消耗令牌
        
        Args:
            key: 限流 key
            tokens: 需要消耗的令牌数
            
        Returns:
            是否允许
        """
        now = time.time()
        
        if key not in self._buckets:
            return self.capacity >= tokens
        
        bucket = self._buckets[key]
        elapsed = now - bucket.last_update
        available = min(self.capacity, bucket.tokens + elapsed * self.rate)
        
        return available >= tokens
    
    def get_remaining(self, key: str) -> float:
        """
        获取剩余令牌数
        
        Args:
            key: 限流 key
            
        Returns:
            剩余令牌数
        """
        now = time.time()
        
        if key not in self._buckets:
            return self.capacity
        
        bucket = self._buckets[key]
        elapsed = now - bucket.last_update
        return min(self.capacity, bucket.tokens + elapsed * self.rate)
    
    def reset(self, key: str):
        """
        重置指定 key 的限流状态
        
        Args:
            key: 限流 key
        """
        if key in self._buckets:
            del self._buckets[key]
            logger.debug(f"Rate limiter reset for key: {key}")
    
    def reset_all(self):
        """重置所有限流状态"""
        self._buckets.clear()
        logger.info("Rate limiter reset for all keys")
    
    def _cleanup_buckets(self, now: float):
        """
        清理过期的 bucket
        
        Args:
            now: 当前时间
        """
        # 清理超过 10 分钟未使用的 bucket
        timeout = 600
        expired_keys = [
            key for key, bucket in self._buckets.items()
            if now - bucket.last_update > timeout
        ]
        
        for key in expired_keys:
            del self._buckets[key]
        
        self._last_cleanup = now
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired rate limit buckets")
    
    def get_stats(self) -> dict:
        """
        获取限流统计信息
        
        Returns:
            统计信息字典
        """
        now = time.time()
        return {
            "total_buckets": len(self._buckets),
            "rate": self.rate,
            "capacity": self.capacity,
            "buckets": {
                key: {
                    "tokens": min(self.capacity, bucket.tokens + (now - bucket.last_update) * self.rate),
                    "last_update": bucket.last_update
                }
                for key, bucket in self._buckets.items()
            }
        }


class AdaptiveRateLimiter(RateLimiter):
    """
    自适应速率限制器
    
    根据系统负载动态调整限流速率
    """
    
    def __init__(
        self,
        min_rate: float = 1.0,
        max_rate: float = 100.0,
        burst: int = 5,
        target_latency: float = 0.1  # 目标延迟（秒）
    ):
        """
        初始化自适应速率限制器
        
        Args:
            min_rate: 最小速率
            max_rate: 最大速率
            burst: 桶容量
            target_latency: 目标延迟
        """
        super().__init__(rate=max_rate, burst=burst)
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.target_latency = target_latency
        self.current_rate = max_rate
        
        self._latency_history: list = []
        self._max_history = 10
    
    def update_latency(self, latency: float):
        """
        更新延迟测量值
        
        Args:
            latency: 延迟（秒）
        """
        self._latency_history.append(latency)
        
        if len(self._latency_history) > self._max_history:
            self._latency_history.pop(0)
        
        # 调整速率
        self._adjust_rate()
    
    def _adjust_rate(self):
        """根据延迟调整速率"""
        if len(self._latency_history) < 3:
            return
        
        avg_latency = sum(self._latency_history) / len(self._latency_history)
        
        if avg_latency > self.target_latency * 2:
            # 延迟过高，降低速率
            self.current_rate = max(self.min_rate, self.current_rate * 0.8)
        elif avg_latency < self.target_latency * 0.5:
            # 延迟较低，提高速率
            self.current_rate = min(self.max_rate, self.current_rate * 1.2)
        
        self.rate = self.current_rate
        
        logger.debug(f"Rate adjusted to {self.rate}/s (avg_latency={avg_latency:.3f}s)")