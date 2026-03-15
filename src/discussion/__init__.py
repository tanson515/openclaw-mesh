"""
OpenClaw Mesh - Discussion Module
讨论协调模块：多 Agent 讨论管理、轮询机制、消息协调、结果汇总
"""

from .discussion_coordinator import DiscussionCoordinator, DiscussionStatus
from .exceptions import (
    DiscussionError,
    DiscussionNotFoundError,
    DiscussionAlreadyExistsError,
    NotCoordinatorError,
    NotParticipantError,
    DiscussionFullError,
    DiscussionTimeoutError,
    SpeechTimeoutError,
)

__all__ = [
    # 主类
    'DiscussionCoordinator',
    'DiscussionStatus',
    # 异常
    'DiscussionError',
    'DiscussionNotFoundError',
    'DiscussionAlreadyExistsError',
    'NotCoordinatorError',
    'NotParticipantError',
    'DiscussionFullError',
    'DiscussionTimeoutError',
    'SpeechTimeoutError',
]
