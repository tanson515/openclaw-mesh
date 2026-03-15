"""
OpenClaw Mesh - Discussion Exceptions
讨论模块异常定义
"""


class DiscussionError(Exception):
    """讨论模块基础异常"""
    pass


class DiscussionNotFoundError(DiscussionError):
    """讨论不存在"""
    pass


class DiscussionAlreadyExistsError(DiscussionError):
    """讨论已存在"""
    pass


class NotCoordinatorError(DiscussionError):
    """非主持人节点"""
    pass


class NotParticipantError(DiscussionError):
    """非参与者"""
    pass


class DiscussionFullError(DiscussionError):
    """讨论已满"""
    pass


class DiscussionTimeoutError(DiscussionError):
    """讨论超时"""
    pass


class SpeechTimeoutError(DiscussionError):
    """发言超时"""
    pass
