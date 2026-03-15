"""
OpenClaw Mesh - Round Robin Manager
轮询管理器：管理讨论发言顺序、超时处理
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)


class TurnStatus(Enum):
    """发言状态枚举"""
    WAITING = "waiting"      # 等待发言
    SPEAKING = "speaking"    # 正在发言
    COMPLETED = "completed"  # 已完成
    TIMEOUT = "timeout"      # 超时
    SKIPPED = "skipped"      # 跳过


@dataclass
class SpeakerTurn:
    """发言者回合信息"""
    speaker_id: str
    round_num: int
    status: TurnStatus = field(default=TurnStatus.WAITING)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    speech_content: Optional[str] = None
    timeout_seconds: float = 120.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "speaker_id": self.speaker_id,
            "round_num": self.round_num,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "speech_content": self.speech_content,
            "timeout_seconds": self.timeout_seconds,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SpeakerTurn":
        """从字典创建"""
        return cls(
            speaker_id=data["speaker_id"],
            round_num=data["round_num"],
            status=TurnStatus(data.get("status", "waiting")),
            start_time=datetime.fromisoformat(data["start_time"]) if data.get("start_time") else None,
            end_time=datetime.fromisoformat(data["end_time"]) if data.get("end_time") else None,
            speech_content=data.get("speech_content"),
            timeout_seconds=data.get("timeout_seconds", 120.0),
        )
    
    def start(self) -> None:
        """开始发言"""
        self.status = TurnStatus.SPEAKING
        self.start_time = datetime.now(timezone.utc)
        logger.debug(f"Speaker {self.speaker_id} started speaking in round {self.round_num}")
    
    def complete(self, content: str) -> None:
        """完成发言"""
        self.status = TurnStatus.COMPLETED
        self.end_time = datetime.now(timezone.utc)
        self.speech_content = content
        logger.debug(f"Speaker {self.speaker_id} completed speaking in round {self.round_num}")
    
    def mark_timeout(self) -> None:
        """标记超时"""
        self.status = TurnStatus.TIMEOUT
        self.end_time = datetime.now(timezone.utc)
        logger.warning(f"Speaker {self.speaker_id} timed out in round {self.round_num}")
    
    def skip(self) -> None:
        """跳过发言"""
        self.status = TurnStatus.SKIPPED
        self.end_time = datetime.now(timezone.utc)
        logger.debug(f"Speaker {self.speaker_id} skipped in round {self.round_num}")
    
    def is_expired(self) -> bool:
        """检查是否超时"""
        if self.status != TurnStatus.SPEAKING or not self.start_time:
            return False
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        return elapsed > self.timeout_seconds
    
    @property
    def duration(self) -> Optional[float]:
        """获取发言持续时间（秒）"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        if self.start_time:
            return (datetime.now(timezone.utc) - self.start_time).total_seconds()
        return None


@dataclass
class RoundInfo:
    """轮次信息"""
    round_num: int
    turns: List[SpeakerTurn] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "round_num": self.round_num,
            "turns": [t.to_dict() for t in self.turns],
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RoundInfo":
        """从字典创建"""
        return cls(
            round_num=data["round_num"],
            turns=[SpeakerTurn.from_dict(t) for t in data.get("turns", [])],
            start_time=datetime.fromisoformat(data["start_time"]) if data.get("start_time") else None,
            end_time=datetime.fromisoformat(data["end_time"]) if data.get("end_time") else None,
        )
    
    def start(self) -> None:
        """开始本轮"""
        self.start_time = datetime.now(timezone.utc)
        logger.debug(f"Round {self.round_num} started")
    
    def end(self) -> None:
        """结束本轮"""
        self.end_time = datetime.now(timezone.utc)
        logger.debug(f"Round {self.round_num} ended")
    
    def get_current_turn(self) -> Optional[SpeakerTurn]:
        """获取当前回合"""
        for turn in self.turns:
            if turn.status == TurnStatus.SPEAKING:
                return turn
            if turn.status == TurnStatus.WAITING:
                return turn
        return None
    
    def get_turn_for_speaker(self, speaker_id: str) -> Optional[SpeakerTurn]:
        """获取指定发言者的回合"""
        for turn in self.turns:
            if turn.speaker_id == speaker_id:
                return turn
        return None
    
    def is_complete(self) -> bool:
        """检查本轮是否完成"""
        for turn in self.turns:
            if turn.status in (TurnStatus.WAITING, TurnStatus.SPEAKING):
                return False
        return True
    
    def get_completed_speeches(self) -> List[Dict[str, Any]]:
        """获取已完成的发言列表"""
        speeches = []
        for turn in self.turns:
            if turn.status == TurnStatus.COMPLETED and turn.speech_content:
                speeches.append({
                    "speaker_id": turn.speaker_id,
                    "round_num": turn.round_num,
                    "content": turn.speech_content,
                    "duration": turn.duration,
                })
        return speeches


class RoundRobinManager:
    """
    轮询管理器
    
    职责:
    1. 管理轮询顺序（Round Robin）
    2. 主持人参与讨论（不是旁观者）
    3. 超时处理（发言超时）
    4. 维护每轮的发言状态
    
    使用示例:
        manager = RoundRobinManager(
            discussion_id="disc-001",
            coordinator_id="mark3",
            participants=["mark3", "mark1", "mark2"],
            max_rounds=5,
            speech_timeout=120.0
        )
        
        # 开始第一轮
        round_info = manager.start_round()
        
        # 获取下一个发言者
        next_speaker = manager.get_next_speaker()
        
        # 标记发言完成
        manager.complete_speech(next_speaker, "发言内容")
        
        # 检查超时
        expired_speakers = manager.check_timeouts()
    """
    
    def __init__(
        self,
        discussion_id: str,
        coordinator_id: str,
        participants: List[str],
        max_rounds: int = 5,
        speech_timeout: float = 120.0,
    ):
        """
        初始化轮询管理器
        
        Args:
            discussion_id: 讨论 ID
            coordinator_id: 主持人节点 ID
            participants: 参与者列表（包括主持人）
            max_rounds: 最大轮数
            speech_timeout: 发言超时时间（秒）
        """
        self.discussion_id = discussion_id
        self.coordinator_id = coordinator_id
        self.participants = participants.copy()
        self.max_rounds = max_rounds
        self.speech_timeout = speech_timeout
        
        # 轮次记录
        self.rounds: List[RoundInfo] = []
        self.current_round: Optional[RoundInfo] = None
        
        # 当前发言者索引
        self._current_speaker_index: int = 0
        
        logger.info(
            f"RoundRobinManager initialized: discussion={discussion_id}, "
            f"coordinator={coordinator_id}, participants={participants}, "
            f"max_rounds={max_rounds}"
        )
    
    def start_round(self) -> RoundInfo:
        """
        开始新一轮讨论
        
        Returns:
            本轮信息
        """
        round_num = len(self.rounds) + 1
        
        if round_num > self.max_rounds:
            logger.warning(f"Max rounds ({self.max_rounds}) reached")
            raise ValueError(f"Maximum rounds ({self.max_rounds}) reached")
        
        # 结束当前轮
        if self.current_round:
            self.current_round.end()
        
        # 创建新轮次
        round_info = RoundInfo(round_num=round_num)
        
        # 为每个参与者创建发言回合
        for speaker_id in self.participants:
            turn = SpeakerTurn(
                speaker_id=speaker_id,
                round_num=round_num,
                timeout_seconds=self.speech_timeout,
            )
            round_info.turns.append(turn)
        
        round_info.start()
        self.rounds.append(round_info)
        self.current_round = round_info
        self._current_speaker_index = 0
        
        logger.info(f"Round {round_num} started for discussion {self.discussion_id}")
        return round_info
    
    def get_next_speaker(self) -> Optional[str]:
        """
        获取下一个发言者
        
        Returns:
            下一个发言者 ID，如果没有则返回 None
        """
        if not self.current_round:
            logger.warning("No active round")
            return None
        
        # 找到下一个等待发言的参与者
        while self._current_speaker_index < len(self.participants):
            speaker_id = self.participants[self._current_speaker_index]
            turn = self.current_round.get_turn_for_speaker(speaker_id)
            
            if turn and turn.status == TurnStatus.WAITING:
                turn.start()
                logger.info(f"Next speaker: {speaker_id} (round {self.current_round.round_num})")
                return speaker_id
            
            self._current_speaker_index += 1
        
        logger.debug("No more speakers in current round")
        return None
    
    def start_speaker_turn(self, speaker_id: str) -> bool:
        """
        开始指定发言者的回合
        
        Args:
            speaker_id: 发言者 ID
            
        Returns:
            是否成功开始
        """
        if not self.current_round:
            logger.warning("No active round")
            return False
        
        turn = self.current_round.get_turn_for_speaker(speaker_id)
        if not turn:
            logger.warning(f"Speaker {speaker_id} not found in current round")
            return False
        
        if turn.status != TurnStatus.WAITING:
            logger.warning(f"Speaker {speaker_id} turn status is {turn.status.value}")
            return False
        
        turn.start()
        logger.info(f"Started turn for speaker {speaker_id}")
        return True
    
    def complete_speech(self, speaker_id: str, content: str) -> bool:
        """
        完成发言
        
        Args:
            speaker_id: 发言者 ID
            content: 发言内容
            
        Returns:
            是否成功完成
        """
        if not self.current_round:
            logger.warning("No active round")
            return False
        
        turn = self.current_round.get_turn_for_speaker(speaker_id)
        if not turn:
            logger.warning(f"Speaker {speaker_id} not found in current round")
            return False
        
        if turn.status != TurnStatus.SPEAKING:
            logger.warning(f"Speaker {speaker_id} is not speaking (status: {turn.status.value})")
            return False
        
        turn.complete(content)
        
        # 更新索引到下一个
        try:
            idx = self.participants.index(speaker_id)
            self._current_speaker_index = idx + 1
        except ValueError:
            pass
        
        logger.info(f"Speech completed by {speaker_id}")
        return True
    
    def skip_speaker(self, speaker_id: str) -> bool:
        """
        跳过发言者
        
        Args:
            speaker_id: 发言者 ID
            
        Returns:
            是否成功跳过
        """
        if not self.current_round:
            logger.warning("No active round")
            return False
        
        turn = self.current_round.get_turn_for_speaker(speaker_id)
        if not turn:
            logger.warning(f"Speaker {speaker_id} not found in current round")
            return False
        
        if turn.status not in (TurnStatus.WAITING, TurnStatus.SPEAKING):
            logger.warning(f"Speaker {speaker_id} turn status is {turn.status.value}")
            return False
        
        turn.skip()
        logger.info(f"Speaker {speaker_id} skipped")
        return True
    
    def check_timeouts(self) -> List[str]:
        """
        检查超时的发言者
        
        Returns:
            超时发言者 ID 列表
        """
        if not self.current_round:
            return []
        
        expired_speakers = []
        for turn in self.current_round.turns:
            if turn.status == TurnStatus.SPEAKING and turn.is_expired():
                turn.mark_timeout()
                expired_speakers.append(turn.speaker_id)
                logger.warning(f"Speaker {turn.speaker_id} timed out")
        
        return expired_speakers
    
    def is_round_complete(self) -> bool:
        """
        检查当前轮是否完成
        
        Returns:
            是否完成
        """
        if not self.current_round:
            return False
        return self.current_round.is_complete()
    
    def is_discussion_complete(self) -> bool:
        """
        检查讨论是否完成（达到最大轮数）
        
        Returns:
            是否完成
        """
        if not self.rounds:
            return False
        
        # 检查是否达到最大轮数
        if len(self.rounds) >= self.max_rounds and self.is_round_complete():
            return True
        
        return False
    
    def get_current_speaker(self) -> Optional[str]:
        """
        获取当前发言者
        
        Returns:
            当前发言者 ID，如果没有则返回 None
        """
        if not self.current_round:
            return None
        
        turn = self.current_round.get_current_turn()
        if turn and turn.status == TurnStatus.SPEAKING:
            return turn.speaker_id
        return None
    
    def get_speeches(self, round_num: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        获取发言记录
        
        Args:
            round_num: 指定轮次，None 表示所有轮次
            
        Returns:
            发言记录列表
        """
        if round_num is not None:
            for round_info in self.rounds:
                if round_info.round_num == round_num:
                    return round_info.get_completed_speeches()
            return []
        
        # 返回所有轮次的发言
        all_speeches = []
        for round_info in self.rounds:
            all_speeches.extend(round_info.get_completed_speeches())
        return all_speeches
    
    def get_discussion_summary(self) -> Dict[str, Any]:
        """
        获取讨论摘要
        
        Returns:
            讨论摘要
        """
        total_speeches = 0
        total_timeouts = 0
        total_skipped = 0
        
        for round_info in self.rounds:
            for turn in round_info.turns:
                if turn.status == TurnStatus.COMPLETED:
                    total_speeches += 1
                elif turn.status == TurnStatus.TIMEOUT:
                    total_timeouts += 1
                elif turn.status == TurnStatus.SKIPPED:
                    total_skipped += 1
        
        return {
            "discussion_id": self.discussion_id,
            "total_rounds": len(self.rounds),
            "max_rounds": self.max_rounds,
            "participants": self.participants,
            "coordinator": self.coordinator_id,
            "total_speeches": total_speeches,
            "total_timeouts": total_timeouts,
            "total_skipped": total_skipped,
            "is_complete": self.is_discussion_complete(),
            "current_round": self.current_round.round_num if self.current_round else None,
            "current_speaker": self.get_current_speaker(),
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "discussion_id": self.discussion_id,
            "coordinator_id": self.coordinator_id,
            "participants": self.participants,
            "max_rounds": self.max_rounds,
            "speech_timeout": self.speech_timeout,
            "rounds": [r.to_dict() for r in self.rounds],
            "current_round": self.current_round.to_dict() if self.current_round else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RoundRobinManager":
        """从字典创建"""
        manager = cls(
            discussion_id=data["discussion_id"],
            coordinator_id=data["coordinator_id"],
            participants=data["participants"],
            max_rounds=data.get("max_rounds", 5),
            speech_timeout=data.get("speech_timeout", 120.0),
        )
        
        # 恢复轮次
        for round_data in data.get("rounds", []):
            manager.rounds.append(RoundInfo.from_dict(round_data))
        
        # 恢复当前轮
        if data.get("current_round"):
            current_round_num = data["current_round"]["round_num"]
            for round_info in manager.rounds:
                if round_info.round_num == current_round_num:
                    manager.current_round = round_info
                    break
        
        return manager
