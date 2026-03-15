"""
OpenClaw Mesh - Discussion Coordinator (v0.6.1)
讨论协调器：多 Agent 讨论管理、消息协调、结果汇总
简化版：合并 RoundRobinManager，4种状态，4种消息类型
"""

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any

import aiohttp

from src.network.message_transport import MessageTransport, Message

logger = logging.getLogger(__name__)


class DiscussionStatus(Enum):
    """讨论状态 - 简化为4种"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    TIMEOUT = "timeout"


@dataclass
class DiscussionInfo:
    """讨论信息"""
    discussion_id: str
    topic: str
    context: str
    coordinator_id: str
    participants: List[str]
    user_id: str
    max_rounds: int = 5
    speech_timeout: float = 120.0
    status: DiscussionStatus = DiscussionStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    summary: Optional[str] = None


class DiscussionCoordinator:
    """
    讨论协调器 v0.6.1
    - 合并 RoundRobinManager
    - 4种消息类型：REQUEST_SPEECH / SPEECH_RESPONSE / DISCUSSION_END / DISCUSSION_NOTIFY
    - SpeakerTurn 简化为字典
    """
    
    MSG_REQUEST = "request_speech"
    MSG_RESPONSE = "speech_response"
    MSG_END = "discussion_end"
    MSG_NOTIFY = "discussion_notify"
    
    def __init__(
        self,
        node_id: str,
        message_transport: MessageTransport,
        local_agent: Optional[Any] = None,
        max_rounds: int = 5,
        speech_timeout: float = 120.0,
    ):
        self.node_id = node_id
        self.transport = message_transport
        self.local_agent = local_agent
        self.max_rounds = max_rounds
        self.speech_timeout = speech_timeout
        
        # 简化的数据结构：所有信息合并到一个字典
        self._discs: Dict[str, Dict] = {}
        self._pending: Dict[str, asyncio.Future] = {}
        self._results: Dict[str, asyncio.Future] = {}
        self._user_end: Dict[str, bool] = {}
        
        # 注册处理器
        self.transport.register_handler(self.MSG_RESPONSE, self._on_response)
        # 新增：处理 REQUEST_SPEECH（远程节点被邀请发言）
        self.transport.register_handler(self.MSG_REQUEST, self._on_request_speech)
        # 新增：处理 DISCUSSION_END（讨论结束通知）
        self.transport.register_handler(self.MSG_END, self._on_discussion_end)
        # 新增：处理 DISCUSSION_NOTIFY（接收其他节点发言）
        self.transport.register_handler(self.MSG_NOTIFY, self._on_notify)
    
    async def create_discussion(
        self,
        topic: str,
        participants: List[str],
        context: str = "",
        user_id: str = "",
        max_rounds: Optional[int] = None,
    ) -> str:
        """创建讨论 - 兼容 MeshAgent API 的别名"""
        return await self.create(
            topic=topic,
            participants=participants,
            context=context,
            user_id=user_id,
            max_rounds=max_rounds,
        )

    async def create(
        self,
        topic: str,
        participants: List[str],
        context: str = "",
        user_id: str = "",
        max_rounds: Optional[int] = None,
    ) -> str:
        """创建讨论"""
        did = str(uuid.uuid4())
        all_participants = [self.node_id] + [p for p in participants if p != self.node_id]
        
        info = DiscussionInfo(
            discussion_id=did,
            topic=topic,
            context=context,
            coordinator_id=self.node_id,
            participants=all_participants,
            user_id=user_id,
            max_rounds=max_rounds or self.max_rounds,
            speech_timeout=self.speech_timeout,
        )
        
        self._discs[did] = {
            "info": info,
            "speeches": [],  # 发言记录列表（字典格式）
            "round": 0,
        }
        self._user_end[did] = False
        
        logger.info(f"讨论创建: {did}, 主题='{topic}', 参与者={all_participants}")
        
        # 启动讨论任务
        self._discs[did]["task"] = asyncio.create_task(self._run(did))
        return did
    
    async def _run(self, did: str):
        """运行讨论主循环"""
        disc = self._discs.get(did)
        if not disc:
            return
        
        info = disc["info"]
        info.status = DiscussionStatus.RUNNING
        info.started_at = datetime.now(timezone.utc)
        
        try:
            for r in range(1, info.max_rounds + 1):
                if self._user_end.get(did, False):
                    break
                
                disc["round"] = r
                logger.info(f"讨论 {did}: 第 {r} 轮开始")
                
                for speaker in info.participants:
                    if self._user_end.get(did, False):
                        break
                    
                    content = await self._collect(did, speaker, r)
                    if content:
                        speech = {"speaker": speaker, "content": content, 
                                 "timestamp": time.time(), "round": r}
                        disc["speeches"].append(speech)
                        await self._broadcast(did, speech)
                
                await asyncio.sleep(0.5)
            
            info.status = DiscussionStatus.COMPLETED
            
        except asyncio.CancelledError:
            info.status = DiscussionStatus.TIMEOUT
            raise
        except Exception as e:
            logger.error(f"讨论 {did} 错误: {e}")
            info.status = DiscussionStatus.TIMEOUT
        finally:
            await self._finalize(did)
    
    async def _collect(self, did: str, speaker: str, round_num: int) -> Optional[str]:
        """收集发言"""
        if speaker == self.node_id:
            return await self._host_speak(did, round_num)
        return await self._request_remote(did, speaker, round_num)
    
    async def _host_speak(self, did: str, round_num: int) -> Optional[str]:
        """主持人发言"""
        disc = self._discs[did]
        info = disc["info"]
        recent = disc["speeches"][-5:] if disc["speeches"] else []
        
        if self.local_agent and hasattr(self.local_agent, "discuss_topic"):
            try:
                return await self.local_agent.discuss_topic(
                    topic=info.topic, context=info.context,
                    previous_views=recent, role="host"
                )
            except Exception as e:
                logger.error(f"主持人发言失败: {e}")
        
        return f"[{self.node_id}] 确认: {info.topic}"
    
    async def _request_remote(self, did: str, speaker: str, round_num: int) -> Optional[str]:
        """请求远程发言"""
        info = self._discs[did]["info"]
        key = f"{did}:{speaker}:{round_num}"
        
        fut = asyncio.get_event_loop().create_future()
        self._pending[key] = fut
        
        recent = self._discs[did]["speeches"][-5:] if self._discs[did]["speeches"] else []
        payload = {
            "discussion_id": did, "topic": info.topic,
            "context": info.context, "round": round_num,
            "previous": recent, "timeout": info.speech_timeout,
        }
        
        try:
            await self.transport.send_message(
                to_node=speaker, msg_type=self.MSG_REQUEST, payload=payload
            )
            return await asyncio.wait_for(fut, timeout=info.speech_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"发言超时: {speaker}")
            return None
        except Exception as e:
            logger.error(f"请求发言失败: {e}")
            return None
        finally:
            self._pending.pop(key, None)
    
    async def _broadcast(self, did: str, speech: Dict):
        """广播发言"""
        info = self._discs[did]["info"]
        payload = {"discussion_id": did, "speech": speech}
        
        for p in info.participants:
            if p != self.node_id:
                try:
                    await self.transport.send_message(
                        to_node=p, msg_type=self.MSG_NOTIFY, payload=payload
                    )
                except Exception as e:
                    logger.warning(f"广播失败 {p}: {e}")
    
    async def _finalize(self, did: str):
        """结束讨论"""
        disc = self._discs.get(did)
        if not disc:
            return
        
        info = disc["info"]
        info.ended_at = datetime.now(timezone.utc)
        info.summary = await self._summarize(did)
        
        # 发送结果
        payload = {
            "discussion_id": did, "topic": info.topic,
            "status": info.status.value, "summary": info.summary,
            "transcript": disc["speeches"],
        }
        for p in info.participants:
            if p != self.node_id:
                try:
                    await self.transport.send_message(
                        to_node=p, msg_type=self.MSG_END, payload=payload
                    )
                except Exception as e:
                    logger.warning(f"发送结果失败 {p}: {e}")
        
        # 通知等待者
        if did in self._results and not self._results[did].done():
            self._results[did].set_result(self.summary(did))
    
    async def _summarize(self, did: str) -> str:
        """生成总结"""
        disc = self._discs[did]
        info = disc["info"]
        
        if self.local_agent and hasattr(self.local_agent, "summarize_discussion"):
            try:
                return await self.local_agent.summarize_discussion(
                    topic=info.topic, messages=disc["speeches"]
                )
            except Exception as e:
                logger.error(f"生成总结失败: {e}")
        
        lines = [f"讨论: {info.topic}", f"状态: {info.status.value}",
                f"参与者: {', '.join(info.participants)}",
                f"轮数: {disc['round']}", "", "发言:"]
        for s in disc["speeches"]:
            lines.append(f"  [{s['speaker']} 第{s['round']}轮]: {s['content'][:100]}...")
        return "\n".join(lines)
    
    async def _on_response(self, msg: Message):
        """处理发言响应"""
        p = msg.payload
        key = f"{p['discussion_id']}:{msg.from_node}:{p.get('round', 0)}"
        fut = self._pending.get(key)
        if fut and not fut.done():
            fut.set_result(p.get("content"))

    async def _on_request_speech(self, message: Message) -> None:
        """
        远程节点收到发言请求
        各节点用自己的 LLM 生成回答
        """
        p = message.payload
        discussion_id = p["discussion_id"]
        topic = p["topic"]
        context = p["context"]
        round_num = p.get("round", 1)
        previous = p.get("previous", [])

        logger.info(f"[{self.node_id}] 收到讨论请求: {topic} (第{round_num}轮)")

        # 调用本地 LLM 生成观点
        content = await self._generate_speech(
            topic=topic,
            context=context,
            previous_views=previous,
            round_num=round_num
        )

        # 回复给协调者
        await self.submit(discussion_id, content, round_num)

    async def _generate_speech(
        self,
        topic: str,
        context: str,
        previous_views: List[Dict],
        round_num: int
    ) -> str:
        """
        使用本地 LLM 生成发言内容
        各节点用自己的 LLM 配置（从环境变量读取）
        """
        # 构建提示词
        prompt = f"""你是 OpenClaw Mesh 网络中的 Agent {self.node_id}。

讨论主题: {topic}
背景信息: {context}
当前轮次: 第 {round_num} 轮
"""

        if previous_views:
            prompt += "\n之前的发言:\n"
            for view in previous_views[-3:]:  # 最近 3 条
                prompt += f"  [{view['speaker']}]: {view['content'][:150]}...\n"

        prompt += f"""
请给出你的观点。作为 {self.node_id}，你有独特的视角。
请简洁回答（100-200字）。
"""

        # 调用 LLM API（使用环境变量配置）
        try:
            api_key = os.getenv("KIMI_API_KEY") or os.getenv("OPENAI_API_KEY")
            if not api_key:
                return f"[{self.node_id}] 未配置 LLM API Key，请设置 KIMI_API_KEY 或 OPENAI_API_KEY 环境变量"

            # 检测使用哪个提供商
            if os.getenv("KIMI_API_KEY"):
                # Kimi API
                url = "https://api.moonshot.cn/v1/chat/completions"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                data = {
                    "model": "kimi-k2.5",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7,
                    "max_tokens": 300
                }
            else:
                # OpenAI API
                url = "https://api.openai.com/v1/chat/completions"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                data = {
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7,
                    "max_tokens": 300
                }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data, timeout=30) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        return result["choices"][0]["message"]["content"].strip()
                    else:
                        error_text = await resp.text()
                        logger.error(f"LLM API 错误: {resp.status} - {error_text}")
                        return f"[{self.node_id}] LLM 调用失败: {resp.status}"

        except Exception as e:
            logger.error(f"生成发言失败: {e}")
            return f"[{self.node_id}] 生成观点时出错: {str(e)}"

    async def _on_discussion_end(self, message: Message) -> None:
        """讨论结束通知"""
        p = message.payload
        logger.info(f"[{self.node_id}] 讨论结束: {p.get('topic')}")
        logger.info(f"总结: {p.get('summary', '无')[:200]}...")

    async def _on_notify(self, message: Message) -> None:
        """接收其他节点的发言（用于实时显示）"""
        p = message.payload
        speech = p.get("speech", {})
        speaker = speech.get('speaker', 'unknown')
        content = speech.get('content', '')[:100]
        logger.info(f"[{self.node_id}] 收到发言: [{speaker}] {content}...")
    
    async def submit(self, did: str, content: str, round_num: int) -> bool:
        """参与者提交发言"""
        if did not in self._discs:
            return False
        
        info = self._discs[did]["info"]
        try:
            await self.transport.send_message(
                to_node=info.coordinator_id,
                msg_type=self.MSG_RESPONSE,
                payload={"discussion_id": did, "content": content, "round": round_num}
            )
            return True
        except Exception as e:
            logger.error(f"提交发言失败: {e}")
            return False
    
    async def end(self, did: str) -> bool:
        """结束讨论"""
        if did not in self._discs:
            return False
        
        info = self._discs[did]["info"]
        if info.coordinator_id != self.node_id:
            return False
        
        self._user_end[did] = True
        
        task = self._discs[did].get("task")
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        info.status = DiscussionStatus.COMPLETED
        await self._finalize(did)
        return True
    
    def summary(self, did: str) -> Optional[Dict]:
        """获取讨论摘要"""
        disc = self._discs.get(did)
        if not disc:
            return None
        
        info = disc["info"]
        return {
            "discussion_id": did,
            "topic": info.topic,
            "status": info.status.value,
            "coordinator": info.coordinator_id,
            "participants": info.participants,
            "rounds": disc["round"],
            "summary": info.summary,
            "transcript": disc["speeches"],
        }
    
    def list_all(self, status: Optional[DiscussionStatus] = None) -> List[Dict]:
        """列出所有讨论"""
        result = []
        for did, disc in self._discs.items():
            info = disc["info"]
            if status is None or info.status == status:
                result.append({
                    "discussion_id": did,
                    "topic": info.topic,
                    "status": info.status.value,
                    "participants": info.participants,
                })
        return result
    
    async def wait(self, did: str, timeout: float = 300.0) -> Optional[Dict]:
        """等待讨论结果"""
        disc = self._discs.get(did)
        if not disc:
            return None
        
        info = disc["info"]
        if info.status in (DiscussionStatus.COMPLETED, DiscussionStatus.TIMEOUT):
            return self.summary(did)
        
        fut = asyncio.get_event_loop().create_future()
        self._results[did] = fut
        
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._results.pop(did, None)
    
    async def cleanup(self):
        """清理资源"""
        for disc in self._discs.values():
            task = disc.get("task")
            if task and not task.done():
                task.cancel()
        
        tasks = [d["task"] for d in self._discs.values() if d.get("task")]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self._discs.clear()
        self._pending.clear()
        self._results.clear()
