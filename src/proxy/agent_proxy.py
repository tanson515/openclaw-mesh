"""
OpenClaw Mesh - Agent Proxy (Refactored v0.6.1)
Agent Execution Module: Remote command execution, permission control, Agent query, message proxy
"""

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from enum import Enum
from functools import lru_cache

from src.constants import VERSION
from src.network.message_transport import MessageTransport, Message
from .permission_manager import PermissionManager
from .request_handler import RequestHandler, RequestResult, RequestStatus

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    """Execution Status Enumeration - 4 states"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ExecutionResult:
    """Command Execution Result Data Class"""
    execution_id: str
    command: str
    status: ExecutionStatus
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    executed_by: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        result["duration"] = self.duration
        return result


@dataclass
class AgentInfo:
    """Agent Information Data Class"""
    node_id: str
    version: str = VERSION
    status: str = "online"
    capabilities: List[str] = field(default_factory=lambda: [
        "command_execution", "agent_info", "message_proxy"
    ])
    uptime: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AgentProxy(RequestHandler):
    """
    Agent Proxy Executor - Refactored with RequestHandler base class
    
    Responsibilities:
    1. Remote command execution (with cancel support)
    2. Simple query (/agent/info)
    3. Message proxy
    4. Whitelist control & result cache
    """
    
    # Message types
    MSG_EXECUTE_REQ = "agent_execute_request"
    MSG_EXECUTE_RESP = "agent_execute_response"
    MSG_QUERY_REQ = "agent_query_request"
    MSG_QUERY_RESP = "agent_query_response"
    MSG_PROXY_REQ = "agent_proxy_message"
    MSG_PROXY_RESP = "agent_proxy_response"
    
    def __init__(
        self,
        node_id: str,
        message_transport: MessageTransport,
        permission_manager: Optional[PermissionManager] = None,
        local_agent: Optional[Any] = None,
        max_concurrent: int = 10,
        default_timeout: float = 30.0,
    ):
        super().__init__(node_id, message_transport)
        self.permission_manager = permission_manager or PermissionManager()
        self.local_agent = local_agent
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        
        # Execution tracking
        self._executions: Dict[str, asyncio.Task] = {}
        self._results: Dict[str, ExecutionResult] = {}
        self._max_cache = 100
        
        self._start_time = time.time()
        self._register_handlers()
        
        logger.info(f"AgentProxy initialized: {node_id}")
    
    def _register_handlers(self) -> None:
        """Register message handlers"""
        handlers = {
            self.MSG_EXECUTE_REQ: self._on_execute_request,
            self.MSG_EXECUTE_RESP: self._on_execute_response,
            self.MSG_QUERY_REQ: self._on_query_request,
            self.MSG_QUERY_RESP: self._on_response,
            self.MSG_PROXY_REQ: self._on_proxy_request,
            self.MSG_PROXY_RESP: self._on_response,
        }
        for msg_type, handler in handlers.items():
            self.message_transport.register_handler(msg_type, handler)
    
    # ============ Public API ============
    
    async def execute_command(
        self,
        node_id: str,
        command: str,
        timeout: float = 30.0,
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecutionResult:
        """Execute command on remote node"""
        result = await self.send_request(
            node_id=node_id,
            msg_type=self.MSG_EXECUTE_REQ,
            payload={
                "command": command,
                "timeout": timeout,
                "working_dir": working_dir,
                "env": env,
            },
            timeout=timeout + 10
        )
        
        if result.status == RequestStatus.SUCCESS:
            data = result.data
            return ExecutionResult(
                execution_id=data.get("execution_id", ""),
                command=data.get("command", command),
                status=ExecutionStatus(data.get("status", "failed")),
                stdout=data.get("stdout", ""),
                stderr=data.get("stderr", ""),
                exit_code=data.get("exit_code"),
                error_message=data.get("error_message"),
                executed_by=data.get("executed_by"),
            )
        
        return ExecutionResult(
            execution_id=result.request_id,
            command=command,
            status=ExecutionStatus.FAILED,
            error_message=result.error_message or "Unknown error",
        )
    
    async def query_agent(self, node_id: str, query: str = "info", timeout: float = 10.0) -> Dict[str, Any]:
        """Query remote agent info"""
        if node_id == self.node_id or not node_id:
            return self._get_local_info()
        
        result = await self.send_request(
            node_id=node_id,
            msg_type=self.MSG_QUERY_REQ,
            payload={"query": query},
            timeout=timeout
        )
        
        return result.data if result.status == RequestStatus.SUCCESS else {"error": result.error_message}
    
    async def proxy_message(
        self,
        node_id: str,
        message: str,
        user_id: Optional[str] = None,
        timeout: float = 60.0,
    ) -> Dict[str, Any]:
        """Proxy message to target node"""
        result = await self.send_request(
            node_id=node_id,
            msg_type=self.MSG_PROXY_REQ,
            payload={
                "message": message,
                "user_id": user_id,
            },
            timeout=timeout
        )
        
        return result.data if result.status == RequestStatus.SUCCESS else {"status": "failed", "error": result.error_message}
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel executing command"""
        task = self._executions.get(execution_id)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            if execution_id in self._results:
                self._results[execution_id].status = ExecutionStatus.FAILED
                self._results[execution_id].error_message = "Cancelled by user"
            return True
        return False
    
    # ============ Request Handlers ============
    
    async def _on_execute_request(self, message: Message) -> None:
        """Handle execute request"""
        payload = message.payload
        execution_id = payload.get("execution_id") or str(uuid.uuid4())
        command = payload.get("command", "")
        from_node = message.from_node
        
        logger.info(f"Execute request from {from_node}: {command}")
        
        # Permission check
        if not self.permission_manager.is_node_allowed(from_node):
            await self._send_execute_response(from_node, execution_id, command, "Node not allowed")
            return
        
        if not self.permission_manager.check(command):
            await self._send_execute_response(from_node, execution_id, command, "Command not allowed")
            return
        
        # Check concurrent limit
        if len(self._executions) >= self.max_concurrent:
            await self._send_execute_response(from_node, execution_id, command, "Max concurrent reached")
            return
        
        # Start execution
        task = asyncio.create_task(
            self._execute_local(
                execution_id=execution_id,
                command=command,
                timeout=payload.get("timeout", self.default_timeout),
                working_dir=payload.get("working_dir"),
                env=payload.get("env"),
                from_node=from_node,
            )
        )
        self._executions[execution_id] = task
    
    async def _on_query_request(self, message: Message) -> None:
        """Handle query request"""
        await self.handle_request(
            message=message,
            handler_fn=lambda p: self._handle_query(p.get("query", "info"))
        )
    
    async def _on_proxy_request(self, message: Message) -> None:
        """Handle proxy request"""
        await self.handle_request(
            message=message,
            handler_fn=self._handle_proxy
        )
    
    # ============ Response Handlers ============
    
    async def _on_execute_response(self, message: Message) -> None:
        """Handle execute response"""
        payload = message.payload
        request_id = payload.get("request_id")
        result_data = payload.get("result", {})
        
        # Parse ExecutionResult from dict
        if "status" in result_data:
            result_data["status"] = ExecutionStatus(result_data["status"])
        
        future = self._pending_responses.pop(request_id, None)
        if future and not future.done():
            future.set_result(result_data)
    
    async def _on_response(self, message: Message) -> None:
        """Generic response handler for query/proxy"""
        await self.handle_response(message)
    
    # ============ Implementation ============
    
    async def _execute_local(
        self,
        execution_id: str,
        command: str,
        timeout: float,
        working_dir: Optional[str],
        env: Optional[Dict[str, str]],
        from_node: str,
    ) -> None:
        """Execute command locally"""
        start_time = time.time()
        
        result = ExecutionResult(
            execution_id=execution_id,
            command=command,
            status=ExecutionStatus.RUNNING,
            start_time=start_time,
            executed_by=self.node_id,
        )
        
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=working_dir,
                env={**os.environ, **env} if env else None,
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
                result.status = ExecutionStatus.SUCCESS if process.returncode == 0 else ExecutionStatus.FAILED
                result.stdout = stdout.decode("utf-8", errors="replace")
                result.stderr = stderr.decode("utf-8", errors="replace")
                result.exit_code = process.returncode
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                result.status = ExecutionStatus.FAILED
                result.error_message = f"Timeout after {timeout}s"
                
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
        finally:
            result.end_time = time.time()
            self._results[execution_id] = result
            self._executions.pop(execution_id, None)
            await self._send_execute_response(from_node, execution_id, command, result=result)
    
    async def _send_execute_response(
        self,
        to_node: str,
        execution_id: str,
        command: str,
        error: Optional[str] = None,
        result: Optional[ExecutionResult] = None,
    ) -> None:
        """Send execute response"""
        if result is None:
            result = ExecutionResult(
                execution_id=execution_id,
                command=command,
                status=ExecutionStatus.FAILED,
                error_message=error,
                executed_by=self.node_id,
            )
        
        await self.send_response(
            to_node=to_node,
            request_id=execution_id,
            result=result.to_dict(),
            msg_type=self.MSG_EXECUTE_RESP
        )
    
    async def _handle_query(self, query: str) -> Dict[str, Any]:
        """Handle local query"""
        query_lower = query.lower()
        
        if query_lower in ("info", "status", "/agent/info"):
            return self._get_local_info()
        
        return {"node_id": self.node_id, "query": query, "info": self._get_local_info()}
    
    async def _handle_proxy(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle local proxy message"""
        message = payload.get("message", "")
        user_id = payload.get("user_id")
        
        if self.local_agent and hasattr(self.local_agent, "process_message"):
            try:
                agent_result = await self.local_agent.process_message(message=message, user_id=user_id)
                return {"status": "success", "result": agent_result, "executed_by": self.node_id}
            except Exception as e:
                return {"status": "failed", "error": str(e), "executed_by": self.node_id}
        
        return {"status": "failed", "error": "Local agent not available", "executed_by": self.node_id}
    
    def _get_local_info(self) -> Dict[str, Any]:
        """Get local agent info"""
        return {
            "node_id": self.node_id,
            "version": VERSION,
            "status": "online",
            "uptime": time.time() - self._start_time,
            "capabilities": ["command_execution", "agent_info", "message_proxy"],
            "active_executions": len(self._executions),
            "cached_results": len(self._results),
        }
    
    # ============ Whitelist API ============
    
    def add_allowed_command(self, pattern: str) -> None:
        """Add allowed command pattern"""
        self.permission_manager.add_allowed_command(pattern)
    
    def add_denied_command(self, pattern: str) -> None:
        """Add denied command pattern"""
        self.permission_manager.add_denied_command(pattern)
    
    # ============ Stats ============
    
    def get_stats(self) -> Dict[str, Any]:
        """Get proxy statistics"""
        return {
            "node_id": self.node_id,
            "active_executions": len(self._executions),
            "cached_results": len(self._results),
            "uptime": time.time() - self._start_time,
        }
    
    def get_execution_result(self, execution_id: str) -> Optional[ExecutionResult]:
        """Get cached execution result"""
        return self._results.get(execution_id)
    
    def get_active_executions(self) -> List[str]:
        """Get active execution IDs"""
        return list(self._executions.keys())
