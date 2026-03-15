"""
OpenClaw Mesh - Request Handler
Abstract base class for handling request-response message patterns
"""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, Any, Awaitable, Callable
from enum import Enum

from src.network.message_transport import MessageTransport, Message

logger = logging.getLogger(__name__)


class RequestStatus(Enum):
    """Request status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class RequestResult:
    """Request result data class"""
    request_id: str
    status: RequestStatus
    data: Dict[str, Any]
    error_message: Optional[str] = None


class RequestHandler(ABC):
    """
    Abstract base class for request-response pattern handling
    
    Covers three message types:
    - Execute: Remote command execution
    - Query: Node info query (/agent/info)
    - Proxy: Message proxy to target nodes
    """
    
    def __init__(
        self,
        node_id: str,
        message_transport: MessageTransport,
    ):
        self.node_id = node_id
        self.message_transport = message_transport
        self._pending_responses: Dict[str, asyncio.Future] = {}
    
    async def send_request(
        self,
        node_id: str,
        msg_type: str,
        payload: Dict[str, Any],
        timeout: float
    ) -> RequestResult:
        """
        Send request and wait for response
        
        Args:
            node_id: Target node ID
            msg_type: Message type
            payload: Request payload
            timeout: Timeout in seconds
            
        Returns:
            RequestResult with status and data
        """
        request_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self._pending_responses[request_id] = future
        
        payload["request_id"] = request_id
        payload["requested_by"] = self.node_id
        
        try:
            await self.message_transport.send_message(
                to_node=node_id,
                msg_type=msg_type,
                payload=payload
            )
            
            result_data = await asyncio.wait_for(future, timeout=timeout)
            return RequestResult(
                request_id=request_id,
                status=RequestStatus.SUCCESS,
                data=result_data
            )
            
        except asyncio.TimeoutError:
            logger.warning(f"Request timeout: {request_id}")
            return RequestResult(
                request_id=request_id,
                status=RequestStatus.FAILED,
                data={},
                error_message="Request timeout"
            )
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return RequestResult(
                request_id=request_id,
                status=RequestStatus.FAILED,
                data={},
                error_message=str(e)
            )
        finally:
            self._pending_responses.pop(request_id, None)
    
    async def handle_request(
        self,
        message: Message,
        handler_fn: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]
    ) -> None:
        """
        Handle incoming request
        
        Args:
            message: Received message
            handler_fn: Async function to process the request
        """
        payload = message.payload
        request_id = payload.get("request_id")
        from_node = message.from_node
        
        try:
            result = await handler_fn(payload)
            await self.send_response(from_node, request_id, result)
        except Exception as e:
            logger.error(f"Request handler failed: {e}")
            await self.send_response(
                from_node,
                request_id,
                {"status": "failed", "error": str(e)}
            )
    
    async def send_response(
        self,
        to_node: str,
        request_id: str,
        result: Dict[str, Any],
        msg_type: str
    ) -> None:
        """
        Send response to requester
        
        Args:
            to_node: Target node
            request_id: Original request ID
            result: Result data
            msg_type: Response message type
        """
        payload = {
            "request_id": request_id,
            "result": result,
        }
        
        try:
            await self.message_transport.send_message(
                to_node=to_node,
                msg_type=msg_type,
                payload=payload
            )
        except Exception as e:
            logger.error(f"Failed to send response: {e}")
    
    async def handle_response(self, message: Message) -> None:
        """
        Handle incoming response
        
        Args:
            message: Response message
        """
        payload = message.payload
        request_id = payload.get("request_id")
        result = payload.get("result", {})
        
        future = self._pending_responses.pop(request_id, None)
        if future and not future.done():
            future.set_result(result)
