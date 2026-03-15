"""OpenClaw Mesh - File Transfer (v0.6.1) - P2P分块传输、断点续传"""

import asyncio
import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Awaitable, Set

logger = logging.getLogger(__name__)


class TransferError(Exception): pass
class TransferNotFoundError(TransferError): pass
class TransferCancelledError(TransferError): pass
class TransferFailedError(TransferError): pass
class NodeNotFoundError(TransferError): pass


@dataclass
class TransferCallback:
    on_progress: Optional[Callable[[str, float, int, int], Awaitable[None]]] = None
    on_complete: Optional[Callable[[str, str], Awaitable[None]]] = None
    on_error: Optional[Callable[[str, str], Awaitable[None]]] = None
    on_cancel: Optional[Callable[[str], Awaitable[None]]] = None


@dataclass
class TransferInfo:
    transfer_id: str
    file_name: str
    file_size: int
    file_checksum: str
    chunk_size: int
    total_chunks: int
    source_node: str
    target_node: str
    save_path: Optional[str] = None
    status: str = "pending"
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "TransferInfo":
        return cls(**data)


class FileTransfer:
    """文件传输管理器 v0.6.1 - >10MB分块，≤10MB直接传输"""
    
    CHUNK_SIZE = 1024 * 1024
    MAX_CONCURRENT = 5
    TIMEOUT = 30.0
    MAX_RETRIES = 5
    DIRECT_THRESHOLD = 10 * 1024 * 1024
    
    def __init__(self, node_id: str, node_discovery: Any, https_client: Any,
                 temp_dir: str = "./temp", port: int = 8443):
        self.node_id = node_id
        self.node_discovery = node_discovery
        self.https_client = https_client
        self.port = port
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        self._transfers: Dict[str, TransferInfo] = {}
        self._bitmaps: Dict[str, bytearray] = {}
        self._part_files: Dict[str, Path] = {}
        self._callbacks: Dict[str, TransferCallback] = {}
        self._cancelled: Set[str] = set()
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        self._running = False
    
    async def start(self):
        self._running = True
        logger.info("FileTransfer started")
    
    async def stop(self):
        self._running = False
        for tid in list(self._callbacks.keys()):
            await self.cancel_transfer(tid)
        logger.info("FileTransfer stopped")
    
    async def send_file(self, target_node: str, file_path: str,
                        callback: Optional[TransferCallback] = None) -> str:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        node = await self._get_node(target_node)
        if not node:
            raise NodeNotFoundError(f"目标节点不存在: {target_node}")
        
        tid = str(uuid.uuid4())
        if callback:
            self._callbacks[tid] = callback
        
        try:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            checksum = self._calc_checksum(file_path)
            
            use_chunks = file_size > self.DIRECT_THRESHOLD
            total = (file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE if use_chunks else 1
            actual_chunk = self.CHUNK_SIZE if use_chunks else file_size
            
            info = TransferInfo(
                transfer_id=tid, file_name=file_name, file_size=file_size,
                file_checksum=checksum, chunk_size=actual_chunk, total_chunks=total,
                source_node=self.node_id, target_node=target_node, status="running"
            )
            self._transfers[tid] = info
            
            await self._send_request(node, info)
            if not await self._wait_accept(tid):
                raise TransferFailedError("目标节点拒绝接收")
            
            if use_chunks:
                await self._transfer_chunks(tid, file_path, node, total)
            else:
                await self._transfer_direct(tid, file_path, node)
            
            await self._send_complete(node, tid)
            info.status = "completed"
            
            if callback and callback.on_complete:
                await callback.on_complete(tid, file_path)
            
            logger.info(f"传输完成: {tid}")
            return tid
            
        except Exception as e:
            logger.error(f"传输失败: {e}")
            if tid in self._transfers:
                self._transfers[tid].status = "failed"
                self._transfers[tid].error_message = str(e)
            if tid in self._callbacks and self._callbacks[tid].on_error:
                await self._callbacks[tid].on_error(tid, str(e))
            raise TransferFailedError(f"传输失败: {e}")
    
    async def handle_file_request(self, data: dict) -> dict:
        tid = data["transfer_id"]
        save_path = str(self.temp_dir / "received" / data["file_name"])
        
        info = TransferInfo(
            transfer_id=tid, file_name=data["file_name"], file_size=data["file_size"],
            file_checksum=data["file_checksum"], chunk_size=data["chunk_size"],
            total_chunks=data["total_chunks"], source_node=data["source_node"],
            target_node=self.node_id, save_path=save_path,
        )
        self._transfers[tid] = info
        self._bitmaps[tid] = bytearray(data["total_chunks"])
        
        part = self.temp_dir / f"{tid}_{data['file_name']}.part"
        self._part_files[tid] = part
        part.parent.mkdir(parents=True, exist_ok=True)
        with open(part, "wb") as f:
            f.truncate(data["file_size"])
        
        return {"transfer_id": tid, "accepted": True, "target_node": self.node_id}
    
    async def handle_chunk(self, tid: str, idx: int, data: bytes) -> bool:
        info = self._transfers.get(tid)
        if not info:
            return False
        
        part = self._part_files.get(tid)
        if not part:
            return False
        
        try:
            with open(part, "r+b") as f:
                f.seek(idx * info.chunk_size)
                f.write(data)
            
            if tid in self._bitmaps:
                self._bitmaps[tid][idx] = 1
            
            if tid in self._bitmaps and all(self._bitmaps[tid]):
                self._assemble(tid)
            return True
        except Exception as e:
            logger.error(f"保存块失败: {e}")
            return False
    
    async def handle_complete(self, tid: str) -> bool:
        info = self._transfers.get(tid)
        if not info:
            return False
        
        bitmap = self._bitmaps.get(tid)
        if bitmap and not all(bitmap):
            return False
        
        try:
            self._assemble(tid)
            return True
        except Exception as e:
            logger.error(f"合并失败: {e}")
            return False
    
    async def cancel_transfer(self, tid: str) -> bool:
        if tid not in self._transfers:
            return False
        
        self._cancelled.add(tid)
        self._transfers[tid].status = "failed"
        
        if tid in self._callbacks and self._callbacks[tid].on_cancel:
            await self._callbacks[tid].on_cancel(tid)
        
        self._cleanup(tid)
        return True
    
    async def get_status(self, tid: str) -> Optional[Dict]:
        info = self._transfers.get(tid)
        if not info:
            return None
        
        completed = sum(1 for b in self._bitmaps.get(tid, []) if b)
        return {
            "transfer_id": tid, "status": info.status,
            "progress": (completed / info.total_chunks * 100) if info.total_chunks > 0 else 0,
            "completed_chunks": completed, "total_chunks": info.total_chunks,
        }
    
    def _calc_checksum(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    
    def _assemble(self, tid: str) -> str:
        info = self._transfers[tid]
        part = self._part_files[tid]
        
        actual = self._calc_checksum(str(part))
        if actual != info.file_checksum:
            part.unlink(missing_ok=True)
            raise TransferFailedError("校验和验证失败")
        
        save = Path(info.save_path)
        save.parent.mkdir(parents=True, exist_ok=True)
        part.rename(save)
        
        info.status = "completed"
        self._cleanup(tid)
        return str(save)
    
    def _cleanup(self, tid: str):
        self._transfers.pop(tid, None)
        self._bitmaps.pop(tid, None)
        part = self._part_files.pop(tid, None)
        if part and part.exists():
            part.unlink(missing_ok=True)
    
    async def _get_node(self, node_id: str):
        if hasattr(self.node_discovery, 'get_node'):
            return await self.node_discovery.get_node(node_id)
        return None
    
    async def _send_request(self, node: Dict, info: TransferInfo):
        url = f"https://{node.get('ip')}:{self.port}/api/files/request"
        payload = {
            "transfer_id": info.transfer_id, "file_name": info.file_name,
            "file_size": info.file_size, "file_checksum": info.file_checksum,
            "chunk_size": info.chunk_size, "total_chunks": info.total_chunks,
            "source_node": self.node_id,
        }
        resp = await self.https_client.post(url, json_data=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
    
    async def _wait_accept(self, tid: str, timeout: float = 30.0) -> bool:
        await asyncio.sleep(0.1)
        return True
    
    async def _transfer_direct(self, tid: str, path: str, node: Dict):
        url = f"https://{node.get('ip')}:{self.port}/api/files/{tid}/chunks/0"
        
        with open(path, "rb") as f:
            data = f.read()
        
        for i in range(self.MAX_RETRIES):
            try:
                resp = await self.https_client.post(
                    url, data=data, headers={"Content-Type": "application/octet-stream"},
                    timeout=self.TIMEOUT * 2
                )
                resp.raise_for_status()
                return
            except Exception as e:
                if i < self.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** i)
                else:
                    raise e
    
    async def _transfer_chunks(self, tid: str, path: str, node: Dict, total: int):
        async def send(idx: int):
            async with self._semaphore:
                if tid in self._cancelled:
                    raise TransferCancelledError()
                
                with open(path, "rb") as f:
                    f.seek(idx * self.CHUNK_SIZE)
                    data = f.read(self.CHUNK_SIZE)
                
                url = f"https://{node.get('ip')}:{self.port}/api/files/{tid}/chunks/{idx}"
                
                for i in range(self.MAX_RETRIES):
                    try:
                        resp = await self.https_client.post(
                            url, data=data, headers={"Content-Type": "application/octet-stream"},
                            timeout=self.TIMEOUT
                        )
                        resp.raise_for_status()
                        return
                    except Exception as e:
                        if i < self.MAX_RETRIES - 1:
                            await asyncio.sleep(2 ** i)
                        else:
                            raise TransferFailedError(f"块 {idx} 失败: {e}")
        
        tasks = [send(i) for i in range(total)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failed = [i for i, r in enumerate(results) if isinstance(r, Exception)]
        if failed:
            raise TransferFailedError(f"块传输失败: {failed}")
    
    async def _send_complete(self, node: Dict, tid: str):
        url = f"https://{node.get('ip')}:{self.port}/api/files/{tid}/complete"
        try:
            await self.https_client.post(url, timeout=self.TIMEOUT)
        except Exception as e:
            logger.warning(f"发送完成通知失败: {e}")
