"""
OpenClaw Mesh - Chunk Manager
分块管理模块：负责文件分块、校验和断点续传
"""

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class ChunkStatus(Enum):
    """块状态枚举"""
    PENDING = "pending"      # 等待传输
    TRANSFERRING = "transferring"  # 传输中
    COMPLETED = "completed"  # 传输完成
    FAILED = "failed"        # 传输失败
    VERIFIED = "verified"    # 已校验


@dataclass
class ChunkInfo:
    """块信息数据类"""
    index: int                      # 块索引
    offset: int                     # 文件偏移量
    size: int                       # 块大小
    checksum: str                   # SHA-256 校验和
    status: ChunkStatus = field(default=ChunkStatus.PENDING)
    retry_count: int = field(default=0)
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "index": self.index,
            "offset": self.offset,
            "size": self.size,
            "checksum": self.checksum,
            "status": self.status.value,
            "retry_count": self.retry_count,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ChunkInfo":
        """从字典创建"""
        return cls(
            index=data["index"],
            offset=data["offset"],
            size=data["size"],
            checksum=data["checksum"],
            status=ChunkStatus(data.get("status", "pending")),
            retry_count=data.get("retry_count", 0),
        )


@dataclass
class TransferInfo:
    """传输信息数据类"""
    transfer_id: str                # 传输 ID
    file_name: str                  # 文件名
    file_size: int                  # 文件大小
    file_checksum: str              # 文件整体校验和
    chunk_size: int                 # 块大小
    total_chunks: int               # 总块数
    source_node: str                # 源节点
    target_node: str                # 目标节点
    save_path: Optional[str] = None # 保存路径
    chunks: Dict[int, ChunkInfo] = field(default_factory=dict)
    status: str = field(default="pending")  # pending/running/paused/completed/failed/cancelled
    progress: float = field(default=0.0)    # 进度百分比
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "transfer_id": self.transfer_id,
            "file_name": self.file_name,
            "file_size": self.file_size,
            "file_checksum": self.file_checksum,
            "chunk_size": self.chunk_size,
            "total_chunks": self.total_chunks,
            "source_node": self.source_node,
            "target_node": self.target_node,
            "save_path": self.save_path,
            "chunks": {k: v.to_dict() for k, v in self.chunks.items()},
            "status": self.status,
            "progress": self.progress,
            "error_message": self.error_message,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "TransferInfo":
        """从字典创建"""
        return cls(
            transfer_id=data["transfer_id"],
            file_name=data["file_name"],
            file_size=data["file_size"],
            file_checksum=data["file_checksum"],
            chunk_size=data["chunk_size"],
            total_chunks=data["total_chunks"],
            source_node=data["source_node"],
            target_node=data["target_node"],
            save_path=data.get("save_path"),
            chunks={int(k): ChunkInfo.from_dict(v) for k, v in data.get("chunks", {}).items()},
            status=data.get("status", "pending"),
            progress=data.get("progress", 0.0),
            error_message=data.get("error_message"),
        )


class ChunkManager:
    """
    分块管理器
    
    职责:
    1. 文件分块计算
    2. 块校验和计算
    3. 断点续传状态管理
    4. 传输进度跟踪
    
    特性:
    - 支持可变块大小（默认 1MB）
    - SHA-256 校验
    - 断点续传状态持久化
    """
    
    DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB
    MAX_RETRIES = 5
    
    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE, temp_dir: str = "./temp"):
        """
        初始化分块管理器
        
        Args:
            chunk_size: 块大小（字节），默认 1MB
            temp_dir: 临时文件目录
        """
        self.chunk_size = chunk_size
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 内存中的传输状态
        self._transfers: Dict[str, TransferInfo] = {}
        
        logger.info(f"ChunkManager initialized: chunk_size={chunk_size}, temp_dir={temp_dir}")
    
    def calculate_file_checksum(self, file_path: str) -> str:
        """
        计算文件 SHA-256 校验和
        
        Args:
            file_path: 文件路径
            
        Returns:
            SHA-256 校验和（十六进制字符串）
            
        Raises:
            FileNotFoundError: 文件不存在
            IOError: 读取文件失败
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        sha256_hash = hashlib.sha256()
        
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256_hash.update(chunk)
        except IOError as e:
            raise IOError(f"读取文件失败: {e}")
        
        return sha256_hash.hexdigest()
    
    def calculate_chunk_checksum(self, data: bytes) -> str:
        """
        计算数据块的 SHA-256 校验和
        
        Args:
            data: 数据块
            
        Returns:
            SHA-256 校验和（十六进制字符串）
        """
        return hashlib.sha256(data).hexdigest()
    
    def split_file_into_chunks(self, file_path: str, transfer_id: str) -> TransferInfo:
        """
        将文件分块并创建传输信息
        
        Args:
            file_path: 文件路径
            transfer_id: 传输 ID
            
        Returns:
            TransferInfo 对象
            
        Raises:
            FileNotFoundError: 文件不存在
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        file_checksum = self.calculate_file_checksum(file_path)
        
        # 计算总块数
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        
        # 创建传输信息
        transfer_info = TransferInfo(
            transfer_id=transfer_id,
            file_name=file_name,
            file_size=file_size,
            file_checksum=file_checksum,
            chunk_size=self.chunk_size,
            total_chunks=total_chunks,
            source_node="",  # 由调用者设置
            target_node="",  # 由调用者设置
        )
        
        # 计算每个块的校验和
        with open(file_path, "rb") as f:
            for i in range(total_chunks):
                offset = i * self.chunk_size
                f.seek(offset)
                chunk_data = f.read(self.chunk_size)
                
                chunk_info = ChunkInfo(
                    index=i,
                    offset=offset,
                    size=len(chunk_data),
                    checksum=self.calculate_chunk_checksum(chunk_data),
                )
                transfer_info.chunks[i] = chunk_info
        
        # 保存到内存
        self._transfers[transfer_id] = transfer_info
        
        logger.info(f"文件分块完成: {file_name}, {total_chunks} chunks, {file_size} bytes")
        
        return transfer_info
    
    def create_receive_transfer(self, transfer_id: str, file_name: str, 
                                file_size: int, file_checksum: str,
                                chunk_size: int, total_chunks: int,
                                source_node: str, target_node: str,
                                save_path: str) -> TransferInfo:
        """
        创建接收端传输信息
        
        Args:
            transfer_id: 传输 ID
            file_name: 文件名
            file_size: 文件大小
            file_checksum: 文件校验和
            chunk_size: 块大小
            total_chunks: 总块数
            source_node: 源节点
            target_node: 目标节点
            save_path: 保存路径
            
        Returns:
            TransferInfo 对象
        """
        transfer_info = TransferInfo(
            transfer_id=transfer_id,
            file_name=file_name,
            file_size=file_size,
            file_checksum=file_checksum,
            chunk_size=chunk_size,
            total_chunks=total_chunks,
            source_node=source_node,
            target_node=target_node,
            save_path=save_path,
        )
        
        # 初始化所有块为 pending 状态
        for i in range(total_chunks):
            offset = i * chunk_size
            size = min(chunk_size, file_size - offset)
            
            transfer_info.chunks[i] = ChunkInfo(
                index=i,
                offset=offset,
                size=size,
                checksum="",  # 接收时校验和未知
            )
        
        # 保存到内存
        self._transfers[transfer_id] = transfer_info
        
        # 持久化状态
        self._save_transfer_state(transfer_id)
        
        logger.info(f"创建接收传输: {transfer_id}, {file_name}")
        
        return transfer_info
    
    def get_chunk_data(self, file_path: str, chunk_index: int) -> bytes:
        """
        获取指定块的数据
        
        Args:
            file_path: 文件路径
            chunk_index: 块索引
            
        Returns:
            块数据
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 块索引无效
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        offset = chunk_index * self.chunk_size
        
        with open(file_path, "rb") as f:
            f.seek(offset)
            return f.read(self.chunk_size)
    
    def save_chunk(self, transfer_id: str, chunk_index: int, 
                   chunk_data: bytes, verify: bool = True) -> bool:
        """
        保存接收到的块
        
        Args:
            transfer_id: 传输 ID
            chunk_index: 块索引
            chunk_data: 块数据
            verify: 是否验证校验和
            
        Returns:
            是否保存成功
            
        Raises:
            KeyError: 传输 ID 不存在
        """
        if transfer_id not in self._transfers:
            raise KeyError(f"传输不存在: {transfer_id}")
        
        transfer_info = self._transfers[transfer_id]
        
        if chunk_index not in transfer_info.chunks:
            raise ValueError(f"无效的块索引: {chunk_index}")
        
        chunk_info = transfer_info.chunks[chunk_index]
        
        # 验证校验和
        if verify and chunk_info.checksum:
            actual_checksum = self.calculate_chunk_checksum(chunk_data)
            if actual_checksum != chunk_info.checksum:
                logger.error(f"块校验失败: transfer_id={transfer_id}, chunk={chunk_index}")
                chunk_info.status = ChunkStatus.FAILED
                return False
        
        # 保存块到临时文件
        temp_chunk_path = self._get_chunk_temp_path(transfer_id, chunk_index)
        temp_chunk_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(temp_chunk_path, "wb") as f:
            f.write(chunk_data)
        
        # 更新块状态
        chunk_info.status = ChunkStatus.COMPLETED
        chunk_info.checksum = self.calculate_chunk_checksum(chunk_data)
        
        # 更新进度
        self._update_progress(transfer_id)
        
        # 持久化状态
        self._save_transfer_state(transfer_id)
        
        logger.debug(f"块保存成功: transfer_id={transfer_id}, chunk={chunk_index}")
        
        return True
    
    def assemble_file(self, transfer_id: str) -> str:
        """
        合并所有块为完整文件
        
        Args:
            transfer_id: 传输 ID
            
        Returns:
            最终文件路径
            
        Raises:
            KeyError: 传输 ID 不存在
            RuntimeError: 块不完整或合并失败
        """
        if transfer_id not in self._transfers:
            raise KeyError(f"传输不存在: {transfer_id}")
        
        transfer_info = self._transfers[transfer_id]
        
        if not transfer_info.save_path:
            raise ValueError("保存路径未设置")
        
        # 检查所有块是否完成
        incomplete_chunks = [
            i for i, chunk in transfer_info.chunks.items()
            if chunk.status != ChunkStatus.COMPLETED
        ]
        
        if incomplete_chunks:
            raise RuntimeError(f"块不完整: {incomplete_chunks}")
        
        # 创建目标目录
        save_path = Path(transfer_info.save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 合并文件
        try:
            with open(save_path, "wb") as outfile:
                for i in range(transfer_info.total_chunks):
                    temp_chunk_path = self._get_chunk_temp_path(transfer_id, i)
                    
                    if not temp_chunk_path.exists():
                        raise RuntimeError(f"临时块文件不存在: {temp_chunk_path}")
                    
                    with open(temp_chunk_path, "rb") as infile:
                        outfile.write(infile.read())
            
            # 验证文件完整性
            actual_checksum = self.calculate_file_checksum(str(save_path))
            if actual_checksum != transfer_info.file_checksum:
                # 删除损坏的文件
                save_path.unlink(missing_ok=True)
                raise RuntimeError("文件校验失败")
            
            # 更新状态
            transfer_info.status = "completed"
            self._save_transfer_state(transfer_id)
            
            # 清理临时块文件
            self._cleanup_temp_chunks(transfer_id)
            
            logger.info(f"文件合并完成: {save_path}")
            
            return str(save_path)
            
        except Exception as e:
            logger.error(f"文件合并失败: {e}")
            raise RuntimeError(f"文件合并失败: {e}")
    
    def get_transfer_info(self, transfer_id: str) -> Optional[TransferInfo]:
        """
        获取传输信息
        
        Args:
            transfer_id: 传输 ID
            
        Returns:
            TransferInfo 对象，不存在则返回 None
        """
        return self._transfers.get(transfer_id)
    
    def get_pending_chunks(self, transfer_id: str) -> List[int]:
        """
        获取待传输的块索引列表
        
        Args:
            transfer_id: 传输 ID
            
        Returns:
            待传输块索引列表
        """
        if transfer_id not in self._transfers:
            return []
        
        transfer_info = self._transfers[transfer_id]
        
        return [
            i for i, chunk in transfer_info.chunks.items()
            if chunk.status in (ChunkStatus.PENDING, ChunkStatus.FAILED)
        ]
    
    def get_completed_chunks(self, transfer_id: str) -> Set[int]:
        """
        获取已完成传输的块索引集合
        
        Args:
            transfer_id: 传输 ID
            
        Returns:
            已完成块索引集合
        """
        if transfer_id not in self._transfers:
            return set()
        
        transfer_info = self._transfers[transfer_id]
        
        return {
            i for i, chunk in transfer_info.chunks.items()
            if chunk.status == ChunkStatus.COMPLETED
        }
    
    def update_chunk_status(self, transfer_id: str, chunk_index: int, 
                           status: ChunkStatus, error: Optional[str] = None):
        """
        更新块状态
        
        Args:
            transfer_id: 传输 ID
            chunk_index: 块索引
            status: 新状态
            error: 错误信息（可选）
        """
        if transfer_id not in self._transfers:
            return
        
        transfer_info = self._transfers[transfer_id]
        
        if chunk_index not in transfer_info.chunks:
            return
        
        chunk_info = transfer_info.chunks[chunk_index]
        chunk_info.status = status
        
        if status == ChunkStatus.FAILED:
            chunk_info.retry_count += 1
            if error:
                logger.error(f"块传输失败: transfer_id={transfer_id}, chunk={chunk_index}, error={error}")
        
        self._update_progress(transfer_id)
        self._save_transfer_state(transfer_id)
    
    def update_transfer_status(self, transfer_id: str, status: str, 
                               error_message: Optional[str] = None):
        """
        更新传输状态
        
        Args:
            transfer_id: 传输 ID
            status: 新状态
            error_message: 错误信息（可选）
        """
        if transfer_id not in self._transfers:
            return
        
        transfer_info = self._transfers[transfer_id]
        transfer_info.status = status
        
        if error_message:
            transfer_info.error_message = error_message
        
        self._save_transfer_state(transfer_id)
        
        logger.info(f"传输状态更新: transfer_id={transfer_id}, status={status}")
    
    def remove_transfer(self, transfer_id: str):
        """
        移除传输记录
        
        Args:
            transfer_id: 传输 ID
        """
        if transfer_id in self._transfers:
            del self._transfers[transfer_id]
        
        # 清理临时文件
        self._cleanup_temp_chunks(transfer_id)
        
        # 删除状态文件
        state_path = self._get_state_path(transfer_id)
        if state_path.exists():
            state_path.unlink()
        
        logger.info(f"传输记录已移除: {transfer_id}")
    
    def _update_progress(self, transfer_id: str):
        """更新传输进度"""
        if transfer_id not in self._transfers:
            return
        
        transfer_info = self._transfers[transfer_id]
        completed = sum(1 for c in transfer_info.chunks.values() if c.status == ChunkStatus.COMPLETED)
        transfer_info.progress = (completed / transfer_info.total_chunks) * 100
    
    def _get_chunk_temp_path(self, transfer_id: str, chunk_index: int) -> Path:
        """获取块临时文件路径"""
        return self.temp_dir / transfer_id / f"chunk_{chunk_index}.tmp"
    
    def _get_state_path(self, transfer_id: str) -> Path:
        """获取状态文件路径"""
        return self.temp_dir / transfer_id / "state.json"
    
    def _save_transfer_state(self, transfer_id: str):
        """持久化传输状态"""
        if transfer_id not in self._transfers:
            return
        
        state_path = self._get_state_path(transfer_id)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        
        transfer_info = self._transfers[transfer_id]
        
        with open(state_path, "w") as f:
            json.dump(transfer_info.to_dict(), f, indent=2)
    
    def load_transfer_state(self, transfer_id: str) -> Optional[TransferInfo]:
        """
        从磁盘加载传输状态
        
        Args:
            transfer_id: 传输 ID
            
        Returns:
            TransferInfo 对象，不存在则返回 None
        """
        state_path = self._get_state_path(transfer_id)
        
        if not state_path.exists():
            return None
        
        try:
            with open(state_path, "r") as f:
                data = json.load(f)
            
            transfer_info = TransferInfo.from_dict(data)
            self._transfers[transfer_id] = transfer_info
            
            logger.info(f"传输状态已加载: {transfer_id}")
            
            return transfer_info
            
        except Exception as e:
            logger.error(f"加载传输状态失败: {transfer_id}, error={e}")
            return None
    
    def _cleanup_temp_chunks(self, transfer_id: str):
        """清理临时块文件"""
        transfer_dir = self.temp_dir / transfer_id
        
        if transfer_dir.exists():
            import shutil
            shutil.rmtree(transfer_dir, ignore_errors=True)
            
            logger.debug(f"临时文件已清理: {transfer_id}")
    
    def get_all_transfers(self) -> Dict[str, TransferInfo]:
        """
        获取所有传输信息
        
        Returns:
            传输 ID 到 TransferInfo 的映射
        """
        return self._transfers.copy()
    
    def should_retry_chunk(self, transfer_id: str, chunk_index: int) -> bool:
        """
        判断块是否应该重试
        
        Args:
            transfer_id: 传输 ID
            chunk_index: 块索引
            
        Returns:
            是否应该重试
        """
        if transfer_id not in self._transfers:
            return False
        
        transfer_info = self._transfers[transfer_id]
        
        if chunk_index not in transfer_info.chunks:
            return False
        
        chunk_info = transfer_info.chunks[chunk_index]
        
        return chunk_info.retry_count < self.MAX_RETRIES
