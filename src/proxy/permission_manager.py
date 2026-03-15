"""
OpenClaw Mesh - Permission Manager (Simplified v0.6.1)
Permission Management Module: Whitelist/Blacklist control, command filtering
"""

import logging
import re
from collections import deque
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class PermissionManager:
    """
    Simplified Permission Manager
    
    Features:
    1. Command whitelist/blacklist check using regex patterns
    2. Node access control
    3. Recent 100 audit records
    """
    
    def __init__(self):
        self.allowed_patterns: List[str] = []
        self.denied_patterns: List[str] = []
        self.allowed_nodes: set = set()
        self.audit_log: deque = deque(maxlen=100)
        self.default_allow: bool = False
    
    def check(self, command: str) -> bool:
        """Check if command is allowed"""
        # Check deny list first
        for pattern in self.denied_patterns:
            if re.search(pattern, command):
                self.audit_log.append({"cmd": command, "allowed": False, "pattern": pattern})
                return False
        
        # Check allow list
        for pattern in self.allowed_patterns:
            if re.search(pattern, command):
                self.audit_log.append({"cmd": command, "allowed": True, "pattern": pattern})
                return True
        
        # No matching rule
        self.audit_log.append({"cmd": command, "allowed": self.default_allow})
        return self.default_allow
    
    def is_node_allowed(self, node_id: str) -> bool:
        """Check if node is allowed access"""
        if not self.allowed_nodes:
            return True
        return node_id in self.allowed_nodes
    
    def add_allowed_command(self, pattern: str) -> None:
        """Add allowed command pattern"""
        self.allowed_patterns.append(pattern)
        logger.info(f"Added allowed pattern: {pattern}")
    
    def add_denied_command(self, pattern: str) -> None:
        """Add denied command pattern"""
        self.denied_patterns.append(pattern)
        logger.info(f"Added denied pattern: {pattern}")
    
    def add_allowed_node(self, node_id: str) -> None:
        """Add allowed node"""
        self.allowed_nodes.add(node_id)
        logger.info(f"Added allowed node: {node_id}")
    
    def get_audit_log(self) -> List[Dict[str, Any]]:
        """Get recent audit records (max 100)"""
        return list(self.audit_log)
