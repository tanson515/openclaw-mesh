#!/usr/bin/env python3
"""
OpenClaw Mesh - 模块入口

用法:
    python -m openclaw_mesh --help
    python -m openclaw_mesh start --config config.yaml
"""

import sys
from pathlib import Path

# 确保 src 在路径中
sys.path.insert(0, str(Path(__file__).parent))

from src.cli import main

if __name__ == '__main__':
    sys.exit(main())
