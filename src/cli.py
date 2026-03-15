#!/usr/bin/env python3
"""
OpenClaw Mesh - CLI 入口
命令行界面：启动、配置、管理 Mesh 节点

用法:
    python -m openclaw_mesh start --config config.yaml
    python -m openclaw_mesh status
    python -m openclaw_mesh stop
"""

import argparse
import asyncio
import json
import logging
import os
import platform
import signal
import sys
from pathlib import Path
from typing import Optional
import psutil

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent))

from src.constants import VERSION, VERSION_NAME
from src.mesh_agent import MeshAgent, MeshConfig, create_agent


# 默认配置
DEFAULT_CONFIG = {
    'host': '0.0.0.0',
    'port': 8443,
    'max_connections': 100,
    'tls_mode': 'tailscale',
    'chunk_size': 1024 * 1024,
    'max_concurrent_transfers': 5,
    'temp_dir': './temp',
    'heartbeat_interval': 30,
    'suspect_threshold': 3,
    'offline_threshold': 6,
    'queue_max_size': 1000,
    'queue_retry_interval': 60,
    'proxy_timeout': 300,
    'max_participants': 10,
    'speak_timeout': 120.0,
    'log_level': 'INFO',
    'data_dir': './data',
}


def setup_logging(log_level: str, log_file: Optional[str] = None) -> None:
    """
    设置日志
    
    Args:
        log_level: 日志级别
        log_file: 日志文件路径
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
    )


def generate_node_id() -> str:
    """生成节点 ID"""
    import uuid
    import socket
    
    hostname = socket.gethostname()
    unique_id = uuid.uuid4().hex[:8]
    return f"{hostname}-{unique_id}"


def create_default_config(config_path: str, node_id: Optional[str] = None) -> None:
    """
    创建默认配置文件
    
    Args:
        config_path: 配置文件路径
        node_id: 节点 ID
    """
    config = DEFAULT_CONFIG.copy()
    config['node_id'] = node_id or generate_node_id()
    
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # 生成 YAML 格式
    yaml_content = f"""# {VERSION_NAME} 配置文件

# 节点信息
node_id: {config['node_id']}
tailscale_auth_key: null  # Tailscale API 密钥（可选）

# 网络配置
host: {config['host']}
port: {config['port']}
max_connections: {config['max_connections']}

# 安全配置
tls_mode: {config['tls_mode']}  # tailscale | self_signed | custom_ca
cert_path: null
key_path: null
ca_cert_path: null

# 传输配置
chunk_size: {config['chunk_size']}  # 1MB
max_concurrent_transfers: {config['max_concurrent_transfers']}
temp_dir: {config['temp_dir']}

# 心跳配置
heartbeat_interval: {config['heartbeat_interval']}
suspect_threshold: {config['suspect_threshold']}
offline_threshold: {config['offline_threshold']}

# 队列配置
queue_max_size: {config['queue_max_size']}
queue_retry_interval: {config['queue_retry_interval']}

# 代理配置
allowed_commands: []  # 允许的命令模式列表
proxy_timeout: {config['proxy_timeout']}

# 讨论配置
max_participants: {config['max_participants']}
speak_timeout: {config['speak_timeout']}

# 日志配置
log_level: {config['log_level']}
log_file: null

# 数据目录
data_dir: {config['data_dir']}
"""
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    
    print(f"Created default config: {config_path}")
    print(f"Node ID: {config['node_id']}")



def check_and_kill_old_process(port: int, pid_file: Path = None) -> bool:
    """检查并杀掉占用端口的旧进程"""
    # 检查 PID 文件
    if pid_file and pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            try:
                proc = psutil.Process(old_pid)
                if proc.is_running():
                    print("Killing old process (PID: " + str(old_pid) + ")")
                    proc.terminate()
                    proc.wait(timeout=5)
            except psutil.NoSuchProcess:
                pass
        except (ValueError, psutil.NoSuchProcess):
            pass
    
    # 检查端口占用
    for conn in psutil.net_connections():
        if conn.laddr.port == port and conn.status == 'LISTEN':
            try:
                proc = psutil.Process(conn.pid)
                print("Killing process " + str(proc.pid) + " using port " + str(port))
                proc.terminate()
                proc.wait(timeout=5)
                return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    return True


async def cmd_start(args: argparse.Namespace) -> int:
    """
    启动命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    # 检查并杀掉旧进程
    check_and_kill_old_process(args.port or 8443)

    # 加载配置
    if args.config:
        if not Path(args.config).exists():
            print(f"Error: Config file not found: {args.config}")
            print(f"Run 'openclaw-mesh init --config {args.config}' to create one.")
            return 1
        config = MeshAgent.load_config(args.config)
    else:
        # 使用默认配置
        node_id = args.node_id or generate_node_id()
        config = MeshConfig(node_id=node_id, **DEFAULT_CONFIG)
    
    # 覆盖配置
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port
    if args.data_dir:
        config.data_dir = args.data_dir
    if args.log_level:
        config.log_level = args.log_level
    if args.log_file:
        config.log_file = args.log_file
    
    # 设置日志
    setup_logging(config.log_level, config.log_file)
    
    # 创建并启动 Agent
    agent = MeshAgent(config)
    
    # 守护进程模式
    if args.daemon:
        import tempfile
        pid_dir = Path(tempfile.gettempdir())
        pid_file = Path(args.pid_file or pid_dir / f"openclaw-mesh-{config.node_id}.pid")
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        print(f"Started in daemon mode (PID: {os.getpid()})")
        print(f"PID file: {pid_file}")
    
    try:
        await agent.run()
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 0
    except (RuntimeError, OSError, ConnectionError) as e:
        print(f"Error: {e}")
        return 1


async def cmd_init(args: argparse.Namespace) -> int:
    """
    初始化命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    config_path = args.config or './config.yaml'
    
    if Path(config_path).exists() and not args.force:
        print(f"Config file already exists: {config_path}")
        print("Use --force to overwrite")
        return 1
    
    create_default_config(config_path, args.node_id)
    return 0


async def cmd_status(args: argparse.Namespace) -> int:
    """
    状态命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    # 检查 PID 文件
    pid_file = Path(args.pid_file or '/tmp/openclaw-mesh-*.pid')
    
    if pid_file.exists():
        with open(pid_file) as f:
            pid = f.read().strip()
        
        # 检查进程是否存在
        try:
            os.kill(int(pid), 0)
            print(f"OpenClaw Mesh is running (PID: {pid})")
            return 0
        except ProcessLookupError:
            print(f"OpenClaw Mesh is not running (stale PID file: {pid_file})")
            return 1
    else:
        print("OpenClaw Mesh is not running")
        return 1


async def cmd_stop(args: argparse.Namespace) -> int:
    """
    停止命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    import glob
    
    # 查找 PID 文件
    pid_pattern = args.pid_file or '/tmp/openclaw-mesh-*.pid'
    pid_files = glob.glob(pid_pattern)
    
    if not pid_files:
        print("No running OpenClaw Mesh instances found")
        return 1
    
    stopped = 0
    for pid_file in pid_files:
        with open(pid_file) as f:
            pid = f.read().strip()
        
        try:
            if platform.system() == "Windows":
                import subprocess
                subprocess.run(['taskkill', '/PID', pid, '/F'], check=False)
            else:
                os.kill(int(pid), signal.SIGTERM)
            print(f"Stopped OpenClaw Mesh (PID: {pid})")
            Path(pid_file).unlink()
            stopped += 1
        except ProcessLookupError:
            print(f"Process not found (PID: {pid})")
            Path(pid_file).unlink()
        except PermissionError:
            print(f"Permission denied (PID: {pid})")
    
    return 0 if stopped > 0 else 1


async def cmd_send(args: argparse.Namespace) -> int:
    """
    发送消息命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    if not args.config:
        print("Error: --config required")
        return 1
    
    config = MeshAgent.load_config(args.config)
    setup_logging('WARNING')
    
    agent = MeshAgent(config)
    await agent.start()
    
    try:
        success = await agent.send_message(
            to_node=args.to,
            message_type=args.type or 'chat',
            payload={'content': args.message},
        )
        
        if success:
            print(f"Message sent to {args.to}")
            return 0
        else:
            print(f"Failed to send message to {args.to}")
            return 1
    finally:
        await agent.stop()


async def cmd_nodes(args: argparse.Namespace) -> int:
    """
    节点列表命令
    
    Args:
        args: 命令行参数
    
    Returns:
        退出码
    """
    if not args.config:
        print("Error: --config required")
        return 1
    
    config = MeshAgent.load_config(args.config)
    setup_logging('WARNING')
    
    agent = MeshAgent(config)
    await agent.start()
    
    try:
        if args.online:
            nodes = await agent.get_online_nodes()
            print(f"Online nodes ({len(nodes)}):")
        else:
            nodes = await agent.get_nodes()
            print(f"All nodes ({len(nodes)}):")
        
        for node in nodes:
            status_icon = '🟢' if node.status == 'online' else '🟡' if node.status == 'suspect' else '🔴'
            print(f"  {status_icon} {node.node_id} ({node.ip}) - {node.status}")
        
        return 0
    finally:
        await agent.stop()


def main() -> int:
    """
    主入口函数
    
    Returns:
        退出码
    """
    parser = argparse.ArgumentParser(
        prog='openclaw-mesh',
        description=f'{VERSION_NAME} - P2P Network Node',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize configuration
  openclaw-mesh init --config config.yaml

  # Start node
  openclaw-mesh start --config config.yaml

  # Start with custom port
  openclaw-mesh start --config config.yaml --port 9000

  # Start in daemon mode
  openclaw-mesh start --config config.yaml --daemon

  # Send message
  openclaw-mesh send --config config.yaml --to node2 --message "Hello!"

  # List nodes
  openclaw-mesh nodes --config config.yaml --online
        """
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # init 命令
    init_parser = subparsers.add_parser('init', help='Initialize configuration file')
    init_parser.add_argument('--config', '-c', help='Config file path')
    init_parser.add_argument('--node-id', help='Node ID')
    init_parser.add_argument('--force', '-f', action='store_true', help='Overwrite existing config')
    
    # start 命令
    start_parser = subparsers.add_parser('start', help='Start the mesh node')
    start_parser.add_argument('--config', '-c', help='Config file path')
    start_parser.add_argument('--node-id', help='Node ID (if no config)')
    start_parser.add_argument('--host', help='Host to bind')
    start_parser.add_argument('--port', '-p', type=int, help='Port to listen on')
    start_parser.add_argument('--data-dir', help='Data directory')
    start_parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Log level')
    start_parser.add_argument('--log-file', help='Log file path')
    start_parser.add_argument('--daemon', '-d', action='store_true', help='Run in daemon mode')
    start_parser.add_argument('--pid-file', help='PID file path')
    
    # stop 命令
    stop_parser = subparsers.add_parser('stop', help='Stop the mesh node')
    stop_parser.add_argument('--pid-file', help='PID file pattern')
    
    # status 命令
    status_parser = subparsers.add_parser('status', help='Check node status')
    status_parser.add_argument('--pid-file', help='PID file path')
    
    # send 命令
    send_parser = subparsers.add_parser('send', help='Send message to a node')
    send_parser.add_argument('--config', '-c', required=True, help='Config file path')
    send_parser.add_argument('--to', required=True, help='Target node ID')
    send_parser.add_argument('--message', '-m', required=True, help='Message content')
    send_parser.add_argument('--type', '-t', help='Message type')
    
    # nodes 命令
    nodes_parser = subparsers.add_parser('nodes', help='List known nodes')
    nodes_parser.add_argument('--config', '-c', required=True, help='Config file path')
    nodes_parser.add_argument('--online', action='store_true', help='Show only online nodes')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # 执行命令
    commands = {
        'init': cmd_init,
        'start': cmd_start,
        'stop': cmd_stop,
        'status': cmd_status,
        'send': cmd_send,
        'nodes': cmd_nodes,
    }
    
    cmd_func = commands.get(args.command)
    if not cmd_func:
        print(f"Unknown command: {args.command}")
        return 1
    
    try:
        return asyncio.run(cmd_func(args))
    except KeyboardInterrupt:
        print("\nInterrupted")
        return 130
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
        print(f"Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
