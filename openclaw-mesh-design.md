# OpenClaw Mesh 功能设计文档

## 1. 概述

OpenClaw Mesh 是一个完全去中心化的 P2P 网络节点系统，基于 Tailscale + HTTPS + SQLite 构建，用于实现多节点间的安全通信、文件传输和远程命令执行。

### 1.1 设计目标
- **去中心化**：无单点故障，节点间直接通信
- **安全传输**：TLS 1.3 + Ed25519 消息签名
- **自动发现**：基于 Tailscale 的节点自动发现
- **跨平台**：支持 Linux、Windows、macOS

### 1.2 核心特性
| 特性 | 说明 |
|------|------|
| 🔒 安全传输 | TLS 1.3 + Ed25519 消息签名 |
| 🌐 P2P 网络 | 基于 Tailscale 的节点发现 |
| 💾 本地存储 | SQLite + WAL 模式 |
| 📁 文件传输 | P2P 分块传输、断点续传 |
| 🤖 Agent 代理 | 远程命令执行、权限控制 |
| 💬 讨论协调 | 多 Agent 讨论管理 |
| 📊 自适应心跳 | 动态节点状态管理 |
| 🪟 Windows 支持 | 完全兼容 Windows 10/11 |

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────┐
│         OpenClaw Mesh v0.5.9            │
├─────────────────────────────────────────┤
│  Layer 4: 安全层 (TLS + Ed25519)        │
│  - HTTPS 传输                           │
│  - 消息签名验证                         │
│  - 速率限制                             │
├─────────────────────────────────────────┤
│  Layer 3: 应用层                        │
│  - 消息通信                             │
│  - 文件传输                             │
│  - Agent 代理执行                       │
│  - 讨论协调                             │
├─────────────────────────────────────────┤
│  Layer 2: P2P 网络层                    │
│  - 节点发现 (Tailscale/Gossip)          │
│  - HTTPS API                            │
│  - 心跳管理                             │
│  - 离线队列                             │
├─────────────────────────────────────────┤
│  Layer 1: 本地存储层 (SQLite)           │
│  - 节点信息                             │
│  - 消息队列                             │
│  - 文件传输状态                         │
└─────────────────────────────────────────┘
```

### 2.2 模块关系图

```
MeshAgent (主控制器)
    ├── MeshConfig (配置管理)
    ├── SQLiteStorage (存储层)
    ├── CertificateManager (证书管理)
    ├── KeyManager (密钥管理)
    ├── MessageSigner (消息签名)
    ├── HTTPSClient/Server (HTTPS 传输)
    ├── BoundedOfflineQueue (离线队列)
    ├── NodeDiscovery (节点发现)
    ├── MessageTransport (消息传输)
    ├── HeartbeatManager (心跳管理)
    ├── FileTransfer (文件传输)
    ├── AgentProxy (Agent 代理)
    └── DiscussionCoordinator (讨论协调)
```

---

## 3. 核心功能设计

### 3.1 节点发现 (Node Discovery)

#### 3.1.1 设计原理
- **Tailscale 集成**：利用 Tailscale 的 mesh 网络发现 peers
- **Gossip 协议**：节点间定期交换已知节点列表
- **健康检查**：通过 HTTPS /health 端点验证节点可用性

#### 3.1.2 发现流程
```
1. 从 Tailscale API 获取 peers 列表
2. 对每个 peer 进行健康检查 (/health)
3. 保存响应的节点到本地数据库
4. 与已知节点进行 Gossip 交换
5. 更新节点状态 (online/suspect/offline)
```

#### 3.1.3 跨平台实现
| 平台 | 实现方式 |
|------|----------|
| Linux/macOS | 优先 HTTP API，失败回退 CLI |
| Windows | 使用 `tailscale status --json` CLI |

### 3.2 安全传输

#### 3.2.1 TLS 配置模式
| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `tailscale` | 使用 Tailscale 内置证书 | 推荐 |
| `self_signed` | 自动生成自签名证书 | 测试 |
| `custom_ca` | 使用自定义 CA | 企业环境 |
| `none` | 不使用 TLS | 仅测试 |

#### 3.2.2 消息签名
- **算法**：Ed25519
- **内容**：消息类型 + 时间戳 + payload + 发送者
- **验证**：防止篡改和重放攻击

#### 3.2.3 SSL 验证处理 (Tailscale 模式)
```python
# Tailscale 证书的 SAN 是主机名，但连接使用 IP
# 需要跳过 SSL 验证（Tailscale 传输层已加密）
verify_mode = "none" if tls_mode == "tailscale" else "peer"
```

### 3.3 消息传输

#### 3.3.1 消息格式
```python
@dataclass
class Message:
    id: str              # 消息唯一ID
    type: str            # 消息类型
    from_node: str       # 发送者
    to_node: str         # 接收者
    payload: dict        # 消息内容
    timestamp: datetime  # 时间戳
    signature: str       # 签名
```

#### 3.3.2 消息类型
| 类型 | 说明 |
|------|------|
| `chat` | 聊天消息 |
| `command` | 命令执行 |
| `file` | 文件传输 |
| `heartbeat` | 心跳 |
| `agent_execute_request` | Agent 执行请求 |
| `agent_execute_response` | Agent 执行响应 |
| `discussion_join` | 加入讨论 |
| `discussion_speech` | 发言 |
| `discussion_leave` | 离开讨论 |

#### 3.3.3 离线队列
- 当目标节点离线时，消息进入队列
- 定期重试，最多 10 次
- 支持优先级和过期时间

### 3.4 心跳管理

#### 3.4.1 心跳机制
```
间隔：30 秒
超时：10 秒
 suspect 阈值：3 次失败
offline 阈值：6 次失败
```

#### 3.4.2 状态转换
```
online → suspect → offline
  ↑___________________|
  (恢复检测)
```

### 3.5 文件传输

#### 3.5.1 分块传输
- **块大小**：1MB (可配置)
- **并发数**：最多 5 个并发传输
- **断点续传**：支持，记录已传输块

#### 3.5.2 传输流程
```
1. 发送方：分块 → 逐块发送
2. 接收方：验证校验和 → 确认
3. 完成后：合并块 → 验证完整性
```

### 3.6 Agent 代理

#### 3.6.1 权限控制
- 基于节点 ID 的访问控制
- 命令白名单机制
- 执行超时限制 (默认 300 秒)

#### 3.6.2 执行流程
```
1. 请求方发送 execute_request
2. 目标方验证权限
3. 执行命令
4. 返回 execute_response
```

### 3.7 讨论协调

#### 3.7.1 讨论模式
- **轮询发言**：Round Robin
- **超时机制**：120 秒发言超时
- **最大轮数**：默认 5 轮

#### 3.7.2 状态管理
- 讨论状态：pending/active/paused/completed
- 参与者状态：waiting/speaking/finished

---

## 4. 数据存储

### 4.1 数据库设计

#### 4.1.1 表结构
```sql
-- 节点信息
CREATE TABLE nodes (
    node_id TEXT PRIMARY KEY,
    name TEXT,
    ip TEXT UNIQUE,
    tailscale_hostname TEXT,
    capabilities TEXT,  -- JSON
    public_key TEXT,
    last_seen TIMESTAMP,
    status TEXT,  -- online/suspect/offline
    version TEXT,
    port INTEGER DEFAULT 8443
);

-- 消息队列
CREATE TABLE message_queue (
    id INTEGER PRIMARY KEY,
    message_id TEXT UNIQUE,
    to_node TEXT,
    message_type TEXT,
    payload TEXT,  -- JSON
    status TEXT,  -- pending/delivered/failed
    retry_count INTEGER DEFAULT 0,
    next_retry_at TIMESTAMP
);

-- 心跳失败记录
CREATE TABLE heartbeat_fails (
    node_id TEXT PRIMARY KEY,
    fail_count INTEGER DEFAULT 0,
    last_fail_at TIMESTAMP,
    last_success_at TIMESTAMP
);

-- 讨论
CREATE TABLE discussions (
    id TEXT PRIMARY KEY,
    topic TEXT,
    participants TEXT,  -- JSON
    status TEXT,
    created_at TIMESTAMP
);
```

#### 4.1.2 WAL 模式
```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=10000;
PRAGMA busy_timeout=5000;
```

### 4.2 Windows 兼容
- 使用 `win_compat.py` 包装同步 sqlite3
- ThreadPoolExecutor 实现异步接口

---

## 5. 配置设计

### 5.1 配置示例
```yaml
# 节点信息
node_id: "mark3"  # 每个节点唯一
tailscale_auth_key: "tskey-auth-xxx"

# 网络配置
host: "0.0.0.0"
port: 8443

# TLS 配置
tls_mode: "tailscale"  # tailscale | self_signed | custom_ca

# 数据目录
data_dir: "./data"
temp_dir: "./temp"

# 心跳配置
heartbeat_interval: 30
suspect_threshold: 3
offline_threshold: 6

# 队列配置
queue_max_size: 1000
queue_retry_interval: 60

# 文件传输
chunk_size: 1048576  # 1MB
max_concurrent_transfers: 5
```

### 5.2 节点命名规范
| 节点 | node_id | Tailscale 主机名 |
|------|---------|-----------------|
| mark1 | mark1 | schh |
| mark2 | mark2 | laptop-vsgbtehk |
| mark3 | mark3 | iv-yeggmfixa8bw80dou47a |

---

## 6. API 设计

### 6.1 REST API

#### 6.1.1 健康检查
```http
GET /health
Response: {"service": "openclaw-mesh", "version": "0.5.9", "node_id": "mark3", "status": "healthy"}
```

#### 6.1.2 节点交换 (Gossip)
```http
POST /api/nodes/exchange
Request: {"from_node": "mark3", "nodes": [...]}
Response: {"nodes": [...]}
```

#### 6.1.3 发送消息
```http
POST /api/messages
Request: Message 对象
Response: {"status": "accepted"}
```

### 6.2 CLI 命令
```bash
# 初始化配置
python -m src init --config config.yaml

# 启动节点
python -m src start --config config.yaml

# 查看状态
python -m src status

# 停止节点
python -m src stop

# 发送消息
python -m src send --to mark1 --message "Hello"

# 列出节点
python -m src nodes
```

---

## 7. 安全设计

### 7.1 威胁模型
| 威胁 | 防护措施 |
|------|----------|
| 中间人攻击 | TLS 1.3 + 证书验证 |
| 消息篡改 | Ed25519 签名 |
| 重放攻击 | 时间戳 + 签名缓存 |
| 未授权访问 | 节点白名单 + 命令白名单 |
| DoS 攻击 | 速率限制 |

### 7.2 密钥管理
- 私钥存储：`data/keys/{node_id}.key`
- 权限：Linux 0o600，Windows 继承 ACL
- 生成：首次启动自动生成 Ed25519 密钥对

---

## 8. 部署指南

### 8.1 系统要求
- Python 3.9+
- Tailscale 客户端
- 2GB+ RAM
- 1GB+ 磁盘空间

### 8.2 安装步骤
```bash
# 1. 解压
tar -xzf openclaw-mesh-v0.5.9.tar.gz
cd openclaw-mesh-v0.5.9

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置
cp config.yaml.example config.yaml
vim config.yaml  # 修改 node_id

# 4. 启动
python -m src start --config config.yaml
```

### 8.3 防火墙配置
- 开放 8443 端口 (或自定义端口)
- Tailscale 网络内部通信需要允许

---

## 9. 故障排查

### 9.1 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| 无法发现节点 | Tailscale 未连接 | 检查 `tailscale status` |
| SSL 验证失败 | 证书不匹配 | 使用 `verify_mode=none` (Tailscale模式) |
| 端口被占用 | 其他服务占用 | 更换端口或停止冲突服务 |
| 权限拒绝 | 文件权限问题 | Linux: 检查 0o600 权限 |

### 9.2 日志位置
```
logs/mesh.log
```

### 9.3 调试模式
```yaml
# config.yaml
log_level: "DEBUG"
```

---

## 10. 版本历史

| 版本 | 日期 | 主要变更 |
|------|------|----------|
| v0.5.9 | 2026-03-07 | SSL 修复、异常处理增强、JSON 安全解析 |
| v0.5.9 | 2026-03-06 | Windows 兼容性支持 |
| v0.5.9 | 2026-03-05 | 完整 MeshAgent 实现 |
| v0.4.0 | 2026-03-03 | 初始版本发布 |

---

## 11. 参考资料

- [Tailscale 文档](https://tailscale.com/kb/)
- [Python asyncio](https://docs.python.org/3/library/asyncio.html)
- [SQLite WAL 模式](https://sqlite.org/wal.html)
- [Ed25519 签名](https://ed25519.cr.yp.to/)

---

*文档版本: v0.5.9*
*更新日期: 2026-03-07*