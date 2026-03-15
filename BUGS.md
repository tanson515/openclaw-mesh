# OpenClaw Mesh v0.6.2 Bug 记录

## 待修复 Bug (mark2 需要修复)

### 1. 大小写不匹配
**严重程度**: 高  
**状态**: ✅ 已修复

**问题描述**:  
Tailscale 返回 `laptop-vsgbtehk`（小写），但数据库存储了 `LAPTOP-VSGBTEHK`（大写），导致节点查找失败。

**修复内容**:  
- `set_local_node_info()`: 存储时统一转为小写
- `upsert_node()`: 存储时统一转为小写
- `get_node()`: 查询时统一转为小写

---

### 2. KeyError: 'id'
**严重程度**: 中  
**状态**: ✅ 已修复

**问题描述**:  
消息处理时报错缺少 'id' 字段。

**修复内容**:  
- `Message.from_dict()`: 更健壮的消息解析
  - 缺少 id 时自动生成 UUID
  - 支持 message_type -> type 回退
  - 支持 content -> payload.content 回退

---

### 3. mark1 获取本地 Tailscale IP 失败
**严重程度**: 中  
**状态**: 待排查

**问题描述**:  
mark1 本地节点 IP 显示为 0.0.0.0，说明启动时获取 Tailscale IP 失败。

**原因分析**:  
`_get_tailscale_status_via_cli()` 在初始化本地节点时调用失败，但异常被静默吞没。

**现象**:  
- mark1 显示 IP: 0.0.0.0
- 日志中没有显示 "Got local Tailscale IP" 或错误信息
- 即使代码中有 `except Exception as e: logger.warning(...)` 也没有输出

**排查方向**:  
1. 检查 `_get_tailscale_status_via_cli()` 调用失败时是否正确记录日志
2. 检查异常处理是否吞掉了错误
3. 检查 Windows 平台 tailscale.exe 路径是否正确
4. 检查是否是 Tailscale 服务未启动

**相关代码位置**:  
- `node_discovery.py:709` - 初始化时调用 `_get_tailscale_status_via_cli()`

---

## 待验证问题

### 4. 心跳消息堆积
**严重程度**: 低  
**状态**: 待确认

**问题描述**:  
消息队列中有 42 条 pending 状态的心跳消息，可能是网络不通或目标节点不在线。

---

## 已知兼容性问题

### 5. aiohttp API 变更
**严重程度**: 低  
**状态**: 已修复 (v0.6.2)

`.aclose()` 方法在较新的 aiohttp 版本中已移除，需使用 `.close()`。

---

## 版本历史

- **v0.6.2** (2026-03-12): 修复数据库字段、Tailscale 主机名获取、动态速率限制
- **v0.6.1** (2026-03-12): Agent 代理重构、文件传输简化
