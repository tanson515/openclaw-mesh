# OpenClaw Mesh v0.7.0

Decentralized P2P network node based on Tailscale + HTTPS + SQLite.

## Features

- Secure Transport: TLS 1.3 + Ed25519 message signing
- P2P Network: Tailscale-based node discovery
- Local Storage: SQLite + WAL mode
- File Transfer: P2P chunked transfer, resume support
- Agent Proxy: Remote command execution, permission control
- Discussion Coordination: Multi-Agent discussion management
- **Multi-Model LLM Support**: Each node can use its own LLM (MiniMax, Kimi, OpenAI, Anthropic, Ollama)
- Adaptive Heartbeat: Dynamic node status management
- Windows Support: Fully compatible with Windows 10/11
- macOS/Linux Support: Cross-platform compatible

## Quick Start

### Installation

```bash
tar -xzf openclaw-mesh-v0.7.0.tar.gz
cd openclaw-mesh-v0.6.9
pip install -r requirements.txt
```

### Configuration

1. Get Tailscale hostname (node_id)
   ```bash
   tailscale status --json | grep -i hostname
   ```

2. Edit config.yaml
   ```yaml
   node_id: "your-hostname"
   tailscale_auth_key: "tskey-auth-xxx"
   host: "0.0.0.0"
   port: 8443
   ```

### LLM Configuration (Optional)

Each node can configure its own LLM for discussion:

```yaml
llm:
  provider: "minimax"  # or: kimi, openai, anthropic, ollama
  minimax:
    api_key: "your-api-key"
    model: "MiniMax-M2.5"
```

### Run

```bash
python -m src.cli start --config config.yaml
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `python -m src.cli start --config config.yaml` | Start node |
| `python -m src.cli stop` | Stop node |
| `python -m src.cli status` | Check status |

## System Requirements

- Python 3.9+
- Tailscale network
- 2GB+ RAM
- 1GB+ disk space

## License

MIT License
