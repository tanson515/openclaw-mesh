"""
LLM Client - 统一多模型接口
支持: Kimi, OpenAI, Claude, Ollama, 其他 OpenAI-compatible API
"""

import os
import aiohttp
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class LLMConfig:
    """LLM 配置"""
    provider: str = "openai"
    api_key: Optional[str] = None
    model: str = "gpt-4"
    base_url: str = ""
    temperature: float = 0.7
    max_tokens: int = 300
    timeout: int = 30
    extra_params: Dict[str, Any] = field(default_factory=dict)


class LLMClient:
    """统一 LLM 客户端"""
    
    PROVIDER_CONFIGS = {
        "kimi": {
            "base_url": "https://api.moonshot.cn/v1",
            "model": "kimi-k2.5",
        },
        "minimax": {
            "base_url": "https://api.minimaxi.com/anthropic",
            "model": "MiniMax-M2.5",
        },
        "openai": {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4",
        },
        "anthropic": {
            "base_url": "https://api.anthropic.com/v1",
            "model": "claude-3-opus-20240229",
        },
        "ollama": {
            "base_url": "http://localhost:11434/v1",
            "model": "llama3.2",
        },
    }
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self.provider = config.provider.lower()
        
        # 使用默认配置补充
        defaults = self.PROVIDER_CONFIGS.get(self.provider, {})
        if not config.base_url:
            self.config.base_url = defaults.get("base_url", "")
        if not config.model:
            self.config.model = defaults.get("model", "gpt-4")
    
    def _resolve_api_key(self) -> Optional[str]:
        """解析 API Key（支持环境变量）"""
        key = self.config.api_key or ""
        
        # 检查是否是环境变量引用 ${VAR} 或 $VAR
        if key.startswith("${") and key.endswith("}"):
            var_name = key[2:-1]
            return os.getenv(var_name)
        elif key.startswith("$"):
            var_name = key[1:]
            return os.getenv(var_name)
        
        # 尝试从环境变量获取
        env_key = os.getenv(f"{self.provider.upper()}_API_KEY")
        if env_key:
            return env_key
            
        return key if key else None
    
    def _build_request(self, prompt: str) -> tuple:
        """构建请求"""
        api_key = self._resolve_api_key()
        
        if self.provider == "anthropic":
            # Anthropic 特殊格式
            url = f"{self.config.base_url}/messages"
            logger.info(f"[LLM] 使用 {self.provider} API, URL: {url}")
            headers = {
                "x-api-key": api_key or "",
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json"
            }
            data = {
                "model": self.config.model,
                "max_tokens": self.config.max_tokens,
                "temperature": self.config.temperature,
                "messages": [{"role": "user", "content": prompt}]
            }
        elif self.provider == "minimax":
            # MiniMax 使用 OpenAI 兼容格式
            url = f"{self.config.base_url}/chat/completions"
            logger.info(f"[LLM] 使用 {self.provider} API, URL: {url}")
            headers = {
                "Authorization": f"Bearer {api_key}" if api_key else "",
                "Content-Type": "application/json"
            }
            data = {
                "model": self.config.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
            }
        else:
            # OpenAI-compatible 格式 (Kimi, OpenAI, Ollama)
            url = f"{self.config.base_url}/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}" if api_key else "",
                "Content-Type": "application/json"
            }
            data = {
                "model": self.config.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
            }
        
        # 添加额外参数
        if self.config.extra_params:
            data.update(self.config.extra_params)
        
        return url, headers, data
    
    def _parse_response(self, result: dict) -> str:
        """解析响应"""
        try:
            if self.provider == "anthropic":
                return result["content"][0]["text"].strip()
            elif self.provider == "minimax":
                # MiniMax 使用 OpenAI 兼容格式
                return result["choices"][0]["message"]["content"].strip()
            else:
                # OpenAI-compatible
                return result["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError) as e:
            logger.error(f"解析响应失败: {e}, result: {result}")
            return f"[{self.provider}] 解析响应失败"
    
    async def generate(self, prompt: str) -> str:
        """生成回复"""
        try:
            url, headers, data = self._build_request(prompt)
            
            # Ollama 不需要 api_key
            if self.provider == "ollama":
                headers.pop("Authorization", None)
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    headers=headers, 
                    json=data, 
                    timeout=aiohttp.ClientTimeout(total=self.config.timeout)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        return self._parse_response(result)
                    else:
                        error_text = await resp.text()
                        logger.error(f"LLM API 错误: {resp.status} - {error_text}")
                        return f"[{self.provider}] API 错误: {resp.status}"
        
        except aiohttp.ClientError as e:
            logger.error(f"LLM 请求失败: {e}")
            return f"[{self.provider}] 网络错误: {str(e)}"
        except Exception as e:
            logger.error(f"LLM 调用异常: {e}")
            return f"[{self.provider}] 错误: {str(e)}"


class LLMClientFactory:
    """LLM 客户端工厂"""
    
    @staticmethod
    def from_config(config: dict) -> Optional[LLMClient]:
        """
        从配置字典创建 LLMClient
        注意：如果配置为空或不完整，返回 None（让节点自己配置）
        """
        if not config:
            return None
            
        provider = config.get("provider")
        if not provider:
            return None
            
        provider = provider.lower()
        provider_config = config.get(provider, {})
        
        if not provider_config and provider != "ollama":
            logger.warning(f"未找到 {provider} 的配置")
            return None
        
        # 解析 API Key（支持环境变量引用）
        api_key = provider_config.get("api_key", "") if provider_config else ""
        if api_key.startswith("${") and api_key.endswith("}"):
            api_key = os.getenv(api_key[2:-1], "")
        elif api_key.startswith("$"):
            api_key = os.getenv(api_key[1:], "")
        
        if not api_key:
            logger.warning(f"未找到 {provider} 的 API Key")
            return None
        
        llm_config = LLMConfig(
            provider=provider,
            api_key=api_key,
            model=provider_config.get("model"),
            base_url=provider_config.get("base_url"),
            temperature=provider_config.get("temperature", 0.7),
            max_tokens=provider_config.get("max_tokens", 300),
            timeout=provider_config.get("timeout", 30),
            extra_params=provider_config.get("extra_params", {})
        )
        
        return LLMClient(llm_config)
