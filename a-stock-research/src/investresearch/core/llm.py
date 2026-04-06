"""LLM统一接口 - 双提供商路由（百炼 Anthropic + 火山 OpenAI）

百炼 (Bailian): Anthropic兼容协议, 支持 qwen3 系列
火山 (Volcengine): OpenAI兼容协议, 支持 doubao/deepseek 系列
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import httpx
from dotenv import load_dotenv

from .exceptions import LLMError, LLMRateLimitError, LLMResponseError
from .logging import get_logger

logger = get_logger("llm")

load_dotenv()

# ============================================================
# 模型别名 -> (provider, model_id)
# ============================================================
MODEL_ALIASES: dict[str, tuple[str, str]] = {
    # 百炼模型 (Anthropic协议)
    # 千问系列
    "qwen3-coder": ("bailian", "qwen3-coder-next"),
    "qwen3-coder-plus": ("bailian", "qwen3-coder-plus"),
    "qwen3-plus": ("bailian", "qwen3.5-plus"),
    "qwen3-max": ("bailian", "qwen3-max-2026-01-23"),
    # 智谱
    "glm-5": ("bailian", "glm-5"),
    "glm-4.7": ("bailian", "glm-4.7"),
    # Kimi
    "kimi-k2.5": ("bailian", "kimi-k2.5"),
    # MiniMax
    "minimax-m2.5": ("bailian", "MiniMax-M2.5"),
    # 火山模型 (OpenAI协议)
    # 豆包系列
    "doubao-code": ("volcengine", "doubao-seed-2.0-code"),
    "doubao-pro": ("volcengine", "doubao-seed-2.0-pro"),
    "doubao-lite": ("volcengine", "doubao-seed-2.0-lite"),
    "doubao-seed-code": ("volcengine", "doubao-seed-code"),
    # DeepSeek/Kimi/GLM/MiniMax (火山托管)
    "deepseek-v3.2": ("volcengine", "deepseek-v3.2"),
    "volc-kimi-k2.5": ("volcengine", "kimi-k2.5"),
    "volc-glm-4.7": ("volcengine", "glm-4.7"),
    "volc-minimax-m2.5": ("volcengine", "minimax-m2.5"),
}

LAYER_DEFAULTS: dict[str, str] = {
    "data_layer": "qwen3-coder",
    "analysis_layer": "qwen3-plus",
    "decision_layer": "doubao-pro",
    "reporting": "qwen3-coder",
}

MODEL_FALLBACKS: dict[str, list[str]] = {
    "qwen3-max": ["qwen3-plus", "qwen3-coder", "doubao-pro", "doubao-lite"],
    "qwen3-plus": ["qwen3-coder", "doubao-pro", "doubao-lite"],
    "qwen3-coder": ["qwen3-plus", "doubao-pro", "doubao-lite"],
    "qwen3-coder-plus": ["qwen3-plus", "qwen3-coder", "doubao-pro"],
    "doubao-pro": ["qwen3-plus", "qwen3-coder", "doubao-lite"],
    "doubao-lite": ["qwen3-plus", "qwen3-coder"],
    "deepseek-v3.2": ["qwen3-plus", "qwen3-coder", "doubao-pro"],
}


class LLMRouter:
    """LLM双提供商路由器

    bailian   -> Anthropic兼容 (Messages API)
    volcengine -> OpenAI兼容 (Chat Completions API)
    """

    def __init__(self) -> None:
        self._http_client: httpx.AsyncClient | None = None
        self._token_usage: dict[str, dict[str, int]] = {}

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is not None and not self._http_client.is_closed:
            return self._http_client
        self._http_client = httpx.AsyncClient(timeout=180.0)
        return self._http_client

    def _reset_http_client(self) -> None:
        """强制重建HTTP客户端"""
        self._http_client = None

    # ============================================================
    # Provider 配置获取
    # ============================================================

    def _get_provider_config(self, provider: str) -> dict[str, str]:
        """获取提供商的API配置"""
        if provider == "bailian":
            api_key = os.environ.get("BAILIAN_API_KEY", "")
            base_url = os.environ.get(
                "BAILIAN_BASE_URL",
                "https://coding.dashscope.aliyuncs.com/apps/anthropic",
            )
            if not api_key:
                raise LLMError("百炼API Key未配置，请设置 BAILIAN_API_KEY 环境变量")
            return {"api_key": api_key, "base_url": base_url, "protocol": "anthropic"}

        elif provider == "volcengine":
            api_key = os.environ.get("VOLCENGINE_API_KEY", "")
            base_url = os.environ.get(
                "VOLCENGINE_BASE_URL",
                "https://ark.cn-beijing.volces.com/api/coding",
            )
            if not api_key:
                raise LLMError("火山API Key未配置，请设置 VOLCENGINE_API_KEY 环境变量")
            return {"api_key": api_key, "base_url": base_url, "protocol": "openai"}

        raise LLMError(f"不支持的提供商: {provider}")

    # ============================================================
    # Anthropic 协议 (百炼)
    # ============================================================

    async def _call_anthropic(
        self,
        prompt: str,
        model_id: str,
        config: dict[str, str],
        system_prompt: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 8192,
    ) -> str:
        """通过Anthropic Messages API调用"""
        messages = [{"role": "user", "content": prompt}]
        request_body: dict[str, Any] = {
            "model": model_id,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system_prompt:
            request_body["system"] = system_prompt

        response = await self.http_client.post(
            f"{config['base_url']}/v1/messages",
            headers={
                "x-api-key": config["api_key"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=request_body,
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("retry-after", "60"))
            raise LLMRateLimitError("bailian", retry_after)

        if response.status_code != 200:
            error_text = response.text[:500]
            raise LLMError(f"百炼API调用失败(status={response.status_code}): {error_text}")

        data = response.json()
        content_blocks = data.get("content", [])
        return " ".join(
            block.get("text", "") for block in content_blocks if block.get("type") == "text"
        )

    # ============================================================
    # OpenAI 协议 (火山引擎)
    # ============================================================

    async def _call_openai(
        self,
        prompt: str,
        model_id: str,
        config: dict[str, str],
        system_prompt: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 8192,
    ) -> str:
        """通过OpenAI Chat Completions API调用"""
        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = await self.http_client.post(
            f"{config['base_url']}/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config['api_key']}",
                "content-type": "application/json",
            },
            json={
                "model": model_id,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("retry-after", "60"))
            raise LLMRateLimitError("volcengine", retry_after)

        if response.status_code != 200:
            error_text = response.text[:500]
            raise LLMError(f"火山API调用失败(status={response.status_code}): {error_text}")

        data = response.json()
        return data["choices"][0]["message"]["content"]

    def _fallback_candidates(self, model: str) -> list[str]:
        """Return an ordered list of backup models for the given primary model."""
        configured = MODEL_FALLBACKS.get(model, [])
        defaults = ["qwen3-plus", "qwen3-coder", "doubao-pro", "doubao-lite"]
        candidates: list[str] = []
        for candidate in [*configured, *defaults]:
            if candidate == model or candidate not in MODEL_ALIASES:
                continue
            if candidate not in candidates:
                candidates.append(candidate)
        return candidates

    async def _call_with_fallback_models(
        self,
        *,
        prompt: str,
        system_prompt: str | None,
        primary_model: str,
        temperature: float | None,
        max_tokens: int | None,
    ) -> str:
        """Retry a rate-limited request against backup models."""
        fallback_models = self._fallback_candidates(primary_model)
        last_error: Exception | None = None

        for fallback_model in fallback_models:
            try:
                logger.warning(
                    f"LLM主模型限流，切换备用模型 | primary={primary_model} -> fallback={fallback_model}"
                )
                return await self.call(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    model=fallback_model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    allow_fallback=False,
                )
            except (LLMError, LLMRateLimitError) as exc:
                last_error = exc
                logger.warning(f"备用模型调用失败 | model={fallback_model} | error={exc}")

        if last_error is not None:
            raise last_error
        raise LLMRateLimitError("fallback", 60)

    # ============================================================
    # 统一调用接口
    # ============================================================

    async def call(
        self,
        prompt: str,
        system_prompt: str | None = None,
        model: str = "qwen3-plus",
        temperature: float | None = None,
        max_tokens: int | None = None,
        allow_fallback: bool = True,
    ) -> str:
        """调用LLM

        Args:
            prompt: 用户提示词
            system_prompt: 系统提示词
            model: 模型别名
            temperature: 温度参数
            max_tokens: 最大token数

        Returns:
            LLM响应文本
        """
        if model not in MODEL_ALIASES:
            raise LLMError(f"未知模型别名: {model}, 可用: {list(MODEL_ALIASES.keys())}")

        provider, model_id = MODEL_ALIASES[model]
        provider_config = self._get_provider_config(provider)

        temp = temperature if temperature is not None else 0.1
        tokens = max_tokens if max_tokens is not None else 8192

        start_time = time.time()
        try:
            if provider_config["protocol"] == "anthropic":
                content = await self._call_anthropic(
                    prompt=prompt, model_id=model_id, config=provider_config,
                    system_prompt=system_prompt, temperature=temp, max_tokens=tokens,
                )
            else:
                content = await self._call_openai(
                    prompt=prompt, model_id=model_id, config=provider_config,
                    system_prompt=system_prompt, temperature=temp, max_tokens=tokens,
                )

            elapsed = time.time() - start_time
            logger.info(
                f"LLM调用完成 | model={model}({model_id}) | provider={provider} | "
                f"elapsed={elapsed:.1f}s | chars={len(content)}"
            )

            self._record_usage(model, len(content))
            return content

        except LLMRateLimitError:
            if allow_fallback:
                return await self._call_with_fallback_models(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    primary_model=model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            raise
        except LLMError:
            raise
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                logger.warning(f"事件循环已关闭，重建HTTP客户端 | model={model}")
                self._reset_http_client()
                return await self._call_with_retry(
                    prompt, system_prompt, model, provider, model_id, provider_config, temp, tokens, start_time
                )
            raise LLMError(f"LLM调用失败(model={model}): {e}") from e
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"LLM调用失败 | model={model} | elapsed={elapsed:.1f}s | error={e}")
            raise LLMError(f"LLM调用失败(model={model}): {e}") from e

    async def _call_with_retry(
        self, prompt: str, system_prompt: str | None, model: str,
        provider: str, model_id: str, provider_config: dict[str, str],
        temp: float, tokens: int, start_time: float,
    ) -> str:
        """事件循环关闭后的重试"""
        try:
            if provider_config["protocol"] == "anthropic":
                content = await self._call_anthropic(
                    prompt=prompt, model_id=model_id, config=provider_config,
                    system_prompt=system_prompt, temperature=temp, max_tokens=tokens,
                )
            else:
                content = await self._call_openai(
                    prompt=prompt, model_id=model_id, config=provider_config,
                    system_prompt=system_prompt, temperature=temp, max_tokens=tokens,
                )

            elapsed = time.time() - start_time
            logger.info(f"LLM调用完成(重试) | model={model} | elapsed={elapsed:.1f}s")
            self._record_usage(model, len(content))
            return content
        except Exception as e:
            raise LLMError(f"LLM调用失败(重试, model={model}): {e}") from e

    async def call_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        model: str = "qwen3-coder",
    ) -> dict[str, Any]:
        """调用LLM并解析JSON响应"""
        json_instruction = (
            "\n\n重要：你必须以合法的JSON格式输出结果，不要输出任何其他文字。"
            "不要用```json```包裹。"
        )
        effective_system = (system_prompt or "") + json_instruction

        raw = await self.call(
            prompt=prompt, system_prompt=effective_system, model=model, temperature=0.0,
        )
        return self._parse_json(raw)

    def _parse_json(self, text: str) -> dict[str, Any]:
        """从LLM响应中提取JSON"""
        text = text.strip()
        # 去除markdown包裹
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # 尝试提取JSON块
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
            raise LLMResponseError(
                f"无法解析LLM JSON输出",
                raw_response=text[:500],
            )

    def _record_usage(self, model: str, chars: int) -> None:
        """记录使用统计"""
        if model not in self._token_usage:
            self._token_usage[model] = {"calls": 0, "total_chars": 0}
        self._token_usage[model]["calls"] += 1
        self._token_usage[model]["total_chars"] += chars

    def get_layer_model(self, layer: str) -> str:
        """获取指定层的默认模型"""
        return LAYER_DEFAULTS.get(layer, "qwen3-plus")

    def get_usage_stats(self) -> dict[str, dict[str, int]]:
        """获取使用统计"""
        return dict(self._token_usage)

    async def close(self) -> None:
        """关闭HTTP连接"""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()


# 全局LLM路由器实例
llm_router = LLMRouter()
