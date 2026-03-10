"""
LLM 调用封装 - 统一 OpenAI / Anthropic 接口
"""
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class LLM:
    """统一 LLM 调用层"""

    def __init__(self, config):
        self.config = config
        self.provider = config.provider
        self.client = self._init_client()

    def _init_client(self):
        if self.provider == "anthropic":
            import anthropic
            return anthropic.Anthropic(api_key=self.config.api_key)
        else:
            import openai
            return openai.OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
            )

    def chat(self, system_prompt: str, user_message: str) -> str:
        """普通对话"""
        try:
            if self.provider == "anthropic":
                resp = self.client.messages.create(
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )
                return resp.content[0].text
            else:
                resp = self.client.chat.completions.create(
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                )
                return resp.choices[0].message.content
        except Exception as e:
            logger.error(f"LLM 调用失败: {e}")
            raise

    def chat_with_tools(
        self, system_prompt: str, messages: list, tools: list[dict]
    ) -> dict:
        """
        带工具调用的对话（Function Calling）
        返回: {"content": str, "tool_calls": [{"name":..., "arguments":...}]}
        """
        try:
            if self.provider == "anthropic":
                return self._anthropic_tool_call(system_prompt, messages, tools)
            else:
                return self._openai_tool_call(system_prompt, messages, tools)
        except Exception as e:
            logger.error(f"LLM tool call 失败: {e}")
            raise

    # ======================== OpenAI ========================

    def _openai_tool_call(self, system_prompt, messages, tools):
        openai_tools = []
        for t in tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t["description"],
                    "parameters": t["parameters"],
                },
            })

        all_messages = [{"role": "system", "content": system_prompt}] + messages

        resp = self.client.chat.completions.create(
            model=self.config.model,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
            messages=all_messages,
            tools=openai_tools if openai_tools else None,
        )

        msg = resp.choices[0].message
        result = {"content": msg.content or "", "tool_calls": []}

        if msg.tool_calls:
            for tc in msg.tool_calls:
                result["tool_calls"].append({
                    "id": tc.id,
                    "name": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                })

        return result

    # ======================== Anthropic ========================

    def _anthropic_tool_call(self, system_prompt, messages, tools):
        anthropic_tools = []
        for t in tools:
            anthropic_tools.append({
                "name": t["name"],
                "description": t["description"],
                "input_schema": t["parameters"],
            })

        resp = self.client.messages.create(
            model=self.config.model,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
            system=system_prompt,
            messages=messages,
            tools=anthropic_tools if anthropic_tools else None,
        )

        result = {"content": "", "tool_calls": []}

        for block in resp.content:
            if block.type == "text":
                result["content"] += block.text
            elif block.type == "tool_use":
                result["tool_calls"].append({
                    "id": block.id,
                    "name": block.name,
                    "arguments": block.input,
                })

        return result
