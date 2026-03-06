"""POST /api/chat — proxy requests to the MAS serving endpoint."""

import os
import uuid
import logging
import aiohttp

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import get_serving_credentials

logger = logging.getLogger(__name__)
router = APIRouter()

# In-memory conversation history keyed by conversation_id
_conversations: dict[str, list[dict]] = {}

MAS_ENDPOINT = os.getenv("MAS_ENDPOINT_NAME", "mas-c69b95e4-endpoint")


def _extract_message(body: dict) -> str:
    """Extract the assistant's text from various MAS response formats."""
    output = body.get("output", body)

    # Case 1: output is a plain string
    if isinstance(output, str):
        return output

    # Case 2: output is a list of message objects
    # e.g. [{"type": "message", "content": "..."}, ...] or
    #      [{"role": "assistant", "content": "..."}]
    if isinstance(output, list):
        for msg in reversed(output):
            if isinstance(msg, dict):
                content = msg.get("content", "")
                if isinstance(content, str) and content:
                    return content
                # content might itself be a list of text blocks
                if isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict):
                            parts.append(block.get("text", str(block)))
                        elif isinstance(block, str):
                            parts.append(block)
                    if parts:
                        return "\n".join(parts)
        # Fallback: stringify the list
        return str(output)

    # Case 3: output is a dict with choices (OpenAI format)
    if isinstance(output, dict) and "choices" in output:
        return output["choices"][0]["message"]["content"]

    # Fallback
    return str(output)


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


def _extract_steps(body: dict) -> list[dict]:
    """Extract intermediate tool calls and thinking from the MAS response."""
    output = body.get("output", body)
    if not isinstance(output, list) or len(output) < 2:
        return []

    steps = []
    # Everything except the last item is intermediate
    for item in output[:-1]:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type", "")

        if item_type == "function_call":
            steps.append({
                "type": "tool_call",
                "name": item.get("name", "unknown"),
                "arguments": item.get("arguments", ""),
            })
        elif item_type == "function_result":
            steps.append({
                "type": "tool_result",
                "name": item.get("name", ""),
                "output": item.get("output", ""),
            })
        elif item_type == "message" or item.get("role"):
            # Intermediate assistant/system message → treat as thinking
            content = item.get("content", "")
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict):
                        parts.append(block.get("text", str(block)))
                    elif isinstance(block, str):
                        parts.append(block)
                content = "\n".join(parts)
            if content:
                steps.append({"type": "thinking", "content": str(content)})
    return steps


class ChatResponse(BaseModel):
    response: str
    conversation_id: str
    steps: list[dict] = []


@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    conversation_id = req.conversation_id or str(uuid.uuid4())

    if conversation_id not in _conversations:
        _conversations[conversation_id] = []

    history = _conversations[conversation_id]
    history.append({"role": "user", "content": req.message})

    try:
        host, token = get_serving_credentials()
        if not token:
            raise ValueError("No authentication token available")

        url = f"{host}/serving-endpoints/{MAS_ENDPOINT}/invocations"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {"input": history}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                body = await resp.json()
                logger.info(f"MAS response status={resp.status} keys={list(body.keys()) if isinstance(body, dict) else type(body)}")

                if resp.status != 200:
                    error_msg = body.get("message", body.get("error", str(body)))
                    raise ValueError(f"MAS returned {resp.status}: {error_msg}")

                # Extract assistant message and intermediate steps
                assistant_message = _extract_message(body)
                steps = _extract_steps(body)

        history.append({"role": "assistant", "content": assistant_message})
        return ChatResponse(response=assistant_message, conversation_id=conversation_id, steps=steps)

    except Exception as e:
        logger.exception(f"MAS endpoint error: {e}")
        if history and history[-1]["role"] == "user":
            history.pop()
        raise HTTPException(status_code=502, detail=f"MAS endpoint error: {e}")
