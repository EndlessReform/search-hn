"""Durable, renderer-free trajectory capture at model and tool boundaries.

Recording model inputs distinguishes a tool returning a target from a later
model request actually receiving it. Every line is flushed and fsynced. A killed
process can at worst leave a partial final line; earlier events remain readable.
"""

import json
import os
import uuid
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path

from agents import RunHooks
from pydantic import BaseModel


def json_default(value):
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Unsupported trajectory value: {type(value)}")


class Journal:
    """Append complete JSON events synchronously; one owner per file."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            self._preserve_partial_tail(path)
        self.file = path.open("a", encoding="utf-8")

    @staticmethod
    def _preserve_partial_tail(path: Path):
        """Preserve a torn append separately before resuming a JSONL journal.

        Walk backwards in bounded chunks, rather than loading the history. The
        tail is saved and fsynced before truncation; complete prior events remain
        byte-for-byte unchanged. This matters for resumed prompt generation.
        """
        with path.open("rb+") as stream:
            size = stream.seek(0, os.SEEK_END)
            if not size:
                return
            stream.seek(-1, os.SEEK_END)
            if stream.read(1) == b"\n":
                return
            position = size
            while position:
                start = max(0, position - 65536)
                stream.seek(start)
                block = stream.read(position - start)
                newline = block.rfind(b"\n")
                if newline >= 0:
                    position = start + newline + 1
                    break
                position = start
            stream.seek(position)
            partial = path.with_name(path.name + "." + uuid.uuid4().hex + ".partial")
            with partial.open("xb") as saved:
                while chunk := stream.read(65536):
                    saved.write(chunk)
                saved.flush()
                os.fsync(saved.fileno())
            stream.truncate(position)
            stream.flush()
            os.fsync(stream.fileno())

    def write(self, event: str, **fields):
        self.file.write(
            json.dumps(
                {"event": event, "at": datetime.now(UTC).isoformat(), **fields},
                default=json_default,
                ensure_ascii=False,
            )
            + "\n"
        )
        self.file.flush()
        os.fsync(self.file.fileno())

    def close(self):
        self.file.close()


class JournalHooks(RunHooks):
    """Capture complete model inputs/outputs and tool outputs without UI imports."""

    def __init__(self, journal: Journal):
        self.journal = journal
        self.request = 0

    async def on_llm_start(self, context, agent, system_prompt, input_items):
        self.request += 1
        self.journal.write(
            "model_input",
            request=self.request,
            system_prompt=system_prompt,
            items=input_items,
        )

    async def on_llm_end(self, context, agent, response):
        self.journal.write(
            "model_output",
            request=self.request,
            items=response.output,
            usage=response.usage,
            response_id=response.response_id,
        )

    async def on_tool_end(self, context, agent, tool, result):
        self.journal.write(
            "tool_output", request=self.request, tool=tool.name, output=result
        )
