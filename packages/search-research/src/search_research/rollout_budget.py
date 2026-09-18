"""Durable shared spending guard for streaming OpenRouter experiments.

Reserve before dispatch, replace only after a billed completion, and keep unknown
failed charges. Disabling SDK retries makes every possibly billed attempt visible.
A depleted ledger pauses dispatch rather than cancelling unrelated conversations.
"""

import json
import uuid

from agents.models.interface import Model, ModelProvider
from openai import APIStatusError
from search_agent.journal import Journal, JournalHooks

from search_research.dataset import read_jsonl


class BudgetPaused(Exception):
    """This case is resumable after an explicit increase to the spending ceiling."""


class Budget:
    def __init__(self, path, ceiling=8.0):
        assert ceiling > 0
        self.ceiling = ceiling
        self.journal = Journal(path)  # Repair a torn append before reading it.
        self.charges = {}
        self.paused = None
        for row in read_jsonl(path):
            if row["event"] == "charge":
                self.charges[row["id"]] = row
        self.journal.write("run_budget", ceiling_usd=ceiling)

    @property
    def total(self):
        return sum(r["usd"] for r in self.charges.values())

    def pause(self, reason):
        if self.paused is None:
            self.paused = reason
            self.journal.write("paused", reason=reason)

    def reserve(self, bound, case):
        # One UTF-8 byte per token is an upper bound for this text-only payload.
        # $0.25/M also covers cache-write premiums; output includes reasoning.
        amount = (bound * 0.25 + 4096 * 1.20) / 1e6
        if self.paused:
            raise BudgetPaused(self.paused)
        if self.total + amount > self.ceiling:
            self.pause(
                "Run spending ceiling reached; increase --budget-usd after recharge"
            )
            raise BudgetPaused(self.paused)
        id = uuid.uuid4().hex
        self.set(id, amount, "reserved", case)
        return id

    def set(self, id, usd, kind, case):
        assert usd >= 0
        row = {"id": id, "usd": usd, "kind": kind, "case": case}
        self.journal.write("charge", **row)
        self.charges[id] = row
        if self.total > self.ceiling:
            self.pause(
                "Reported usage exceeded conservative reservation; inspect before resume"
            )

    def summary(self):
        return {
            "guard_usd": self.ceiling,
            "accounted_usd": self.total,
            "reported_usd": sum(
                r["usd"] for r in self.charges.values() if r["kind"] == "reported"
            ),
            "unknown_reserved_usd": sum(
                r["usd"] for r in self.charges.values() if r["kind"] == "reserved"
            ),
            "paused": self.paused,
        }


class MeteredHooks(JournalHooks):
    """Reserve independently for each turn without changing model-visible text."""

    def __init__(self, journal, budget, case):
        super().__init__(journal)
        self.budget, self.case = budget, case
        self.id = None

    async def on_llm_start(self, context, agent, system_prompt, input_items):
        bound = (
            len(json.dumps([system_prompt, input_items], ensure_ascii=False).encode())
            + 16384
        )
        self.id = self.budget.reserve(bound, self.case)
        await super().on_llm_start(context, agent, system_prompt, input_items)

    async def on_llm_end(self, context, agent, response):
        assert self.id is not None
        if self.budget.charges[self.id]["kind"] == "reserved":
            usage = response.usage
            usd = (
                (usage.input_tokens - usage.input_tokens_details.cached_tokens) * 0.20
                + usage.input_tokens_details.cached_tokens * 0.02
                + usage.input_tokens_details.cache_write_tokens * 0.05
                + usage.output_tokens * 1.20
            ) / 1e6
            self.budget.set(self.id, usd, "estimated", self.case)
        await super().on_llm_end(context, agent, response)


class BilledModel(Model):
    """Record OpenRouter's actual streamed cost before SDK usage drops extras."""

    def __init__(self, model, hooks):
        self.model, self.hooks = model, hooks

    async def get_response(self, *args, **kwargs):
        raise AssertionError("This experiment requires streaming")

    async def stream_response(self, *args, **kwargs):
        try:
            async for event in self.model.stream_response(*args, **kwargs):
                if (
                    event.type == "response.completed"
                    and event.response.usage is not None
                ):
                    usage = event.response.usage.model_dump()
                    if usage.get("cost") is not None:
                        assert self.hooks.id is not None
                        self.hooks.budget.set(
                            self.hooks.id,
                            float(usage["cost"]),
                            "reported",
                            self.hooks.case,
                        )
                        self.hooks.journal.write(
                            "billing", response_id=event.response.id, usage=usage
                        )
                yield event
        except APIStatusError as exc:
            if exc.status_code in (401, 402, 403):
                self.hooks.budget.pause(
                    f"OpenRouter HTTP {exc.status_code}: check credit/access before resuming"
                )
            raise


class BilledProvider(ModelProvider):
    def __init__(self, provider, hooks):
        self.provider, self.hooks = provider, hooks

    def get_model(self, model_name):
        return BilledModel(self.provider.get_model(model_name), self.hooks)
