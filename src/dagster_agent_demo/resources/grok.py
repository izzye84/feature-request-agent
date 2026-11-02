"""
GrokResource — a Dagster resource for the xAI Grok API
======================================================

There is no published ``dagster-xai`` library, so this resource mirrors the
design of ``dagster_anthropic.AnthropicResource``:

  * a ``ConfigurableResource`` holding the API key / base URL
  * ``get_client(context)`` / ``get_client_for_asset(context, asset_key)``
    context managers that yield a ready-to-use client
  * automatic token-usage logging to asset materialization metadata

xAI exposes an OpenAI-compatible API (https://docs.x.ai/overview), so the
yielded client is an ``openai.OpenAI`` instance pointed at ``api.x.ai``.
Usage is logged by wrapping ``client.chat.completions.create``.
"""

from collections import defaultdict
from collections.abc import Generator
from contextlib import contextmanager
from functools import wraps
from weakref import WeakKeyDictionary

import dagster as dg
from openai import OpenAI
from pydantic import Field, PrivateAttr

XAI_BASE_URL = "https://api.x.ai/v1"

# Running per-context totals, so multiple calls within one asset accumulate
# into a single set of metadata values rather than overwriting each other.
_context_to_counters: WeakKeyDictionary = WeakKeyDictionary()


def _add_to_asset_metadata(
    context: dg.AssetExecutionContext, usage_metadata: dict, output_name: str | None
) -> None:
    if context not in _context_to_counters:
        _context_to_counters[context] = defaultdict(int)
    counters = _context_to_counters[context]

    for metadata_key, delta in usage_metadata.items():
        counters[metadata_key] += delta
    context.add_output_metadata(dict(counters), output_name)


def _with_usage_metadata(
    context: dg.AssetExecutionContext, output_name: str | None, func
):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        usage = response.usage
        if usage is None:
            return response

        cached = 0
        if usage.prompt_tokens_details is not None:
            cached = usage.prompt_tokens_details.cached_tokens or 0
        reasoning = 0
        if usage.completion_tokens_details is not None:
            reasoning = usage.completion_tokens_details.reasoning_tokens or 0

        usage_metadata = {
            "grok.calls": 1,
            "grok.prompt_tokens": usage.prompt_tokens,
            "grok.cached_prompt_tokens": cached,
            "grok.completion_tokens": usage.completion_tokens,
            "grok.reasoning_tokens": reasoning,
            "grok.total_tokens": usage.total_tokens,
        }
        _add_to_asset_metadata(context, usage_metadata, output_name)
        return response

    return wrapper


class GrokResource(dg.ConfigurableResource):
    """Wrapper over an OpenAI-compatible client pointed at the xAI Grok API.

    Usage of ``chat.completions.create`` (call count and tokens) is logged to
    asset metadata when the client is obtained from an asset context. Usage is
    not logged from op contexts, nor for streaming responses.

    Examples:
        .. code-block:: python

            @dg.asset(kinds={"grok"})
            def grok_asset(context: dg.AssetExecutionContext, grok: GrokResource):
                with grok.get_client(context) as client:
                    response = client.chat.completions.create(
                        model="grok-4.7",
                        messages=[{"role": "user", "content": "Write a sentence on cats"}],
                    )

            defs = dg.Definitions(
                assets=[grok_asset],
                resources={"grok": GrokResource(api_key=dg.EnvVar("XAI_API_KEY"))},
            )
    """

    api_key: str = Field(
        description="xAI API key. Create one at https://console.x.ai"
    )
    base_url: str = Field(
        default=XAI_BASE_URL,
        description="Base URL of the xAI API.",
    )

    _client: OpenAI = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._client = OpenAI(api_key=self.api_key, base_url=self.base_url)

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        self._client.close()

    @contextmanager
    def get_client(
        self, context: dg.AssetExecutionContext | dg.OpExecutionContext
    ) -> Generator[OpenAI, None, None]:
        """Yields an ``openai.OpenAI`` client configured for the xAI API.

        In an asset context, usage is logged to that asset's materialization.
        """
        yield from self._get_client(context=context, asset_key=None)

    @contextmanager
    def get_client_for_asset(
        self, context: dg.AssetExecutionContext, asset_key: dg.AssetKey
    ) -> Generator[OpenAI, None, None]:
        """Like ``get_client``, but logs usage to the given asset of a multi-asset."""
        yield from self._get_client(context=context, asset_key=asset_key)

    def _get_client(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        asset_key: dg.AssetKey | None,
    ) -> Generator[OpenAI, None, None]:
        if not isinstance(context, dg.AssetExecutionContext):
            yield self._client
            return

        if asset_key is None:
            if len(context.assets_def.keys_by_output_name) > 1:
                raise dg.DagsterInvariantViolationError(
                    "The argument `asset_key` must be specified for multi_asset "
                    "with more than one asset."
                )
            asset_key = context.asset_key
        output_name = context.output_for_asset_key(asset_key)

        # Unlike dagster_anthropic, restore the original method on exit so
        # repeated get_client calls don't stack wrappers and double-count usage.
        completions = self._client.chat.completions
        original_create = completions.create
        completions.create = _with_usage_metadata(context, output_name, original_create)
        try:
            yield self._client
        finally:
            completions.create = original_create
