from pathlib import Path

import dagster as dg
from dagster_anthropic import AnthropicResource


defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        resources={
            "anthropic": AnthropicResource(api_key=dg.EnvVar("ANTHROPIC_API_KEY")),
        }
    ),
)
