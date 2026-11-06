from pathlib import Path

import dagster as dg

from dagster_agent_demo.resources import GrokResource


defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        resources={
            "grok": GrokResource(api_key=dg.EnvVar("XAI_API_KEY")),
        }
    ),
)
