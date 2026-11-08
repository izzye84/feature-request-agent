from pathlib import Path

import dagster as dg

from dagster_agent_demo.resources import CursorResource, GrokResource

REPO = "izzye84/feature-request-agent"

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        resources={
            "grok": GrokResource(api_key=dg.EnvVar("XAI_API_KEY")),
            "cursor": CursorResource(
                api_key=dg.EnvVar("CURSOR_API_KEY"),
                repo_url=f"https://github.com/{REPO}",
            ),
        }
    ),
)
