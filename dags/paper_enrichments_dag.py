import json
import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gemma4:26b")
PROMPT_VERSION = "v1"

PROMPT = """You are extracting structured metadata from a research abstract.
Respond ONLY a JSON object, now markdown fences, with exactly these keys:
"summary": one-sentence summary (string),
"methods": list of method/technique names (list of strings),
"datasets": list of dataset names mentioned (list of strings), [] if none (list of strings),
"topics": list of 2-4 topics (list of strings).

Title: {title}

Abstract: {abstract}
"""

