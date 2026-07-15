"""
pipeline_agent.py
──────────────────
Agentic pipeline runner using LangChain + Gemini 1.5 Flash (free).
Works for both Snowflake and Databricks — pass --warehouse.

Usage:
  python agent/pipeline_agent.py --warehouse snowflake --month 2024-01
  python agent/pipeline_agent.py --warehouse databricks --month 2024-01
  python agent/pipeline_agent.py --warehouse snowflake --dry-run
"""

import os
import sys
import subprocess
import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

for d in [Path(__file__).parent, Path(__file__).parent.parent]:
    if (d / ".env").exists():
        load_dotenv(d / ".env")
        break

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_llm():
    # OPTION 1: Gemini 1.5 Flash — FREE
    from langchain_google_genai import ChatGoogleGenerativeAI
    return ChatGoogleGenerativeAI(
        model="gemini-1.5-flash",
        temperature=0,
        google_api_key=os.environ["GEMINI_API_KEY"],
    )
    # OPTION 2: Ollama — uncomment for fully offline
    # from langchain_ollama import ChatOllama
    # return ChatOllama(model="llama3", temperature=0)
    # OPTION 3: Claude Haiku — uncomment for best quality
    # from langchain_anthropic import ChatAnthropic
    # return ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0,
    #     api_key=os.environ["ANTHROPIC_API_KEY"])


def run_shell(cmd, cwd=None):
    log.info(f"$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd or str(PROJECT_ROOT))
    return {
        "stdout": result.stdout[-3000:] if result.stdout else "",
        "stderr": result.stderr[-1500:] if result.stderr else "",
        "returncode": result.returncode,
        "success": result.returncode == 0,
    }


def build_tools():
    dbt_dir = str(PROJECT_ROOT / "dbt")
    script_dir = str(PROJECT_ROOT / "scripts")

    return {
        "verify_versions": {
            "description": "Confirm dbt-core version matches the pinned requirements (catches Fusion-vs-Core drift)",
            "parameters": {},
            "fn": lambda: run_shell(f"python {script_dir}/verify_versions.py"),
        },
        "download_and_load": {
            "description": "Download TLC parquet for a month and load into the warehouse",
            "parameters": {"month": "YYYY-MM string"},
            "fn": None,  # set in run_agent() based on --warehouse
        },
        "dbt_run": {
            "description": "Run all dbt models",
            "parameters": {"select": "optional model/tag, default 'all'"},
            "fn": lambda select="all": run_shell(
                f"cd {dbt_dir} && dbt run --profiles-dir ." + (f" --select {select}" if select != "all" else "")
            ),
        },
        "dbt_test": {
            "description": "Run dbt data quality tests",
            "parameters": {},
            "fn": lambda: run_shell(f"cd {dbt_dir} && dbt test --profiles-dir ."),
        },
        "dbt_snapshot": {
            "description": "Run dbt snapshots (SCD Type 2 zone tracking)",
            "parameters": {},
            "fn": lambda: run_shell(f"cd {dbt_dir} && dbt snapshot --profiles-dir ."),
        },
        "dbt_docs": {
            "description": "Generate dbt lineage documentation",
            "parameters": {},
            "fn": lambda: run_shell(f"cd {dbt_dir} && dbt docs generate --profiles-dir ."),
        },
    }


SYSTEM_PROMPT = """You are a data pipeline orchestration agent for the NYC TLC taxi pipeline.
Warehouse: {warehouse}

Available tools: {tools}

Always call verify_versions FIRST before anything else — if dbt versions
don't match the pinned requirements, stop and report it rather than
proceeding, since results would be unreliable.

Pipeline order after version check:
1. download_and_load
2. dbt_run
3. dbt_test
4. dbt_snapshot
5. dbt_docs

Step filter: {step}
Dry run: {dry_run}

Respond ONLY with valid JSON:
  {{"action": "tool_name", "parameters": {{...}}}}
  {{"action": "done", "summary": "..."}}"""


def run_agent(warehouse, month, step="all", dry_run=False):
    llm = get_llm()
    tools = build_tools()
    # Fix the loader command for the specific warehouse
    loader_script = "load_to_snowflake.py" if warehouse == "snowflake" else "load_to_databricks.py"
    tools["download_and_load"]["fn"] = lambda month: run_shell(
        f"python {PROJECT_ROOT}/scripts/{loader_script} --month {month}"
    )

    tools_desc = json.dumps({n: {"description": t["description"], "parameters": t["parameters"]} for n, t in tools.items()}, indent=2)
    task = f"Run the TLC pipeline.\nMonth: {month}\nStep: {step}\nDry run: {dry_run}\nTime: {datetime.utcnow().isoformat()}"

    from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
    system = SYSTEM_PROMPT.format(warehouse=warehouse, tools=tools_desc, step=step, dry_run=dry_run)
    history = [SystemMessage(content=system), HumanMessage(content=task)]
    results = []

    log.info(f"Agent starting — warehouse={warehouse}, month={month}")

    for i in range(14):
        response = llm.invoke(history)
        content = response.content.strip()
        if content.startswith("```"):
            content = "\n".join(l for l in content.split("\n") if not l.strip().startswith("```")).strip()
        try:
            decision = json.loads(content)
        except json.JSONDecodeError:
            history.append(AIMessage(content=content))
            history.append(HumanMessage(content="Respond only with valid JSON."))
            continue

        history.append(AIMessage(content=json.dumps(decision)))

        if decision.get("action") == "done":
            summary = decision.get("summary", "Pipeline complete.")
            print(f"\n{'='*60}\nAGENT SUMMARY — {warehouse.upper()}\n{'='*60}\n{summary}\n{'='*60}")
            return {"status": "success", "summary": summary, "log": results}

        tool_name = decision.get("action")
        params = decision.get("parameters", {})
        if tool_name not in tools:
            history.append(HumanMessage(content=f"Unknown tool '{tool_name}'. Available: {list(tools.keys())}"))
            continue

        if dry_run:
            result = {"stdout": f"[DRY RUN] {tool_name}({params})", "success": True, "returncode": 0, "stderr": ""}
        else:
            result = tools[tool_name]["fn"](**params)

        results.append({"tool": tool_name, "params": params, "result": result})
        log.info(f"  -> {'OK' if result['success'] else 'FAILED'}")

        feedback = json.dumps({"tool": tool_name, "success": result["success"], "stdout": result["stdout"][:2000], "stderr": result["stderr"][:500]})
        history.append(HumanMessage(content=f"Tool result: {feedback}"))

    return {"status": "incomplete", "log": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", choices=["snowflake", "databricks"], required=True)
    parser.add_argument("--month", type=str, default="2024-03")
    parser.add_argument("--step", type=str, default="all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_agent(args.warehouse, args.month, args.step, args.dry_run)
