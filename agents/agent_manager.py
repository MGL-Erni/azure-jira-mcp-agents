import pandas as pd
from fastmcp import FastMCP
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage
from dotenv import load_dotenv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()

mcp = FastMCP('agent_manager')
llm = ChatOpenAI(model="gpt-4", temperature=0)

def agent_manager_node(state):
    logs_df = pd.read_csv(state["log_path"])
    tickets = []

    for _, row in logs_df.iterrows():

        # ──────────────────────────────────────────────────────────────────────
        # ▼ UPDATED: normalise ticket_exists & pull new columns safely
        ticket_status = str(row.get("ticket_exists", "unknown")).strip().lower()
        target_ids        = row.get("target_resource_ids", "")
        target_principals = row.get("target_resource_principal_names", "")
        modified_props    = row.get("modified_properties", "")
        # ▲ UPDATED
        # ──────────────────────────────────────────────────────────────────────

        prompt = f"""
                Log Entry:
                Operation: {row['operation']}
                Result: {row['result']}
                User: {row['user_principal_name']}
                Message: {row['message']}
                Risk Level: {row['risk_level']}
                Ticket Already Exists: {ticket_status}

                Target Resource IDs: {target_ids}
                Target Principals: {target_principals}
                Modified Properties: {modified_props}

                1. Should this log trigger:
                - a NEW Jira ticket, or
                - an UPDATE to an existing ticket?

                2. What issue type should this be? Choose one of:
                - Story
                - Feature
                - Request
                - Bug

                Reply in this format:
                action: create or update
                type: Story or Feature or Request or Bug
                reason: <brief explanation>
                """

        response = llm.invoke([HumanMessage(content=prompt)]).content.strip().lower()

        # Default values
        action = "create"
        issue_type = "Task"
        explanation = ""

        # Parse LLM response
        lines = response.splitlines()
        for line in lines:
            if line.startswith("action:"):
                action = line.replace("action:", "").strip()
            elif line.startswith("type:"):
                issue_type = line.replace("type:", "").strip().capitalize()
            elif line.startswith("reason:"):
                explanation = line.replace("reason:", "").strip()

        # Validate against allowed Jira types
        valid_types = {"Story", "Feature", "Request", "Bug"}
        if issue_type not in valid_types:
            issue_type = "Task"  # fallback

        tickets.append({
            "user": row['user_principal_name'],
            "operation": row['operation'],
            "message": row['message'],
            "risk_level": row['risk_level'],
            "action": action,
            "issue_type": issue_type,
            "explanation": explanation,
            "ticket_key": str(row.get("ticket_key", "")).strip() if action == "update" else ""
        })

    return {
        "ticket_actions": tickets,
        "log_path": state["log_path"]
    }
