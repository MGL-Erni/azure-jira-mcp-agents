import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from fastmcp import FastMCP
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()
mcp = FastMCP('agent_jira')

# Load credentials from .env
JIRA_URL = os.getenv("JIRA_URL")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
JIRA_PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY")


# ──────────────────────────────────────────────────────────────────────────────
# ▼ UPDATED: make the test & write-back tolerant of “false” / “no” / etc.
def update_csv_ticket_key(csv_path, user_email, operation, new_key):
    """Update the original CSV to add the new ticket key."""
    df = pd.read_csv(csv_path)

    # Normalise columns so comparisons work no matter the capitalisation/wording
    df["ticket_exists"] = df["ticket_exists"].astype(str).str.lower()
    df["ticket_key"] = df["ticket_key"].astype(str)

    falsy = {"false", "no", "0", ""}

    match = (
        (df["user_principal_name"] == user_email) &
        (df["operation"] == operation) &
        (df["ticket_exists"].isin(falsy))        # ← key change
    )

    df.loc[match, "ticket_key"] = new_key
    df.loc[match, "ticket_exists"] = "true"      # keep it boolean-ish

    df.to_csv(csv_path, index=False)
    print(f"✅ Updated CSV for user {user_email} with ticket {new_key}")
# ▲ UPDATED
# ──────────────────────────────────────────────────────────────────────────────

def create_jira_ticket(data, log_path):
    title = f"[{data['operation']}] - {data['user']}"
    description = f"""
                    Log Message: {data['message']}
                    Risk Level: {data['risk_level']}
                    Explanation: {data['explanation']}
                    """.strip()

    payload = {
        "fields": {
            "project": {"key": JIRA_PROJECT_KEY},
            "summary": title,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {"type": "text", "text": description}
                        ]
                    }
                ]
            },
            "issuetype": {
                "name": data.get("issue_type", "Task")  # use value from manager
            }
        }
    }

    url = f"{JIRA_URL}/rest/api/3/issue"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)

    response = requests.post(url, json=payload, headers=headers, auth=auth)

    if response.status_code == 201:
        ticket_key = response.json()["key"]
        print(f"🆕 Created Jira ticket: {ticket_key}")
        update_csv_ticket_key(log_path, data["user"], data["operation"], ticket_key)
        return {"status": "created", "ticket_key": ticket_key, "user": data["user"]}
    else:
        return None
        # print(f"❌ Failed to create Jira ticket: {response.status_code}")
        # print(response.text)
        # return {"status": "error", "user": data["user"]}


def update_jira_ticket(data):
    issue_key = data.get("ticket_key")
    if not issue_key:
        print(f"⚠️ No issue key to update for user {data['user']}")
        return {"status": "error", "reason": "missing_ticket_key", "user": data["user"]}

    new_description = f"""
                    [UPDATE]
                    Log Message: {data['message']}
                    Risk Level: {data['risk_level']}
                    Explanation: {data['explanation']}
                    """.strip()

    payload = {
        "fields": {
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {"type": "text", "text": new_description}
                        ]
                    }
                ]
            }
        }
    }

    url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)

    response = requests.put(url, json=payload, headers=headers, auth=auth)

    if response.status_code == 204:
        print(f"♻️ Updated Jira ticket: {issue_key}")
        return {"status": "updated", "ticket_key": issue_key}
    else:
        print(f"❌ Failed to update Jira ticket {issue_key}: {response.status_code}")
        print(response.text)
        return {"status": "error", "ticket_key": issue_key, "user": data["user"]}


def agent_jira_node(state):
    results = []
    log_path = state["log_path"]  # Needed to update the CSV

    for ticket in state.get("ticket_actions", []):
        if ticket["action"] == "update":
            result = update_jira_ticket(ticket)
        else:
            result = create_jira_ticket(ticket, log_path)
        results.append(result)

        time.sleep(5)

    return {"jira_results": results}
