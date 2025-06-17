from typing import TypedDict, List, Dict
from langgraph.graph import StateGraph
from agents.agent_manager import agent_manager_node
from agents.agent_jira import agent_jira_node
from agents.agent_azure import run
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from threading import Thread

# ──────────────────────────────────────────────────────────────────────────
# ▼ UPDATED: make extra keys optional so we can start with just `log_path`
class AgentState(TypedDict, total=False):
    log_path: str
    ticket_actions: List[Dict]
    jira_results: List[Dict]
# ▲ UPDATED
# ──────────────────────────────────────────────────────────────────────────

# Initialize LangGraph
builder = StateGraph(AgentState)
builder.add_node("agent_manager", agent_manager_node)
builder.add_node("agent_jira", agent_jira_node)

# start AZ log listener agent
Thread(target=run).start()

# Define flow: agent_manager → agent_jira
builder.set_entry_point("agent_manager")
builder.add_edge("agent_manager", "agent_jira")

# Compile the graph so it can be invoked
graph = builder.compile()

# CLI usage
if __name__ == "__main__":
    # Manually set a file for testing
    log_file = "logs/entra_user_logs.csv"  # data source
    result = graph.invoke({
        "log_path": log_file
    })

    print("\n✅ Final Result:\n", result)
