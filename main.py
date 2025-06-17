import pandas as pd
import os
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from agent_runner import graph

LOG_PATH = "logs/entra_user_logs.csv"

def process_new_logs():

    df = pd.read_csv(LOG_PATH)

    # ──────────────────────────────────────────────────────────────────────
    # ▼ UPDATED: handle “processed” values that could be yes/no/true/false
    if "processed" not in df.columns:           # safety-net for fresh files
        df["processed"] = "false"

    falsy = {"false", "no", "0", ""}

    new_logs = df[df["processed"].astype(str).str.lower().isin(falsy)]
    # ▲ UPDATED
    # ──────────────────────────────────────────────────────────────────────

    if new_logs.empty:
        print("⏳ No new logs to process.")
        return

    print(f"📥 Found {len(new_logs)} new logs... Processing...")

    # Save only new logs temporarily
    temp_path = "temp_logs_to_process.csv"
    new_logs.to_csv(temp_path, index=False)

    # Run your agents
    result = graph.invoke({"log_path": temp_path})

    # ──────────────────────────────────────────────────────────────────────
    # ▼ UPDATED: mark as “true” instead of “yes” so it matches the new style
    df.loc[new_logs.index, "processed"] = "true"
    # ▲ UPDATED
    # ──────────────────────────────────────────────────────────────────────

    df.to_csv(LOG_PATH, index=False)

    print("✅ Processed logs and updated CSV.\n")


if os.path.exists(LOG_PATH):
    os.remove(LOG_PATH)
with open(LOG_PATH, 'w') as f:
    f.write('timestamp,operation,result,user_principal_name,message,risk_level,ticket_exists,ticket_key,processed,target_resource_ids,target_resource_principal_names,modified_properties')


if __name__ == "__main__":
    print("🔁 Watching for new logs in:", LOG_PATH)
    try:
        while True:
            process_new_logs()
            time.sleep(10)  # check every 10 seconds
    except KeyboardInterrupt:
        print("🛑 Monitoring stopped by user.")
