#!/usr/bin/env python3
"""
Pull the latest N *UserManagement* audit events, deterministically extract the
columns we care about, score each event, and append to audit_log.csv.

CSV header
----------
timestamp,operation,result,user_principal_name,message,risk_level,
ticket_exists,ticket_key,processed,
target_resource_ids,target_resource_principal_names,modified_properties
"""
# ─── imports ───────────────────────────────────────────────────────
import os, csv, json, argparse, asyncio, aiohttp, pathlib, threading, re
from fastmcp import FastMCP
from typing import List, Dict, Any
from dotenv import load_dotenv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()
mcp = FastMCP('agent_azure')
TENANT   = os.environ["GRAPH_TENANT"]
APP_ID   = os.environ["GRAPH_CLIENT_ID"]
SECRET   = os.environ["GRAPH_CLIENT_SECRET"]
CSV_PATH = "logs/entra_user_logs.csv"
CSV_LOCK = threading.Lock()

# ─── risk scoring (rules) ─────────────────────────────────────────
def risk(evt: Dict[str,Any]) -> str:
    if evt["result"].lower() == "failure":
        return "Critical"
    if evt["operationType"] in ("Add", "Delete") or "Password" in evt["activityDisplayName"]:
        return "High"
    return "Low"

# ─── deterministic field extractors ───────────────────────────────
_brackets = re.compile(r'^[\[\"]+|[\]\"]+$')

def _strip(val: str) -> str:
    """Remove leading/ending brackets & quotes from Graph stringified arrays."""
    return _brackets.sub("", val or "")

def extract_row(evt: Dict[str,Any]) -> Dict[str,str]:
    ts   = evt["activityDateTime"]
    op   = evt["activityDisplayName"]
    res  = evt["result"]

    initiator = evt["initiatedBy"]
    upn = (
        (initiator.get("user") or {}).get("userPrincipalName")
        or (initiator.get("app") or {}).get("displayName")
        or ""
    )

    # targetResources block
    ids, names, mprops = [], [], []
    for tr in evt.get("targetResources", []):
        ids.append(tr.get("id", ""))
        if tr.get("userPrincipalName"):
            names.append(tr["userPrincipalName"])
        for mp in tr.get("modifiedProperties", []):
            disp = mp.get("displayName", "")
            new  = _strip(mp.get("newValue", ""))
            mprops.append(f"{disp}={new}")

    return {
        "timestamp": ts,
        "operation": op,
        "result":    res,
        "user_principal_name": upn,
        "message": f'{evt["operationType"]} corrId={evt["correlationId"]}',
        "risk_level": risk(evt),
        "ticket_exists": "False",
        "ticket_key":    "",
        "processed":     "False",
        "target_resource_ids":             "|".join(ids),
        "target_resource_principal_names": "|".join(names),
        "modified_properties":             "|".join(mprops),
    }

# ─── Graph helpers ────────────────────────────────────────────────
async def token(sess:aiohttp.ClientSession)->str:
    url = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"
    form = {"grant_type":"client_credentials","client_id":APP_ID,
            "client_secret":SECRET,"scope":"https://graph.microsoft.com/.default"}
    async with sess.post(url,data=form) as r:
        r.raise_for_status()
        return (await r.json())["access_token"]

async def fetch(sess:aiohttp.ClientSession, tkn:str, top:int)->List[Dict[str,Any]]:
    url = ("https://graph.microsoft.com/v1.0/auditLogs/directoryAudits"
           "?$filter=category eq 'UserManagement'"
           f"&$orderby=activityDateTime desc&$top={top}")
    hdr = {"Authorization":f"Bearer {tkn}"}
    items=[]
    while url and len(items)<top:
        async with sess.get(url,headers=hdr) as r:
            r.raise_for_status()
            j=await r.json()
            items.extend(j.get("value",[]))
            url=j.get("@odata.nextLink")
    return items[:top]

# ─── CSV writer (safe) ────────────────────────────────────────────
def append(rows: List[Dict[str,str]]):
    with CSV_LOCK:
        path = pathlib.Path(CSV_PATH)
        new_file = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if new_file:
                w.writeheader()
            w.writerows(rows)

# ─── core async logic ─────────────────────────────────────────────
async def _run_async(top:int = 10):
    """Core async logic to fetch, transform, and persist audit events."""
    async with aiohttp.ClientSession() as sess:
        tkn   = await token(sess)
        evts  = await fetch(sess, tkn, top)
        print(f"Fetched {len(evts)} UserManagement events")

        rows = [extract_row(e) for e in evts]
        append(rows)
        print(f"Appended {len(rows)} rows to {CSV_PATH}")

# ─── public synchronous entrypoint ────────────────────────────────
def run(top:int = 10):
    """Public entrypoint so other modules can simply `Thread(target=run).start()`."""
    asyncio.run(_run_async(top))

# ─── CLI usage ────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch Azure AD UserManagement audit events and append them to a CSV.")
    ap.add_argument("--top", type=int, default=10, help="Number of latest events to fetch (default: 10)")
    args = ap.parse_args()

    # Re‑use the same synchronous entrypoint so behaviour is identical
    run(args.top)
