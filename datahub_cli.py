import os
import sys
import json
import argparse
import requests
from pathlib import Path

# Core Configuration
DEFAULT_API_URL = "http://localhost:8000"
TOKEN_FILE = Path.home() / ".datahub_token"

def get_api_url():
    return os.getenv("DATAHUB_URL", DEFAULT_API_URL)

def get_token():
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    return None

def save_token(token):
    TOKEN_FILE.write_text(token)

def auth_headers():
    token = get_token()
    if not token:
        print("❌ Error: Not authenticated. Please run 'python datahub_cli.py login' first.")
        sys.exit(1)
    return {"Authorization": f"Bearer {token}"}

# ============================================================
# COMMAND EXECUTORS
# ============================================================

def handle_login(args):
    """Authenticate and store JWT token locally"""
    url = f"{get_api_url()}/auth/login"
    try:
        data = {"username": args.username, "password": args.password}
        res = requests.post(url, data=data) # Form-encoded for OAuth2PasswordRequestForm
        if res.status_code == 200:
            token = res.json().get("access_token")
            save_token(token)
            print(f"✅ Successfully logged in as {args.username}! Token saved.")
        else:
            print(f"❌ Login failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_push(args):
    """Module 3 & 4: Push a file via Multipart streaming, triggers Module 5 formatting"""
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    url = f"{get_api_url()}/metadata/upload-and-commit"
    headers = auth_headers()
    
    print(f"🚀 Pushing {file_path.name} to branch '{args.branch}' of project '{args.project}'...")

    try:
        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f)}
            data = {
                "project_name": args.project,
                "message": args.message,
                "branch": args.branch
            }
            if args.metrics:
                data["custom_metrics"] = args.metrics

            res = requests.post(url, headers=headers, files=files, data=data)

        if res.status_code == 200:
            result = res.json()
            print("✅ Commit Successful!")
            print(f"   Commit Hash: {result.get('commit_hash')}")
            print(f"   Deduplicated (CAS): {result.get('is_duplicate_blob')}")
            stats = result.get("metadata", {})
            print(f"   Rows: {stats.get('row_count')} | Cols: {stats.get('column_count')}")
            if stats.get('ai_summary'):
                print(f"   AI Summary: {stats.get('ai_summary')}")
        else:
            print(f"❌ Push failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_log(args):
    """Module 6: Query Engine & Graph traversal"""
    url = f"{get_api_url()}/projects/{args.project}/log"
    headers = auth_headers()
    if args.metric:
        url += f"?metric_filter={args.metric}"

    print(f"🔍 Fetching commit log for '{args.project}'...")
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            commits = res.json()
            if not commits:
                print("No commits found.")
                return
            for c in commits:
                print(f"\ncommit {c['commit_hash']}")
                print(f"Author:     {c.get('author', 'Unknown')}")
                print(f"Date:       {c['created_at']}")
                print(f"Parent:     {c.get('parent_hash', 'Root')}")
                print(f"\n    {c['message']}\n")
                for m in c.get("metadata", []):
                    print(f"    [Blob] {m['file_name']} ({m['row_count']} rows x {m['column_count']} cols)")
                    if m.get("custom_metrics"):
                        print(f"    [Metrics] {m['custom_metrics']}")
        else:
            print(f"❌ Log fetch failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_init(args):
    """Initialize a dummy configuration"""
    print("✅ Initialized empty DataHub repository locally.")
    print(f"Target API set to: {get_api_url()}")

# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="DataHub Distributed Version Control CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: login
    parser_login = subparsers.add_parser("login", help="Authenticate with the DataHub backend")
    parser_login.add_argument("username", help="Your DataHub username")
    parser_login.add_argument("password", help="Your DataHub password")

    # Command: init
    parser_init = subparsers.add_parser("init", help="Initialize local DataHub mapping")

    # Command: push
    parser_push = subparsers.add_parser("push", help="Push dataset and trigger metadata extraction natively")
    parser_push.add_argument("file", help="Path to the dataset (CSV, JSON, Parquet)")
    parser_push.add_argument("project", help="Destination project repository")
    parser_push.add_argument("-m", "--message", required=True, help="Commit message")
    parser_push.add_argument("-b", "--branch", default="main", help="Branch target (default: main)")
    parser_push.add_argument("--metrics", help="JSON string representing model metrics dict e.g., '{\"accuracy\": 0.95}'")

    # Command: log
    parser_log = subparsers.add_parser("log", help="Display DAG commit history and query metrics")
    parser_log.add_argument("project", help="Project repository name")
    parser_log.add_argument("--metric", help="Dynamic SQL condition filter (e.g., 'accuracy > 0.9')")

    args = parser.parse_args()

    if args.command == "login":
        handle_login(args)
    elif args.command == "init":
        handle_init(args)
    elif args.command == "push":
        handle_push(args)
    elif args.command == "log":
        handle_log(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
