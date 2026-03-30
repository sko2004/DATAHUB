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

def output_result(args, result, success_msg=None):
    """Module 3: --json output support"""
    if hasattr(args, 'json') and args.json:
        print(json.dumps(result, indent=2))
    else:
        if success_msg:
            print(success_msg)
        elif isinstance(result, dict) and "message" in result:
            print(f"ℹ️ {result['message']}")
        else:
            # Fallback print for complex objects can be handled by individual handlers
            pass

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
            output_result(args, res.json(), f"✅ Successfully logged in as {args.username}! Token saved.")
        else:
            print(f"❌ Login failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_logout(args):
    """Remove locally stored authentication token"""
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        print("✅ Logged out successfully.")
    else:
        print("ℹ️ Already logged out.")

def handle_whoami(args):
    """Check current authentication status"""
    url = f"{get_api_url()}/auth/me"
    try:
        res = requests.get(url, headers=auth_headers())
        if res.status_code == 200:
            user = res.json()
            print(f"👤 Authenticated as: {user['username']} ({user['role']})")
            print(f"📧 Email: {user['email']}")
        else:
            print(f"❌ Session invalid or expired. Please login again.")
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
                import json
                try:
                    # Validate JSON metrics before sending
                    metrics_data = json.loads(args.metrics)
                    data["custom_metrics"] = json.dumps(metrics_data)
                except json.JSONDecodeError:
                    print("❌ Error: --metrics must be a valid JSON string (e.g., '{\"accuracy\": 0.95}')")
                    return

            res = requests.post(url, headers=headers, files=files, data=data)

        if res.status_code in (200, 202):
            result = res.json()
            print("✅ Commit Accepted! (Metadata indexing running in background)")
            print(f"   Commit Hash : {result.get('commit_hash')}")
            print(f"   Project     : {result.get('project')}")
            print(f"   Branch      : {result.get('branch')}")
            print(f"   Deduplicated: {result.get('is_duplicate_blob')}")
        else:
            print(f"❌ Push failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_pull(args):
    """Module 3: Pull/Download a file by its Metadata ID (or version)"""
    url = f"{get_api_url()}/metadata/{args.metadata_id}/download"
    headers = auth_headers()
    
    print(f"📥 Pulling dataset version {args.metadata_id}...")
    try:
        res = requests.get(url, headers=headers, stream=True)
        if res.status_code == 200:
            # Extract filename from header if possible
            cd = res.headers.get("Content-Disposition")
            filename = f"pulled_{args.metadata_id}.data"
            if cd and "filename=" in cd:
                filename = cd.split("filename=")[1].strip('"')
            
            output_path = Path(args.output or filename)
            with open(output_path, "wb") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Successfully pulled to: {output_path}")
        else:
            print(f"❌ Pull failed: {res.text}")
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

def handle_diff(args):
    """Module 3: column-level statistical diff between two commits"""
    url = f"{get_api_url()}/projects/compare"
    params = {"commit_a": args.commit_a, "commit_b": args.commit_b}
    
    try:
        res = requests.get(url, headers=auth_headers(), params=params)
        if res.status_code == 200:
            result = res.json()
            if args.json:
                print(json.dumps(result, indent=2))
                return
            
            print(f"\n📊 Statistical Diff: {args.commit_a[:8]} -> {args.commit_b[:8]}")
            print("-" * 60)
            for col, d in result["column_diffs"].items():
                print(f"Column: {col}")
                if d["dtype_changed"]: print("  ⚠️ Dtype changed!")
                for m, val in d["metrics"].items():
                    sign = "+" if val >= 0 else ""
                    print(f"  {m:<15}: {sign}{val}")
            print()
        else:
            print(f"❌ Diff failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_branch(args):
    """Module 3: Branch management"""
    if args.subcommand == "list":
        url = f"{get_api_url()}/projects/{args.project}/branches"
        res = requests.get(url, headers=auth_headers())
        output_result(args, res.json())
    elif args.subcommand == "create":
        url = f"{get_api_url()}/projects/{args.project}/branches"
        data = {"name": args.name}
        res = requests.post(url, headers=auth_headers(), json=data)
        output_result(args, res.json(), f"✅ Branch '{args.name}' created.")
    elif args.subcommand == "delete":
        url = f"{get_api_url()}/projects/{args.project}/branches/{args.name}"
        res = requests.delete(url, headers=auth_headers())
        output_result(args, res.json(), f"✅ Branch '{args.name}' deleted.")

def handle_pr(args):
    """Module 3: Pull Request management"""
    if args.subcommand == "list":
        url = f"{get_api_url()}/projects/{args.project}/pulls"
        res = requests.get(url, headers=auth_headers())
        output_result(args, res.json())
    elif args.subcommand == "create":
        url = f"{get_api_url()}/projects/{args.project}/pulls"
        data = {"title": args.title, "source": args.source, "target": args.target}
        res = requests.post(url, headers=auth_headers(), json=data)
        output_result(args, res.json(), f"✅ Pull Request created.")
    elif args.subcommand == "merge":
        url = f"{get_api_url()}/pulls/{args.id}/merge"
        res = requests.post(url, headers=auth_headers())
        output_result(args, res.json(), f"✅ Pull Request merged.")

def handle_chat(args):
    """Ask the DataHub AI about a specific dataset or general data topics"""
    url = f"{get_api_url()}/ai/chat"
    headers = auth_headers()
    
    data = {"question": args.question}
    if args.metadata_id:
        data["metadata_id"] = args.metadata_id

    print("🤖 Processing query...")
    try:
        res = requests.post(url, headers=headers, json=data)
        if res.status_code == 200:
            result = res.json()
            print(f"\n{'-'*60}\n🤖 DataHub AI:\n{result['answer']}\n{'-'*60}\n")
        else:
            print(f"❌ AI Chat failed: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

def handle_projects(args):
    """List all available projects"""
    url = f"{get_api_url()}/projects/"
    try:
        res = requests.get(url, headers=auth_headers())
        if res.status_code == 200:
            projects = res.json()
            if not projects:
                print("No projects found.")
                return
            print(f"\n{'ID':<4} | {'NAME':<20} | {'OWNER':<15} | {'CREATED AT'}")
            print("-" * 60)
            for p in projects:
                created = p.get('created_at', 'N/A').split('T')[0]
                print(f"{p['id']:<4} | {p['name']:<20} | {p['owner']:<15} | {created}")
            print()
        else:
            print(f"❌ Failed to fetch projects: {res.text}")
    except Exception as e:
        print(f"❌ Network error: {e}")

# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="DataHub Distributed Version Control CLI")
    # Global flags
    parser.add_argument("--json", action="store_true", help="Output result in machine-readable JSON format")

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

    # Command: pull
    parser_pull = subparsers.add_parser("pull", help="Download a specific dataset version (Metadata ID)")
    parser_pull.add_argument("metadata_id", type=int, help="The numeric ID of the metadata/version to pull")
    parser_pull.add_argument("-o", "--output", help="Optional output path")

    # Command: whoami
    subparsers.add_parser("whoami", help="Display current user session info")

    # Command: logout
    subparsers.add_parser("logout", help="Clear local authentication session")

    # Command: projects
    subparsers.add_parser("projects", help="List all accessible projects")

    # Command: chat
    parser_chat = subparsers.add_parser("chat", help="Ask the AI about a dataset")
    parser_chat.add_argument("question", help="Your natural language question")
    parser_chat.add_argument("--id", dest="metadata_id", type=int, help="Optional Metadata ID for context")

    # Command: diff
    parser_diff = subparsers.add_parser("diff", help="Compare statistics between two commits")
    parser_diff.add_argument("commit_a", help="First commit hash")
    parser_diff.add_argument("commit_b", help="Second commit hash")

    # Command: branch
    parser_branch = subparsers.add_parser("branch", help="Branch management")
    branch_subs = parser_branch.add_subparsers(dest="subcommand")
    branch_list = branch_subs.add_parser("list", help="List branches")
    branch_list.add_argument("project", help="Project name")
    branch_create = branch_subs.add_parser("create", help="Create branch")
    branch_create.add_argument("project", help="Project name")
    branch_create.add_argument("name", help="Branch name")
    branch_delete = branch_subs.add_parser("delete", help="Delete branch")
    branch_delete.add_argument("project", help="Project name")
    branch_delete.add_argument("name", help="Branch name")

    # Command: pr
    parser_pr = subparsers.add_parser("pr", help="Pull Request management")
    pr_subs = parser_pr.add_subparsers(dest="subcommand")
    pr_list = pr_subs.add_parser("list", help="List PRs")
    pr_list.add_argument("project", help="Project name")
    pr_create = pr_subs.add_parser("create", help="Create PR")
    pr_create.add_argument("project", help="Project name")
    pr_create.add_argument("title", help="PR title")
    pr_create.add_argument("source", help="Source branch")
    pr_create.add_argument("target", help="Target branch")
    pr_merge = pr_subs.add_parser("merge", help="Merge PR")
    pr_merge.add_argument("id", type=int, help="PR ID")

    args = parser.parse_args()

    if args.command == "login":
        handle_login(args)
    elif args.command == "logout":
        handle_logout(args)
    elif args.command == "whoami":
        handle_whoami(args)
    elif args.command == "projects":
        handle_projects(args)
    elif args.command == "chat":
        handle_chat(args)
    elif args.command == "diff":
        handle_diff(args)
    elif args.command == "branch":
        handle_branch(args)
    elif args.command == "pr":
        handle_pr(args)
    elif args.command == "init":
        handle_init(args)
    elif args.command == "push":
        handle_push(args)
    elif args.command == "pull":
        handle_pull(args)
    elif args.command == "log":
        handle_log(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
