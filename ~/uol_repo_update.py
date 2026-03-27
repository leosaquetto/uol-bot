cat > ~/uol_repo_update.py <<'EOF'
#!/usr/bin/env python3
import base64
import json
import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌ biblioteca 'requests' não encontrada. instale com: python3 -m pip install requests", file=sys.stderr)
    sys.exit(1)

def die(msg: str, code: int = 1) -> None:
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(code)

def load_env() -> tuple[str, str, str]:
    token = os.environ.get("GITHUB_PAT", "").strip()
    repo = os.environ.get("GITHUB_REPO", "").strip()
    branch = os.environ.get("GITHUB_BRANCH", "main").strip()

    if not token:
        die("GITHUB_PAT não definido. execute: source ~/uol_repo_env")
    if not repo:
        die("GITHUB_REPO não definido")

    return token, repo, branch

def github_headers(token: str) -> dict:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def get_file_info(token: str, repo: str, branch: str, path: str) -> dict | None:
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    resp = requests.get(
        url,
        headers=github_headers(token),
        params={"ref": branch},
        timeout=30,
    )

    if resp.status_code == 404:
        return None

    if not resp.ok:
        die(f"erro ao consultar arquivo: {resp.status_code}\n{resp.text}")

    return resp.json()

def update_file(
    token: str,
    repo: str,
    branch: str,
    repo_path: str,
    local_path: str,
    message: str,
) -> None:
    local_file = Path(local_path).expanduser()
    if not local_file.exists():
        die(f"arquivo local não encontrado: {local_path}")

    file_bytes = local_file.read_bytes()
    content_b64 = base64.b64encode(file_bytes).decode("utf-8")

    file_info = get_file_info(token, repo, branch, repo_path)
    sha = file_info.get("sha") if file_info else None

    payload = {
        "message": message,
        "content": content_b64,
        "branch": branch,
    }

    if sha:
        payload["sha"] = sha
        action = "🔄 atualizando"
    else:
        action = "✨ criando"

    print(f"{action} {repo_path}...")

    url = f"https://api.github.com/repos/{repo}/contents/{repo_path}"
    resp = requests.put(
        url,
        headers=github_headers(token),
        json=payload,
        timeout=30,
    )

    if not resp.ok:
        die(f"erro ao atualizar arquivo: {resp.status_code}\n{resp.text}")

    data = resp.json()
    commit = data.get("commit", {})

    print(json.dumps({
        "ok": True,
        "arquivo": repo_path,
        "branch": branch,
        "commit_sha": (commit.get("sha") or "")[:7],
        "commit_url": commit.get("html_url", ""),
    }, ensure_ascii=False, indent=2))

def main() -> None:
    if len(sys.argv) < 4:
        print('uso: python3 uol_repo_update.py "repo/path.ext" "/caminho/local.ext" "mensagem"')
        sys.exit(1)

    repo_path = sys.argv[1].strip()
    local_path = sys.argv[2].strip()
    message = sys.argv[3].strip()

    if not repo_path or not local_path or not message:
        die("argumentos vazios")

    token, repo, branch = load_env()
    update_file(token, repo, branch, repo_path, local_path, message)

if __name__ == "__main__":
    main()
EOF

chmod +x ~/uol_repo_update.py
