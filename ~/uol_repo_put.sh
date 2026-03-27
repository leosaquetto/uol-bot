cat > ~/uol_repo_put.sh <<'EOF'
#!/bin/bash

set -euo pipefail

if [ $# -lt 3 ]; then
  cat >&2 << 'USAGE'
❌ uso: ~/uol_repo_put.sh "repo/path.ext" "/caminho/local.ext" "mensagem"

exemplos:
  ~/uol_repo_put.sh "bot_leouol.py" "$HOME/bot_leouol.py" "refina bot"
  ~/uol_repo_put.sh "pending_offers.json" "$HOME/pending_offers.json" "limpa pending"
  ~/uol_repo_put.sh ".github/workflows/bot_leouol_consumer.yml" "$HOME/bot_leouol_consumer.yml" "ajusta workflow"
USAGE
  exit 1
fi

REPO_PATH="$1"
LOCAL_PATH="$2"
COMMIT_MSG="$3"

if [ ! -f ~/uol_repo_env ]; then
  echo "❌ arquivo ~/uol_repo_env não encontrado"
  exit 1
fi

source ~/uol_repo_env

if [ -z "${GITHUB_PAT:-}" ]; then
  echo "❌ GITHUB_PAT não definido em ~/uol_repo_env"
  exit 1
fi

python3 ~/uol_repo_update.py "$REPO_PATH" "$LOCAL_PATH" "$COMMIT_MSG"
EOF

chmod +x ~/uol_repo_put.sh
