cat > ~/uol_repo_apply_fixes.sh <<'EOF'
#!/bin/bash

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}🚀 uol bot - aplicador de correções${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

if [ ! -f ~/uol_repo_env ]; then
  echo -e "${RED}❌ arquivo ~/uol_repo_env não encontrado${NC}"
  exit 1
fi

source ~/uol_repo_env

if [ ! -f ~/uol_repo_update.py ]; then
  echo -e "${RED}❌ arquivo ~/uol_repo_update.py não encontrado${NC}"
  exit 1
fi

update_repo_file() {
  local REPO_PATH="$1"
  local LOCAL_PATH="$2"
  local COMMIT_MSG="$3"

  if [ ! -f "$LOCAL_PATH" ]; then
    echo -e "${YELLOW}⚠️ arquivo não encontrado: $LOCAL_PATH${NC}"
    return 1
  fi

  echo -e "${YELLOW}⏳ atualizando $REPO_PATH...${NC}"
  python3 ~/uol_repo_update.py "$REPO_PATH" "$LOCAL_PATH" "$COMMIT_MSG"
  echo ""
}

show_menu() {
  echo -e "${BLUE}escolha uma opção:${NC}"
  echo "  1) atualizar bot_leouol.py"
  echo "  2) limpar pending_offers.json"
  echo "  3) atualizar historico_leouol.json"
  echo "  4) atualizar latest_offers.json"
  echo "  5) atualizar workflow do consumer"
  echo "  6) aplicar todas as correções"
  echo "  7) sair"
  echo ""
  read -p "opção (1-7): " CHOICE
}

while true; do
  show_menu

  case "$CHOICE" in
    1)
      update_repo_file "bot_leouol.py" "$HOME/bot_leouol.py" "refina bot_leouol.py"
      ;;
    2)
      update_repo_file "pending_offers.json" "$HOME/pending_offers.json" "limpa pending_offers.json"
      ;;
    3)
      update_repo_file "historico_leouol.json" "$HOME/historico_leouol.json" "atualiza historico_leouol.json"
      ;;
    4)
      update_repo_file "latest_offers.json" "$HOME/latest_offers.json" "atualiza latest_offers.json"
      ;;
    5)
      update_repo_file ".github/workflows/bot_leouol_consumer.yml" "$HOME/bot_leouol_consumer.yml" "ajusta workflow consumer"
      ;;
    6)
      echo -e "${BLUE}🔄 aplicando todas as correções...${NC}"
      echo ""

      [ -f ~/bot_leouol.py ] && update_repo_file "bot_leouol.py" "$HOME/bot_leouol.py" "refina bot_leouol.py"
      [ -f ~/pending_offers.json ] && update_repo_file "pending_offers.json" "$HOME/pending_offers.json" "limpa pending_offers.json"
      [ -f ~/historico_leouol.json ] && update_repo_file "historico_leouol.json" "$HOME/historico_leouol.json" "atualiza historico_leouol.json"
      [ -f ~/latest_offers.json ] && update_repo_file "latest_offers.json" "$HOME/latest_offers.json" "atualiza latest_offers.json"
      [ -f ~/bot_leouol_consumer.yml ] && update_repo_file ".github/workflows/bot_leouol_consumer.yml" "$HOME/bot_leouol_consumer.yml" "ajusta workflow consumer"

      echo -e "${GREEN}✅ todas as correções aplicadas!${NC}"
      break
      ;;
    7)
      echo -e "${GREEN}saindo...${NC}"
      exit 0
      ;;
    *)
      echo -e "${RED}❌ opção inválida${NC}"
      ;;
  esac

  echo ""
done
EOF

chmod +x ~/uol_repo_apply_fixes.sh
