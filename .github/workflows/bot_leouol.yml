name: BOT LEOUOL - Scraper

on:
  schedule:
    # 07:00 às 20:00 Brasília (10:00 às 23:00 UTC) - a cada 5 minutos
    - cron: "*/5 10-23 * * *"
    
    # 21:00 às 23:00 Brasília (00:00 às 02:00 UTC) - a cada 5 minutos
    - cron: "*/5 0-2 * * *"
    
    # 00:00 às 06:00 Brasília (03:00 às 09:00 UTC) - a cada 1 hora
    - cron: "0 3-9 * * *"
    
  workflow_dispatch:

concurrency:
  group: bot-leouol
  cancel-in-progress: true

permissions:
  contents: write

jobs:
  run-bot:
    name: Buscar Ofertas
    runs-on: ubuntu-latest
    timeout-minutes: 3

    steps:
      - name: 📥 Baixar código
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: 🐍 Configurar Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: 🔄 Restaurar histórico
        shell: bash
        run: |
          if [ ! -f historico_leouol.json ]; then
            echo '{"ids": []}' > historico_leouol.json
          fi

      - name: 📦 Instalar dependências
        shell: bash
        run: |
          pip install -r requirements.txt

      - name: 🤖 Executar BOT
        env:
          TELEGRAM_TOKEN: ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          GRUPO_COMENTARIO_ID: ${{ secrets.GRUPO_COMENTARIO_ID }}
        shell: bash
        run: python bot_leouol.py

      - name: 💾 Salvar histórico
        if: always()
        shell: bash
        run: |
          git config --global user.email "bot@leouol.com"
          git config --global user.name "BOT LEOUOL"
          git add historico_leouol.json
          git diff-index --quiet HEAD || git commit -m "Atualiza histórico [skip ci]" && git push
