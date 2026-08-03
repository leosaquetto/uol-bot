# Fallback legado: Mac e Scriptable

> Este fluxo não é mais o caminho automático principal. Desde a migração para
> o Cloudflare Worker, ele deve ser usado somente em rollback ou diagnóstico
> manual. O Worker consulta o Clube UOL e envia ao Telegram sem depender do Mac.

O código do fallback legado continua preservado em **duas vias**, mas ambas
estão inativas enquanto os workflows permanecem em `.github/workflows-archive/`:

1. **Via A:** iOS chama o Mac via SSH para rodar `mac_uol_scraper.js`.
2. **Via B:** se SSH falhar, timeout, ou retorno sem sucesso confirmado, o
   atalho pode continuar para as 3 partes do Scriptable no iPhone.

Não execute nenhuma via enquanto o Worker estiver publicando. Antes de uma
recuperação, coloque o Worker em `shadow`, confirme o modo e remova o webhook do
Telegram se o consumidor restaurado usar `getUpdates`.

## Comando no Mac (SSH action)

Exemplo direto no Atalhos (Run script over SSH):

```bash
cd /Users/leosaquetto/Developer/GitHub/uol-bot && \
TRIGGER_GITHUB_WORKFLOW=1 \
EDGE_PROFILE_DIR="/Users/leosaquetto/Documents/GrabNumberAutomator/edge-profile" \
/usr/local/bin/node mac_uol_scraper.js
```

O exemplo pressupõe que `GITHUB_TOKEN` já foi carregado com segurança no
ambiente local. Não coloque o valor do token no Atalho, no comando salvo ou em
logs. Só habilite `TRIGGER_GITHUB_WORKFLOW=1` depois de restaurar e validar o
workflow indicado por `GITHUB_WORKFLOW_FILENAME`.

## Regra de arquitetura (sem concorrência)

- **Plano A (Mac):** roda primeiro via SSH durante uma recuperação contida.
- **Plano B (iOS):** só roda se o Plano A não devolver `MAC_OK workflow_trigger=ok`.
- Não há escrita concorrente no mesmo arquivo final do fluxo Scriptable: o Mac grava snapshot próprio (`snapshots/mac-uol-offers.json`).

## Como montar a lógica no Atalhos (iOS)

1. **Ação 1:** Run Script over SSH (comando acima).
2. **Ação 2:** `If` resultado **contains** `MAC_OK` **e** **contains** `workflow_trigger=ok`:
   - `Stop this Shortcut` (sucesso no Mac, não roda o fluxo iOS).
3. **Else**:
   - Executa seu fluxo atual dividido (parte 1 → parte 2 → parte 3 no Scriptable).

### Regra explícita para `workflow_trigger`

No retorno do SSH, trate assim:

- `MAC_OK ... workflow_trigger=ok` → sucesso completo (encerra o atalho).
- `MAC_FAIL ... workflow_trigger=failed` → handoff não confirmado; o processo retorna código 3.
- `MAC_FAIL ... workflow_trigger=skipped` → dispatch desligado; o processo retorna código 3.
- qualquer saída sem `MAC_OK` → **fallback automático** para Scriptable.

O script nunca imprime `MAC_OK` apenas porque gravou um arquivo ou fez upload
do snapshot. A única confirmação aceita é o dispatch HTTP 204.

## Sobre o erro recorrente “Não foi possível executar Run Script”

Esse erro geralmente é de conectividade/execução do SSH (Mac dormindo, rede diferente, chave/senha, timeout curto, etc.).
Com o fluxo acima, mesmo que isso aconteça, você preserva execução pelo iOS automaticamente.

## Arquivos que este script grava

Por padrão ele grava o payload em um caminho **específico do UOL**:

- `~/Library/Mobile Documents/com~apple~CloudDocs/Shortcuts/ClubeUol/mac-uol-offers.json`

Também faz upload para o GitHub no caminho:

- `snapshots/mac-uol-offers.json`

## Variáveis de ambiente suportadas

- `OUT_FILE` (arquivo local no iCloud)
- `UOL_TARGET_URL`
- `MAX_CARDS`
- `EDGE_PROFILE_DIR`
- `GITHUB_TOKEN`
- `GITHUB_REPO_OWNER`
- `GITHUB_REPO_NAME`
- `GITHUB_BRANCH`
- `GITHUB_TARGET_PATH`
- `GITHUB_WORKFLOW_FILENAME`
- `TRIGGER_GITHUB_WORKFLOW` (`0` por padrão; recuperação falha de modo seguro)
- `REQUIRE_GITHUB_UPLOAD` (`1` por padrão)

Com `REQUIRE_GITHUB_UPLOAD=1`, se não tiver `GITHUB_TOKEN`, o script retorna `MAC_FAIL` e o atalho cai corretamente no fallback iOS.
