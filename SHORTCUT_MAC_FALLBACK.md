# Fluxo recomendado: iOS tenta Mac primeiro, fallback para Scriptable

Sim, é viável fazer exatamente esse desenho em **duas vias**:

1. **Via A (preferida):** iOS chama o Mac via SSH para rodar `mac_uol_scraper.js`.
2. **Via B (fallback):** se SSH falhar, timeout, ou retorno sem `MAC_OK`, o atalho continua para as 3 partes do Scriptable no iPhone.

## Comando no Mac (SSH action)

Exemplo direto no Atalhos (Run script over SSH):

```bash
cd /Users/leosaquetto/Documents/BotLeoUol && \
GITHUB_TOKEN="SEU_TOKEN" \
EDGE_PROFILE_DIR="/Users/leosaquetto/Documents/GrabNumberAutomator/edge-profile" \
/usr/local/bin/node mac_uol_scraper.js
```


## Regra de arquitetura (sem concorrência)

- **Plano A (Mac):** roda primeiro via SSH.
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
- `MAC_OK ... workflow_trigger=failed` → **fallback automático** para Scriptable.
- `MAC_OK ... workflow_trigger=skipped` → **fallback automático** para Scriptable.
- qualquer saída sem `MAC_OK` → **fallback automático** para Scriptable.

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
- `REQUIRE_GITHUB_UPLOAD` (`1` por padrão)

Com `REQUIRE_GITHUB_UPLOAD=1`, se não tiver `GITHUB_TOKEN`, o script retorna `MAC_FAIL` e o atalho cai corretamente no fallback iOS.
