# Ponto de partida — Clube UOL Premium / UOL

Estado consolidado em **18/10/2026**, fuso **America/Sao_Paulo**, após confirmação
do proprietário no iPhone. Este arquivo permite arquivar a conversa e continuar
sem reler todo o histórico. Não contém sessões, cookies, tokens ou vouchers.

## Leia primeiro

1. Este handoff.
2. `stories-hunter/ARCHITECTURE.md`: fontes, build, publicação e updater.
3. `stories-hunter/PREMIUM_MIGRATION.md`: nomes e integração com Atalhos/MacSend.
4. Somente os arquivos da próxima tarefa. Não reindexar o repositório inteiro.

`stories-hunter/HANDOFF_STORIES_HUNTER.md` contém contratos detalhados e seções
históricas. Este handoff prevalece para o estado final da conversa. Em particular,
os textos antigos sobre duas automações e ntfy exclusivo fora do gatilho
`Scriptable` foram superados: **existe uma automação unificada**.

## Estado confirmado

- Nome oficial/instalado: **Clube UOL Premium**; versão stable **5.8.1**.
- O usuário confirmou a abertura da 5.8.1 no iPhone com captura dos Ajustes às
  15:32 e disse: “ta tudo ok, isso aí!”. A captura anterior de 5.8.0 era do script
  antigo, aberto por engano; o próprio usuário identificou isso.
- O usuário recebeu o aviso ntfy. Recepção do aviso, execução do Atalho e
  instalação por OTA são evidências distintas. Não afirmar que essa execução
  específica instalou a versão: a 5.8.1 já existia também no iCloud.
- O updater `update-auto` é silencioso, sem notificação de conclusão. Sem nova
  versão, retorna JSON ao Atalho e não grava recibo sincronizado com o Mac.
- Último commit de código conferido: `49ba8104ddc7cbdcefede4fa82cb0c6af73824b2`.
  [CI desse SHA: success](https://github.com/leosaquetto/uol-bot/actions/runs/39149915884).
- Há alteração operacional pré-existente em
  `docs/operations/2026-10-15-uol-reliability.md`. **Preservar; não incluir em commits
  alheios, reverter ou substituir.** A criação deste handoff não alterou runtime.

### Release pública

| Item | Valor |
| --- | --- |
| Versão | 5.8.1 |
| Canal | stable |
| Artefato | `stories-hunter/dist/Hunter.js` |
| Espelho compatível | `scriptable_uol_stories_hunter.js` |
| Tamanho de ambos | **664.889 bytes** |
| SHA-256 | `5a8a8ef33870d8231e34cfe208bb570f91cbacc2ea8e2aad0d0a514ff783f35c` |
| Commit imutável do artefato | `350b0a7644f0f6e7fb437d002e570974a65fa6f9` |

- [Manifest stable](https://raw.githubusercontent.com/leosaquetto/uol-bot/main/stories-hunter/releases/stable.json)
- [Artefato imutável](https://raw.githubusercontent.com/leosaquetto/uol-bot/350b0a7644f0f6e7fb437d002e570974a65fa6f9/stories-hunter/dist/Hunter.js)

Os hashes e tamanhos locais foram reconferidos ao criar este handoff. A instalação
privada via iCloud pode ser maior por conter bootstrap autorizado; nunca usar esse
arquivo privado como artefato público ou imprimi-lo em logs.

## Arquitetura que foi aprovada e implementada

**Fontes modulares → build reproduzível → um único script autocontido → updater.**
Não introduzir `importModule`, File Bookmarks, instalação multiparte ou download
obrigatório de código em cada abertura só para diminuir o arquivo principal.

| Área | Arquivos principais em `stories-hunter/` |
| --- | --- |
| Scriptable, inicialização, armazenamento e ponte WebView | `native/host.js` |
| OTA, validação, backup, recuperação e migração de nome | `native/updater.js` |
| Interface | `ui/shell.html`, `ui/styles.css`, `ui/app.js`, `startup.html` |
| Recursos versionados | `assets/icons.json`, `logo_base64.txt` |
| Coleta, aprovação de aparelhos, resgates | `collector.js`, `access.js`, `redemption.js` |
| Montagem | `assemble.cjs`, `assemble-ui.cjs`, `build.js` |
| Release e distribuição | `release.cjs`, `distribute.cjs`, `private-install.cjs`, `install-code.cjs` |
| Atalhos | `shortcuts/unify.cjs`, JSONs em `shortcuts/` |

A extração inicial preservou o artefato byte a byte. A deduplicação de logo/SVGs
preservou HTML final, CSS, callbacks e aparência nas fixtures congeladas.
Referência 5.7.9: **782.163 bytes**; após deduplicação: **642.655 bytes**;
5.8.0 com updater robusto: **661.663 bytes**; 5.8.1: **664.889 bytes**.
Evidências detalhadas: `stories-hunter/REFACTOR_CHECKPOINTS.md`.

Não misturar novas otimizações de desempenho, inicialização, cache, renderização,
prefetch, transporte ou respostas assíncronas atrasadas com manutenção estrutural.
Essas oportunidades ficaram para tarefas separadas, com medição anterior.

## Atualização: fluxo atual no iPhone

Uma única automação **Update Scriptable**:

1. Receber notificação do **ntfy**, filtro **Título contém Scriptable**.
2. Se **Título da notificação contém Clube UOL Premium**:
   - Dicionário: `action = update-auto`.
   - Ação nativa Scriptable: executar **Clube UOL Premium**, entrada = Dicionário.
   - Parar o Atalho; não baixar/salvar o script pelo endpoint do Mac.
3. Outros títulos `Scriptable`: preservar fluxo genérico existente do MacSend.
   Antes de salvar, nomes Premium/legado também são desviados para o updater.
4. Entrada desconhecida não autoriza sobrescrever o último arquivo do Mac.

Não enviar `channel` no Dicionário: preserva preferência local; padrão é stable.
O atalho separado **Update Clube UOL Premium** é somente auxiliar manual; não
exige outra automação. Outros scripts continuam atendidos pelo fluxo genérico.

### Correção real feita pelo usuário no Atalho

A condição com `Entrada do Atalho contém ...` apareceu inválida no iPhone.
O usuário corrigiu para **Título da notificação**. A leitura somente de leitura do
Atalho sincronizado confirmou **ambas as condições de roteamento** usando:

```json
{
  "Type": "TriggerOutput",
  "TriggerIdentifier": "WFNotificationTrigger",
  "Aggrandizements": [
    { "Type": "WFPropertyVariableAggrandizement", "PropertyName": "Title" }
  ]
}
```

**Diferença ainda não incorporada ao gerador:** o HEAD usa uma ação Texto para
converter `ExtensionInput`; o Atalho instalado usa diretamente `TriggerOutput`
com propriedade Title e possui 38 ações. Se voltar a gerar/importar a automação,
incorporar primeiro o esquema real acima e testar. Não substituir a correção
funcional do usuário por um arquivo antigo preparado.

Os arquivos personalizados assinados contêm referências privadas à pasta iCloud:
ficam em `~/.config/uol-ticket-research/premium581/unified/`, não no Git.
O CLI `shortcuts` não importa silenciosamente. Não editar seu banco privado para
contornar a confirmação do sistema. A consulta ao banco nesta conversa foi read-only.

### Publicação e aviso

Seguir `ARCHITECTURE.md`: prepare → testes → commit do artefato → beta → promoção
dos mesmos bytes a stable → Git/CI → conferir manifest/hash remotos → avisar.

```sh
node stories-hunter/build.js --prepare
node --test stories-hunter/test-*.cjs
# Após publicação e verificação do artefato stable:
node stories-hunter/build.js --code-only
```

**`--code-only` envia uma notificação real**, título `Scriptable - Clube UOL Premium`.
Não usar como teste inofensivo ou repetir sem necessidade. Não instala no iCloud
nem altera o alvo staged do MacSend. `--code-only --install` é instalação explícita.
Nunca executar build legado sem flags para simples atualização: pode sincronizar
dados privados. Não executar instalação pelo Mac e updater do iPhone em paralelo.

O updater verifica versão, tamanho, SHA-256, UTF-8, envelope e sintaxe antes de
staging/backup/substituição/readback. Journal e backups OTA são locais ao aparelho;
`FileManager.move` não oferece garantia documentada de atomicidade completa.
Não enfraquecer controles de conflito para obter sucesso aparente.

## Nome antigo: pendência pequena, mas real

A última inspeção encontrou **dois arquivos no iCloud Scriptable**:

- `Clube UOL Premium.js`: 5.8.1, instalação privada de 740.252 bytes.
- `Clube UOL - Stories Hunter.js`: cópia pública antiga 5.8.0, 661.663 bytes,
  SHA-256 `47c0d133dec0cb5df566cb73446a5870a24dbfaf8ed98d0be8c5fcd05ee48fd1`.

Diretório: `~/Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/`.
Não afirmar que a duplicata já foi removida. Ela explica a confusão da captura.
Também foi observado que o atalho **de abertura** chamado Clube UOL Premium ainda
usava URL antiga. Orientação enviada: trocar a ação Abrir URLs para
`scriptable:///run/Clube%20UOL%20Premium`. O usuário confirmou abrir a versão correta,
mas não confirmou explicitamente a edição desse lançador.

Se for concluir limpeza: reconferir referências/arquivos, preservar backup
verificado e dados privados, corrigir lançadores antes de retirar código antigo.
Nenhum redirect shim foi implementado; foi apenas avaliado. A migração existente
recusa dois scripts com conteúdo diferente, por segurança; preservar esse contrato.

## Contratos do produto a preservar

- Servidor/Workers são principais; iPhone complementa descoberta pública de
  Portal e Stories, enviando ao intake. Não envia WhatsApp diretamente.
- Aprovação individual de aparelhos; permissão de coleta e permissões pessoais
  independentes. Sessões privadas no Keychain; resgates separados por conta.
- Reserva antes do resgate, recibo antes de afirmar sucesso, resultado incerto sem
  repetição automática. Não testar refatoração consumindo benefícios reais.
- IDs Instagram grandes preservados; deduplicação entre aparelhos e servidor;
  limites/recuos persistentes do coletor. Não trocar sessão/transporte automaticamente.
- Primeira abertura após atualização não inicia coleta. Atualização é de código.
- Home/sessões passadas, datas cronológicas, thumbnails, margens, temas, filtros,
  curadoria, Histórico e datas reais de resgate têm contratos nos testes/documentos.
- Histórico abre no mês atual; Editor’s Choice pode mostrar todos os destacados.
- Barra: Padrão/Roxo/Laranja/Branco; preferência local preservada. Baseline em
  `stories-hunter/NAVIGATION_BAR_BASELINE.md`.
- Navegador interno fecha e volta ao Hunter; Safari externo é opção explícita.
- Reconexão UOL é login oficial pelo fluxo existente. Última captura mostrava
  “Sessão expirada”; isso não é prova de falha do updater. Não renovar sessão
  pessoal silenciosamente nem alterar a outra conta.

## Validação já feita — não repetir sem mudança

- Baseline original: 178/178 testes. Refatoração/updater 5.8.0: 218/218.
- Rename 5.8.1: 229/229. Testes posteriores de Atalhos incluíram suíte focada de
  25 testes e 9 testes do gerador após ajuste de tipo. CI atual success, link acima.
- Testes sintéticos cobrem equivalência, integridade, resposta parcial/perdida,
  recuperação, migração, conflito e preservação de dados.
- Verificação física: usuário confirmou UI/contas/tema na 5.8.0 e abertura da
  5.8.1. Não confundir isso com prova do resultado de cada execução automática.
- Nenhum teste novo foi executado para produzir este handoff; somente conferências
  de estado, hashes, documentos e resultado da CI existente.

## Operação UOL separada do aplicativo

Para backend, consultar apenas `docs/operations/2026-10-15-uol-reliability.md` e o
manifesto privado `~/.config/uol-ticket-research/uol-publication-2026-10-16.json`
antes de verificações relacionadas. Não assumir SHA antigo como Worker atual.
Endpoint conhecido: `https://uol-telegram-shadow-pilot.leosaquetto.workers.dev`.
Não foi feita nova verificação de saúde de produção ao criar este handoff.

Renovação técnica: `docs/operations/uol-credential-renewal.md` e estado privado
`~/.config/uol-ticket-research/uol-credential-renewal-state.json`. Última validade
documentada: **23/10/2026 04:04:28 UTC / 01:04:28 São Paulo**. Emissor/refresh
continuam não comprovados no runbook; automação preparada não prova renovação.
Não repetir investigação esgotada nem pedir ao usuário procedimento desconhecido.

IDs de automações mencionados nesta conversa: `renovar-credencial-t-cnica-uol` e
`verificar-uol-ap-s-corre-es`. Estado/agendamento atual não foram consultados neste
handoff. Se necessário continuar/transferir, conferir os registros existentes;
não criar duplicatas. Arquivar conversa não equivale a comprovar parada/transferência.

Manter tiers gratuitos, guard primário 512, evidências históricas, recibos e
distinção `published` / `runtimeVerified` / `ready`. Quota de três dias completos:
17–19/10 UTC, só concluir após fechar 19/10 e confirmação posterior. Preservar
`billing_unknown` para hibernation até interpretação oficial comprovada.

## Como começar a próxima conversa

Pedido inicial sugerido:

> Leia `docs/operations/2026-10-18-clube-uol-premium-handoff.md` no repositório
> `/Users/leosaquetto/Developer/GitHub/uol-bot`. Esse é o ponto de partida da conversa
> anterior. Preserve a alteração operacional pendente e os dados privados. O Clube
> UOL Premium 5.8.1 já foi confirmado no iPhone. Não reenvie notificações, publique
> versões ou refaça validações sem uma mudança que justifique. Vou passar a próxima
> tarefa a partir desse estado.
