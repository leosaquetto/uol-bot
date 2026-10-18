# Ponto de partida — Clube UOL Premium / UOL

Estado consolidado em **18/10/2026**, fuso **America/Sao_Paulo**, após aprovação
do header final pelo proprietário e publicação da **5.8.5**. Este arquivo permite arquivar a conversa e continuar
sem reler todo o histórico. Não contém sessões, cookies, tokens ou vouchers.

## Leia primeiro

1. Este handoff.
2. `stories-hunter/ARCHITECTURE.md`: fontes, build, publicação e updater.
3. `stories-hunter/PREMIUM_MIGRATION.md`: nomes e integração com Atalhos/MacSend.
4. Somente os arquivos da próxima tarefa. Não reindexar o repositório inteiro.

`stories-hunter/HANDOFF_STORIES_HUNTER.md` contém contratos detalhados e seções
históricas. Este handoff prevalece para o estado final da conversa. Em particular,
os textos antigos sobre duas automações e ntfy exclusivo fora do gatilho
`Scriptable` foram superados. O bootstrap atual usa a automação de copiar/substituir
pelo MacSend preparada pelo usuário; não reaplicar o roteamento antigo por engano.

## Estado confirmado

- Nome oficial/instalado: **Clube UOL Premium**; versão stable **5.8.5**.
- Após corrigir o título do aviso, conferir o MacSend e reenviar o gatilho, o
  usuário aprovou o resultado: “nossa agora sim obrigado!”. A aprovação visual
  encerra este ticket; não representa leitura independente da versão/hash do iPhone.
- Recepção do aviso, execução do Atalho, escrita do arquivo e runtime exato são
  evidências distintas. Os recibos e limites de prova ficam nas evidências privadas.
- Desde a 5.8.4, a abertura normal tenta instalar uma versão nova antes da UI,
  com fallback sem rede, gates de integridade e uma única tentativa de reabertura.
  A mensagem de sucesso exige a nova versão em execução e recibo/hash correspondentes.
- O updater `update-auto` é silencioso, sem notificação de conclusão. Sem nova
  versão, retorna JSON ao Atalho e não grava recibo sincronizado com o Mac.
- Commit de fontes/artefato: `48716a5525fbf3d44513496296df236ab7003c79`.
- Commit de publicação: `af0b5165648ff7071419e338e758c08000d371f1`.
  [CI da publicação: success](https://github.com/leosaquetto/uol-bot/actions/runs/39172780833).
- Há alteração operacional pré-existente em
  `docs/operations/2026-10-15-uol-reliability.md` e em
  `docs/operations/uol-credential-renewal.md`, além do diretório não rastreado
  `saquettoos/`. **Preservar; não incluir em commits alheios, reverter ou substituir.**

### Release pública

| Item | Valor |
| --- | --- |
| Versão | 5.8.5 |
| Canal | stable |
| Artefato | `stories-hunter/dist/Hunter.js` |
| Espelho compatível | `scriptable_uol_stories_hunter.js` |
| Tamanho de ambos | **685.800 bytes** |
| SHA-256 | `cabfcef72ca0f2838f13017bb4d7d4eaf0933046d1c9676d87acbfc22f95ad21` |
| Commit imutável do artefato | `48716a5525fbf3d44513496296df236ab7003c79` |

- [Manifest stable](https://raw.githubusercontent.com/leosaquetto/uol-bot/main/stories-hunter/releases/stable.json)
- [Artefato imutável](https://raw.githubusercontent.com/leosaquetto/uol-bot/48716a5525fbf3d44513496296df236ab7003c79/stories-hunter/dist/Hunter.js)

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
2. O usuário preparou o caminho de baixar o conteúdo do MacSend e substituir o
   arquivo, após o updater antigo retornar `preserved` repetidamente.
3. Antes do aviso, conferir o alvo global `/scriptable`: deve ser `clube-uol` e
   entregar **Clube UOL Premium.js**, com os mesmos bytes/hash do stable publicado.
   O alvo foi preparado com autorização do usuário. Nunca enviar o gatilho enquanto
   o endpoint entrega outro produto ou uma origem/versão não conferida.
4. O título obrigatório é **Scriptable - Clube UOL Premium**. Um aviso sem
   `Scriptable` chega ao ntfy, mas **não dispara a automação**. Esse erro ocorreu,
   foi corrigido e o gatilho correto foi reenviado; não repetir o aviso agora.
5. Outros scripts mantêm seus fluxos existentes. Não alterar seus arquivos,
   lançadores ou automações para distribuir o Clube UOL.

Ao usar o auxiliar nativo, não enviar `channel` no Dicionário: preserva preferência local; padrão é stable.
O atalho separado **Update Clube UOL Premium** é somente auxiliar manual; não
exige outra automação. Outros scripts continuam atendidos pelo fluxo genérico.

O caminho nativo `action = update-auto` continua disponível como auxiliar, mas não
substitui silenciosamente o bootstrap por MacSend que o usuário deixou preparado.
A 5.8.3 acrescentou fallback de escrita após reconciliação do `move`; isso não
comprova, sozinho, instalação no iPhone. Backup/journal e gates permanecem obrigatórios.

### Histórico do roteamento anterior — não reaplicar automaticamente

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

A inspeção anterior às entregas 5.8.2–5.8.5 encontrou **dois arquivos no iCloud
Scriptable**. Estes números são históricos, não uma nova leitura do aparelho:

- `Clube UOL Premium.js`: 5.8.1 original, instalação privada de 740.252 bytes.
  Depois houve reparo pontual somente do updater, com backup privado 0600 e
  verificação dos bytes externos ao bloco; não tratar o hash original como atual.
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

### Header aprovado na 5.8.5

- Header de vidro: **somente marca, atualizar, sino/badge e avatar/Ajustes**.
  Reflexo/contorno sutil visível mesmo no fundo preto, blur fosco, transparência,
  cantos inferiores arredondados, safe area, posição fixa e resposta à rolagem.
- O usuário rejeitou as composições com catálogo, frases de coleta ou uma caixa
  operacional expandida dentro do header. Não voltar a colocá-las ali.
- Resumo discreto junto à contagem de ofertas no conteúdo: **Em cache**,
  **Atualizado** ou **Verificar coleta**, conforme o estado real.
- Um toque abre **Catálogo e coleta**, com total, fontes, recebidas e diagnósticos.
  Nenhum erro abre a folha sozinho ou aumenta o header. Fechamento por botão,
  fundo/Escape, contenção do foco e retorno ao controle de origem.
- Estados, cooldown/backoff, aprovação/acesso, autenticação e limites continuam
  distintos; mudança somente de apresentação. Coletor, cache, sessões, acesso,
  resgates e contratos nativos foram preservados.
- Fontes: `ui/shell.html`, `ui/styles.css`, `ui/app.js`. Medição/offset do header
  seguem dinâmicos; botões mantêm área de toque de 44 px e reduced motion.

### Demais contratos

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

- Entrega 5.8.5: **75 testes focados aprovados**, checks finais de montagem,
  release/header **24/24** (sobrepostos, não somar como testes únicos), e CI success.
- Prévia sintética em Chromium isolado: 320/390/430 px, estados normais e de
  atenção, fontes, textos longos, badge, folha por toque, Escape/foco e vidro após
  rolagem. A espera final por `load` estourou timeout; a etapa de rolagem foi
  validada separadamente sem repetir a matriz. O navegador integrado ficou
  indisponível por timeout de CDP. Não chamar Chromium de prova de Safari/WebKit.
- Manifest, artefato imutável, espelho stable e payload MacSend conferidos.
  Aprovação visual final do usuário registrada; runtime/hash exato no iPhone não
  foi lido independentemente. Evidências privadas 0600:
  `~/.config/uol-ticket-research/release-5.8.5-evidence.json`.

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

O registro local `renovar-credencial-t-cnica-uol` foi conferido ao encerrar:
heartbeat **ACTIVE**, intervalo de **6 horas**, vinculado a esta conversa. Foi
preservado, sem alteração de prompt, agenda ou credencial. O pedido de arquivar
este ticket de UI não autoriza parar a manutenção técnica. Se necessário
continuar/transferir, conferir o registro existente; não criar duplicatas nem
tratar arquivamento como prova de parada/transferência. A automação histórica
`verificar-uol-ap-s-corre-es` não constava na listagem local consultada.

Manter tiers gratuitos, guard primário 512, evidências históricas, recibos e
distinção `published` / `runtimeVerified` / `ready`. Quota de três dias completos:
17–19/10 UTC, só concluir após fechar 19/10 e confirmação posterior. Preservar
`billing_unknown` para hibernation até interpretação oficial comprovada.

## Como começar a próxima conversa

Pedido inicial sugerido:

> Leia `docs/operations/2026-10-18-clube-uol-premium-handoff.md` no repositório
> `/Users/leosaquetto/Developer/GitHub/uol-bot`. Esse é o ponto de partida da conversa
> anterior. Preserve as alterações operacionais pendentes e os dados privados. A
> stable é 5.8.5 e o header final foi aprovado pelo usuário. Não reenvie notificações, publique
> versões ou refaça validações sem uma mudança que justifique. Vou passar a próxima
> tarefa a partir desse estado.
