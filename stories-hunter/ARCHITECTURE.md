# Clube UOL Premium: fontes e publicação

O aparelho instala **um único script autocontido**. Não usa `importModule` nem
File Bookmarks. UI, ícones e logo ficam no script; desde a 5.8.4, a abertura tenta
atualizar o código com segurança, mas continua funcionando sem esse download.

## Organização

| Fonte | Responsabilidade |
| --- | --- |
| `native/host.js` | Inicialização, armazenamento e ponte Scriptable/WebView, na ordem original. |
| `native/updater.js` | Descoberta, integridade, instalação e recuperação de versões. |
| `ui/shell.html`, `ui/styles.css`, `ui/app.js` | Grandes blocos originais da interface, concatenados na mesma ordem e no mesmo escopo. |
| `startup.html` | Tela inicial e estado de falha existentes. |
| `assets/icons.json`, `logo_base64.txt` | Fontes versionadas únicas dos recursos utilizados. |
| `collector.js`, `access.js`, `redemption.js` | Contratos existentes, sem subdivisão ou alteração nesta refatoração. |
| `assemble-ui.cjs`, `assemble.cjs` | Montagem determinística; somente entradas do repositório. |
| `build.js` | Entrada CLI e verificação da versão promovida antes da distribuição. |
| `private-install.cjs`, `distribute.cjs` | Bootstrap privado legado e efeitos de instalação/distribuição separados do build público. |
| `release.cjs`, `releases/*.json` | Artefato imutável, canais e promoção dos mesmos bytes. |
| `dist/Hunter.js` | Artefato público canônico, sem sessões, cookies ou vouchers. |

`scriptable_uol_stories_hunter.js`, na raiz, continua sendo o espelho **stable**
para instalações antigas e MacSend. Um build candidato não altera esse espelho.
O nome instalado é **Clube UOL Premium.js**. Migração e Atalho dedicado em
[PREMIUM_MIGRATION.md](PREMIUM_MIGRATION.md).

Não há novas camadas de eventos, roteamento, armazenamento ou transporte das
funções do produto. A montagem substitui placeholders localmente. O HTML final,
incluindo CSS, logo e SVGs, é comparado com a referência congelada da 5.7.9.

## Publicar uma versão

Na raiz do repositório:

1. Alterar as fontes e `SCRIPT_VERSION` em `native/host.js`.
2. Executar `node stories-hunter/build.js --prepare` e
   `node --test stories-hunter/test-*.cjs`. O prepare não lê configuração privada,
   não instala e não envia notificações.
3. Revisar e commitar fontes + `stories-hunter/dist/Hunter.js`.
4. Executar `node stories-hunter/release.cjs prepare --channel beta --commit SHA_COMPLETO`.
   O comando exige que o artefato local coincida com o commit indicado.
5. Após a validação adequada à mudança, executar
   `node stories-hunter/release.cjs promote`. A promoção copia os bytes do commit
   beta, sem recompilar, e atualiza stable e o espelho legado juntos.
6. Commitar manifests e espelho; publicar com Git. Conferir CI do SHA publicado,
   manifest remoto, tamanho e SHA-256 do artefato remoto antes da distribuição.
7. Executar `node stories-hunter/build.js --code-only`: envia somente o aviso
   dedicado ao updater. Instalação/migração local é explícita, com
   `--code-only --install`; conferir os bytes e o backup antes de usar o aparelho.

Não usar o modo legado sem flags para uma atualização de código: ele também
sincroniza dados privados. `--code-only --install` mantém o bootstrap privado legado
na instalação iCloud; os artefatos GitHub/MacSend continuam públicos e sanitizados.

Stable/beta são dois pequenos manifests no mesmo repositório. Não exigem novo
serviço nem uma aprovação extra quando apenas o proprietário estiver usando o
Hunter. Uma beta validada pode ser promovida exatamente como foi testada.

## Atualização no aparelho

- Desde a 5.8.4, a abertura normal tenta atualizar antes de criar a WebView,
  iniciar coleta ou abrir dados privados. O manifest tem timeout de 3 segundos;
  o artefato mantém 20 segundos. Sem novidade ou sem rede, abre o dashboard
  existente. Não há instalação em background nem `Promise.race` abandonando
  uma escrita em andamento, e não se repete a consulta depois do dashboard.
- Uma instalação verificada permite uma única tentativa de reabertura, registrada
  no journal; a execução antiga termina sem criar outra WebView. Apenas a versão
  nova em execução, com arquivo e hash iguais ao recibo, mostra uma vez
  **Atualizado para a versão X**. A primeira abertura mantém a pausa de coleta.
  Falha de instalação/reabertura mostra pendência, nunca sucesso presumido.
- O updater interno e `action: update` verificam e pedem confirmação antes de
  instalar. A nova ação explícita `update-auto` aplica os mesmos controles sem
  alerta nem reabertura automática; a próxima abertura mantém a pausa de coleta.
- A configuração normal da automação `Update Scriptable` recebe os avisos ntfy. O título
  `Scriptable - Clube UOL Premium` executa `action: update-auto` antes de consultar
  o Mac; os outros scripts mantêm as ações existentes. O gerador preserva o gatilho
  e os parâmetros originais; detalhes e validação no aparelho em `PREMIUM_MIGRATION.md`.
- `channel` = `beta` no mesmo Dicionário faz adesão explícita; `stable` volta ao
  canal normal. Sem esse campo, preserva a preferência local. Não há downgrade
  automático ao trocar de canal. O retorno ao Atalho contém estado, motivo,
  versão quando disponível, canal e `updaterVersion`, nunca credenciais.
- Ações `collect` e `notification` continuam headless e não atualizam ao abrir.
  O bootstrap pontual via MacSend para um updater antigo quebrado está descrito
  em `PREMIUM_MIGRATION.md`; não é executado automaticamente pelo build.

## Garantias e limites do updater

O manifest fixa versão, protocolo, canal, URL de commit imutável, tamanho e
SHA-256. Antes de instalar, o código verifica HTTP/URL/tipo, UTF-8, bytes, hash,
versão, marcadores e sintaxe, sem executar o candidato. Usa staging no diretório
do script, backup local verificado e journal por transação. Confere a versão atual
antes da substituição e lê novamente o arquivo instalado. Falhas recuperáveis
restauram o backup; resultado ambíguo é reconciliado antes de nova tentativa.

Sem atualização, rede indisponível, download inválido e protocolo incompatível
têm estados distintos. Nenhuma dessas condições exige baixar código para abrir
o Hunter. Cookies, Keychain, reservas, recibos e vouchers não são escritos pelo
updater. Backups não são apagados automaticamente nesta versão.

[FileManager.move](https://docs.scriptable.app/filemanager/) permite substituir
o destino, mas uma tentativa no iPhone retornou `preserved`: a substituição não
foi confirmada e o hash anterior permaneceu intacto. Desde a 5.8.3, após um move
falho ou sem efeito, o updater relê o destino antes de decidir qualquer escrita.
Se já contém o candidato exato, não repete a operação. Se continua exatamente
igual ao alvo anterior, exige journal integral inalterado, backup e staging
verificados e usa uma única tentativa de `writeString`, seguida de readback.
Alvo divergente ou journal alterado bloqueia o fallback. A restauração usa as
mesmas condições; não remove o script instalado nem apaga backups.

Nenhuma dessas APIs documenta atomicidade contra queda do processo, durabilidade
ou compare-and-swap entre processos. Existe uma janela residual entre a última
conferência e a substituição; não executar os dois caminhos simultaneamente.
Se o script ficar ilegível antes de iniciar, ele não consegue executar sua própria
recuperação: o backup local permanece disponível e a distribuição oficial permite
reinstalar somente o código. `recoverHunterUpdate()` roda no próximo fluxo de
atualização quando o script ainda consegue iniciar.

Um updater antigo afetado não instala sua própria correção apenas com um novo
manifest. A recuperação pontual deve preservar a versão instalada e todos os
bytes fora do bloco do updater, com backup privado 0600 e comparação do arquivo
inteiro antes/depois. Não usar instalação/migração ampla nem tocar no nome legado.
Essa recuperação não comprova sincronização no iPhone. O bootstrap alternativo
via MacSend exige origem/arquivo conferidos e autorização para trocar o alvo
genérico. Nas atualizações pelo updater, somente o retorno `installed` e a versão
nova no aparelho confirmam o resultado, não o aceite da notificação ntfy.

[Request](https://docs.scriptable.app/request/) não fornece cancelamento incremental
documentado: limites de 8 KiB (manifest) e 2 MiB (artefato) são validados depois do
download. O hash detecta divergência em relação ao manifest; a origem confiável é
o repositório oficial via HTTPS, não uma assinatura independente desse repositório.

## Referência e validação

Ver [REFACTOR_CHECKPOINTS.md](REFACTOR_CHECKPOINTS.md). Testes usam dados sintéticos,
incluindo contas, vouchers, falhas de disco/rede e concorrência. A comparação de
HTML não substitui a confirmação física no Scriptable; publicação, entrega pelo
MacSend e execução no iPhone são evidências separadas.
