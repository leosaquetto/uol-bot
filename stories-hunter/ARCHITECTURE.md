# Clube UOL Premium: fontes e publicação

O aparelho instala **um único script autocontido**. Não usa `importModule`, File
Bookmarks nem download de código durante a execução normal. As funcionalidades
que já precisavam de rede continuam precisando; UI, ícones e logo ficam no script.

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

- Depois de carregar o dashboard, uma consulta não bloqueante verifica apenas o
  manifest. Uma versão nova usa o banner já existente. Abertura inicial após
  atualização e ações de coleta headless não fazem essa consulta.
- O updater interno e `action: update` verificam e pedem confirmação antes de
  instalar. A nova ação explícita `update-auto` aplica os mesmos controles sem
  alerta nem reabertura automática; a próxima abertura mantém a pausa de coleta.
- Uma automação `Update Scriptable` recebe os avisos ntfy. O título
  `Scriptable - Clube UOL Premium` executa `action: update-auto` antes de consultar
  o Mac; os outros scripts mantêm as ações existentes. O gerador preserva o gatilho
  e os parâmetros originais; detalhes e validação no aparelho em `PREMIUM_MIGRATION.md`.
- `channel` = `beta` no mesmo Dicionário faz adesão explícita; `stable` volta ao
  canal normal. Sem esse campo, preserva a preferência local. Não há downgrade
  automático ao trocar de canal. O retorno ao Atalho contém estado, motivo,
  versão quando disponível e canal, nunca credenciais.

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
o destino, mas não documenta atomicidade contra queda do processo, durabilidade
ou compare-and-swap entre processos. Existe uma janela residual entre a última
conferência e o move; não executar os dois caminhos de atualização simultaneamente.
Se o script ficar ilegível antes de iniciar, ele não consegue executar sua própria
recuperação: o backup local permanece disponível e a distribuição oficial permite
reinstalar somente o código. `recoverHunterUpdate()` roda no próximo fluxo de
atualização quando o script ainda consegue iniciar.

[Request](https://docs.scriptable.app/request/) não fornece cancelamento incremental
documentado: limites de 8 KiB (manifest) e 2 MiB (artefato) são validados depois do
download. O hash detecta divergência em relação ao manifest; a origem confiável é
o repositório oficial via HTTPS, não uma assinatura independente desse repositório.

## Referência e validação

Ver [REFACTOR_CHECKPOINTS.md](REFACTOR_CHECKPOINTS.md). Testes usam dados sintéticos,
incluindo contas, vouchers, falhas de disco/rede e concorrência. A comparação de
HTML não substitui a confirmação física no Scriptable; publicação, entrega pelo
MacSend e execução no iPhone são evidências separadas.
