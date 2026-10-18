# Clube UOL Premium — migração 5.8.1

Nome instalado: **Clube UOL Premium.js**. O nome técnico `Hunter.js`, diretório
`stories-hunter`, URLs de release e nomes de arquivos de dados permanecem estáveis.
O marcador histórico do cabeçalho também permanece: o updater 5.8.0 o exige para
aceitar a versão nova. Isso é compatibilidade técnica, não o nome do produto.

## Dependências identificadas

| Ponto | Tratamento |
| --- | --- |
| Build/instalação iCloud | Novo nome canônico. Instalação explícita guarda backup antes de substituir e remover o antigo. |
| Scriptable em aparelho existente | A primeira execução 5.8.1 com o nome antigo verifica, guarda backup e move apenas esse script. |
| Updater, reabertura e WebView | Resolvem o nome efetivo da instalação após migração. Journal/backup antigos permanecem disponíveis. |
| Manifest | Campos adicionais `product=clube-uol-premium`, `scriptName=Clube UOL Premium`; protocolo/URLs preservados. |
| MacSend | Target `clube-uol`, origem e rotas preservados; apenas filename/display_name mudam. |
| ntfy | Título `Scriptable - Clube UOL Premium`, identificado pela única automação. Mesmo tópico existente. |
| Atalho `Update Scriptable` | Roteia Premium antes do download; preserva as ações originais para outros scripts. Nenhuma publicação Premium altera o alvo genérico staged. |
| Atalho lançador `Clube UOL Premium` | O existente ainda apontava ao script antigo; novo arquivo fonte aponta ao novo nome. |
| Web clip | URL de abertura atualizada; perfis já instalados não são alterados remotamente. |
| Dados, Keychain, temas e vouchers | Nenhuma renomeação, migração de conteúdo ou regravação. |

Não mantemos dois scripts completos nem um alias executável permanente. Uma
instalação antiga atualiza para 5.8.1 no mesmo arquivo e migra ao executar a versão
nova. Nomes personalizados não são renomeados automaticamente. Se já existem os
dois nomes com conteúdos diferentes, a migração bloqueia em vez de escolher um.
Duplicata byte-idêntica é removida apenas depois de backup e verificação.

`FileManager.move` não promete compare-and-swap entre processos; a janela residual
de concorrência é a mesma limitação documentada do updater. Não executar instalação
Mac/iCloud e updater do aparelho simultaneamente.

## Publicação normal

Build, commit, prepare beta, promote stable e Git/CI seguem `ARCHITECTURE.md`.
Depois de conferir manifest e artefato remotos:

```
node stories-hunter/build.js --code-only
```

Esse comando gera o artefato público e publica **somente o aviso dedicado**. Não
lê bootstrap privado, não grava o script iCloud e não faz stage no MacSend. O
updater instalado cuida de download, verificação, backup e instalação.

### Recuperação pontual pelo MacSend

Se o updater antigo permanece em `preserved`, uma publicação nova não comprova
que ele consegue instalar a própria correção. O proprietário pode preparar a
automação de copiar/substituir o código pelo MacSend como bootstrap explícito.
Antes do aviso ntfy:

1. Conferir release, CI, hash e tamanho remotos; manter o espelho stable sanitizado.
2. Ler `/scriptable/status`. O endpoint genérico `/scriptable` usa o alvo staged;
   se outro produto estiver selecionado, confirmar que sua entrega terminou antes
   de trocar para `clube-uol`. Não sobrescrever uma entrega concorrente.
3. Fazer stage explícito por `/scriptable/stage?target=clube-uol`. Conferir no
   endpoint usado pelo Atalho o filename `Clube UOL Premium.js` e os bytes exatos
   da publicação, antes de enviar um único aviso com o título dedicado.
4. Preservar sessões, Keychain, vouchers, arquivos de dados, nome legado e backups.
   Não usar build legado, instalação/migração ampla ou ACK artificial.

O stage é estado global do MacSend; o GET de código também o atualiza. Não testar
ou trocar alvos durante a entrega. Aceite ntfy, download e instalação no iPhone
são gates distintos. Confirmar a versão no aparelho; a partir da 5.8.4, aberturas
normais tentam atualizar automaticamente e só avisam sucesso com recibo verificado.

Para instalação inicial ou migração explícita do proprietário pelo Mac:

```
node stories-hunter/build.js --code-only --install
```

Esse caminho preserva o bootstrap legado e guarda backup privado em
`~/.config/uol-ticket-research/premium-code-backups/`. Remove o nome antigo somente
depois de ler e comprovar o novo arquivo. Não usar o modo legado sem flags numa
atualização de código, pois ele também sincroniza dados.

`patch-macsend-name.py` altera/verifica somente os dois labels do registro local;
não reinicia, não publica e não muda staged target. Aplicar com o servidor ocioso
e conferir os outros registros depois de reiniciar somente seu subprocesso.

## Novo Atalho

`shortcuts/Update Clube UOL Premium.json` é a fonte versionada. O arquivo
`.shortcut` assinado é importável. Duas ações:

1. **Dicionário**: chave `action`, tipo Texto, valor `update-auto`.
2. **Scriptable → Executar Script**: `Clube UOL Premium`; parâmetro = saída do
   Dicionário; **Mostrar ao Executar** e **Executar no App** desativados.

Não enviar `channel`: stable é padrão, e a preferência beta explícita permanece.
Não há download, gravação de arquivo, menus ou confirmação de instalação nesse
Atalho. Permissão inicial do iOS/Scriptable, se solicitada, pertence ao sistema.
O código recebe a entrada em `args.shortcutParameter`, conforme
[documentação Scriptable](https://docs.scriptable.app/args/).

## Uma automação para todos os scripts

O pedido posterior substitui a configuração de duas automações: manter somente
**Update Scriptable**, com **ntfy → Título contém Scriptable**. O link compartilhado
pelo proprietário forneceu as 16 ações originais e o `WFNotificationTrigger` real.
`shortcuts/unify.cjs` preserva todos esses parâmetros, inclusive a pasta Scriptable,
e acrescenta a decisão antes da gravação:

- Entrada contendo `Clube UOL Premium`: executar diretamente as duas ações nativas
  do updater, sem consultar o Mac. Não depende do atalho dedicado já importado.
- Outra entrada contendo `Scriptable`: baixar pelo endpoint existente. Antes de
  salvar, desviar os nomes Premium atual e antigo para o updater interno. Outros
  scripts continuam pelas mesmas ações de salvar, avisar e abrir.
- Entrada ausente/desconhecida: encerrar sem download nem gravação, com diagnóstico.

`unifyWorkflow` preserva todos os metadados e o único gatilho do original. O arquivo
personalizado contém referência privada à pasta iCloud e não deve ser commitado.
Gerar a partir do JSON privado compartilhado e assinar com `shortcuts sign`.
O gerador e os testes sintéticos são públicos; original, backup e resultado ficam
em `~/.config/uol-ticket-research/premium581/unified/`.

Importar o arquivo **Update Scriptable.shortcut** escolhendo **Substituir** o
existente. Conferir que há apenas um gatilho ntfy, habilitado para executar
imediatamente. A Apple pode desabilitar gatilhos na importação; a preservação no
arquivo não prova que a automação ficou ativada no aparelho.

É necessário validar que a notificação do ntfy chega às ações como entrada
contendo o título. Se vier vazia, o atalho para com diagnóstico: não escolher o
último script do Mac como substituto da identidade da notificação. Não enviar
aviso de teste enquanto o atalho antigo ainda estiver ativo, pois ele grava o
arquivo antes de identificar o produto. O primeiro teste real depende da importação.

O atalho **Update Clube UOL Premium** pode continuar como ação manual opcional;
não requer uma segunda automação. Não alterar ou remover os lançadores dos outros
scripts.

No lançador existente **Clube UOL Premium**, trocar a URL da ação **Abrir URLs**
   por `scriptable:///run/Clube%20UOL%20Premium` (ou importar o lançador atualizado
   quando seu arquivo assinado estiver disponível).

Automações Pessoais e autorização de importação não podem ser criadas pelo CLI
`shortcuts`. O CLI permite listar/executar/visualizar/assinar, não importar sem UI.
Não editar o banco privado de Atalhos para contornar essa limitação. A assinatura
Apple pode falhar por rede; não tratar arquivo preparado como instalado.

## Provas

Testes sintéticos cobrem migração, conflito, duplicata, resposta perdida, backup,
falha de rename, preservação de arquivos e compatibilidade do updater 5.8.0.
Comparação visual permite somente os labels de produto alterados; CSS e outros
outputs permanecem fixados. Não há resgate, login ou troca de sessão nos testes.
Publicação, instalação iCloud, importação do Atalho e execução real no iPhone
continuam sendo gates distintos.
