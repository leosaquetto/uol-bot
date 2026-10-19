# saquettoOS

Launcher Scriptable com seis apps: Clube UOL Premium, Bilhete Único, Super Calendário, media-downloader, PillTracker e Last.fm Small. O script instalado se chama **saquettoOS**. Grade, dock, busca e painéis usam a estrutura do Tickets for Leo, os logos fornecidos e superfícies inspiradas no UOL.

## Uso

- Abra `saquettoOS` no Scriptable: a primeira tela já contém os apps, com entrada suave em camadas. Aparência acompanha o sistema; Ajustes permite claro/escuro e material vidro/sólido. Favoritos ficam locais.
- A tela inicial respeita a área segura do Scriptable e mantém busca/dock fixos. Painéis têm rolagem própria; se o teclado deixa pouco espaço, somente a grade de apps também pode rolar. **Mostrar nomes dos apps**, em Ajustes, vale para grade e dock e começa desativado.
- Ícones padrão usam vidro neutro no modo escuro e vidro tingido pelo degradê Hedgy no claro, com símbolos claros em ambos. O contorno é suave, sem aro branco. Clube UOL usa o SVG fornecido no mesmo tratamento. Grade e dock têm dimensões iguais; material sólido/redução de transparência mantém fundo opaco.
- Os seis apps usam suas interfaces originais do Scriptable sobre a central. Fechar a tela do app devolve o controle ao saquettoOS. A central pausa sua ponte enquanto o app está ativo; não encerra por limite de 90 segundos. Temas da central continuam disponíveis; cada app conserva sua própria aparência.
- O runtime lê a fonte instalada e exige um SHA-256 em `compatibility.json` antes da execução. Conserva `Script.name`, `module.filename` e URLs do app; `Script.complete()` do filho não encerra a central. Calendário e media-downloader recebem apenas ajustes de `await` em memória para aguardar suas telas. Os scripts instalados, seus arquivos, sessões e Keychain não são reescritos pelo launcher.
- O fluxo padrão não injeta iframe, CSS, localStorage ou fetch nos apps. Eles executam suas próprias rotinas nativas, inclusive permissões, login, coleta, calendário e medicamentos. A opção **Executar fora da central** usa a URL oficial do Scriptable e não promete retorno automático; também serve para uma versão ainda não reconhecida pela central.
- Novo usuário solicita acesso pelo nome e confere o código com Leo. O administrador aprova ou suspende o usuário e escolhe os apps permitidos. Cada instalação tem uma identidade; múltiplos aparelhos não são vinculados automaticamente.
- O servidor confere a permissão antes de cada abertura e atualização de outro app. Sem conexão, a interface abre, mas ações protegidas aguardam acesso verificável. O saquettoOS não pode revogar a execução direta de scripts já instalados.
- Atualizações de código verificadas estão disponíveis para saquettoOS e Clube UOL Premium. Os outros cinco aparecem como **sem fonte configurada**; seus arquivos privados não são publicados pelo launcher.
- Notificações são opt-in. Há teste local, avisos de atualização e mudança de acesso observada durante uma consulta. Isso não é um serviço de push em segundo plano; uma notificação local já agendada pode chegar com o Scriptable fechado.

## Construção e validação

```sh
node saquettoos/build.cjs --preview
node --test saquettoos/test-*.cjs saquettoos/server/test-access.mjs
```

O build lê apenas fontes e assets versionados. Gera um script autocontido em `dist/saquettoOS.js`; a prévia ignorada pelo Git usa dados sintéticos e bloqueia ações nativas. Não lê Keychain, configurações privadas ou outros scripts instalados.

O servidor isolado fica em `server/`. Usa a versão de Wrangler já instalada no Worker UOL vizinho; não modifica seu código, bindings, dados ou deploy. Ver `server/README.md` para o contrato de aprovação.

## Publicação e instalação

1. Executar validações e commitar o artefato público.
2. `node saquettoos/release.cjs prepare COMMIT_COMPLETO` fixa os bytes no canal beta.
3. Após validação, `node saquettoos/release.cjs promote` publica os mesmos bytes em stable; commitar e enviar manifests.
4. Verificar tamanho/hash do artefato remoto antes de instalar ou avisar.
5. `node saquettoos/install.cjs` instala somente `saquettoOS.js` no iCloud local, com backup quando já existe e conferência de hash. Sincronização e execução no iPhone são verificações distintas.

O updater valida origem HTTPS fixa, commit imutável, canal, tamanho, SHA-256, UTF-8, marcadores, versão e sintaxe sem executar o candidato. Faz staging, backup local e journal por app, detecta conflitos e confere o arquivo instalado. Não atualiza dados. O hash depende da confiança no repositório oficial; não é assinatura independente. A API `FileManager.move` não garante CAS nem atomicidade contra queda de processo. Não executar os updaters UOL e saquettoOS simultaneamente para o mesmo app.

Recuperação conserva o backup e qualquer terceiro arquivo desconhecido. Se a substituição ficar ambígua, apresenta `recovery_required`; não repete uma escrita incerta. Não há limpeza automática de backups.

## Administrador e credenciais

O endpoint de bootstrap exige segredo privado e só cria o primeiro administrador. `provision-owner.cjs` guarda a identidade de Leo fora do Git em `~/.config/saquettoos/`, verifica o perfil remoto e permite reconciliar uma resposta perdida sem criar outra identidade. `install.cjs --owner-bootstrap` entrega a identidade verificada em arquivo temporário privado no iCloud. Na primeira abertura, saquettoOS verifica a gravação no Keychain e remove somente esse arquivo temporário. Não distribuir esse bootstrap a outras pessoas.

O script público não contém token de administrador. UI recebe apenas perfil, permissões e estado seguro. O cadastro saquettoOS é separado da aprovação de coleta, sessões e contas UOL. Os scripts nativos continuam dentro da mesma área de confiança do Scriptable; a aprovação do launcher não cria um sandbox entre scripts.

## Atalhos e notificações

Entrada nativa Scriptable aceita dicionário `{ action: 'update-auto' }` para atualizar o próprio saquettoOS; `appId: 'clube-uol'` direciona uma atualização autorizada do UOL. `channel: 'beta'` é opt-in; ausência preserva o canal atual. Resultado contém status verificável, sem credenciais. Atualizações automáticas não abrem/coletam/resgatam benefícios.

Os atalhos auxiliares em `shortcuts/` abrem ou atualizam saquettoOS. Toda distribuição ntfy usa o título **Scriptable - saquettoOS**. A automação pessoal atual baixa e salva o alvo genérico do MacSend; o registro local `saquettoos` aponta para `dist/saquettoOS.js`. Depois de verificar a release remota, preparar esse alvo e conferir filename, tamanho e SHA-256 no endpoint utilizado pelo Atalho antes de enviar o aviso. O aceite ntfy não comprova instalação no iPhone; não repetir envio ambíguo sem conferir o recibo. O build não envia notificações.

Para passar futuramente ao updater interno sem MacSend, adicionar antes do ramo genérico a condição **Título da notificação contém saquettoOS**, executar saquettoOS com `action=update-auto` e encerrar o atalho. Preservar o ramo dedicado ao UOL e `TriggerOutput → Title`. Não alterar a automação pessoal por escrita direta no banco do Atalhos.

## Limites da validação

Testes com mocks verificam controle de acesso, integridade, recuperação e dados preservados. Prévia de navegador verifica a interface; não prova o runtime do Scriptable. Abertura dos seis apps, Rich Notification, permissões iOS, sincronização iCloud e fluidez devem ser confirmadas no iPhone. Nenhum teste deve consumir benefícios UOL, alterar doses/histórico de medicamentos ou gravar calendários reais.

Desde 1.0.7, fechar a central libera a espera por uma avaliação da WebView que não respondeu, recusa avaliações enfileiradas e ignora respostas tardias. Isso não cancela trabalho nativo de um app em execução: seu encerramento continua aguardado. A correção elimina o travamento reproduzido em teste; a recorrência da tela preta no aparelho ainda exige verificação real.

## Execução e retorno (1.0.3)

A [URL oficial de execução](https://docs.scriptable.app/urlscheme/) inicia um script, mas não documenta uma pilha de retorno entre scripts. O caminho padrão usa uma sessão local aguardada: [WebView.present](https://docs.scriptable.app/webview/#-present) e [UITable.present](https://docs.scriptable.app/uitable/#-present) resolvem quando a tela é fechada. A decisão de manter a central apresentada por baixo usa esse contrato; a sobreposição nativa entre central e app ainda precisa de confirmação no iPhone. Links internos que iniciam outra execução por URL seguem o comportamento original do Scriptable.
