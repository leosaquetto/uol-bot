# saquettoOS

Launcher Scriptable com seis apps: Clube UOL Premium, Bilhete Único, Super Calendário, media-downloader, PillTracker e Last.fm Small. O script instalado se chama **saquettoOS**. Grade, dock, busca e painéis usam a estrutura do Tickets for Leo, os logos fornecidos e superfícies inspiradas no UOL.

## Uso

- Abra `saquettoOS` no Scriptable. Aparência acompanha o sistema; Ajustes permite claro/escuro e material vidro/sólido. Favoritos ficam locais.
- Os seis apps abrem dentro da central. UOL e Bilhete usam suas WebViews adaptadas; Calendário, media-downloader e PillTracker usam telas web ligadas à lógica original; Last.fm mantém a chave de API no lado nativo. Login, permissões iOS, Photos e compartilhamento podem abrir folhas nativas. O botão Início encerra a sessão interna. Os scripts standalone permanecem intactos.
- O runtime lê a fonte instalada localmente e exige um SHA-256 em `compatibility.json` antes de adaptá-la. Versão desconhecida bloqueia a integração até uma atualização do saquettoOS; a abertura separada é explícita. Nunca copia fontes privadas para este repositório. Ajustes HTML ficam em armazenamento local separado por app, enquanto arquivos e Keychain originais continuam com a lógica original.
- PillTracker exige dados já configurados no app original. Arquivo ausente, JSON inválido, conflito de gravação ou readback divergente bloqueiam novas alterações; a integração não reinicializa dados médicos. Media Downloader começa com URL informada pelo usuário.
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
