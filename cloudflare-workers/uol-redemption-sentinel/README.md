# Sentinela de resgate do Clube UOL

Worker separado do monitor existente. Cada conta tem um Durable Object e sua própria trava de uma tentativa por mês. O cadastro aceita várias contas; a campanha inicial usa somente `leo`.

A campanha `zayn-sp-2026-10-10` procura **2 ingressos para Zayn, 10/10/2026, Nubank Parque SP**. O ciclo é de 30 segundos. A campanha termina em **10/10/2026 às 18h de São Paulo** (`2026-10-10T21:00:00Z`). Depois do prazo, nenhum resgate é permitido; a entrega de notificações já pendentes pode continuar.

## Travas

- Artista, data, quantidade, local e conta precisam corresponder à campanha. Divergência ou informação insuficiente bloqueia a tentativa.
- A validação usa os detalhes completos. Título e identificador da oferta, isoladamente, não autorizam resgate. Texto reconhecido na arte pode identificar o artista; conflitos com outros dados bloqueiam a oferta.
- Identidade e sessão devem estar verificadas, e a disponibilidade da cota mensal precisa ser atestada antes da ativação.
- A reserva mensal é gravada antes da requisição de resgate. Erro, timeout, reinício, pausa ou troca de campanha nunca liberam essa reserva.
- O resultado só é confirmado após aparecer no histórico. Uma resposta ambígua não autoriza repetir a requisição.
- A outbox do ntfy possui retry independente: falha de notificação não provoca novo resgate.
- `probe` é somente leitura. O CLI não possui comando de resgate.

## Autenticação e novas contas

O secret `ACCOUNTS_JSON` tem este formato (exemplo fictício):

```json
{
  "leo": {
    "login": "login-da-conta",
    "password": "senha-da-conta",
    "expectedName": "NOME COMPLETO VERIFICADO NO HISTÓRICO",
    "enabled": true
  }
}
```

**Login e senha cadastrados não bastam para habilitar uma conta.** O fluxo validado recupera a sessão do Clube a partir de uma sessão UOL existente. Login do zero sem intervenção humana não foi comprovado; o UOL pode exigir CAPTCHA ou confirmação de acesso. Quando a sessão existente deixa de funcionar, a sentinela bloqueia a conta e avisa. Não há resolução automática de desafios humanos.

Para uma nova conta, é necessário preparar uma sessão SSO válida e confirmar seu login no SAC oficial do UOL. O campo `identity` do bootstrap registra uma verificação feita pelo operador; não transforma uma declaração recebida da conta em prova remota. A trava é associada ao hash do login normalizado. Se diferentes logins pertencerem ao mesmo CPF, configure o mesmo `quotaOwnerKey` privado em todos eles para compartilhar a trava. Uma conta com cota utilizada não deve ser ativada naquele mês.

### Preparar o registro de credenciais

Use Node.js 22 ou superior. Mantenha arquivos privados fora de qualquer repositório Git. O importador exige caminho absoluto de saída, grava com permissão `0600`, mescla por identificador e nunca imprime valores. Ele não publica secrets nem faz deploy.

```sh
UOL_PRIVATE_DIR="$HOME/.config/uol-sentinel"
mkdir -p "$UOL_PRIVATE_DIR"
chmod 700 "$UOL_PRIVATE_DIR"

node scripts/import-account.mjs \
  --input "$HOME/Downloads/credenciais_uol.txt" \
  --output "$UOL_PRIVATE_DIR/accounts.json" \
  --account leo \
  --section Leo \
  --expected-name 'NOME COMPLETO VERIFICADO NO HISTÓRICO'
```

O formato de texto aceito usa a seção `Leo` e os campos `Login:` e `Senha:`. Também aceita JSON com o mesmo formato do registro. Para importar todas as contas de um JSON, omita `--account`; para selecionar uma, informe o identificador. Registros existentes de outros usuários são preservados. O arquivo resultante é o conteúdo do secret `ACCOUNTS_JSON`; publique-o pelo procedimento privado de secrets do Worker, sem copiá-lo ao repositório ou a logs.

### Bootstrap e operação

O CLI usa `https://uol-redemption-sentinel.leosaquetto.workers.dev` por padrão. `--base-url` aceita outra origem HTTPS. O token administrativo vem de um arquivo `0600` por `--token-file` ou de `SENTINEL_ADMIN_TOKEN`. Não passe tokens ou cookies pela linha de comando.

O arquivo privado de bootstrap tem a estrutura abaixo. `cookies` deve conter a sessão real no formato aceito pelo cliente UOL; nunca use os valores fictícios deste exemplo para ativar uma conta.

```json
{
  "cookies": [],
  "identity": {
    "login": "login-verificado-no-SAC",
    "verified": true,
    "source": "https://sac.uol.com.br/"
  },
  "quotaAttestedMonth": "2026-10"
}
```

```sh
node scripts/manage.mjs bootstrap --account leo \
  --token-file "$UOL_PRIVATE_DIR/admin-token" \
  --data "$UOL_PRIVATE_DIR/leo-bootstrap.json"

node scripts/manage.mjs probe --account leo \
  --token-file "$UOL_PRIVATE_DIR/admin-token"

node scripts/manage.mjs status --account leo \
  --token-file "$UOL_PRIVATE_DIR/admin-token"

node scripts/manage.mjs activate --account leo \
  --campaign zayn-sp-2026-10-10 \
  --token-file "$UOL_PRIVATE_DIR/admin-token"

node scripts/manage.mjs pause --account leo \
  --token-file "$UOL_PRIVATE_DIR/admin-token"
```

`activate` habilita o ciclo que pode realizar o resgate real quando todas as condições forem satisfeitas. `pause` interrompe novas tentativas; não desfaz uma requisição já enviada e não remove a trava mensal. Os demais comandos não resgatam benefícios. Não use a rota `/resgatar` ou o botão “Utilizar este benefício” para teste.

### Outra oportunidade

Inclua uma campanha em `campaigns.json` com identificador novo, artista e aliases explícitos, data completa, quantidade, local, cidade, contas permitidas e prazo com fuso horário. Publique o Worker, confirme `status.workerVersion` com a versão publicada e execute `probe` antes de ativar o novo identificador. Uma campanha expirada fica inativa até essa ativação explícita. Se a oportunidade ocorrer em outro mês, pause e refaça o bootstrap com a disponibilidade da cota daquele mês; a trava do mês anterior continua preservada.

## Notificações

A URL do tópico ntfy fica na configuração privada. O remetente usa JSON em `https://ntfy.sh` e só confirma entrega ao serviço com HTTP de sucesso e recibo válido. O aviso contém conta, artista, quantidade, data, local e link geral de “Meus Resgates”; não inclui senha, cookies, código do ingresso ou URL privada de voucher.

Avisos são deduplicados pela outbox. Erros têm backoff persistente entre 1 e 15 minutos. Se o serviço aceitar a publicação e a resposta se perder, o retry pode duplicar o aviso; a trava de resgate continua intacta. Aceite pelo ntfy não comprova recebimento físico no telefone.

## Verificação

```sh
npm test
npm run test:worker
npm run check:bundle
```

Os testes usam credenciais fictícias, SQLite temporário e fetch simulado. O conjunto Worker executa no runtime local com rede externa bloqueada. Não acessam contas reais, publicam mensagens ou acionam resgate. Em produção, `bootstrap`, `probe` e a rota administrativa `probe-artwork` validam somente leitura e OCR; a transação irreversível não é exercitada como validação.

O runtime dos testes usa compatibilidade `2026-08-08`, suportada pelo workerd local instalado; a publicação usa `2026-10-07`. Na verificação de 07/10, passaram 65 testes Node e 10 testes Worker. Dois casos temporizados ficaram sem validação conclusiva no ambiente: um prazo de 40 ms encerrou antes da aquisição do navegador; outro recebeu alarmes automáticos extras porque a data simulada já estava no passado do relógio do runtime. A leitura autenticada do catálogo, a recuperação SSO e o OCR de uma arte pública foram comprovados no Worker publicado, sem resgate nem publicação ntfy de teste.
