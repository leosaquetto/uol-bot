# Ensaio de estabilidade dos Stories — 72 horas

O usuário posteriormente autorizou ativar o [monitor integrado ao bot](INSTAGRAM-MONITOR.md). Este ensaio isolado permanece como instrumento de teste; não bloqueia aquela ativação nem inicia um segundo coletor.

Estado em 09/10/2026: código validado com fixtures; **não instalado nem ativado no Oracle**. A leitura HTTP pontual do estudo anterior permanece comprovada. Nesta etapa, os navegadores controlados pelo app não responderam; a alternativa isolada conseguiu abrir a página de login, mas a tentativa seguinte terminou por timeout e não exportou sessão. Isso é falha de infraestrutura, não prova de CAPTCHA, senha inválida ou revogação da conta. As 72 horas não começaram.

## Arquivos

- `instagram.py`: GET autenticado de `/stories/clubeuol/`, parser de JSON inerte e atualização de cookies recebidos. No máximo duas requisições; apenas um redirecionamento observado para o mesmo hostname e caminho. Nunca faz login ou envia senha.
- `trial.py`: prazo persistente, orçamento, histórico SQLite e descoberta de Story novo. Não consulta UOL, envia notificações nem realiza resgate. Links de outros benefícios podem constar no histórico; só a rota `campanhasdeingresso` interessa à futura integração de ingressos.
- `deploy/uol-instagram-trial.service`: serviço proposto para Oracle, conta própria, diretório privado e teto de memória de 64 MiB. Nenhum navegador no servidor.
- `test_instagram.py` e `test_trial.py`: transporte falso, parser, renovação de cookies, reinício, falhas e limites, sem rede.

## Sessão e estado

O usuário autorizou o uso completo da sessão dedicada no Oracle. A sessão deve chegar por SSH em `/var/lib/uol-instagram-trial/session.json`, com dono `uol-instagram-trial`, modo 0600 e diretório 0700. Nunca passar cookies ou senha em argumentos de processo, logs, repositório ou relatórios.

Formato privado esperado: `user_agent`, `headers`, `cookies` e, opcionalmente, `cookie_header` observados na navegação autenticada. O cliente filtra cabeçalhos e cookies, usa CookieJar e salva alterações `Set-Cookie` atomicamente. Isso atualiza cookies fornecidos pelo Instagram; **não comprova renovação de login ou duração ilimitada da sessão**. O cabeçalho Cookie importado é descartado na primeira persistência para não sobrescrever valores novos.

No diretório privado do ensaio:

| Arquivo | Finalidade |
| --- | --- |
| `session.json` | Sessão privada, excluída de provas e backups de relatório |
| `history.sqlite` | Estado durável, observações, Stories e eventos sanitizados |
| `status.json` | Espelho do estado para acompanhamento; SQLite é a fonte durável |
| `trial.lock` | Exclusão de dois processos concorrentes |

O primeiro resultado válido estabelece a base: os Stories já existentes não contam como novos. IDs e destinos deduplicam após reinício. Mudança de destino no mesmo Story gera um evento distinto. Uma falha não apaga o último conjunto conhecido.

URLs persistidas precisam ser públicas e estruturais no hostname exato `clube.uol.com.br`. Parâmetros conhecidos de rastreamento são removidos; queries desconhecidas/privadas e destinos externos são descartados. Não são armazenados HTML bruto, valores de cookies ou parâmetros do redirecionamento Instagram.

## Limites e encerramento

Uma coleta a cada 120–130 segundos após a anterior, uma por vez, até 72 horas contadas da inicialização do estado. Orçamento total: 2.160 tentativas e 4.320 GETs reservados. Cada tentativa reserva dois GETs e o intervalo mínimo em SQLite **antes** da rede. Um crash pode consumir reserva sem resultado, mas reiniciar não recupera esse orçamento nem provoca consulta imediata.

- `found` ou `empty` com reel do perfil correto: observação válida. HTTP 200 sem estrutura reconhecida é inconclusivo.
- 401/403 ou retorno de login/desafio: encerra em `auth_required`.
- 429: encerra em `rate_limited`; nenhuma nova consulta automática neste ensaio.
- Falhas transitórias: espera progressiva, até uma hora; seis falhas consecutivas encerram em `suspended`.
- Falha ao persistir cookies: suspende imediatamente e preserva a contagem da requisição realizada.
- Prazo ou orçamento: encerra sem novas consultas. `completed` significa fim do prazo; não significa aprovação automática da solução.

O estado terminal permanece terminal após reinício do serviço. Um ensaio novo exige diretório novo e sessão válida; preservar o histórico anterior. A unidade só reinicia falha de processo, nunca religa automaticamente um estado terminal. Os limites são conservadores; nenhuma quota segura do Instagram foi comprovada.

## Validação e ativação futura

Testes locais:

```sh
cd services/uol-ticket-research
python3 -m unittest test_instagram test_trial
```

Com sessão válida e permissões privadas preparadas no Oracle, instalar somente `instagram.py` e `trial.py` em `/opt/uol-instagram-trial` (código pertencente a root), além da unidade systemd. A primeira execução termina sem daemon:

```sh
sudo -u uol-instagram-trial python3 -B /opt/uol-instagram-trial/trial.py \
  --data-dir /var/lib/uol-instagram-trial \
  --session-file /var/lib/uol-instagram-trial/session.json --once
```

Ativar o serviço apenas se essa coleta for válida e corresponder ao perfil esperado. Conferir capacidade do Oracle, dono/permissões da sessão e os serviços existentes antes da instalação. Reiniciar o serviço e verificar persistência do estado, sessão e intervalo sem duplicação. Nenhum serviço existente deve ser alterado.

Acompanhar exclusivamente os registros do servidor; não realizar nova consulta ao Instagram pelo Codex para verificar cada ciclo. Depois do prazo, avaliar falhas, capacidade, recuperação, cookies atualizados e chegada de Story novo real. Fixtures comprovam a lógica; um Story real posterior à base comprova a detecção futura. Se nenhum for publicado/observado, esse critério continua pendente.

Entrega ao ntfy e integração com o monitor existente ficam para a etapa seguinte. Não há outbox ou envio implementado neste ensaio. Nenhuma sentinela de resgate foi reativada.
