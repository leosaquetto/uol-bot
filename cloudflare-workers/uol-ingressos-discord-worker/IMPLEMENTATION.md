# Registro da implementação — UOL Ingressos no Discord

## Objetivo

Criar um monitor remoto e independente do Mac para detectar rapidamente novas
campanhas de ingressos do Clube UOL e enviá-las diretamente a um canal do
Discord.

O piloto é isolado do fluxo existente de Telegram e das automações do
Backstage/GrabNumber.

## Arquitetura implantada

O Worker `uol-ingressos-discord-pilot` executa o seguinte fluxo:

1. um alarme de Durable Object inicia a coleta;
2. o Worker baixa `https://clube.uol.com.br/?order=new` sem utilizar cache;
3. o HTML é processado por `HTMLRewriter`;
4. somente cards da categoria `Ingressos Exclusivos` com links em
   `/campanhasdeingresso/` são considerados;
5. cada campanha recebe um identificador normalizado a partir do link;
6. o estado persistido no KV determina se a campanha já era conhecida;
7. uma campanha nova é enviada ao webhook do Discord;
8. o resultado do envio e o ID da mensagem são persistidos;
9. o Durable Object programa a próxima execução.

O intervalo nominal é de 1 minuto. Como o próximo alarme é programado após a
conclusão da coleta, o intervalo real observado foi de aproximadamente 62
segundos.

## Recursos da Cloudflare

- Worker: `uol-ingressos-discord-pilot`
- URL de saúde:
  `https://uol-ingressos-discord-pilot.leosaquetto.workers.dev/health`
- Durable Object: `TicketAlarm`
- KV binding: `UOL_TICKETS_STATE`
- KV namespace: `633cda9adb884af6a94fa929c640bdda`
- Secret: `DISCORD_WEBHOOK_URL`
- Secret: `ADMIN_TOKEN`
- Modo de entrega: `live`

Os valores dos secrets não são armazenados no GitHub.

## Por que foi usado Durable Object Alarm

A conta já utiliza os cinco Cron Triggers disponíveis no plano gratuito para o
Worker do Backstage. Um sexto Cron Trigger não pôde ser criado.

O Durable Object Alarm:

- não consome um dos cinco slots de Cron Trigger;
- mantém uma única cadeia serializada de execuções;
- permite que o Worker reprograme seu próximo ciclo;
- preserva a independência das automações do Backstage.

O alarme não é um cron de relógio exato. Alguns segundos podem ser acrescentados
ao intervalo por causa do tempo da coleta.

## Baseline e deduplicação

Na primeira execução, todas as campanhas presentes foram registradas como
`baseline` e não foram enviadas. Isso impediu que o canal recebesse ofertas
antigas ao ativar o monitor.

Depois do baseline:

- link novo: estado `pending`;
- envio concluído: estado `sent`, com data e ID da mensagem;
- falha: estado `failed`, elegível para nova tentativa;
- link já registrado: nenhuma nova mensagem.

O estado é limitado às 200 campanhas mais recentes.

## Validações realizadas

- 8 testes automatizados aprovados;
- empacotamento remoto do Wrangler aprovado;
- baseline criado sem envio de oferta antiga;
- webhook configurado como secret;
- mensagem controlada de teste recebida no Discord;
- execução automática em modo `live` confirmada;
- duas execuções consecutivas observadas às `00:28:22` e `00:29:24`;
- consumo observado de 1 a 5 ms de CPU nas execuções otimizadas;
- uma campanha encontrada no navegador público e uma campanha encontrada pelo
  Worker, confirmando a cobertura correta naquele momento;
- nenhuma oferta pendente e nenhuma falha de envio após a ativação.

## Consumo estimado no plano gratuito

Com uma execução por minuto:

- aproximadamente 1.440 invocações por dia;
- aproximadamente 1,44% da franquia de 100 mil requisições diárias;
- aproximadamente 2.880 escritas de linha do Durable Object por dia;
- aproximadamente 2,88% da franquia diária correspondente;
- KV escrito somente quando o baseline ou o estado de uma campanha muda.

O Worker do Backstage não foi alterado.

## Rotas operacionais

- `GET /health`: apresenta estado sanitizado, sem expor secrets ou ofertas;
- `POST /run`: executa coleta manual autenticada;
- `POST /run?send=1`: permite processamento manual com envio;
- `POST /test`: envia uma mensagem de teste sem alterar o estado das ofertas.

As rotas `POST` exigem `Authorization: Bearer <ADMIN_TOKEN>`.

## Limitações atuais

O Worker atual lê os dados disponíveis no card da listagem:

- título;
- link;
- categoria;
- imagem da listagem.

Ele não abre a página de detalhes de cada campanha para capturar validade,
descrição completa, parceiro e outras informações usadas pelo fluxo mais rico
do Telegram.

## Possível evolução para equivalência com o Telegram

A evolução recomendada mantém a listagem leve a cada minuto e enriquece somente
links novos:

1. coletar a listagem;
2. comparar os IDs com o KV;
3. abrir a página de detalhe apenas para campanhas inéditas;
4. extrair título, validade, descrição, parceiro e imagem principal;
5. persistir o registro completo;
6. enviar o embed ao Discord;
7. marcar como enviado somente após a confirmação do webhook.

Não é recomendável abrir novamente os detalhes de todas as campanhas conhecidas
a cada minuto. Isso aumentaria subrequests, CPU e tempo de execução sem melhorar
a detecção de novidades.

Em uma chegada excepcional com muitas campanhas simultâneas, os itens devem
permanecer como `pending` e ser processados em lotes nos ciclos seguintes. Isso
mantém cada invocação dentro dos limites de subrequests e CPU do plano gratuito.

