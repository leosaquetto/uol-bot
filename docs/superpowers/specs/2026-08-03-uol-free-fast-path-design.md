# UOL Free Fast Path Design

## Objetivo

Manter consulta da API UOL a cada 15 segundos e envio imediato ao Telegram principal, sem confirmação HTML e sem serviço novo, dentro do orçamento gratuito do Durable Objects.

## Fluxo

1. Alarme crítico consulta apenas API.
2. Oferta nova é persistida, preparada com dados da própria API e enviada imediatamente ao canal principal.
3. HTML, canal 2, Discord, comentários, esgotamento e saúde continuam na manutenção.
4. Manutenção roda a cada 60 segundos e pode ser antecipada quando houver erro crítico.

## Redução de custo

- Telemetria mutável de cada ciclo fica em snapshots JSON: uma linha para API, HTML, saúde de fontes, webhook e manutenção.
- `source_observations` só atualiza `last_seen_at` a cada 15 minutos, salvo mudança de link ou título.
- Cada handler de alarme grava seu próximo alarme uma vez.
- Saúde expõe estimativa diária e margem contra 100.000 gravações/dia.

## Limites

- `DELIVERY_CONCURRENCY=6`, igual ao limite de conexões simultâneas do Durable Object.
- Nenhuma Queue, banco ou dependência nova.
- Timeout Telegram permanece 8 segundos.
- Em rajada rara, envio direto pode alongar um ciclo. Oferta individual mantém menor latência.

## Compatibilidade e falhas

- Leitura dos snapshots cai para chaves legadas enquanto migração natural não ocorreu.
- Timeout ambíguo continua preferindo duplicata rara a perda da oferta.
- Coletor Discord legado fica desativado quando `COLLECTOR_ENABLED` estiver ausente.
- Nenhum deploy faz parte desta mudança local.

## Validação

- Testes unitários para snapshot, limitação de observações e orçamento diário.
- Testes arquiteturais para caminho crítico, cadências, concorrência e um `setAlarm` por handler.
- Testes Node do Worker, tipos e bundle dry-run.
