# Consumo global de Durable Objects

O observador `storage_usage.py` consulta Analytics GraphQL da conta Cloudflare e
envia somente totais de linhas lidas/escritas ao Worker existente. Executa no
Oracle, dentro de `uol-instagram-monitor.service`, independente do Mac e do Codex.
Sua instalação e validação operacional são etapas separadas dos testes locais.

## Coleta e isolamento

- Cadência de quinze minutos, reservada em disco antes da rede. Um único worker
  em segundo plano executa as consultas; não há sobreposição nem fila crescente.
  O loop de Stories nunca aguarda essa rede, inclusive em `--once`/encerramento.
- A consulta cobre o dia UTC corrente e os três dias anteriores, agregando todas
  as namespaces da conta. Ausência do grupo corrente é falha, nunca consumo zero.
- Até duas consultas por rodada: global independente e detalhamento por projeto,
  requisições e armazenamento. Erro parcial/truncamento fica desconhecido. Estado
  local registra limiares de 70/80%, previsão e histerese; ver [CONTINUITY.md](CONTINUITY.md).
- A publicação usa `POST /ingest-storage-usage` com credencial própria
  `STORAGE_USAGE_INGEST_TOKEN`, separada de administração e de Stories.
- Falhas de configuração, Analytics ou ingestão ficam isoladas. O último resultado
  concluído permanece em `status.json` como `storageUsageObserver`; detalhes
  sanitizados ficam em `storage-usage-state.json`. Nenhuma credencial, corpo HTTP
  ou identificador de conta entra no resumo de saúde.

## Credenciais e arquivos privados

O token Cloudflare é somente leitura de Analytics, com expiração em **09/11/2026
(9 de novembro de 2026)**. Não concede publicação, administração ou alteração de
recursos. Sua renovação deve preservar o escopo mínimo e ser concluída antes da
expiração; o monitor não cria nem renova tokens automaticamente.

Código: `/opt/uol-instagram-monitor/storage_usage.py` e `monitor.py`.
Configuração e estado: `/var/lib/uol-instagram-monitor` (0700, usuário do serviço):

- `storage-usage.json`: conta, origem permitida do Worker e caminhos absolutos
  `analyticsTokenFile`/`ingestTokenFile`; os valores dos tokens ficam em arquivos
  privados separados, fora do repositório.
- `storage-usage-state.json`: cadência, tentativas, falhas, última amostra,
  resultado de publicação e totais por dia.
- `storage-usage-snapshots/`: snapshots privados com totais diários e horário da
  observação. Preservam evidência dos dias anteriores mesmo após a virada UTC.

Arquivos devem pertencer ao usuário do serviço, com permissão 0600. Não copiar
sessão, configuração, tokens ou conteúdo privado para logs, commits ou relatórios.
Para diagnóstico, retornar apenas o resultado sanitizado e os totais necessários.

## Guard de gravações

O guard usa a amostra global acrescida dos writes locais desde seu recebimento.
Adia trabalho opcional ao atingir **80.000 writes/dia** ou quando a amostra tem
mais de **30 minutos**, está ausente ou pertence a outro dia UTC. Descoberta
principal, probes críticos, entregas e recibos mantêm suas proteções existentes.
O guard não é um teto absoluto de consumo: outros Workers e trabalho crítico
continuam podendo gravar.

O resumo pode adicionar `optionalWorkDeferred`/`optionalWorkReason` para pressão
de outras dimensões ou previsão. O par validado restringe somente manutenção;
preserva payloads antigos, validação temporal e a reserva de trabalhos críticos.

Às 00h UTC (21h de São Paulo), exige nova amostra do dia. Analytics atrasado ou
indisponível não libera a manutenção. `globalFresh:false` degrada readiness.
`STORAGE_USAGE_GLOBAL_GUARD_ENABLED=false` é rollback explícito para o guard
local; não apaga filas, recibos ou histórico.

## Aceitação ainda pendente

Meta operacional: **menos de 70.000 writes globais em cada dia UTC completo**, por
**três dias completos consecutivos após a ativação**, preservando descoberta e
entregas. O dia parcial da publicação não conta. Usar os snapshots e os totais
atualizados dos dias anteriores para confirmar consumo real; projeção local,
aceite da API e processo ativo não comprovam essa meta.

Os três dias completos ainda estão pendentes. Falha ou expiração do token deixa
o guard sem amostra fresca e mantém trabalho opcional adiado; não suspende o
monitor de Stories por si só.
