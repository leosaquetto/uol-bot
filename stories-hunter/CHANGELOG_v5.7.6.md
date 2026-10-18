# Stories Hunter 5.7.6

- Ativação por aparelho: solicitação com código, aprovação individual e coleta
  no iPhone sem depender de arquivo de bootstrap no Mac/iCloud.
- Permissões de coleta e contas pessoais separadas. O proprietário pode liberar
  Leonardo, Gabriel ou ambos para resgate compartilhado. Nenhuma conta pessoal
  acompanha uma aprovação técnica por padrão.
- Sessões autorizadas ficam no Keychain. O app verifica permissão atual antes de
  usar sessões compartilhadas; revogar o aparelho bloqueia esse uso no Hunter.
  Uma cópia da sessão fora do app depende da revogação no próprio UOL.
- Resgate usa transporte nativo separado por conta, confirmação do titular,
  reserva durável antes da requisição e voucher novo correspondente como prova.
  Resposta perdida/erro HTTP conserva incerteza, sem repetir o resgate.
- Histórico preserva datas originais, vouchers e perfis. Textos e mídias dos
  recibos são validados antes de renderizar.
- Home e segunda aba consideram também a data pública de validade da sessão.
  Sessões passadas saem dessas telas; hoje e datas desconhecidas são preservados.
- Editor’s Choice consulta todos os destaques, independente do mês ou da idade.
  Histórico normal continua mensal e progressivo.
- Inclui Futebol e margens dos carrosséis da 5.7.5, que não foi distribuída.
- Atualização pelo próprio Hunter usa uma consulta nativa, valida o artefato,
  mantém backup e verifica a gravação. Sessões e vouchers não são substituídos.

Publicação, validação no aparelho e descoberta natural têm evidências separadas
no documento operacional.
