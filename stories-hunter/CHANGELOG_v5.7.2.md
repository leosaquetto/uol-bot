# Stories Hunter 5.7.2

- A janela apresenta primeiro uma tela leve de carregamento com logo embutido,
  texto e indicador indeterminado. Preparação do histórico e consultas nativas
  ocorrem depois da apresentação, sem tela vazia enquanto se aguarda a rede.
- O indicador permanece legível durante pelo menos 800 ms. Depois, um único
  carregamento substitui o documento inicial pelo painel completo. Fechar durante
  o carregamento não reabre a janela nem interrompe trabalho já aceito pelo Worker.
- A fonte Manrope deixa de bloquear a primeira renderização; a fonte do sistema
  aparece imediatamente e a fonte visual é carregada depois da inicialização.
- Falha ao preparar dados mostra um estado de erro legível, preservando os arquivos.
- Home e ativas ocultam sessões com data civil anterior a hoje em São Paulo antes
  de agrupar e contar. Sessões de hoje e ofertas sem data comprovada permanecem;
  o filtro visual não cria esgotamento nem remove registros do histórico.
- Cards comuns dividem a faixa igualmente entre uma, duas ou três datas. Acima
  de três, aparece rolagem horizontal. Destaques conservam suas tags compactas.
- Datas dos cards comuns ficam maiores e mais afastadas do título. Os quatro
  filtros da timeline ficam visíveis em duas linhas, sem começar cortados; títulos
  usam mais espaço, sem truncamento, mantendo a UF junto ao trecho final.
- O botão Atualizar no cabeçalho consulta o catálogo do Worker com ETag, impede
  cliques sobrepostos e mostra atividade. O prazo de dez segundos cobre cabeçalhos
  e corpo; falha preserva o cache. Não inicia coleta extra no Instagram.
- Histórico renderiza lotes de 12 grupos, com carregamento adicional ao avançar
  e botão de continuação. Ano, mês e busca são aplicados antes da paginação.
- O importador privado verifica iCloud e armazenamento local e distingue arquivo
  ausente, inválido e identidade incompatível. Não substitui credenciais existentes.

Abertura da 5.7.1 foi confirmada pelo usuário, com demora. O comportamento da nova
janela de carregamento deve ser confirmado no iPhone após OTA. Testes locais não
comprovam a apresentação real do WKWebView.
