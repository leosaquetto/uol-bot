# Stories Hunter 5.7.1

- O catálogo e o coletor aceitam o CDN atual comprovado nas ofertas públicas,
  `ddrxgn8ucibei.cloudfront.net`, preservando o CDN anterior e bloqueando outros
  hostnames CloudFront. A 5.7.0 liberava somente o hostname histórico e retornava
  imagens vazias no catálogo atual.
- O host fornece o resultado da coleta no documento inicial e mantém as operações
  nativas de WebView em sequência. Remove a avaliação de JavaScript concorrente
  com a apresentação da janela, introduzida na 5.7.0. É uma correção restrita para
  a tela preta relatada; a confirmação visual depende do aparelho.
- O JSON inicial preserva SVGs e caracteres especiais literalmente, sem expansão
  de sequências de substituição (`$&`, `$'`, `$\``) ou escape duplicado de `<`.
- O topo informa configuração ausente, coleta indisponível, autenticação suspensa,
  intervalos e orçamento por fonte. Atualização orienta reabrir para coletar.
- Novo teste integral executa todo o script da WebView nas quatro abas, com cache
  e catálogo offline; teste do artefato gerado verifica os dados embutidos.

Sessões, vouchers, filas e política de entrega preservados. A coleta nativa termina
antes da apresentação da interface; execuções elegíveis podem adicionar a latência
limitada das fontes à abertura. A execução pelo Atalho continua headless.
