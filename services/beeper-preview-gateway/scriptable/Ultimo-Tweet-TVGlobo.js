// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: red; icon-glyph: link;

// Versão 10. Envia o último post ao WhatsApp próprio pelo Beeper/Oracle.
// No Atalhos, deixe Run In App desligado.
// Parâmetro: @usuario, usuario ou https://x.com/usuario.
// Vazio: usa tvglobo. HTML como parâmetro mantém o modo antigo (tvglobo).
// Usa cartão com thumbnail, legenda e link. Cada execução envia novamente.
// Thumbnail: mídia do post; sem mídia, cartão sem imagem.
// O WhatsApp decide o layout final do cartão.
// Texto integral disponível na página; resumo curto só no cartão.
// Link antes do crédito: o WhatsApp iOS oculta o cartão sem a URL no corpo.
// Padrão: título em negrito, texto em monoespaçado, link e crédito.
// A configuração inicial é importada do iCloud para o Keychain.

var perfil = "tvglobo";
var entrada = args.shortcutParameter;
var html = null;
if (entrada !== null && entrada !== undefined && entrada !== "") {
  if (typeof entrada !== "string") {
    throw new Error("Informe um único @ como texto no parâmetro do Atalhos.");
  }
  entrada = entrada.trim();
  if (entrada[0] === "<") html = entrada;
  else if (entrada) {
    perfil = entrada.replace(/^https:\/\/(?:www\.)?(?:x\.com|twitter\.com)\//i, "");
    perfil = perfil.replace(/\/$/, "").replace(/^@/, "");
  }
}
perfil = perfil.toLowerCase();
if (!/^[a-z0-9_]{1,15}$/.test(perfil)) {
  throw new Error("@ inválido. Informe só o usuário ou o link do perfil, sem link de post.");
}
var gatewayToken = await obterToken();

if (html === null || html === undefined) {
  var request = new Request("https://x.com/" + perfil);
  request.timeoutInterval = 20;
  request.headers = { "User-Agent": "Mozilla/5.0" };
  html = await request.loadString();
  if (request.response.statusCode !== 200) {
    throw new Error("O X bloqueou a consulta. HTTP " + request.response.statusCode);
  }
}

if (typeof html !== "string") {
  throw new Error("Passe o HTML como texto ou deixe o parâmetro vazio.");
}

// Reaproveita o nome da página já consultada.
var metaPerfil = lerMetadados(html);
html = html.replace(/<!--[\s\S]*?-->/g, "");
html = html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "");
html = html.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, "");
var artigos = html.match(/<article\b[^>]*>[\s\S]*?<\/article>/gi) || [];
var ultimo = "";

for (var i = 0; i < artigos.length; i++) {
  var links = /<a\b[^>]*\shref\s*=\s*(["'])([^"']+)\1[^>]*>/gi;
  var link;
  while ((link = links.exec(artigos[i])) !== null) {
    var post = link[2].match(/^(?:https:\/\/(?:www\.)?(?:x\.com|twitter\.com))?\/([a-z0-9_]+)\/status\/(\d+)(?:\/?(?:[?#].*)?)$/i);
    if (!post) continue;

    // Só o primeiro permalink identifica o post; os demais podem ser citações.
    if (post[1].toLowerCase() === perfil) {
      var id = post[2];
      if (id.length > ultimo.length) ultimo = id;
      else if (id.length === ultimo.length && id > ultimo) ultimo = id;
    }
    break;
  }
}

if (ultimo === "") {
  throw new Error("O X não entregou posts no HTML. Pode exigir login ou ter alterado a página.");
}

var url = "https://x.com/" + perfil + "/status/" + ultimo;
var detalhes = await obterDetalhes(url);
var titulo = nomeDoPerfil(detalhes.meta, metaPerfil) + " (@" + perfil + ") no X";
// O Beeper recebe Markdown: ** vira negrito no WhatsApp.
// Bloco monoespaçado no texto; código inline destacado só no crédito.
var cerca = "```";
while (detalhes.legenda.indexOf(cerca) !== -1) cerca += "`";
var legendaFormatada = cerca + "\n" + detalhes.legenda + "\n" + cerca;
var mensagem = "ㅤ\n**" + titulo + "**\n" + legendaFormatada + "\n\n🔗 " + url + "\n\n`push by @leosaquetto`";
var envio = new Request("https://163-176-194-58.sslip.io/v1/send-x-post");
envio.method = "POST";
envio.timeoutInterval = 45;
var cabecalhos = {};
cabecalhos["Authorization"] = "Bearer " + gatewayToken;
cabecalhos["Content-Type"] = "application/json";
envio.headers = cabecalhos;
// Nunca encaminhe a credencial a outro endereço por redirecionamento.
envio.onRedirect = function () { return null; };
var preview = {};
preview.title = titulo;
preview.summary = resumoDoCartao(detalhes.legenda);
preview.imageUrl = detalhes.imagem;
var corpo = {};
corpo.link = url;
corpo.text = mensagem;
corpo.preview = preview;
envio.body = JSON.stringify(corpo);

var respostaTexto;
try {
  respostaTexto = await envio.loadString();
} catch (_) {
  throw new Error("Envio sem confirmação. Pode ter chegado; confira seu WhatsApp antes de executar novamente.");
}
var resultado;
try { resultado = JSON.parse(respostaTexto); } catch (_) { resultado = {}; }
var status = envio.response.statusCode;
if ((status !== 200 && status !== 202) || resultado.deliveryState !== "confirmed_by_whatsapp_bridge") {
  var codigo = resultado.code || "resposta_invalida";
  throw new Error("Beeper não confirmou a entrega. HTTP " + status + " / " + codigo);
}
console.log("Post entregue no seu WhatsApp.");
Script.setShortcutOutput(url);
console.log(url);
if (config.runsInApp) Pasteboard.copyString(url);
Script.complete();

async function obterToken() {
  var chave = "tvglobo-beeper-token-v1";
  if (Keychain.contains(chave)) return Keychain.get(chave);
  var fm = FileManager.iCloud();
  var caminho = fm.joinPath(fm.documentsDirectory(), "TVGlobo-Beeper-config.json");
  if (!fm.fileExists(caminho)) {
    throw new Error("A configuração TVGlobo-Beeper-config.json ainda não sincronizou com o Scriptable pelo iCloud.");
  }
  await fm.downloadFileFromiCloud(caminho);
  var dados = JSON.parse(fm.readString(caminho));
  if (typeof dados.token !== "string" || !/^[a-f0-9]{64}$/.test(dados.token)) {
    throw new Error("Configuração do Beeper inválida.");
  }
  Keychain.set(chave, dados.token);
  // Remove somente o arquivo de configuração temporário recém-importado.
  fm.remove(caminho);
  return Keychain.get(chave);
}

function decodificarHtml(texto) {
  var entidades = { amp: "&", quot: '"', apos: "'", lt: "<", gt: ">", nbsp: " " };
  return String(texto || "").replace(/&(#x[0-9a-f]+|#\d+|amp|quot|apos|lt|gt|nbsp);/gi, function (original, valor) {
    if (valor[0] !== "#") return entidades[valor.toLowerCase()];
    var numero = valor[1].toLowerCase() === "x" ? parseInt(valor.slice(2), 16) : parseInt(valor.slice(1), 10);
    return numero >= 0 && numero <= 0x10ffff ? String.fromCodePoint(numero) : original;
  });
}

function lerMetadados(pagina) {
  var tags = pagina.match(/<meta\b[^>]*>/gi) || [];
  var meta = {};
  for (var i = 0; i < tags.length; i++) {
    var atributos = {};
    var regex = /([a-zA-Z_:][a-zA-Z0-9_:.-]*)\s*=\s*(["'])([\s\S]*?)\2/g;
    var atributo;
    while ((atributo = regex.exec(tags[i])) !== null) {
      atributos[atributo[1].toLowerCase()] = decodificarHtml(atributo[3]);
    }
    var nome = atributos.property || atributos.name;
    if (nome) meta[nome.toLowerCase()] = atributos.content || "";
  }
  return meta;
}

function escolherImagem(meta) {
  var candidatas = [meta["og:image"], meta["twitter:image"]];
  var caminho = /^https:\/\/pbs\.twimg\.com\/(?:media|amplify_video_thumb|ext_tw_video_thumb)\//i;
  for (var i = 0; i < candidatas.length; i++) {
    var imagem = String(candidatas[i] || "").trim();
    if (caminho.test(imagem)) return imagem.replace("format=webp", "format=jpg");
  }
  return "";
}

function nomeDoPerfil(metaPost, metaPagina) {
  var nomes = [metaPagina["og:title"], metaPagina["twitter:title"], metaPost["og:title"], metaPost["twitter:title"]];
  var sufixo = new RegExp("^(.*?)\\s*\\(@" + perfil + "\\)(?:\\s+(?:on|no)\\s+(?:X|Twitter))?$", "i");
  for (var i = 0; i < nomes.length; i++) {
    var nome = String(nomes[i] || "").match(sufixo);
    if (nome && nome[1].trim()) return nome[1].trim();
  }
  return perfil === "tvglobo" ? "TV Globo" : perfil;
}

function resumoDoCartao(texto) {
  // Aproxima três linhas; a quebra final depende do WhatsApp e da tela.
  var caracteres = Array.from(texto.replace(/\s+/g, " ").trim());
  return caracteres.length <= 110 ? caracteres.join("") : caracteres.slice(0, 109).join("").trim() + "…";
}

function textoDoPost(pagina, linkPost) {
  var limpa = pagina.replace(/<!--[\s\S]*?-->|<script\b[^>]*>[\s\S]*?<\/script>|<style\b[^>]*>[\s\S]*?<\/style>/gi, "");
  var artigosPost = limpa.match(/<article\b[^>]*>[\s\S]*?<\/article>/gi) || [];
  var relativo = linkPost.replace("https://x.com", "");
  for (var i = 0; i < artigosPost.length; i++) {
    var artigo = artigosPost[i];
    var enderecos = /<a\b[^>]*\shref\s*=\s*(["'])([^"']+)\1/gi;
    var endereco;
    var pertence = false;
    while ((endereco = enderecos.exec(artigo)) !== null) {
      if (endereco[2] === relativo || endereco[2] === linkPost) { pertence = true; break; }
    }
    if (!pertence) continue;
    var divs = /<div\b[^>]*>/gi;
    var div;
    while ((div = divs.exec(artigo)) !== null) {
      if (!/data-testid=["']tweetText["']/i.test(div[0]) &&
          !(/dir=["']auto["']/i.test(div[0]) && /\bwhitespace-pre-wrap\b/.test(div[0]) && /\btext-body\b/.test(div[0]))) continue;
      var tags = /<\/?div\b[^>]*>/gi;
      tags.lastIndex = divs.lastIndex;
      var profundidade = 1;
      var tag;
      while ((tag = tags.exec(artigo)) !== null) {
        profundidade += /^<\//.test(tag[0]) ? -1 : 1;
        if (profundidade !== 0) continue;
        var trecho = artigo.slice(divs.lastIndex, tag.index);
        trecho = trecho.replace(/<a\b([^>]*)>([\s\S]*?)<\/a>/gi, function (original, atributos, conteudo) {
          var href = atributos.match(/\bhref\s*=\s*(["'])([^"']+)\1/i);
          // Links externos podem aparecer abreviados na página, mas o href é integral.
          if (href && /^https?:\/\//i.test(href[2]) && !/^https?:\/\/(?:www\.)?(?:x\.com|twitter\.com)(?:\/|$)/i.test(href[2])) return href[2];
          return conteudo;
        });
        trecho = trecho.replace(/<img\b[^>]*\balt\s*=\s*(["'])([^"']*)\1[^>]*>/gi, "$2");
        trecho = trecho.replace(/<br\s*\/?\s*>|<\/(?:p|div)>/gi, "\n").replace(/<[^>]*>/g, "");
        return decodificarHtml(trecho).trim();
      }
    }
  }
  return "";
}

async function obterDetalhes(linkPost) {
  var req = new Request(linkPost);
  req.timeoutInterval = 20;
  req.headers = { "User-Agent": "Mozilla/5.0" };
  var pagina = await req.loadString();
  if (req.response.statusCode !== 200) {
    throw new Error("O X não disponibilizou a legenda e a imagem. HTTP " + req.response.statusCode);
  }
  var meta = lerMetadados(pagina);
  var textoIntegral = textoDoPost(pagina, linkPost);
  var legenda = textoIntegral || (meta["og:description"] || meta["twitter:description"] || "").trim();
  if (!textoIntegral && legenda.length >= 295 && /(?:…|\.\.\.)$/.test(legenda)) {
    throw new Error("O X entregou apenas uma prévia cortada. Não foi possível obter o texto integral.");
  }
  var imagem = escolherImagem(meta);
  if (!legenda) throw new Error("O X não entregou o texto do post.");
  var detalhes = {};
  detalhes.legenda = legenda;
  detalhes.imagem = imagem;
  detalhes.meta = meta;
  return detalhes;
}
