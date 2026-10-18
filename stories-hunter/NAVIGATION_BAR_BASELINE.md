# Barra de navegação — modelo Padrão

Referência preservada antes da personalização, em 18/10/2026.

- Versão: Hunter 5.7.8.
- Commit de origem: `e6ad7cb5cb45d9bce6d84f50fbbbde7922ec4f7d`.
- CSS SHA-256 (bloco abaixo, sem espaços externos): `53abfd368e3be1e28d4984e3ad12e045cd4ea91f6114c3b8b2402a6177b5414c`.
- Quatro ícones locais: casa, círculo UOL, cartões e mão com bilhetes.
- Selecionado e ponto: `#ffb300`; demais ícones: branco a 45%.
- Vidro preto a 72%, blur de 24px, saturação de 180%.
- Posição, dimensões, ícones, espaçamento e animações permanecem iguais em todos os temas.
- Padrão é o valor inicial e também a restauração explícita na Personalização.

## CSS original

```css
.bottom-nav {
      position: fixed;
      bottom: calc(env(safe-area-inset-bottom, 0px) + 16px);
      left: 50%;
      transform: translateX(-50%);
      width: fit-content;
      max-width: 90%;
      background: rgba(0, 0, 0, 0.72);
      -webkit-backdrop-filter: blur(24px) saturate(180%);
      backdrop-filter: blur(24px) saturate(180%);
      border: 0.5px solid rgba(255, 255, 255, 0.14);
      box-shadow: 0 12px 36px rgba(0, 0, 0, 0.6), inset 0 1px 1px rgba(255, 255, 255, 0.15);
      border-radius: 36px;
      padding: 6px 14px;
      gap: 12px;
      display: flex;
      align-items: center;
      z-index: 9000;
      user-select: none;
    }
    .bottom-nav-item {
      width: 44px;
      height: 44px;
      border-radius: 22px;
      background: transparent;
      border: none;
      cursor: pointer;
      color: rgba(255, 255, 255, 0.45);
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      position: relative;
      padding: 0;
      transition: color 0.2s cubic-bezier(0.16, 1, 0.3, 1), transform 0.15s ease;
      outline: none;
      -webkit-tap-highlight-color: transparent;
    }
    .bottom-nav-item:active {
      transform: scale(0.92);
    }
    .bottom-nav-item .nav-icon-wrap {
      width: 23px;
      height: 23px;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: transform 0.2s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .bottom-nav-item.active {
      color: #ffb300;
    }
    .bottom-nav-item.active .nav-icon-wrap {
      transform: translateY(-2px);
    }
    .bottom-nav-item .active-dot {
      position: absolute;
      bottom: 5px;
      width: 4px;
      height: 4px;
      border-radius: 50%;
      background: #ffb300;
      box-shadow: 0 0 6px #ffb300;
      opacity: 0;
      transform: scale(0);
      transition: opacity 0.22s ease, transform 0.22s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .bottom-nav-item.active .active-dot {
      opacity: 1;
      transform: scale(1);
    }
```

## Marcação original

```html
<nav class="bottom-nav" id="bottomNav" aria-label="Navegação Principal">
    <button class="bottom-nav-item active" data-tab="home" onclick="switchTab('home')" title="Início" aria-label="Início">
      <div class="nav-icon-wrap">__ICON_navHome__</div>
      <span class="active-dot"></span>
    </button>
    <button class="bottom-nav-item" data-tab="tickets" onclick="switchTab('tickets')" title="Ingressos" aria-label="Ingressos">
      <div class="nav-icon-wrap">__ICON_navTickets__</div>
      <span class="active-dot"></span>
    </button>
    <button class="bottom-nav-item" data-tab="history" onclick="switchTab('history')" title="Histórico" aria-label="Histórico">
      <div class="nav-icon-wrap">__ICON_navHistory__</div>
      <span class="active-dot"></span>
    </button>
    <button class="bottom-nav-item" data-tab="redemptions" onclick="switchTab('redemptions')" title="Resgates" aria-label="Resgates">
      <div class="nav-icon-wrap">__ICON_navRedemptions__</div>
      <span class="active-dot"></span>
    </button>
  </nav>
```
