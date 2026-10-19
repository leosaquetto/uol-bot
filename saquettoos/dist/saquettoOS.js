// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: purple; icon-glyph: layer-group;
// saquettoOS Scriptable launcher
const SAQUETTO_VERSION = '1.0.10';
const SAQUETTO_API = 'https://saquettoos-access.leosaquetto.workers.dev';
const SAQUETTO_CATALOG = [{"id":"clube-uol","name":"Clube UOL Premium","scriptName":"Clube UOL Premium","mode":"native","icon":"uol","color":"violet","manifestBase":"https://raw.githubusercontent.com/leosaquetto/uol-bot/main/stories-hunter/releases/","artifactPath":"stories-hunter/dist/Hunter.js","envelope":"uol"},{"id":"bilhete-unico","name":"Bilhete Único","scriptName":"Bilhete Único","mode":"native","icon":"transit","color":"blue"},{"id":"super-calendario","name":"Super Calendário","scriptName":"Super Calendário","mode":"native","icon":"calendar","color":"red"},{"id":"media-downloader","name":"media-downloader","scriptName":"media-downloader","mode":"native","icon":"download","color":"cyan"},{"id":"pilltracker","name":"PillTracker","scriptName":"PillTracker","mode":"native","icon":"pill","color":"pink"},{"id":"lastfm-small","name":"Last.fm Small","scriptName":"Last.fm Small","mode":"native","icon":"music","color":"red"}];
const SAQUETTO_HTML = "\u003c!doctype html>\n\u003chtml lang=\"pt-BR\" data-appearance=\"dark\" data-material=\"glass\">\n\u003chead>\n  \u003cmeta charset=\"utf-8\">\n  \u003cmeta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">\n  \u003cmeta name=\"color-scheme\" content=\"light dark\">\n  \u003cmeta name=\"theme-color\" content=\"#101225\">\n  \u003ctitle>saquettoOS\u003c/title>\n  \u003cstyle>:root{--hedgy-gradient:linear-gradient(110deg,#00a8d3 0%,#1458ec 34%,#763fe6 67%,#d85fdc 100%);font-family:-apple-system,BlinkMacSystemFont,\"SF Pro Display\",\"Segoe UI\",sans-serif;color-scheme:dark;--bg:#0d1020;--text:#f6f7ff;--muted:#afb7d0;--subtle:#858fae;--surface:rgba(28,34,61,.64);--surface-strong:rgba(27,32,54,.96);--line:rgba(221,232,255,.13);--inset:rgba(12,17,36,.32);--accent:#a7b8ff;--accent-solid:#566bfa;--danger:#ff9eab;--green:#8de6c6;--shadow:0 24px 70px rgba(0,0,0,.18);--icon:74px;--icon-surface:linear-gradient(145deg,rgba(255,255,255,.17),rgba(255,255,255,.055) 52%,rgba(255,255,255,.09));--icon-solid:linear-gradient(145deg,#3b3e4c,#252938);--icon-shine:rgba(255,255,255,.1);--icon-shadow:0 7px 17px #00000022,inset 0 1px 3px #ffffff14,inset 0 -2px 4px #00000025;--glyph-fill:linear-gradient(175deg,rgba(255,255,255,.97) 0%,rgba(244,247,255,.87) 32%,rgba(225,229,246,.72) 68%,rgba(185,194,218,.53) 100%);--glyph-depth:rgba(142,153,182,.5);--safe-top:env(safe-area-inset-top,0px);--safe-bottom:env(safe-area-inset-bottom,0px)}\n*{box-sizing:border-box}html{height:100%;overflow:hidden;overscroll-behavior:none;background:var(--bg);-webkit-text-size-adjust:100%}body{margin:0;position:fixed;inset:0;overflow:hidden;overscroll-behavior:none;color:var(--text);font-size:15px;-webkit-font-smoothing:antialiased}button,input,select{font:inherit}button{color:inherit;cursor:pointer;-webkit-tap-highlight-color:transparent}button,input{touch-action:manipulation}button{border:0;background:none}button:disabled{cursor:default;opacity:.45}button:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid var(--accent);outline-offset:4px}button:active:not(:disabled){transform:scale(.97)}input{color:var(--text);min-width:0}svg{width:22px;height:22px;display:block;flex-shrink:0}button svg{pointer-events:none}h1,h2,h3,p{margin:0}[hidden]{display:none!important}.surface{background:var(--surface);border:1px solid var(--line);box-shadow:var(--shadow);-webkit-backdrop-filter:blur(28px) saturate(145%);backdrop-filter:blur(28px) saturate(145%)}\n.wallpaper{position:fixed;inset:0;overflow:hidden;z-index:-1;pointer-events:none;background:radial-gradient(ellipse at 50% 10%,rgba(89,84,157,.16),transparent 55%),var(--bg)}.aurora{position:absolute;border-radius:50%;filter:blur(75px);opacity:.3;transform:rotate(-28deg)}.aurora-one{width:530px;height:500px;background:#142dcb;top:26%;left:calc(50% - 350px)}.aurora-two{width:400px;height:560px;background:#842ee0;top:34%;left:calc(50% + 50px);opacity:.23}.aurora-three{width:420px;height:240px;background:#0d9eaf;top:-180px;left:calc(50% - 320px);opacity:.35}.shell{width:100%;max-width:500px;margin:0 auto;padding:calc(var(--safe-top) + 23px) 25px calc(var(--safe-bottom) + 12px);min-height:100vh;min-height:100dvh;display:flex;flex-direction:column}.topbar{display:flex;justify-content:space-between;align-items:center;gap:12px;min-height:45px}.brand-button{display:flex;align-items:center;gap:9px;padding:0;min-height:44px}.brand-mark{display:flex;align-items:center;justify-content:center;width:33px;height:33px;border-radius:10px;overflow:hidden;background:var(--hedgy-gradient);box-shadow:0 4px 14px #0c4cff25}.brand-mark img{height:127.5%;width:127.5%;max-width:none;flex-shrink:0;object-fit:cover}.brand-mark svg{width:25px;height:25px;color:white}.brand-glyph{display:block;width:100%;height:100%;flex-shrink:0;background:linear-gradient(160deg,#fff,#e9e8fb);-webkit-mask-image:var(--glyph-mask);mask-image:var(--glyph-mask);-webkit-mask-size:contain;mask-size:contain;-webkit-mask-position:center;mask-position:center;-webkit-mask-repeat:no-repeat;mask-repeat:no-repeat}.wordmark{font-size:21px;font-weight:650;letter-spacing:-.8px}.wordmark>span{font-weight:700;background:var(--hedgy-gradient);-webkit-background-clip:text;background-clip:text;color:transparent}.topbar-actions{display:flex;align-items:center;gap:12px}.round-button{width:44px;height:44px;display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--line);border-radius:50%;position:relative;background:var(--inset)}.round-button svg{width:20px;height:20px}.profile-button{width:43px;height:43px;border:1px solid rgba(184,193,255,.45);border-radius:50%;background:var(--hedgy-gradient);display:grid;place-items:center;font-size:13px;letter-spacing:.3px;font-weight:700;color:white;position:relative;box-shadow:inset 0 0 0 3px rgba(255,255,255,.09)}.profile-presence{position:absolute;width:9px;height:9px;right:0;bottom:1px;background:var(--green);border:2px solid var(--bg);border-radius:50%}.profile-presence[data-status=pending]{background:#f6c777}.profile-presence[data-status=revoked]{background:var(--danger)}.notification-dot{position:absolute;right:10px;top:9px;width:6px;height:6px;background:#bec5ff;border-radius:50%;box-shadow:0 0 10px #9aa9ff70}.home-screen{display:flex;flex-direction:column;flex:1}.welcome{padding:41px 0 32px}.eyebrow{font-size:9px;font-weight:750;letter-spacing:1.9px;color:var(--subtle)}.welcome h1{font-size:34px;line-height:1.12;letter-spacing:-1.4px;font-weight:680;margin:12px 0 11px}.welcome h1 span{background:var(--hedgy-gradient);-webkit-background-clip:text;background-clip:text;color:transparent}.welcome-copy{font-size:13px;color:var(--muted);letter-spacing:.05px;line-height:1.5}.library-heading{display:flex;align-items:center;justify-content:space-between;margin-bottom:21px;padding:0 3px}.library-heading h2{font-size:12px;letter-spacing:.4px;font-weight:650;color:var(--muted)}.library-count{font-size:10px;letter-spacing:.25px;color:var(--subtle)}.app-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));column-gap:14px;row-gap:24px;align-content:start}.app-tile{min-width:0;padding:0;display:flex;flex-direction:column;align-items:center;gap:8px;position:relative}.app-icon{display:grid;place-items:center;width:var(--icon);height:var(--icon);border-radius:21px;position:relative;box-shadow:0 7px 16px #06092026,inset 0 1px 1px #ffffff35,inset 0 -1px 1px #00000014;overflow:visible;background:var(--icon-surface);color:#fff}.app-icon>svg{width:34px;height:34px;stroke-width:1.65}.app-icon>img{width:100%;height:100%;object-fit:cover;border-radius:inherit}.app-name{font-size:10.5px;font-weight:530;line-height:1.3;text-align:center;letter-spacing:.02px;width:100%;min-height:27px;display:flex;align-items:flex-start;justify-content:center;color:var(--text);overflow-wrap:anywhere}.app-badge{position:absolute;right:-4px;top:-4px;display:grid;place-items:center;min-width:19px;height:19px;border:2px solid var(--bg);border-radius:10px;background:#9cafff;color:#17214c;font-size:9px;font-weight:800;padding:0 4px;z-index:1}.app-badge svg{width:10px;height:10px;stroke-width:2.4}.app-badge.locked{background:#939bbb;color:#121629}.app-tile[data-available=false] .app-icon{filter:saturate(.6)}.app-tile[data-installed=false] .app-icon{opacity:.72}.app-status{position:absolute;bottom:-4px;height:8px;line-height:1;font-size:8px;letter-spacing:.2px;color:var(--subtle)}.home-bottom{margin-top:auto;padding-top:35px}.update-peek{display:flex;align-items:center;width:100%;padding:13px 0;text-align:left;gap:11px;margin-bottom:15px}.peek-icon{height:32px;width:32px;border:1px solid var(--line);display:grid;place-items:center;border-radius:11px;color:var(--accent);background:var(--inset)}.peek-icon svg{width:16px;height:16px}.update-peek>span:nth-child(2){flex:1;min-width:0}.update-peek strong{font-size:11px;font-weight:600;display:block}.update-peek small{font-size:10px;color:var(--subtle);display:block;margin-top:4px}.peek-arrow{color:var(--subtle)}.peek-arrow svg{width:14px;height:14px}.search-box{height:42px;display:flex;align-items:center;gap:9px;padding:0 14px;border-radius:14px;background:var(--inset);border:1px solid var(--line);margin:0 10px 18px;color:var(--subtle)}.search-box svg{width:16px;height:16px}.search-box input{border:0;background:transparent;outline:0;font-size:16px;width:100%;height:100%;padding:0}.search-box input::placeholder{color:var(--muted)}.search-shortcut{font-size:17px;opacity:.65}.dock{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;border-radius:30px;padding:13px 12px 11px;background:linear-gradient(150deg,rgba(192,205,255,.16),rgba(175,190,241,.06));box-shadow:0 14px 30px #070b1d26,inset 0 1px 1px #ffffff12}.dock .app-icon{width:64px;height:64px;border-radius:19px}.dock .app-icon>svg{width:30px;height:30px}.dock .app-name{font-size:8.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block;min-height:12px;max-width:75px;letter-spacing:-.1px}.dock .app-tile{gap:7px}.dock .app-status{display:none}.dock-placeholder{height:83px;display:grid;place-items:center;color:var(--subtle)}.dock-placeholder svg{width:22px;height:22px;opacity:.45}.home-caption{display:flex;justify-content:center;align-items:center;gap:5px;color:var(--subtle);font-size:8px;letter-spacing:.8px;margin-top:15px}.caption-dot{width:3px;height:3px;background:var(--subtle);border-radius:100%}.empty-search{text-align:center;padding:30px 0;color:var(--muted);font-size:13px}.empty-search>span{display:flex;justify-content:center;margin:0 0 10px}.text-button{min-height:44px;padding:0 10px;color:var(--accent);font-size:12px;font-weight:600}.access-banner{border-radius:18px;padding:15px;margin-bottom:24px;box-shadow:none}.access-banner strong{font-size:12px;font-weight:650}.access-banner p{font-size:11px;line-height:1.55;color:var(--muted);margin-top:5px}.access-banner .text-button{padding:0;min-height:35px}.preview-note{padding:7px 10px;margin:-10px -8px 13px;border:1px dashed var(--line);border-radius:9px;font-size:9px;color:var(--accent);display:flex;justify-content:space-between;gap:8px}.preview-note span{color:var(--subtle)}\n.sheet-backdrop{position:fixed;inset:0;background:rgba(3,7,18,.5);-webkit-backdrop-filter:blur(7px);backdrop-filter:blur(7px);z-index:20;animation:fade-in .18s ease}.sheet{position:fixed;bottom:0;left:50%;transform:translateX(-50%);width:100%;max-width:500px;max-height:90vh;max-height:90dvh;border-radius:30px 30px 0 0;background:var(--surface-strong);z-index:21;display:flex;flex-direction:column;box-shadow:0 -12px 70px #00000035;animation:sheet-in .25s ease;outline:none}.sheet-handle{width:33px;height:4px;border-radius:5px;background:var(--subtle);opacity:.35;flex-shrink:0;margin:10px auto 0}.sheet-header{display:flex;align-items:center;justify-content:space-between;padding:23px 24px 21px;gap:15px;flex-shrink:0}.sheet-header h2{font-size:26px;letter-spacing:-.8px;line-height:1.2;margin-top:6px;font-weight:660}.sheet-header .round-button{width:36px;height:36px;min-width:44px;min-height:44px;background:var(--inset);border-color:transparent}.sheet-content{overflow-y:auto;overscroll-behavior:contain;padding:0 24px calc(var(--safe-bottom) + 28px);-webkit-overflow-scrolling:touch}.panel-intro{font-size:12px;color:var(--muted);line-height:1.6;margin:0 0 23px}.panel-section{margin:23px 0 0}.section-label{font-size:9px;font-weight:700;letter-spacing:1.4px;color:var(--subtle);margin:0 0 10px 2px}.group{border-radius:18px;background:var(--inset);border:1px solid var(--line);overflow:hidden}.group-row{padding:15px;display:flex;align-items:center;gap:11px;min-height:60px;width:100%;text-align:left}.group-row+.group-row{border-top:1px solid var(--line)}.group-row .row-icon{width:29px;height:29px;border-radius:9px;background:var(--line);display:grid;place-items:center;color:var(--accent);flex-shrink:0}.row-icon svg{width:17px;height:17px}.row-text{flex:1;min-width:0}.row-text strong{display:block;font-size:12px;font-weight:600;line-height:1.35}.row-text small{display:block;font-size:10px;line-height:1.5;color:var(--muted);margin-top:4px}.row-chevron{color:var(--subtle)}.row-chevron svg{width:15px;height:15px}.profile-card{display:flex;gap:13px;align-items:center;margin:1px 0 23px;padding:17px;border:1px solid var(--line);border-radius:19px;background:linear-gradient(125deg,#899eff15,#c38aff10)}.profile-avatar{width:47px;height:47px;border-radius:50%;background:var(--hedgy-gradient);display:grid;place-items:center;font-size:16px;color:#fff;font-weight:600;flex-shrink:0}.profile-card h3{font-size:15px;letter-spacing:-.2px}.profile-card p{font-size:10px;color:var(--muted);margin:5px 0 7px}.status-label{font-size:8px;letter-spacing:.3px;padding:4px 7px;border-radius:6px;background:rgba(106,220,183,.1);color:var(--green);display:inline-block}.status-label[data-status=pending]{color:#ddbb78;background:#ddbb7810}.status-label[data-status=revoked]{color:var(--danger);background:#ff8b9e10}.theme-options{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.theme-option{padding:0;min-height:97px;text-align:center;font-size:10px;color:var(--muted);display:flex;flex-direction:column;align-items:center;gap:8px}.theme-preview{width:100%;height:72px;border:1px solid var(--line);border-radius:13px;background:#eeeff6;position:relative;overflow:hidden;display:flex;align-items:center;justify-content:center}.theme-preview:before{content:\"\";height:33px;width:50px;border-radius:7px;background:linear-gradient(140deg,#c8e6ff,#cfc3f6);box-shadow:0 5px 12px #00000015}.theme-preview:after{content:\"\";position:absolute;width:32px;height:6px;border-radius:3px;background:#ffffff90;bottom:24px}.theme-option[data-value=dark] .theme-preview{background:#1d2137}.theme-option[data-value=dark] .theme-preview:before{background:linear-gradient(140deg,#354982,#564778)}.theme-option[data-value=system] .theme-preview{background:linear-gradient(90deg,#eeeff6 50%,#1d2137 50%)}.theme-option[data-value=system] .theme-preview:before{background:linear-gradient(90deg,#c8d7f1 50%,#474d7c 50%)}.theme-option[aria-pressed=true]{color:var(--accent);font-weight:650}.theme-option[aria-pressed=true] .theme-preview{border:2px solid var(--accent);box-shadow:0 0 0 3px #8ca4ff10}.segmented{display:flex;padding:4px;background:var(--inset);border:1px solid var(--line);border-radius:12px;gap:4px}.segmented button{flex:1;min-height:36px;font-size:11px;border-radius:8px;color:var(--muted)}.segmented button[aria-pressed=true]{color:var(--text);background:var(--line);box-shadow:0 2px 7px #0000000a}.segment-icon{display:inline-block;vertical-align:middle;margin-right:5px}.segment-icon svg{width:13px;height:13px}.fine-print{font-size:10px;color:var(--subtle);line-height:1.55;margin-top:10px}.favorite-list{display:flex;gap:8px;flex-wrap:wrap}.favorite-chip{border:1px solid var(--line);background:var(--inset);border-radius:10px;min-height:40px;padding:8px 10px;font-size:10px;color:var(--muted);display:flex;align-items:center;gap:6px}.favorite-chip svg{width:13px;height:13px}.favorite-chip[aria-pressed=true]{border-color:#8ba1ff65;color:var(--accent);background:#839bff15}.version-footer{text-align:center;color:var(--subtle);font-size:9px;letter-spacing:.5px;margin-top:26px}.primary-button{min-height:44px;border-radius:12px;padding:11px 17px;color:#fff;background:var(--accent-solid);font-size:12px;font-weight:600;display:inline-flex;align-items:center;justify-content:center;gap:8px;box-shadow:0 3px 10px #3f51c825}.primary-button svg{width:17px;height:17px}.full-button{width:100%}.secondary-button{min-height:44px;border:1px solid var(--line);background:var(--inset);border-radius:12px;padding:10px 14px;font-size:11px;display:inline-flex;align-items:center;justify-content:center;gap:6px}.secondary-button svg{width:15px;height:15px}.danger-button{color:var(--danger)}.panel-actions{display:flex;gap:9px;margin-top:18px}.panel-actions>*{flex:1}.status-card{padding:24px 16px;text-align:center;border-radius:20px;border:1px solid var(--line);background:linear-gradient(145deg,#8da8ff0a,#ba8dff08);margin-bottom:18px}.status-card>span{display:grid;place-items:center;width:46px;height:46px;border-radius:16px;background:#8fa2ff15;color:var(--accent);margin:0 auto 14px}.status-card h3{font-size:16px;letter-spacing:-.3px;margin-bottom:7px}.status-card p{font-size:11px;color:var(--muted);line-height:1.6;max-width:260px;margin:0 auto}.update-row .mini-icon{width:37px;height:37px;border-radius:11px;flex-shrink:0;background:var(--icon-surface);box-shadow:var(--icon-shadow);color:#eef8ff;display:grid;place-items:center;overflow:hidden}.mini-icon img{width:100%;height:100%;object-fit:cover}.mini-icon svg{width:22px;height:22px}.update-row .secondary-button{font-size:9px;padding:8px 9px;min-height:36px}.version-tag{font-size:9px;color:var(--subtle);white-space:nowrap}.toggle{appearance:none;-webkit-appearance:none;width:38px;height:24px;border-radius:16px;background:var(--line);position:relative;flex-shrink:0;border:1px solid var(--line);cursor:pointer;margin:0}.toggle:after{content:\"\";width:18px;height:18px;background:#fff;border-radius:50%;position:absolute;top:2px;left:2px;box-shadow:0 1px 3px #00000022;transition:transform .15s}.toggle:checked{background:var(--accent-solid);border-color:var(--accent-solid)}.toggle:checked:after{transform:translateX(14px)}.notice-item{padding:15px;border-bottom:1px solid var(--line)}.notice-item:last-child{border-bottom:0}.notice-item h3{font-size:12px;margin-bottom:6px}.notice-item p{font-size:11px;line-height:1.55;color:var(--muted)}.notice-item time{font-size:9px;color:var(--subtle);display:block;margin-top:9px}.notice-item button{padding:0;margin-top:5px}.input-label{display:block;font-size:10px;font-weight:600;color:var(--muted);margin:0 0 7px}.text-input{width:100%;min-height:45px;border:1px solid var(--line);background:var(--inset);border-radius:12px;padding:12px;font-size:16px}.user-card{border:1px solid var(--line);border-radius:16px;padding:15px;background:var(--inset);margin-top:12px}.user-heading{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:11px}.user-heading strong{font-size:12px;overflow-wrap:anywhere}.user-apps{display:flex;flex-direction:column;gap:1px;margin-top:8px}.user-app-option{display:flex;align-items:center;gap:8px;min-height:32px;font-size:10px;color:var(--muted)}.user-app-option input{accent-color:var(--accent-solid);width:16px;height:16px}.inline-error{border-radius:12px;padding:12px 14px;background:#e96f8910;border:1px solid #e96f8930;color:var(--danger);font-size:11px;line-height:1.6;margin-bottom:16px}.loading-row{display:flex;align-items:center;justify-content:center;gap:9px;padding:25px;color:var(--muted);font-size:12px}.spinner{display:inline-block;width:15px;height:15px;border-radius:50%;border:2px solid var(--line);border-top-color:var(--accent);animation:spin .8s linear infinite}.toast{position:fixed;bottom:calc(var(--safe-bottom) + 22px);left:50%;transform:translateX(-50%);z-index:40;width:max-content;max-width:min(90vw,430px);border-radius:15px;padding:14px 17px;font-size:12px;line-height:1.5;background:var(--surface-strong);animation:fade-in .2s}.toast[data-error=true]{color:var(--danger)}.app-stage{display:flex;flex-direction:column;position:fixed;inset:0;background:var(--bg);z-index:10;padding-top:env(safe-area-inset-top,0px)}.app-stage-header{height:59px;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:6px 12px;flex-shrink:0;box-shadow:none;border-width:0 0 1px}.app-stage-header>strong{font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.back-button{display:flex;align-items:center;gap:3px;min-height:44px;min-width:74px;color:var(--accent);font-size:12px;padding:0}.back-button svg{width:19px;height:19px}.app-stage-header>.round-button{width:36px;height:36px;border:0;min-width:44px;min-height:44px}.app-frame-container{flex:1;min-height:0;background:var(--bg)}.app-frame-container iframe{display:block;width:100%;height:100%;border:0;background:var(--bg)}\nhtml[data-appearance=light]{color-scheme:light;--bg:#f0f2f8;--text:#22263d;--muted:#626d88;--subtle:#76829a;--surface:rgba(255,255,255,.61);--surface-strong:rgba(248,250,255,.97);--line:rgba(79,99,149,.12);--inset:rgba(255,255,255,.46);--accent:#5964c5;--accent-solid:#6066dc;--danger:#b63752;--green:#287b65;--shadow:0 20px 60px rgba(47,64,121,.1);--icon-surface:linear-gradient(135deg,rgba(12,132,170,.44),rgba(48,87,180,.55) 36%,rgba(105,65,169,.52) 72%,rgba(153,80,169,.42));--icon-solid:linear-gradient(135deg,#3e9ebc,#597cbe 36%,#8b6abd 72%,#b489c4);--icon-shine:rgba(230,251,255,.3);--icon-shadow:0 6px 15px #425a9020,inset 0 1px 3px #ffffff16,inset 0 -2px 4px #354b7126;--glyph-fill:linear-gradient(175deg,rgba(255,255,255,.98) 0%,rgba(250,247,255,.9) 32%,rgba(239,235,255,.79) 68%,rgba(203,211,240,.61) 100%);--glyph-depth:rgba(86,77,131,.42)}html[data-appearance=light] .wordmark>span{background:var(--hedgy-gradient);-webkit-background-clip:text;background-clip:text;color:transparent}html[data-appearance=light] .brand-mark{background:transparent;box-shadow:none;border-radius:0;overflow:visible}html[data-appearance=light] .brand-mark img{height:120%;width:120%;object-fit:contain}html[data-appearance=light] .brand-glyph{background:var(--hedgy-gradient)}html[data-appearance=light] .wallpaper{background:radial-gradient(ellipse at 10% 0%,#e3f7fc,transparent 55%),radial-gradient(ellipse at 100% 50%,#eee7fa,transparent 65%),var(--bg)}html[data-appearance=light] .aurora{opacity:.12}html[data-appearance=light] .aurora-one{background:#55a5ed}html[data-appearance=light] .aurora-two{background:#a678ee}html[data-appearance=light] .aurora-three{background:#11aeb9;opacity:.12}html[data-appearance=light] .welcome h1 span{background:var(--hedgy-gradient);-webkit-background-clip:text;background-clip:text}html[data-appearance=light] .dock{background:linear-gradient(135deg,#ffffffa1,#ffffff4f);box-shadow:0 13px 35px #526baa14,inset 0 1px 1px #ffffffcc}html[data-appearance=light] .status-label[data-status=pending]{color:#89621d}html[data-material=solid] .surface{backdrop-filter:none;-webkit-backdrop-filter:none;background:var(--surface-strong)}html[data-material=solid] .aurora{display:none}html[data-material=solid] .dock{background:var(--surface-strong)}html[data-material=solid] .app-icon,html[data-material=solid] .mini-icon{background:var(--icon-solid)}\n/* Two composited color fields move behind the glass, with no sensor or filter animation. */\nhtml[data-appearance=light] .app-grid{position:relative;isolation:isolate}\nhtml[data-appearance=light] .app-grid:before,html[data-appearance=light] .app-grid:after{content:\"\";position:absolute;inset:-32px -8px -28px;z-index:-1;pointer-events:none}\nhtml[data-appearance=light] .app-grid:before{background:radial-gradient(ellipse 108px 98px at 23% 65%,rgba(16,174,207,.5) 0%,rgba(29,187,213,.35) 32%,rgba(29,187,213,0) 76%);animation:glass-orb-cyan 14s ease-in-out infinite alternate}\nhtml[data-appearance=light] .app-grid:after{background:radial-gradient(ellipse 112px 102px at 74% 36%,rgba(134,72,208,.46) 0%,rgba(145,83,213,.31) 34%,rgba(145,83,213,0) 76%);animation:glass-orb-violet 18s ease-in-out -6s infinite alternate}\n@keyframes glass-orb-cyan{from{transform:translate3d(-16px,18px,0)}to{transform:translate3d(42px,-20px,0)}}\n@keyframes glass-orb-violet{from{transform:translate3d(17px,-16px,0)}to{transform:translate3d(-35px,27px,0)}}\n.app-tile[data-open-app=\"pilltracker\"] .app-glyph{width:56%;height:56%}\n.app-tile[data-open-app=\"media-downloader\"] .app-glyph{width:62%;height:62%}\n.app-tile[data-open-app=\"bilhete-unico\"] .app-glyph,.app-tile[data-open-app=\"lastfm-small\"] .app-glyph{width:64.6%;height:64.6%}\n.app-tile[data-open-app=\"lastfm-small\"] .app-glyph:before{display:none}\nhtml[data-material=solid] .app-grid:before,html[data-material=solid] .app-grid:after{display:none}\nhtml[data-material=solid] .app-glyph{--glyph-fill:linear-gradient(175deg,#fff,#e1e5f3)}\n@keyframes spin{to{transform:rotate(360deg)}}@keyframes fade-in{from{opacity:0}to{opacity:1}}@keyframes sheet-in{from{transform:translate(-50%,30px);opacity:.4}to{transform:translate(-50%,0);opacity:1}}\n/* Home is a fixed phone canvas; only sheets and app content own vertical scrolling. */\n.shell{--shell-inset:25px;--grid-gap:14px;--tile-size:min(var(--icon),calc((min(100vw,500px) - var(--shell-inset)*2 - var(--grid-gap)*3)/4));height:100vh;height:100dvh;height:var(--viewport-height,100dvh);min-height:0;overflow:hidden;padding:calc(var(--safe-top) + 20px) 25px max(12px,var(--safe-bottom));isolation:isolate}\n.topbar{flex-shrink:0}.home-screen{flex:1;min-height:0;overflow:hidden}.welcome{flex-shrink:0;padding:30px 0 25px}.welcome h1{font-size:32px;margin:10px 0}.welcome-copy{font-size:13px}.library-heading{flex-shrink:0;min-height:36px;margin-bottom:16px;gap:10px}.library-count{margin-left:auto}.update-peek{width:36px;height:36px;min-width:36px;margin:0;padding:0;gap:0;justify-content:center;border-radius:12px}.update-peek .peek-icon{width:32px;height:32px}.update-peek>span:nth-child(2){position:absolute;width:1px;height:1px;padding:0;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap}.update-peek .peek-arrow{display:none}\n.app-grid{flex-shrink:0;row-gap:24px;column-gap:14px;padding:4px 0}.app-tile{min-height:44px}.app-icon{width:var(--tile-size);height:auto;aspect-ratio:1;border-radius:28%;box-shadow:var(--icon-shadow)}.app-icon:before{content:\"\";position:absolute;inset:1px;border-radius:inherit;background:radial-gradient(ellipse at 24% 0%,var(--icon-shine),transparent 64%);pointer-events:none}.app-icon>svg{width:68%;height:68%;stroke-width:1.5;position:relative;filter:drop-shadow(0 2px 4px #00000017)}.app-glyph{display:block;width:68%;height:68%;position:relative;pointer-events:none}.app-glyph:before,.app-glyph:after{content:\"\";position:absolute;inset:0;-webkit-mask-image:var(--glyph-mask);mask-image:var(--glyph-mask);-webkit-mask-size:contain;mask-size:contain;-webkit-mask-position:center;mask-position:center;-webkit-mask-repeat:no-repeat;mask-repeat:no-repeat}.app-glyph:before{background:var(--glyph-depth);transform:translateY(.8px)}.app-glyph:after{background:var(--glyph-fill)}.app-icon-artwork:before,.app-icon-artwork:after{display:none}.app-name{min-height:28px;font-size:10.5px}.app-status{position:static;font-size:8px;height:auto}.app-tile:has(.app-status){gap:5px}html[data-show-app-names=false] .app-name{display:none}.home-bottom{flex-shrink:0;position:relative;margin-top:auto;padding-top:16px}.search-box{height:44px;flex-shrink:0;margin:0 6px 14px}.dock{min-height:88px;align-items:start;gap:6px;padding:11px 3px;border-radius:29px}.dock .app-icon{width:var(--tile-size);height:auto;border-radius:28%}.dock .app-icon>svg{width:68%;height:68%}.dock .app-name{min-height:12px;font-size:8.5px}.dock-placeholder{height:var(--tile-size)}.home-caption{margin-top:10px;font-size:8px;line-height:12px}.empty-search{flex-shrink:1;min-height:0;padding:18px 0 0}.access-banner{flex-shrink:0;max-height:115px;overflow-y:auto;margin-bottom:10px;padding:10px 12px;border-radius:14px}.access-banner p{margin-top:3px}.access-banner .text-button{min-height:32px}\n.sheet{max-height:calc(var(--viewport-height,100dvh) - var(--safe-top) - 20px);bottom:calc(100% - var(--viewport-height,100%))}.sheet-content{min-height:0}.toast{bottom:calc(100% - var(--viewport-height,100%) + var(--safe-bottom) + 16px)}\n.shell[data-enter=true] .topbar{animation:home-layer-in .85s cubic-bezier(.2,.7,.2,1) .12s both;--entry-y:12px;--entry-scale:.99}.shell[data-enter=true] .welcome{animation:home-layer-in .9s cubic-bezier(.2,.7,.2,1) .2s both;--entry-y:20px;--entry-scale:.98}.shell[data-enter=true] .library-heading{animation:home-layer-in .9s cubic-bezier(.2,.7,.2,1) .27s both;--entry-y:26px;--entry-scale:.98}.shell[data-enter=true] #app-grid [data-entrance] .app-icon{animation:home-layer-in .9s cubic-bezier(.2,.7,.2,1) calc(.32s + var(--entrance-index)*.045s) both;--entry-y:34px;--entry-scale:.9}.shell[data-enter=true] .home-bottom{animation:home-layer-in .9s cubic-bezier(.2,.7,.2,1) .7s both;--entry-y:24px;--entry-scale:.97}\n@keyframes home-layer-in{from{opacity:0;transform:translate3d(0,var(--entry-y,22px),0) scale(var(--entry-scale,.96))}to{opacity:1;transform:translate3d(0,0,0) scale(1)}}\n@media(min-width:600px) and (min-height:800px){.shell{--grid-gap:20px;padding-top:calc(var(--safe-top) + 35px);padding-bottom:max(22px,var(--safe-bottom))}.welcome{padding-top:40px;padding-bottom:30px}.welcome h1{font-size:40px}.app-grid{column-gap:20px;row-gap:29px}.app-name{font-size:11px}.dock{padding:14px 3px}.sheet{bottom:22px;border-radius:28px;max-height:calc(var(--viewport-height,100dvh) - var(--safe-top) - 44px)}.sheet-content{padding-bottom:27px}}\n@media(max-width:374px){:root{--icon:64px}.shell{--shell-inset:18px;--grid-gap:9px;padding-left:18px;padding-right:18px}.wordmark{font-size:19px}.brand-mark{width:29px;height:29px}.topbar-actions{gap:7px}.welcome h1{font-size:29px}.app-grid{column-gap:9px;row-gap:20px}.app-name{font-size:9.5px}.dock{padding-left:3px;padding-right:3px}.dock .app-name{font-size:8px}.sheet-content{padding-left:18px;padding-right:18px}.sheet-header{padding-left:18px;padding-right:18px}}\n@media(max-height:800px){.welcome{padding:24px 0 18px}.welcome h1{font-size:30px;margin:9px 0 0}.welcome-copy{display:none}.app-grid{row-gap:20px}.library-heading{margin-bottom:12px}}\n@media(max-height:700px){:root{--icon:66px}.shell{padding-top:calc(var(--safe-top) + 14px);padding-bottom:max(10px,var(--safe-bottom))}.welcome{padding:19px 0 15px}.welcome h1{font-size:28px}.app-grid{row-gap:16px}.app-name{min-height:25px;font-size:10px}.home-bottom{padding-top:12px}.dock{min-height:78px;padding-top:10px;padding-bottom:10px}.search-box{margin-bottom:11px}.home-caption{margin-top:7px}}\n@media(max-height:600px){.welcome{padding:14px 0}.welcome .eyebrow{display:none}.welcome h1{font-size:25px;margin:0}.library-heading{margin-bottom:7px;min-height:30px}.app-grid{row-gap:12px}.home-caption{display:none}.home-bottom{padding-top:10px}}\n@media(max-height:480px){.shell{padding-top:calc(var(--safe-top) + 7px);padding-bottom:max(7px,var(--safe-bottom));--icon:44px}.topbar{min-height:40px}.welcome,.preview-note,.library-heading{display:none}.app-grid{row-gap:8px;padding-top:8px;flex-shrink:1;min-height:0;grid-auto-rows:max-content;align-content:start;overflow-y:auto;overscroll-behavior:contain}.app-tile{gap:4px}.app-name{font-size:9px;min-height:20px}.home-bottom{padding-top:7px}.search-box{height:42px;margin-bottom:8px}.dock{min-height:62px;padding:8px 3px;border-radius:22px}.dock .app-name{font-size:8px;min-height:11px}.home-caption{display:none}.access-banner{max-height:65px}}\nhtml[data-compact-viewport=true] .shell{padding-top:calc(var(--safe-top) + 7px);padding-bottom:max(7px,var(--safe-bottom));--icon:44px}html[data-compact-viewport=true] .topbar{min-height:40px}html[data-compact-viewport=true] .welcome,html[data-compact-viewport=true] .preview-note,html[data-compact-viewport=true] .library-heading{display:none}html[data-compact-viewport=true] .app-grid{row-gap:8px;padding-top:8px;flex-shrink:1;min-height:0;grid-auto-rows:max-content;align-content:start;overflow-y:auto;overscroll-behavior:contain}html[data-compact-viewport=true] .app-tile{gap:4px}html[data-compact-viewport=true] .app-name{font-size:9px;min-height:20px}html[data-compact-viewport=true] .home-bottom{padding-top:7px}html[data-compact-viewport=true] .search-box{height:42px;margin-bottom:8px}html[data-compact-viewport=true] .dock{min-height:62px;padding:8px 3px;border-radius:22px}html[data-compact-viewport=true] .dock .app-name{font-size:8px;min-height:11px}html[data-compact-viewport=true] .home-caption{display:none}html[data-compact-viewport=true] .access-banner{max-height:65px}\n@media(max-height:480px){html[data-appearance=light] .app-grid:before,html[data-appearance=light] .app-grid:after{inset:0}}\nhtml[data-appearance=light][data-compact-viewport=true] .app-grid:before,html[data-appearance=light][data-compact-viewport=true] .app-grid:after{inset:0}\n@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation:none!important;transition:none!important;scroll-behavior:auto!important}button:active:not(:disabled){transform:none}}\n@media(prefers-reduced-transparency:reduce){.app-grid:before,.app-grid:after{display:none}.app-glyph{--glyph-fill:linear-gradient(175deg,#fff,#e1e5f3)}.surface,.sheet-backdrop{backdrop-filter:none;-webkit-backdrop-filter:none}.surface,.dock{background:var(--surface-strong)}.app-icon,.update-row .mini-icon{background:var(--icon-solid)}}\n\u003c/style>\n\u003c/head>\n\u003cbody>\n  \u003cdiv class=\"wallpaper\" aria-hidden=\"true\">\u003cdiv class=\"aurora aurora-one\">\u003c/div>\u003cdiv class=\"aurora aurora-two\">\u003c/div>\u003cdiv class=\"aurora aurora-three\">\u003c/div>\u003c/div>\n  \u003cmain id=\"shell\" class=\"shell\">\n    \u003cdiv id=\"preview-note\" class=\"preview-note\" hidden>Prévia de interface \u003cspan>Sem conexão com o Scriptable\u003c/span>\u003c/div>\n    \u003cheader class=\"topbar\">\n      \u003cbutton class=\"brand-button\" type=\"button\" data-action=\"home\" aria-label=\"Início do saquettoOS\">\n        \u003cspan class=\"brand-mark\" id=\"brand-mark\" aria-hidden=\"true\">\u003c/span>\n        \u003cspan class=\"wordmark\">saquetto\u003cspan>OS\u003c/span>\u003c/span>\n      \u003c/button>\n      \u003cdiv class=\"topbar-actions\">\n        \u003cbutton id=\"notifications-button\" class=\"round-button\" type=\"button\" data-panel=\"notifications\" aria-label=\"Abrir notificações\">\u003cspan data-icon=\"bell\">\u003c/span>\u003cspan class=\"notification-dot\" id=\"notification-dot\" hidden>\u003c/span>\u003c/button>\n        \u003cbutton id=\"profile-button\" class=\"profile-button\" type=\"button\" data-panel=\"settings\" aria-label=\"Abrir perfil e ajustes\">\u003cspan id=\"profile-initials\">S\u003c/span>\u003cspan class=\"profile-presence\" id=\"profile-presence\">\u003c/span>\u003c/button>\n      \u003c/div>\n    \u003c/header>\n\n    \u003csection id=\"home-screen\" class=\"home-screen\" aria-label=\"Tela inicial\">\n      \u003cdiv class=\"welcome\">\n        \u003cp class=\"eyebrow\" id=\"greeting\">SEU ESPAÇO. SEUS APPS.\u003c/p>\n        \u003ch1>Tudo no seu \u003cspan>lugar.\u003c/span>\u003c/h1>\n        \u003cp class=\"welcome-copy\" id=\"welcome-copy\">Uma casa para os seus scripts.\u003c/p>\n      \u003c/div>\n      \u003cdiv id=\"access-banner\" class=\"access-banner surface\" hidden>\u003c/div>\n      \u003cdiv class=\"library-heading\">\u003ch2>Seus apps\u003c/h2>\u003cspan class=\"library-count\" id=\"library-count\">\u003c/span>\u003cbutton type=\"button\" id=\"update-peek\" class=\"update-peek\" data-panel=\"updates\" aria-labelledby=\"update-peek-title\">\u003cspan class=\"peek-icon\" data-icon=\"sparkles\">\u003c/span>\u003cspan>\u003cstrong id=\"update-peek-title\">Seu espaço, sempre em dia\u003c/strong>\u003csmall id=\"update-peek-copy\">Confira as versões dos seus apps\u003c/small>\u003c/span>\u003cspan class=\"peek-arrow\" data-icon=\"chevron\">\u003c/span>\u003c/button>\u003c/div>\n      \u003cdiv id=\"app-grid\" class=\"app-grid\" role=\"list\" aria-label=\"Aplicativos\">\u003c/div>\n      \u003cdiv id=\"search-empty\" class=\"empty-search\" hidden>\u003cspan data-icon=\"search\">\u003c/span>\u003cp>Nenhum app encontrado.\u003c/p>\u003cbutton class=\"text-button\" type=\"button\" data-action=\"clearSearch\">Limpar busca\u003c/button>\u003c/div>\n      \u003cdiv class=\"home-bottom\">\n        \u003clabel class=\"search-box\">\u003cspan data-icon=\"search\">\u003c/span>\u003cinput id=\"app-search\" type=\"search\" placeholder=\"Buscar nos seus apps\" autocomplete=\"off\" autocapitalize=\"off\" aria-label=\"Buscar aplicativos\">\u003cspan class=\"search-shortcut\" aria-hidden=\"true\">⌕\u003c/span>\u003c/label>\n        \u003cnav id=\"dock\" class=\"dock surface\" aria-label=\"Apps favoritos\">\u003c/nav>\n        \u003cdiv class=\"home-caption\">\u003cspan class=\"caption-dot\">\u003c/span>\u003cspan id=\"home-caption\">saquettoOS\u003c/span>\u003c/div>\n      \u003c/div>\n    \u003c/section>\n\n    \u003csection id=\"app-stage\" class=\"app-stage\" hidden aria-label=\"Aplicativo aberto\">\n      \u003cheader id=\"app-stage-header\" class=\"app-stage-header surface\">\u003cbutton type=\"button\" class=\"back-button\" data-action=\"home\">\u003cspan data-icon=\"back\">\u003c/span>Início\u003c/button>\u003cstrong id=\"app-stage-name\">\u003c/strong>\u003cbutton type=\"button\" class=\"round-button\" data-action=\"standalone\" aria-label=\"Abrir app no Scriptable\">\u003cspan data-icon=\"external\">\u003c/span>\u003c/button>\u003c/header>\n      \u003cdiv id=\"app-frame-container\" class=\"app-frame-container\">\u003c/div>\n    \u003c/section>\n  \u003c/main>\n\n  \u003cdiv id=\"sheet-backdrop\" class=\"sheet-backdrop\" hidden>\u003c/div>\n  \u003csection id=\"sheet\" class=\"sheet surface\" role=\"dialog\" aria-modal=\"true\" aria-labelledby=\"sheet-title\" hidden tabindex=\"-1\">\n    \u003cdiv class=\"sheet-handle\" aria-hidden=\"true\">\u003c/div>\n    \u003cheader class=\"sheet-header\">\u003cdiv>\u003cp class=\"eyebrow\" id=\"sheet-eyebrow\">SAQUETTOOS\u003c/p>\u003ch2 id=\"sheet-title\">Ajustes\u003c/h2>\u003c/div>\u003cbutton type=\"button\" class=\"round-button\" id=\"close-sheet\" aria-label=\"Fechar painel\">\u003cspan data-icon=\"close\">\u003c/span>\u003c/button>\u003c/header>\n    \u003cdiv id=\"sheet-content\" class=\"sheet-content\">\u003c/div>\n  \u003c/section>\n  \u003cdiv id=\"toast\" class=\"toast surface\" role=\"status\" aria-live=\"polite\" hidden>\u003c/div>\n  \u003cscript id=\"saquetto-initial\" type=\"application/json\">__SAQUETTO_INITIAL_JSON__\u003c/script>\n  \u003cscript>(function () {\n  'use strict';\n\n  const $ = (selector) => document.querySelector(selector);\n  const escapeHTML = (value) => String(value == null ? '' : value).replace(/[&\u003c>\"']/g, (char) => ({ '&': '&amp;', '\u003c': '&lt;', '>': '&gt;', '\"': '&quot;', \"'\": '&#39;' })[char]);\n  const paths = {\n    bell: '\u003cpath d=\"M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4\"/>',\n    search: '\u003ccircle cx=\"10.5\" cy=\"10.5\" r=\"6.5\"/>\u003cpath d=\"m16 16 4.5 4.5\"/>',\n    close: '\u003cpath d=\"m6 6 12 12M18 6 6 18\"/>',\n    chevron: '\u003cpath d=\"m9 5 7 7-7 7\"/>',\n    back: '\u003cpath d=\"m15 5-7 7 7 7\"/>',\n    external: '\u003cpath d=\"M14 3h7v7m-1-6-9 9M10 4H4v16h16v-6\"/>',\n    sparkles: '\u003cpath d=\"m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5L12 3ZM20 2v4m-2-2h4\"/>',\n    settings: '\u003cpath d=\"m9 3-1 3-3 1 1 3-2 2 2 2-1 3 3 1 1 3h6l1-3 3-1-1-3 2-2-2-2 1-3-3-1-1-3Z\"/>\u003ccircle cx=\"12\" cy=\"12\" r=\"3\"/>',\n    download: '\u003cpath d=\"M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5\"/>',\n    refresh: '\u003cpath d=\"M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 11.5-2L20 8M4 16l2.5 3A7 7 0 0 0 18 17\"/>',\n    check: '\u003cpath d=\"m5 12 4 4L19 6\"/>',\n    lock: '\u003crect x=\"5\" y=\"10\" width=\"14\" height=\"11\" rx=\"3\"/>\u003cpath d=\"M8 10V7a4 4 0 0 1 8 0v3M12 14v3\"/>',\n    users: '\u003ccircle cx=\"9\" cy=\"8\" r=\"3\"/>\u003cpath d=\"M3 20v-3a6 6 0 0 1 12 0v3M16 5a3 3 0 0 1 0 6m2 3a5 5 0 0 1 3 4v2\"/>',\n    user: '\u003ccircle cx=\"12\" cy=\"8\" r=\"4\"/>\u003cpath d=\"M4 21v-2a8 8 0 0 1 16 0v2\"/>',\n    plus: '\u003cpath d=\"M12 5v14M5 12h14\"/>',\n    heart: '\u003cpath d=\"M12 21 3.5 12.5A5.7 5.7 0 0 1 12 5a5.7 5.7 0 0 1 8.5 7.5Z\"/>',\n    glass: '\u003cpath d=\"m12 2 9 5v10l-9 5-9-5V7l9-5ZM3 7l9 5 9-5M12 12v10\"/>',\n    solid: '\u003crect x=\"4\" y=\"4\" width=\"16\" height=\"16\" rx=\"4\"/>',\n    ticket: '\u003cpath d=\"M3 6h18v4a2 2 0 0 0 0 4v4H3v-4a2 2 0 0 0 0-4V6ZM15 6v2m0 3v2m0 3v2\"/>',\n    transit: '\u003crect x=\"5\" y=\"3\" width=\"14\" height=\"16\" rx=\"4\"/>\u003cpath d=\"M5 11h14M8 6h8M8 22l2-3m6 3-2-3M8 15h1m6 0h1\"/>',\n    calendar: '\u003crect x=\"3\" y=\"5\" width=\"18\" height=\"16\" rx=\"3\"/>\u003cpath d=\"M7 3v4m10-4v4M3 10h18M7 14h2m4 0h2m-8 3h2\"/>',\n    media: '\u003cpath d=\"M5 5h14v14H5V5Z\"/>\u003cpath d=\"m10 8 6 4-6 4V8Z\" fill=\"currentColor\" stroke=\"none\"/>\u003cpath d=\"M2 8v8m20-8v8\"/>',\n    pill: '\u003cpath d=\"M4 12 12 4a5.7 5.7 0 0 1 8 8l-8 8a5.7 5.7 0 0 1-8-8Zm4-4 8 8\"/>\u003cpath d=\"m5.5 13 2-2\"/>',\n    music: '\u003cpath d=\"M9 18V5l12-2v13M9 8l12-2\"/>\u003cellipse cx=\"6\" cy=\"18\" rx=\"3\" ry=\"3\"/>\u003cellipse cx=\"18\" cy=\"16\" rx=\"3\" ry=\"3\"/>'\n  };\n  const icon = (name) => '\u003csvg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\" stroke-linejoin=\"round\" aria-hidden=\"true\">' + (paths[name] || paths.solid) + '\u003c/svg>';\n  const appIcons = { 'clube-uol': 'ticket', 'bilhete-unico': 'transit', 'super-calendario': 'calendar', 'media-downloader': 'media', pilltracker: 'pill', 'lastfm-small': 'music' };\n  const demo = {\n    version: 'Prévia', profile: { name: 'Leo', role: 'admin', status: 'approved' },\n    apps: [\n      { id: 'clube-uol', name: 'Clube UOL Premium', scriptName: 'Clube UOL Premium' },\n      { id: 'bilhete-unico', name: 'Bilhete Único', scriptName: 'Bilhete Único' },\n      { id: 'super-calendario', name: 'Super Calendário', scriptName: 'Super Calendário' },\n      { id: 'media-downloader', name: 'media-downloader', scriptName: 'media-downloader' },\n      { id: 'pilltracker', name: 'PillTracker', scriptName: 'PillTracker' },\n      { id: 'lastfm-small', name: 'Last.fm Small', scriptName: 'Last.fm Small' }\n    ].map((app) => Object.assign(app, { installed: true, allowed: true, mode: 'integrated' })),\n    preferences: { appearance: 'system', material: 'glass', showAppNames: false, favorites: ['clube-uol', 'bilhete-unico', 'super-calendario', 'pilltracker'], notifications: { enabled: false, updates: true, access: true } },\n    notifications: [], assets: {}, accessConfigured: false, update: {}\n  };\n  let state;\n  let preview = window.__SAQUETTO_PREVIEW__ === true;\n  try {\n    const raw = $('#saquetto-initial').textContent;\n    if (raw.indexOf('__SAQUETTO_INITIAL_JSON__') !== -1) { preview = true; state = demo; }\n    else { state = JSON.parse(raw); }\n  } catch (_) { preview = true; state = demo; }\n  state.preferences = Object.assign({}, demo.preferences, state.preferences || {});\n  state.profile = state.profile || { name: '', role: 'user', status: 'loading' };\n  state.apps = Array.isArray(state.apps) ? state.apps : [];\n  state.notifications = Array.isArray(state.notifications) ? state.notifications : [];\n  state.assets = state.assets || {};\n  let panel = null;\n  let panelAppId = null;\n  let activeAppId = null;\n  let activeInstanceId = null;\n  let activeInstanceBase = null;\n  let currentFrame = null;\n  let currentFrameReady = false;\n  let frameOutbox = [];\n  let frameSequence = 0;\n  let appClosing = false;\n  const frameEvaluations = new Map();\n  let entranceTilesRendered = false;\n  let query = '';\n  let users = null;\n  let usersError = '';\n  let busy = false;\n  let toastTimer;\n  let previousFocus;\n  let requestSequence = 0;\n  const pending = new Map();\n  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');\n  const errorMessages = {\n    access_unavailable: 'Não foi possível verificar seu acesso. Tente novamente quando estiver conectado.',\n    access_denied: 'Seu acesso a este app não está autorizado.', approval_required: 'Seu acesso aguarda aprovação do administrador.',\n    app_not_installed: 'Este script ainda não está instalado neste dispositivo.', script_name_conflict: 'Já existe outro script com esse nome. A instalação foi interrompida para preservar seu arquivo.',\n    name_invalid: 'Informe um nome válido para solicitar acesso.', notifications_disabled: 'As notificações estão desativadas. Ative-as nos ajustes.',\n    initialization_failed: 'Não foi possível iniciar o saquettoOS. Feche e abra o script novamente.', channel_invalid: 'O canal de atualização não é válido.',\n    preview_only: 'Esta é uma prévia visual. Abra o saquettoOS no Scriptable para usar esta ação.',\n    native_timeout: 'O Scriptable não respondeu. Tente novamente.', update_not_configured: 'Este app ainda não possui um canal de atualização configurado.',\n    code_mismatch: 'O código não confere. Confirme o código diretamente com a pessoa.', invalid_code: 'O código não confere. Confirme o código diretamente com a pessoa.', confirmation_mismatch: 'O código não confere. Confirme o código diretamente com a pessoa.',\n    admin_required: 'Esta ação exige acesso de administrador.', enrollment_capacity: 'O serviço atingiu o limite de solicitações. Tente novamente mais tarde.',\n    app_busy: 'O app ainda está concluindo uma ação. Aguarde antes de voltar ao início.', app_closing: 'O app está sendo fechado. Aguarde a conclusão.',\n    adapter_source_changed: 'Esta versão do script ainda não foi verificada pela central. Use Executar fora da central ou atualize o saquettoOS.',\n    adapter_entrypoint_changed: 'Esta versão do script precisa de uma atualização de compatibilidade.',\n    app_storage_unreadable: 'Não foi possível ler os ajustes desta tela. O arquivo foi preservado.',\n    app_storage_unverified: 'Não foi possível confirmar a gravação dos ajustes. Feche e reabra o app.',\n    close_app_before_update: 'Volte ao início antes de atualizar seus apps.',\n    pill_data_unavailable: 'Configure o PillTracker no script original antes de abri-lo aqui.',\n    pill_data_unrecognized: 'Os dados do PillTracker precisam de revisão. Nenhuma alteração foi feita.',\n    not_configured: 'Este app ainda não possui um canal de atualização configurado.',\n    download_failed: 'Não foi possível baixar a atualização. A versão instalada foi preservada.',\n    invalid_manifest: 'A atualização recebida não é válida. A instalação foi interrompida.',\n    integrity_failed: 'A verificação da atualização falhou. A instalação foi interrompida.',\n    rolled_back: 'A atualização não foi concluída. A versão anterior foi restaurada.',\n    recovery_required: 'A instalação precisa de recuperação. Nenhuma nova atualização foi aplicada.',\n    concurrent_change: 'O script mudou durante a atualização. A operação foi interrompida para preservar a alteração.'\n  };\n\n  window.__SAQUETTO_QUEUE = window.__SAQUETTO_QUEUE || [];\n  window.saquettoNative = function (action, payload) {\n    if (preview) return Promise.reject(new Error('preview_only'));\n    return new Promise((resolve, reject) => {\n      const id = 'os-' + (++requestSequence) + '-' + Date.now();\n      // Native app sessions end when the user closes the app, not after 90 seconds.\n      const nativeSession = action === 'openApp' && getApp(payload && payload.id)?.mode === 'native';\n      const timer = nativeSession ? null : setTimeout(() => { pending.delete(id); reject(new Error('native_timeout')); }, 90000);\n      pending.set(id, { resolve, reject, timer });\n      window.__SAQUETTO_QUEUE.push({ id, action, payload: payload || {} });\n      if (typeof window.__SAQUETTO_WAKE === 'function') window.__SAQUETTO_WAKE();\n    });\n  };\n  window.saquettoResolve = function (id, result) {\n    const request = pending.get(id);\n    if (!request) return;\n    pending.delete(id); clearTimeout(request.timer);\n    if (result && result.ok) request.resolve(result.data);\n    else {\n      const failure = result && result.error;\n      const error = new Error(typeof failure === 'string' ? failure : (failure && (failure.message || failure.code)) || 'Não foi possível concluir esta ação.');\n      error.code = failure && failure.code;\n      request.reject(error);\n    }\n  };\n  window.saquettoSetState = function (patch) {\n    if (!patch || typeof patch !== 'object') return;\n    if (patch.state) patch = patch.state;\n    state = Object.assign({}, state, patch, { preferences: Object.assign({}, state.preferences, patch.preferences || {}), profile: Object.assign({}, state.profile, patch.profile || {}), assets: Object.assign({}, state.assets, patch.assets || {}) });\n    if (!Array.isArray(state.apps)) state.apps = [];\n    if (!Array.isArray(state.notifications)) state.notifications = [];\n    render();\n  };\n  function applyResult(result) { if (result && result.state) window.saquettoSetState(result.state); }\n  function safeImage(url) { return typeof url === 'string' && /^(data:image\\/(png|jpeg|webp|gif);base64,|https:\\/\\/)/i.test(url) && !(preview && /^https:/i.test(url)) ? url : ''; }\n  function appImage(app) { return safeImage(app.icon); }\n  function appGraphic(app) {\n    const url = appImage(app);\n    if (url) return '\u003cimg src=\"' + escapeHTML(url) + '\" alt=\"\">';\n    // Supplied silhouettes share the same light mask treatment as built-in symbols.\n    const glyphKey = { 'clube-uol': 'uolGlyph', pilltracker: 'pillGlyph', 'lastfm-small': 'lastfmGlyph', 'bilhete-unico': 'transitGlyph', 'super-calendario': 'calendarGlyph', 'media-downloader': 'mediaGlyph' }[app.id];\n    const glyphAsset = glyphKey && state.assets[glyphKey];\n    const supplied = typeof glyphAsset === 'string' && /^data:image\\/svg\\+xml;base64,[A-Za-z0-9+/]+={0,2}$/.test(glyphAsset) ? glyphAsset : '';\n    const mask = supplied || 'data:image/svg+xml,' + encodeURIComponent(icon(appIcons[app.id] || 'solid').replace('\u003csvg ', '\u003csvg xmlns=\"http://www.w3.org/2000/svg\" ').replace('currentColor', '#fff').replace('stroke-width=\"1.8\"', 'stroke-width=\"1.5\"'));\n    return '\u003cspan class=\"app-glyph\" aria-hidden=\"true\" style=\"--glyph-mask:url(' + escapeHTML(JSON.stringify(mask)) + ')\">\u003c/span>';\n  }\n  function initials(name) { return String(name || 'S').trim().split(/\\s+/).slice(0, 2).map((word) => word.charAt(0)).join('').toUpperCase(); }\n  function getApp(id) { return state.apps.find((app) => app.id === id); }\n  function approved() { return state.profile.status === 'approved'; }\n  function admin() { return approved() && state.profile.role === 'admin'; }\n  function allowed(app) { return approved() && app.allowed === true; }\n  function statusText(status) { return ({ approved: 'Acesso aprovado', pending: 'Aguardando aprovação', revoked: 'Acesso revogado', loading: 'Verificando acesso', unavailable: 'Acesso indisponível', expired: 'Solicitação expirada', unregistered: 'Solicite seu acesso' })[status] || 'Acesso não confirmado'; }\n  function statusLabel(status) { return '\u003cspan class=\"status-label\" data-status=\"' + escapeHTML(status) + '\">' + statusText(status) + '\u003c/span>'; }\n  function notify(message, isError) {\n    const toast = $('#toast'); clearTimeout(toastTimer); toast.textContent = message; toast.dataset.error = String(Boolean(isError)); toast.hidden = false;\n    toastTimer = setTimeout(() => { toast.hidden = true; }, isError ? 6500 : 4200);\n  }\n  function errorText(error) { return errorMessages[error.code] || errorMessages[error.message] || error.message || 'Não foi possível concluir esta ação.'; }\n  function setBusy(value) {\n    busy = value;\n    $('#sheet').setAttribute('aria-busy', String(value));\n    $('#sheet-content').querySelectorAll('button,input').forEach((element) => {\n      if (value) { element.dataset.wasDisabled = String(element.disabled); element.disabled = true; }\n      else if (element.dataset.wasDisabled !== undefined) { element.disabled = element.dataset.wasDisabled === 'true'; delete element.dataset.wasDisabled; }\n    });\n  }\n  async function nativeAction(action, payload, callback) {\n    if (busy) return;\n    setBusy(true);\n    try { const result = await window.saquettoNative(action, payload); applyResult(result); if (callback) callback(result || {}); return true; }\n    catch (error) { notify(errorText(error), true); return false; }\n    finally { setBusy(false); }\n  }\n  function applyAppearance() {\n    const appearance = state.preferences.appearance || 'system';\n    const resolved = appearance === 'system' ? (mediaQuery.matches ? 'dark' : 'light') : appearance;\n    document.documentElement.dataset.appearance = resolved === 'light' ? 'light' : 'dark';\n    document.documentElement.dataset.material = state.preferences.material === 'solid' ? 'solid' : 'glass';\n    document.documentElement.dataset.showAppNames = String(state.preferences.showAppNames === true);\n    const brandGlyph = typeof state.assets.brandGlyph === 'string' && /^data:image\\/svg\\+xml;base64,[A-Za-z0-9+/]+={0,2}$/.test(state.assets.brandGlyph) ? state.assets.brandGlyph : '';\n    const logo = safeImage(resolved === 'light' ? state.assets.logoLight : state.assets.logoDark);\n    $('#brand-mark').innerHTML = brandGlyph ? '\u003cspan class=\"brand-glyph\" aria-hidden=\"true\" style=\"--glyph-mask:url(' + escapeHTML(JSON.stringify(brandGlyph)) + ')\">\u003c/span>' : logo ? '\u003cimg src=\"' + escapeHTML(logo) + '\" alt=\"\">' : icon('sparkles');\n    document.querySelector('meta[name=\"theme-color\"]').content = resolved === 'light' ? '#f0f2f8' : '#0d1020';\n  }\n  if (mediaQuery.addEventListener) mediaQuery.addEventListener('change', applyAppearance); else mediaQuery.addListener(applyAppearance);\n  function tile(app, inDock, entranceIndex) {\n    let badge = '';\n    if (!allowed(app) && state.profile.status !== 'loading') badge = '\u003cspan class=\"app-badge locked\">' + icon('lock') + '\u003c/span>';\n    else if (!app.installed) badge = '\u003cspan class=\"app-badge\">' + icon('download') + '\u003c/span>';\n    else if (app.updateAvailable) badge = '\u003cspan class=\"app-badge\">' + icon('refresh') + '\u003c/span>';\n    const name = inDock ? ({ 'clube-uol': 'Clube UOL', 'super-calendario': 'Calendário', 'media-downloader': 'Downloads', 'lastfm-small': 'Last.fm' }[app.id] || app.name) : app.name;\n    const hint = state.profile.status === 'loading' ? 'Verificando acesso' : !allowed(app) ? 'Acesso restrito' : (!app.installed ? 'Não instalado' : (app.mode === 'native' ? 'Interface original' : app.mode === 'integrated' ? 'Integrado' : 'Abre no Scriptable'));\n    return '\u003cbutton type=\"button\" class=\"app-tile\"' + (entranceIndex === undefined ? '' : ' data-entrance=\"' + entranceIndex + '\" style=\"--entrance-index:' + entranceIndex + '\"') + ' data-open-app=\"' + escapeHTML(app.id) + '\" data-installed=\"' + Boolean(app.installed) + '\" data-available=\"' + (state.profile.status === 'loading' ? 'loading' : allowed(app)) + '\" aria-label=\"' + escapeHTML(app.name + '. ' + hint) + '\"' + (inDock ? '' : ' role=\"listitem\"') + '>\u003cspan class=\"app-icon' + (appImage(app) ? ' app-icon-artwork' : '') + '\">' + appGraphic(app) + badge + '\u003c/span>\u003cspan class=\"app-name\">' + escapeHTML(name) + '\u003c/span>' + (!app.installed ? '\u003cspan class=\"app-status\">Não instalado\u003c/span>' : '') + '\u003c/button>';\n  }\n  function renderApps() {\n    const normalized = query.normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase();\n    const filtered = state.apps.filter((app) => String(app.name).normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().includes(normalized));\n    $('#app-grid').innerHTML = filtered.map((app, index) => tile(app, false, entranceTilesRendered ? undefined : index)).join('');\n    $('#library-count').textContent = query ? filtered.length + ' encontrados' : state.apps.length + ' aplicativos';\n    $('#search-empty').hidden = filtered.length > 0;\n    const favoriteIds = Array.isArray(state.preferences.favorites) ? state.preferences.favorites : [];\n    const favorites = favoriteIds.map(getApp).filter(Boolean).slice(0, 4);\n    $('#dock').innerHTML = favorites.map((app, index) => tile(app, true, entranceTilesRendered ? undefined : index + 6)).join('') + Array.from({ length: 4 - favorites.length }, () => '\u003cbutton type=\"button\" class=\"dock-placeholder\" data-panel=\"settings\" aria-label=\"Adicionar favorito\">' + icon('plus') + '\u003c/button>').join('');\n    entranceTilesRendered = true;\n  }\n  function updateItems() { return state.update && Array.isArray(state.update.items) ? state.update.items : []; }\n  function render() {\n    applyAppearance(); renderApps();\n    $('#preview-note').hidden = !preview;\n    $('#profile-initials').textContent = initials(state.profile.name);\n    $('#profile-presence').dataset.status = state.profile.status;\n    $('#profile-button').setAttribute('aria-label', (state.profile.name || 'Seu perfil') + '. ' + statusText(state.profile.status) + '. Abrir ajustes');\n    $('#greeting').textContent = state.profile.name ? 'SEU ESPAÇO, ' + state.profile.name.split(' ')[0].toUpperCase() + '.' : 'SEU ESPAÇO. SEUS APPS.';\n    $('#home-caption').textContent = 'saquettoOS' + (state.version ? ' · ' + state.version : '');\n    $('#notification-dot').hidden = !state.notifications.some((item) => !item.read);\n    const banner = $('#access-banner'); banner.hidden = approved() || state.profile.status === 'loading';\n    if (!approved() && state.profile.status !== 'loading') {\n      const descriptions = { loading: 'Conferindo sua autorização com o saquettoOS.', pending: 'Sua solicitação foi recebida. Leo precisa aprovar seu acesso.', revoked: 'Este dispositivo está sem autorização. Consulte seu administrador.' };\n      banner.innerHTML = '\u003cstrong>' + statusText(state.profile.status) + '\u003c/strong>\u003cp>' + (descriptions[state.profile.status] || 'Peça acesso para abrir os aplicativos do seu espaço.') + '\u003c/p>' + (state.profile.status === 'loading' ? '' : '\u003cbutton type=\"button\" class=\"text-button\" data-panel=\"access\">' + (state.profile.status === 'pending' ? 'Ver solicitação' : 'Gerenciar acesso') + '\u003c/button>');\n    }\n    const available = updateItems().filter((item) => item.available || item.updateAvailable);\n    $('#update-peek-title').textContent = available.length ? available.length + (available.length === 1 ? ' atualização disponível' : ' atualizações disponíveis') : 'Seu espaço, sempre em dia';\n    $('#update-peek-copy').textContent = available.length ? 'Veja o que há de novo nos seus apps' : 'Confira as versões dos seus apps';\n    if (panel) renderPanel();\n  }\n\n  function groupRow(action, title, detail, glyph) { return '\u003cbutton type=\"button\" class=\"group-row\" data-panel=\"' + action + '\">\u003cspan class=\"row-icon\">' + icon(glyph) + '\u003c/span>\u003cspan class=\"row-text\">\u003cstrong>' + title + '\u003c/strong>\u003csmall>' + detail + '\u003c/small>\u003c/span>\u003cspan class=\"row-chevron\">' + icon('chevron') + '\u003c/span>\u003c/button>'; }\n  function settingsPanel() {\n    const prefs = state.preferences;\n    return '\u003cdiv class=\"profile-card\">\u003cdiv class=\"profile-avatar\">' + escapeHTML(initials(state.profile.name)) + '\u003c/div>\u003cdiv>\u003ch3>' + escapeHTML(state.profile.name || 'Seu perfil') + '\u003c/h3>\u003cp>' + (admin() ? 'Administrador do saquettoOS' : 'Seu espaço no Scriptable') + '\u003c/p>' + statusLabel(state.profile.status) + '\u003c/div>\u003c/div>' +\n      '\u003cdiv class=\"section-label\">APARÊNCIA\u003c/div>\u003cdiv class=\"theme-options\">' + [['light', 'Claro'], ['dark', 'Escuro'], ['system', 'Automático']].map(([value, title]) => '\u003cbutton type=\"button\" class=\"theme-option\" data-preference=\"appearance\" data-value=\"' + value + '\" aria-pressed=\"' + (prefs.appearance === value) + '\">\u003cspan class=\"theme-preview\" aria-hidden=\"true\">\u003c/span>' + title + '\u003c/button>').join('') + '\u003c/div>' +\n      '\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">MATERIAL\u003c/div>\u003cdiv class=\"segmented\">' + [['glass', 'Vidro', 'glass'], ['solid', 'Sólido', 'solid']].map(([value, title, glyph]) => '\u003cbutton type=\"button\" data-preference=\"material\" data-value=\"' + value + '\" aria-pressed=\"' + (prefs.material === value) + '\">\u003cspan class=\"segment-icon\">' + icon(glyph) + '\u003c/span>' + title + '\u003c/button>').join('') + '\u003c/div>\u003cp class=\"fine-print\">Automático acompanha a aparência do iPhone. Vidro adiciona transparência e profundidade.\u003c/p>\u003c/div>' +\n      '\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">TELA INICIAL\u003c/div>\u003cdiv class=\"group\">\u003clabel class=\"group-row\">\u003cspan class=\"row-text\">\u003cstrong>Mostrar nomes dos apps\u003c/strong>\u003csmall>Exibir os nomes na tela inicial e no dock\u003c/small>\u003c/span>\u003cinput type=\"checkbox\" class=\"toggle\" data-preference-toggle=\"showAppNames\" aria-label=\"Mostrar nomes dos apps\"' + (prefs.showAppNames === true ? ' checked' : '') + '>\u003c/label>\u003c/div>\u003c/div>' +\n      '\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">FAVORITOS NO DOCK · ATÉ 4\u003c/div>\u003cdiv class=\"favorite-list\">' + state.apps.map((app) => '\u003cbutton type=\"button\" class=\"favorite-chip\" data-favorite=\"' + escapeHTML(app.id) + '\" aria-pressed=\"' + (prefs.favorites || []).includes(app.id) + '\">' + icon((prefs.favorites || []).includes(app.id) ? 'check' : 'plus') + escapeHTML(app.name) + '\u003c/button>').join('') + '\u003c/div>\u003c/div>' +\n      '\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">SEU SISTEMA\u003c/div>\u003cdiv class=\"group\">' + groupRow('updates', 'Central de atualizações', 'saquettoOS e seus aplicativos', 'download') + groupRow('notifications', 'Notificações', 'Alertas, lembretes e novidades', 'bell') + groupRow('access', 'Meu acesso', statusText(state.profile.status), 'user') + (admin() ? groupRow('users', 'Gerenciar usuários', 'Aprovações e acesso por aplicativo', 'users') : '') + '\u003c/div>\u003c/div>\u003cp class=\"version-footer\">saquettoOS ' + escapeHTML(state.version || '') + '\u003c/p>';\n  }\n  function updatesPanel() {\n    const items = updateItems();\n    const available = items.filter((item) => item.available || item.updateAvailable);\n    const checked = state.update && (state.update.checkedAt || state.update.lastCheckedAt || items.length > 0);\n    return '\u003cdiv class=\"status-card\">\u003cspan>' + icon(available.length ? 'download' : 'sparkles') + '\u003c/span>\u003ch3>' + (available.length ? available.length + (available.length === 1 ? ' atualização disponível' : ' atualizações disponíveis') : checked ? 'Verificação concluída' : 'Novidades, em um só lugar') + '\u003c/h3>\u003cp>' + (available.length ? 'Atualize cada app sem sair da sua central.' : checked ? 'Confira abaixo o estado de cada canal de atualização.' : 'Busque a versão mais recente do saquettoOS e dos apps com atualização configurada.') + '\u003c/p>\u003c/div>\u003cbutton type=\"button\" class=\"primary-button full-button\" data-action=\"checkUpdates\">' + icon('refresh') + 'Verificar atualizações\u003c/button>' +\n      '\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">APLICATIVOS E SISTEMA\u003c/div>\u003cdiv class=\"group\">' + (items.length ? items : [{ id: 'saquettoos', name: 'saquettoOS', version: state.version }, ...state.apps]).map((item) => {\n        const app = getApp(item.id) || item;\n        const isAvailable = item.available || item.updateAvailable;\n        const configured = item.status !== 'not_configured' && item.configured !== false && (item.id === 'saquettoos' || item.id === 'clube-uol' || item.channel);\n        const detail = item.error ? errorMessages[item.error] || 'Não foi possível verificar' : !configured ? 'Canal de atualização não configurado' : isAvailable ? String(item.version || 'Instalável') + ' → ' + String(item.latestVersion || item.version || 'Nova versão') : item.status === 'up_to_date' || item.status === 'current' ? 'Versão atualizada' : 'Verifique a disponibilidade';\n        return '\u003cdiv class=\"group-row update-row\">\u003cspan class=\"mini-icon\">' + (item.id === 'saquettoos' ? icon('sparkles') : appGraphic(app)) + '\u003c/span>\u003cspan class=\"row-text\">\u003cstrong>' + escapeHTML(item.name || app.name || 'saquettoOS') + '\u003c/strong>\u003csmall>' + escapeHTML(detail) + '\u003c/small>\u003c/span>' + (isAvailable ? '\u003cbutton type=\"button\" class=\"secondary-button\" data-install=\"' + escapeHTML(item.id) + '\">Atualizar\u003c/button>' : '\u003cspan class=\"version-tag\">' + escapeHTML(item.version || '—') + '\u003c/span>') + '\u003c/div>';\n      }).join('') + '\u003c/div>\u003cp class=\"fine-print\">Instalações e atualizações preservam os dados dos apps. Scripts sem canal configurado continuam abrindo normalmente.\u003c/p>\u003c/div>';\n  }\n  function notificationsPanel() {\n    const prefs = state.preferences.notifications || {};\n    return '\u003cp class=\"panel-intro\">Avisos locais do Scriptable, com atalhos para voltar ao seu espaço. A entrega depende da permissão de notificações do iPhone.\u003c/p>\u003cdiv class=\"group\">' + [['enabled', 'Permitir notificações', 'Ativar os avisos do saquettoOS'], ['updates', 'Atualizações', 'Avisar sobre versões disponíveis ao verificar'], ['access', 'Acesso e aprovações', 'Avisar quando uma mudança for detectada']].map(([key, title, detail]) => '\u003clabel class=\"group-row\">\u003cspan class=\"row-text\">\u003cstrong>' + title + '\u003c/strong>\u003csmall>' + detail + '\u003c/small>\u003c/span>\u003cinput type=\"checkbox\" class=\"toggle\" data-notification-preference=\"' + key + '\" aria-label=\"' + title + '\"' + (prefs[key] ? ' checked' : '') + '>\u003c/label>').join('') + '\u003c/div>\u003cdiv class=\"panel-actions\">\u003cbutton type=\"button\" class=\"secondary-button\" data-action=\"testNotification\">' + icon('bell') + 'Testar notificação\u003c/button>\u003c/div>\u003cp class=\"fine-print\">O saquettoOS verifica novidades quando é executado. Não mantém um processo contínuo em segundo plano.\u003c/p>\u003cdiv class=\"panel-section\">\u003cdiv class=\"section-label\">SUA CENTRAL\u003c/div>' + (state.notifications.length ? '\u003cdiv class=\"group\">' + state.notifications.map((item) => '\u003carticle class=\"notice-item\">\u003ch3>' + escapeHTML(item.title || 'saquettoOS') + '\u003c/h3>\u003cp>' + escapeHTML(item.body || '') + '\u003c/p>' + (item.date ? '\u003ctime>' + escapeHTML(formatDate(item.date)) + '\u003c/time>' : '') + (item.appId && getApp(item.appId) ? '\u003cbutton type=\"button\" class=\"text-button\" data-open-app=\"' + escapeHTML(item.appId) + '\">Abrir app\u003c/button>' : '') + '\u003c/article>').join('') + '\u003c/div>' : '\u003cdiv class=\"status-card\">\u003cspan>' + icon('bell') + '\u003c/span>\u003ch3>Tudo tranquilo por aqui\u003c/h3>\u003cp>Os avisos recebidos aparecem nesta central.\u003c/p>\u003c/div>') + '\u003c/div>';\n  }\n  function accessPanel() {\n    const status = state.profile.status;\n    const code = state.profile.code || state.profile.approvalCode;\n    if (approved()) return '\u003cdiv class=\"status-card\">\u003cspan>' + icon('check') + '\u003c/span>\u003ch3>Bem-vindo, ' + escapeHTML(state.profile.name || 'você') + '\u003c/h3>\u003cp>' + (admin() ? 'Você administra usuários e permissões do saquettoOS.' : 'Seu acesso foi aprovado pelo administrador.') + '\u003c/p>\u003c/div>\u003cdiv class=\"group\">' + state.apps.map((app) => '\u003cdiv class=\"group-row\">\u003cspan class=\"row-icon\">' + icon(allowed(app) ? 'check' : 'lock') + '\u003c/span>\u003cspan class=\"row-text\">\u003cstrong>' + escapeHTML(app.name) + '\u003c/strong>\u003csmall>' + (allowed(app) ? 'Acesso autorizado' : 'Acesso não autorizado') + '\u003c/small>\u003c/span>\u003c/div>').join('') + '\u003c/div>';\n    return '\u003cdiv class=\"status-card\">\u003cspan>' + icon(status === 'loading' ? 'refresh' : 'lock') + '\u003c/span>\u003ch3>' + statusText(status) + '\u003c/h3>\u003cp>' + (status === 'pending' ? 'Compartilhe seu código com Leo para que ele confirme sua solicitação.' : status === 'revoked' ? 'Seu acesso foi revogado pelo administrador.' : 'Leo aprova os usuários e define quais apps cada pessoa pode abrir.') + '\u003c/p>' + (code ? '\u003cp style=\"margin-top:15px;font-size:20px;letter-spacing:4px;color:var(--text)\">' + escapeHTML(code) + '\u003c/p>' : '') + '\u003c/div>' + (!state.accessConfigured ? '\u003cp class=\"inline-error\">O serviço de acesso ainda não está configurado. Nenhuma solicitação será aprovada localmente.\u003c/p>' : '') +\n      (status !== 'pending' && status !== 'loading' ? '\u003clabel class=\"input-label\" for=\"access-name\">Seu nome\u003c/label>\u003cinput class=\"text-input\" id=\"access-name\" autocomplete=\"name\" maxlength=\"60\" placeholder=\"Como você quer ser chamado?\" value=\"' + escapeHTML(state.profile.name || '') + '\">\u003cdiv class=\"panel-actions\">\u003cbutton type=\"button\" class=\"primary-button\" data-action=\"requestAccess\"' + (!state.accessConfigured ? ' disabled' : '') + '>Solicitar acesso\u003c/button>\u003c/div>' : '') + '\u003cdiv class=\"panel-actions\">\u003cbutton type=\"button\" class=\"secondary-button\" data-action=\"refreshState\">' + icon('refresh') + 'Verificar acesso\u003c/button>\u003c/div>';\n  }\n  function usersPanel() {\n    if (!admin()) return '\u003cp class=\"inline-error\">A gestão de usuários exige acesso de administrador.\u003c/p>';\n    let content = '\u003cp class=\"panel-intro\">Confirme o código diretamente com a pessoa antes de aprovar. As permissões são verificadas pelo serviço de acesso.\u003c/p>\u003cbutton type=\"button\" class=\"secondary-button full-button\" data-action=\"refreshUsers\">' + icon('refresh') + 'Recarregar solicitações\u003c/button>';\n    if (usersError) return content + '\u003cdiv class=\"panel-section inline-error\">' + escapeHTML(usersError) + '\u003c/div>';\n    if (users === null) return content + '\u003cdiv class=\"loading-row\">\u003cspan class=\"spinner\">\u003c/span>Buscando usuários…\u003c/div>';\n    if (!users.length) return content + '\u003cdiv class=\"panel-section status-card\">\u003cspan>' + icon('users') + '\u003c/span>\u003ch3>Nenhuma solicitação\u003c/h3>\u003cp>Novas pessoas aparecerão aqui quando pedirem acesso.\u003c/p>\u003c/div>';\n    return content + users.map((user, index) => '\u003carticle class=\"user-card\" data-user-index=\"' + index + '\">\u003cdiv class=\"user-heading\">\u003cstrong>' + escapeHTML(user.name || 'Usuário') + '\u003c/strong>' + statusLabel(user.status) + '\u003c/div>' + (user.role === 'admin' ? '\u003cp class=\"fine-print\">Administrador · permissões gerenciadas no serviço.\u003c/p>' : '\u003clabel class=\"input-label\" for=\"user-code-' + index + '\">Código confirmado com a pessoa\u003c/label>\u003cinput class=\"text-input\" id=\"user-code-' + index + '\" inputmode=\"text\" autocomplete=\"off\" placeholder=\"Informe o código recebido\" maxlength=\"80\">\u003cdiv class=\"user-apps\">' + state.apps.map((app) => '\u003clabel class=\"user-app-option\">\u003cinput type=\"checkbox\" value=\"' + escapeHTML(app.id) + '\"' + ((user.appIds || []).includes(app.id) ? ' checked' : '') + '>' + escapeHTML(app.name) + '\u003c/label>').join('') + '\u003c/div>\u003cdiv class=\"panel-actions\">\u003cbutton type=\"button\" class=\"primary-button\" data-decide-user=\"' + index + '\" data-status=\"approved\">' + (user.status === 'approved' ? 'Salvar acesso' : 'Aprovar') + '\u003c/button>\u003cbutton type=\"button\" class=\"secondary-button danger-button\" data-decide-user=\"' + index + '\" data-status=\"revoked\">Revogar\u003c/button>\u003c/div>') + '\u003c/article>').join('');\n  }\n  function appPanel() {\n    const app = getApp(panelAppId);\n    if (!app) return '\u003cp class=\"inline-error\">Aplicativo não encontrado.\u003c/p>';\n    return '\u003cdiv class=\"status-card\">\u003cspan>' + icon(appIcons[app.id] || 'solid') + '\u003c/span>\u003ch3>' + escapeHTML(app.name) + '\u003c/h3>\u003cp>' + (!allowed(app) ? 'Seu perfil ainda não tem permissão para este app.' : !app.installed ? 'Instale o script no Scriptable para abrir este aplicativo.' : app.mode === 'native' ? 'Abre a interface original do app. Feche a tela do app para voltar à central.' : app.mode === 'integrated' ? 'Este app está preparado para abrir dentro do saquettoOS.' : 'Este app abre sua própria interface no Scriptable.') + '\u003c/p>\u003c/div>\u003cdiv class=\"group\">\u003cdiv class=\"group-row\">\u003cspan class=\"row-text\">\u003cstrong>Nome no Scriptable\u003c/strong>\u003csmall>' + escapeHTML(app.scriptName || app.name) + '\u003c/small>\u003c/span>\u003c/div>\u003cdiv class=\"group-row\">\u003cspan class=\"row-text\">\u003cstrong>Estado\u003c/strong>\u003csmall>' + (!app.installed ? 'Não instalado' : app.mode === 'native' ? 'Interface original' : app.mode === 'integrated' ? 'Integrado' : 'App externo') + '\u003c/small>\u003c/span>\u003cspan class=\"version-tag\">' + escapeHTML(app.version || '') + '\u003c/span>\u003c/div>\u003c/div>\u003cdiv class=\"panel-actions\">' + (!allowed(app) ? '\u003cbutton type=\"button\" class=\"secondary-button\" data-panel=\"access\">Ver meu acesso\u003c/button>' : !app.installed ? '\u003cbutton type=\"button\" class=\"primary-button\" data-panel=\"updates\">Ver instalação e atualizações\u003c/button>' : '\u003cbutton type=\"button\" class=\"primary-button\" data-run-app=\"' + escapeHTML(app.id) + '\">Abrir aplicativo\u003c/button>\u003cbutton type=\"button\" class=\"secondary-button\" data-action=\"standalone\" data-app-id=\"' + escapeHTML(app.id) + '\">Executar fora da central\u003c/button>') + '\u003c/div>';\n  }\n  function formatDate(date) { const parsed = new Date(date); return Number.isNaN(parsed.getTime()) ? String(date) : parsed.toLocaleString('pt-BR', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' }); }\n  function renderPanel() {\n    const titles = { settings: 'Ajustes', updates: 'Atualizações', notifications: 'Notificações', access: 'Meu acesso', users: 'Usuários', app: 'Seu aplicativo' };\n    $('#sheet-title').textContent = titles[panel] || 'saquettoOS';\n    $('#sheet-eyebrow').textContent = panel === 'users' ? 'ADMINISTRAÇÃO' : 'SAQUETTOOS';\n    const renderers = { settings: settingsPanel, updates: updatesPanel, notifications: notificationsPanel, access: accessPanel, users: usersPanel, app: appPanel };\n    $('#sheet-content').innerHTML = (renderers[panel] || settingsPanel)();\n  }\n  function openPanel(next, appId) {\n    if (!panel) previousFocus = document.activeElement;\n    panel = next; panelAppId = appId || null;\n    renderPanel(); $('#sheet').hidden = false; $('#sheet-backdrop').hidden = false;\n    $('#shell').setAttribute('aria-hidden', 'true'); $('#shell').inert = true;\n    document.body.style.overflow = 'hidden'; $('#sheet-content').scrollTop = 0; $('#close-sheet').focus({ preventScroll: true });\n    if (next === 'users' && users === null) loadUsers();\n  }\n  function closePanel() {\n    panel = null; $('#sheet').hidden = true; $('#sheet-backdrop').hidden = true; $('#shell').removeAttribute('aria-hidden'); $('#shell').inert = false; document.body.style.overflow = '';\n    if (previousFocus && previousFocus.isConnected) previousFocus.focus({ preventScroll: true });\n  }\n  async function loadUsers() {\n    if (!admin()) return;\n    usersError = '';\n    if (preview) { users = []; usersError = 'Prévia visual: nenhum usuário real foi consultado.'; if (panel === 'users') renderPanel(); return; }\n    try { const result = await window.saquettoNative('listUsers', {}); users = Array.isArray(result) ? result : (result.users || []); }\n    catch (error) { users = []; usersError = errorText(error); }\n    if (panel === 'users') renderPanel();\n  }\n  async function savePreferences(patch) {\n    if (preview) { state.preferences = Object.assign({}, state.preferences, patch); render(); return; }\n    const saved = await nativeAction('setPreferences', patch, (result) => {\n      if (!result.state) { state.preferences = Object.assign({}, state.preferences, result.preferences || patch); render(); }\n    });\n    if (saved === false && panel) renderPanel();\n  }\n  async function runApp(id) {\n    const app = getApp(id);\n    if (!app) return;\n    if (!allowed(app) || !app.installed) { openPanel('app', id); return; }\n    const opened = await nativeAction('openApp', { id }, () => { if (panel) closePanel(); });\n    if (opened === false && !activeAppId && !preview) openPanel('app', id);\n  }\n  function validBridgeId(value) { return (typeof value === 'string' && value.length > 0 && value.length \u003c= 160) || (typeof value === 'number' && Number.isSafeInteger(value)); }\n  function newInstanceId(base) { return String(base || 'saquetto-frame') + ':' + (++frameSequence); }\n  function frameReply(frame, instanceId, message) {\n    if (frame !== currentFrame || frame.saquettoInvalidated || instanceId !== activeInstanceId || !frame.contentWindow) return false;\n    if (!currentFrameReady) { frameOutbox.push(message); return true; }\n    frame.contentWindow.postMessage(Object.assign({ saquetto: 1, instanceId }, message), '*');\n    return true;\n  }\n  function replaceFrame(html, instanceId) {\n    const frame = document.createElement('iframe');\n    frame.title = $('#app-stage-name').textContent;\n    frame.setAttribute('sandbox', 'allow-scripts allow-forms');\n    frame.setAttribute('referrerpolicy', 'no-referrer');\n    activeInstanceId = instanceId;\n    currentFrame = frame;\n    currentFrameReady = false; frameOutbox = [];\n    frameEvaluations.clear();\n    frame.addEventListener('load', () => {\n      if (frame !== currentFrame || instanceId !== activeInstanceId) return;\n      if (currentFrameReady) {\n        frame.saquettoInvalidated = true; frameOutbox = []; frameEvaluations.clear();\n        window.saquettoHome(); return;\n      }\n      currentFrameReady = true;\n      const messages = frameOutbox; frameOutbox = [];\n      messages.forEach((message) => frameReply(frame, instanceId, message));\n    });\n    frame.srcdoc = String(html || '');\n    $('#app-frame-container').replaceChildren(frame);\n    return instanceId;\n  }\n  window.saquettoShowApp = function (app) {\n    const registered = getApp(app && app.id);\n    if (!registered || registered.mode !== 'integrated' || !allowed(registered) || !registered.installed) return false;\n    if (panel) closePanel();\n    if (currentFrame && activeAppId === app.id && (!app.instanceId || app.instanceId === activeInstanceId)) {\n      $('#home-screen').hidden = true; $('#app-stage').hidden = false;\n      return activeInstanceId;\n    }\n    activeAppId = app.id; appClosing = false;\n    activeInstanceBase = validBridgeId(app.instanceId) ? app.instanceId : 'saquetto-frame';\n    $('#app-stage-name').textContent = app.name || registered.name;\n    $('#home-screen').hidden = true; $('#app-stage').hidden = false;\n    return replaceFrame(app.html, validBridgeId(app.instanceId) ? app.instanceId : newInstanceId());\n  };\n  window.saquettoSetAppHTML = function (value) {\n    if (!activeAppId || !currentFrame || appClosing || !value || typeof value.html !== 'string') return false;\n    return replaceFrame(value.html, newInstanceId(activeInstanceBase));\n  };\n  window.saquettoFrameMessage = function (message) {\n    if (!currentFrame || currentFrame.saquettoInvalidated || appClosing || !message || message.kind !== 'eval' || !validBridgeId(message.id) || typeof message.code !== 'string') return false;\n    if (message.instanceId !== undefined && message.instanceId !== activeInstanceId) return false;\n    frameEvaluations.set(message.id, activeInstanceId);\n    return frameReply(currentFrame, activeInstanceId, { kind: 'eval', id: message.id, code: message.code, useCallback: message.useCallback === true });\n  };\n  window.saquettoHome = async function () {\n    if (!activeAppId || !currentFrame) return true;\n    if (appClosing) return false;\n    appClosing = true;\n    const appId = activeAppId, instanceId = activeInstanceId, frame = currentFrame;\n    $('#app-stage-header')?.setAttribute('aria-busy', 'true');\n    try {\n      const result = await window.saquettoNative('closeApp', { appId, instanceId });\n      if (currentFrame !== frame || activeInstanceId !== instanceId) return false;\n      applyResult(result);\n      activeAppId = null; activeInstanceId = null; activeInstanceBase = null; currentFrame = null; currentFrameReady = false; frameOutbox = []; frameEvaluations.clear();\n      $('#app-stage').hidden = true; $('#app-frame-container').replaceChildren(); $('#home-screen').hidden = false;\n      return true;\n    } catch (error) { notify(errorText(error), true); return false; }\n    finally { appClosing = false; $('#app-stage-header')?.removeAttribute('aria-busy'); }\n  };\n  window.addEventListener('message', async (event) => {\n    const frame = currentFrame, instanceId = activeInstanceId, appId = activeAppId;\n    if (!frame || frame.saquettoInvalidated || !appId || !frame.contentWindow || event.source !== frame.contentWindow) return;\n    const message = event.data;\n    if (!message || typeof message !== 'object' || Array.isArray(message) || message.saquetto !== 1 || !validBridgeId(message.id)) return;\n    if (message.instanceId !== instanceId) return;\n    if (message.kind === 'request') {\n      if (appClosing) { frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: 'app_closing' }); return; }\n      if (typeof message.action !== 'string' || !message.action || message.action.length > 100 || (message.payload !== undefined && (!message.payload || typeof message.payload !== 'object' || Array.isArray(message.payload)))) {\n        frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: 'payload_invalid' }); return;\n      }\n      try {\n        const result = await window.saquettoNative('appAction', { appId, instanceId, request: { id: message.id, action: message.action, payload: message.payload || {} } });\n        frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: true, data: result });\n      } catch (error) { frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: error.code || error.message }); }\n    } else if (message.kind === 'evalResult') {\n      if (frameEvaluations.get(message.id) !== instanceId) return;\n      frameEvaluations.delete(message.id);\n      try {\n        await window.saquettoNative('appEvalResult', { appId, instanceId, id: message.id, value: message.value, error: typeof message.error === 'string' ? message.error : null });\n      } catch (error) {\n        if (frame === currentFrame && instanceId === activeInstanceId) notify(errorText(error), true);\n      }\n    }\n  });\n\n  document.querySelectorAll('[data-icon]').forEach((element) => { element.innerHTML = icon(element.dataset.icon); });\n  $('#app-search').addEventListener('input', (event) => { query = event.target.value; renderApps(); });\n  $('#close-sheet').addEventListener('click', closePanel);\n  $('#sheet-backdrop').addEventListener('click', closePanel);\n  document.addEventListener('keydown', (event) => {\n    if (event.key === 'Escape' && panel) { event.preventDefault(); closePanel(); }\n    if (event.key === 'Tab' && panel) {\n      const items = Array.from($('#sheet').querySelectorAll('button:not(:disabled),input:not(:disabled),[tabindex=\"0\"]'));\n      const first = items[0], last = items[items.length - 1];\n      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }\n      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }\n    }\n  });\n  document.addEventListener('change', async (event) => {\n    if (event.target.dataset.preferenceToggle === 'showAppNames') {\n      await savePreferences({ showAppNames: event.target.checked === true }); return;\n    }\n    const key = event.target.dataset.notificationPreference;\n    if (!key) return;\n    const next = Object.assign({}, state.preferences.notifications || {}, { [key]: event.target.checked });\n    if (preview) { event.target.checked = !event.target.checked; notify(errorMessages.preview_only, true); return; }\n    await nativeAction('setNotificationPreferences', next, (result) => {\n      if (!result.state) { state.preferences.notifications = result.preferences || next; renderPanel(); }\n    });\n    if (panel === 'notifications') renderPanel();\n  });\n  document.addEventListener('click', async (event) => {\n    const button = event.target.closest('button');\n    if (!button || button.disabled) return;\n    const data = button.dataset;\n    if (data.panel) { openPanel(data.panel); return; }\n    if (data.openApp || data.runApp) { await runApp(data.openApp || data.runApp); return; }\n    if (data.preference) { await savePreferences({ [data.preference]: data.value }); return; }\n    if (data.favorite) {\n      let favorites = [...(state.preferences.favorites || [])];\n      if (favorites.includes(data.favorite)) favorites = favorites.filter((id) => id !== data.favorite);\n      else if (favorites.length \u003c 4) favorites.push(data.favorite);\n      else { notify('O dock tem 4 espaços. Remova um favorito para adicionar outro.'); return; }\n      await savePreferences({ favorites }); return;\n    }\n    if (data.install) {\n      await nativeAction('installUpdate', { id: data.install }, (result) => {\n        if (result.status === 'installed') {\n          const item = updateItems().find((entry) => entry.id === data.install);\n          if (item) Object.assign(item, { available: false, updateAvailable: false, status: 'up_to_date', version: result.version || item.latestVersion });\n          render(); notify('Atualização instalada. Abra novamente para usar a nova versão.');\n        } else notify(errorMessages[result.status] || 'Atualização não concluída (' + String(result.status || 'estado não confirmado') + ').', true);\n      }); return;\n    }\n    if (data.decideUser !== undefined) {\n      const user = users && users[Number(data.decideUser)];\n      const card = button.closest('.user-card'); if (!user || !card || !admin()) return;\n      const code = card.querySelector('.text-input').value.trim().toUpperCase();\n      if (!/^[A-F0-9]{8}$/.test(code)) { notify('Informe os 8 caracteres do código confirmado com a pessoa.', true); return; }\n      const appIds = Array.from(card.querySelectorAll('input[type=\"checkbox\"]:checked')).map((input) => input.value);\n      await nativeAction('decideUser', { id: user.id, status: data.status, appIds, code }, () => { users = null; loadUsers(); }); return;\n    }\n    switch (data.action) {\n      case 'home': if (panel) closePanel(); if (activeAppId) await window.saquettoHome(); else if (!preview) await nativeAction('returnHome', {}); break;\n      case 'standalone': {\n        const id = data.appId || activeAppId || panelAppId;\n        if (!id) break;\n        if (activeAppId && !await window.saquettoHome()) break;\n        await nativeAction('launchStandalone', { id }); break;\n      }\n      case 'clearSearch': query = ''; $('#app-search').value = ''; renderApps(); $('#app-search').focus(); break;\n      case 'checkUpdates': await nativeAction('checkUpdates', {}, (result) => { if (Array.isArray(result.items)) { state.update = result; render(); } }); break;\n      case 'testNotification': await nativeAction('testNotification', {}, () => { notify('Notificação solicitada ao Scriptable.'); }); break;\n      case 'requestAccess': {\n        const name = ($('#access-name') && $('#access-name').value || '').trim();\n        if (name.length \u003c 2) { notify('Informe seu nome para solicitar acesso.', true); return; }\n        await nativeAction('requestAccess', { name }); break;\n      }\n      case 'refreshState': await nativeAction('getState', {}, (result) => { if (!result.state && result.profile) window.saquettoSetState(result); }); break;\n      case 'refreshUsers': users = null; renderPanel(); await loadUsers(); break;\n    }\n  });\n  // Track the visible WebView when the keyboard opens, without disabling pinch zoom.\n  function fitViewport() {\n    const viewport = window.visualViewport;\n    if (!viewport || viewport.scale > 1.05 || !Number.isFinite(viewport.height) || viewport.height \u003c= 0) return;\n    document.documentElement.style.setProperty('--viewport-height', Math.round(viewport.height) + 'px');\n    const style = window.getComputedStyle(document.documentElement);\n    const safeTop = parseFloat(style.getPropertyValue('--safe-top')) || 0;\n    const safeBottom = parseFloat(style.getPropertyValue('--safe-bottom')) || 0;\n    document.documentElement.dataset.compactViewport = String(viewport.height - safeTop - safeBottom \u003c 480);\n  }\n  if (window.visualViewport) {\n    window.visualViewport.addEventListener('resize', fitViewport);\n    window.addEventListener('resize', fitViewport);\n    fitViewport();\n  }\n  $('#shell').dataset.enter = 'true';\n  $('.home-bottom').addEventListener('animationend', (event) => {\n    if (event.target === $('.home-bottom') && event.animationName === 'home-layer-in') $('#shell').dataset.enter = 'false';\n  });\n  render();\n  window.__SAQUETTO_RENDERED = true;\n}());\n\u003c/script>\n\u003c/body>\n\u003c/html>\n";
const SAQUETTO_ASSETS = {"brandGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxMjU0IDEyNTQiPjxwYXRoIGZpbGw9IiNmZmYiIGQ9IiBNMC45OTk5OTYsNzg5LjQ2ODYyOCAgIEMxMS44NDY2NTUsNzgzLjU4MDU2NiAyMi41Mzk4MjUsNzc3LjgyNDE1OCAzMy41Njc1MDUsNzcyLjgwMjI0NiAgIEM3My43MzIyNTQsNzU0LjUxMTUzNiAxMTYuMDU3Mzk2LDc0NS44NzczMTkgMTYwLjA5MTYxNCw3NDUuMjI4ODgyICAgQzE2NS41ODQ4NTQsNzQ1LjE0Nzk0OSAxNzEuMDgwMzA3LDc0NS4yMTc3NzMgMTc4LjAyNTU3NCw3NDUuMjE3NzczICAgQzE1NS43NTEyMzYsNzIwLjUyNTMzMCAxMzEuNDU1NTY2LDcwMC4yNzM5MjYgMTAzLjk2OTg1Niw2ODMuNzQxOTQzICAgQzc2LjUyNzk2OSw2NjcuMjM2Mzg5IDQ3LjIzNzMzOSw2NTUuMTYxMDcyIDE1LjEyMTExOSw2NDYuMTc4MTAxICAgQzcxLjcxODQ2OCw2MjEuNjgwNjAzIDEyOS4wMjAyOTQsNjA1LjYzMTQwOSAxOTAuMDAwMzM2LDYwNC42ODI5ODMgICBDMjUwLjk0ODc3Niw2MDMuNzM1MDQ2IDMwOS4yMTY1NTMsNjE0LjY5NzY5MyAzNjMuODc1NjQxLDY0My4xMTQwMTQgICBDMzM1LjQyODE2Miw1ODkuOTYxNjcwIDI5NS43MTYyNzgsNTQ3LjYwNDYxNCAyNDMuMTk5MjY1LDUxNS41NDYwODIgICBDMjU0LjE0ODcxMiw1MTMuOTU4NTU3IDI2My44NzM5MzIsNTEyLjIwNTUwNSAyNzMuNjc1Mzg1LDUxMS4xODI1NTYgICBDMzQxLjk5NzcxMSw1MDQuMDUxODgwIDQwOC44MjcxNzksNTEwLjg5ODEwMiA0NzMuNTIyNzA1LDUzNC4zMzkzNTUgICBDNTI4LjUzOTkxNyw1NTQuMjczNzQzIDU3Ni43ODU4MjgsNTg0Ljc2NjA1MiA2MTQuMDY0MzkyLDYzMC41NzM2NjkgICBDNjI3LjgwNjM5Niw2NDcuNDU5NzE3IDYzOS44MjM0ODYsNjY1Ljc2ODU1NSA2NTIuMzI3NTc2LDY4My42MzgxODQgICBDNjY4LjY4MDkwOCw3MDcuMDA4ODUwIDY4Ny4wMjI5NDksNzI4LjM4OTgzMiA3MTAuNzc5NTQxLDc0NC42NDIyNzMgICBDNzQ3LjI5NzQ4NSw3NjkuNjI0ODc4IDc4Ny43Mzg4OTIsNzc3LjUzNzIzMSA4MzEuMjAxMjMzLDc3Mi41MTIwMjQgICBDODU5LjcwMjk0Miw3NjkuMjE2NTUzIDg4Ni4xNTg4NzUsNzU5LjI5NzA1OCA5MTEuOTc0MzA0LDc0Ny40MzEyNzQgICBDOTE1LjI3MDAyMCw3NDUuOTE2NDQzIDkxNy4xMTkwODAsNzQ2LjM1ODUyMSA5MTkuMTMzODUwLDc0OC44ODk5NTQgICBDOTI1Ljc1MTc3MCw3NTcuMjA0NzEyIDkyNi43NTE4MzEsNzY2LjkyODQwNiA5MjUuMzg1NTU5LDc3Ni44NTY2MjggICBDOTIyLjUxMDgwMyw3OTcuNzQ1Nzg5IDkxMy4yMDE1OTksODE2LjIwOTM1MSA5MDIuMzkyNjM5LDgzMy45MjI3MjkgICBDODc4LjY1MjEwMCw4NzIuODI4MDY0IDg0Ny4wNDg4ODksOTA0LjcwMDAxMiA4MTEuOTAzOTkyLDkzMy4xNzI0MjQgICBDNzgxLjQ4MzgyNiw5NTcuODE3MTM5IDc1MC4yNDI5MjAsOTgxLjQ1MjIwOSA3MTkuOTM1OTEzLDEwMDYuMjMyMDU2ICAgQzY4OS4xNDY2NjcsMTAzMS40MDYyNTAgNjYwLjYwMDc2OSwxMDU4Ljk0Nzc1NCA2MzguMzk3NjQ0LDEwOTIuMjg5OTE3ICAgQzYxOC42MTc3OTgsMTEyMS45OTI5MjAgNjA0Ljg1NzA1NiwxMTUzLjk5ODQxMyA2MDMuNTcwMDY4LDExOTAuMzEzMzU0ICAgQzYwMi44ODQzMzgsMTIwOS42NjUyODMgNjA1Ljg4MjIwMiwxMjI4LjQ4NjgxNiA2MTIuNTE4MzcyLDEyNDYuNzA1NTY2ICAgQzYxMy40MjkwMTYsMTI0OS4yMDUzMjIgNjE0LjI4MjM0OSwxMjUxLjcyNTk1MiA2MTUuMDgxMTE2LDEyNTQuNjE4NDA4ICAgQzQ2OS4zMTI0MzksMTI1NS4wMDAwMDAgMzIzLjYyNDg3OCwxMjU1LjAwMDAwMCAxNzcuNDU3MTg0LDEyNTQuNTkyMTYzICAgQzE3NC42NDE0OTUsMTI0MS40NTEwNTAgMTcyLjI5NjQ0OCwxMjI4LjcxOTM2MCAxNjkuOTc0NjI1LDEyMTUuOTgzNjQzICAgQzE2OC45MDc4ODMsMTIxMC4xMzIyMDIgMTY1LjQ4NTc3OSwxMjA4LjQ5NzkyNSAxNjAuNjUwMjY5LDEyMTEuNzQxMzMzICAgQzE0NC4xMDM4MDYsMTIyMi44Mzk3MjIgMTI5Ljc0NjE1NSwxMjM2LjMwMzU4OSAxMTcuNTExOTg2LDEyNTIuMDIzODA0ICAgQzExNi44NDEwMTEsMTI1Mi44ODU5ODYgMTE2LjQ5NjQ3NSwxMjU0LjAwMjA3NSAxMTUuOTk5OTg1LDEyNTUuMDAwMDAwICAgQzk4LjMxMjQzOSwxMjU1LjAwMDAwMCA4MC42MjQ4NzgsMTI1NS4wMDAwMDAgNjIuNjE2NTQ3LDEyNTQuNjE2OTQzICAgQzYxLjk3NDQ3MiwxMjUzLjExNTQ3OSA2MS42NjE5NzIsMTI1MS45OTQ2MjkgNjEuMzMwNjI0LDEyNTAuODc5MjcyICAgQzQzLjc2MjQ0NCwxMTkxLjc0NjgyNiA0Ny42MTUzMjYsMTEzNC43MzczMDUgNzguMjE3ODA0LDEwODAuMzc0ODc4ICAgQzc5LjU1ODg2MSwxMDc3Ljk5MjY3NiA4MC44NDgzODEsMTA3NS41ODE1NDMgODIuNTM1MDY1LDEwNzIuNTAyODA4ICAgQzgwLjM4NzM1MiwxMDcyLjgzNzUyNCA3OS4wODg2OTIsMTA3Mi45NTgyNTIgNzcuODI5MTE3LDEwNzMuMjQ4NDEzICAgQzUxLjk3OTMxNywxMDc5LjIwMzYxMyAyNy42Mzc4MDYsMTA4OC44MjA4MDEgNS4yMjA1ODAsMTEwMy4xMzA0OTMgICBDMy45MzgzNTksMTEwMy45NDg5NzUgMi40MTI0NjQsMTEwNC4zODU2MjAgMS4wMDAwMDAsMTEwNS4wMDAwMDAgICBDMS4wMDAwMDAsMTA2NC45NzkxMjYgMS4wMDAwMDAsMTAyNC45NTgyNTIgMS4zMTMyMjMsOTg0LjMzMDYyNyAgIEMyNC4wMjM4MzYsOTQ2LjM4ODc5NCA1NS44MDk3NjksOTE5LjAwODU0NSA5NC4yODY2MTMsODk3LjYwMzI3MSAgIEM3My45MDkzNjMsODkyLjAwMzY2MiA1NC4zMDIyMzgsODg5LjY5MDY3NCAzNC40MDM4NzcsODg5LjkyMzgyOCAgIEMyMy4yNjY1NzksODkwLjA1NDMyMSAxMi4xMzQ0NTYsODkwLjYyNjcwOSAxLjAwMDAwMCw4OTEuMDAwMDAwICAgQzEuMDAwMDAwLDg1Ny4zMTI0MzkgMS4wMDAwMDAsODIzLjYyNDg3OCAwLjk5OTk5Niw3ODkuNDY4NjI4ICB6Ii8+PHBhdGggZmlsbD0iI2ZmZiIgZD0iIE0xMDEyLjkyNDAxMSw2NDEuOTE1MjgzICAgQzEwMjYuMjM1NzE4LDY4MS45NTgxMzAgMTA1Ni4zNjI0MjcsNzAwLjE1NTAyOSAxMDk1LjU0MzA5MSw3MDcuMTAxMzE4ICAgQzEwNzEuNTA3MDgwLDcxMC44MDQ1NjUgMTA0OS42MjA0ODMsNzE4LjcxMjIxOSAxMDMyLjgzNTQ0OSw3MzYuODY2MjcyICAgQzEwMTYuMDkyMzQ2LDc1NC45NzUwOTggMTAwOS4yNjQwMzgsNzc3LjM2NjAyOCAxMDA2LjAyNjkxNyw4MDEuNDM5NDUzICAgQzEwMDIuNTU5MTQzLDc3OC4wMjgwMTUgOTk2LjUwNzg3NCw3NTUuNjMzMjQwIDk4MC4yNzYwMDEsNzM3LjUyMTkxMiAgIEM5NjMuODY5NTA3LDcxOS4yMTU4ODEgOTQyLjUwMjEzNiw3MTAuODgwMDA1IDkxOC4yMTQ4NDQsNzA3LjMxNjg5NSAgIEM5NzIuODE5ODI0LDY5OC40NzM0NTAgMTAwMS4zMzgzMTgsNjY1Ljg1NTY1MiAxMDA1Ljg4OTI4Miw2MTEuNjk1ODAxICAgQzEwMDguMjI3MDUxLDYyMS43NjEyOTIgMTAxMC41MjMwMTAsNjMxLjY0Njk3MyAxMDEyLjkyNDAxMSw2NDEuOTE1MjgzICB6Ii8+PC9zdmc+","uolGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjEgMTggNTM0IDUzNCI+PHBhdGggZmlsbD0iI2ZmZiIgc3Ryb2tlPSIjZmZmIiBzdHJva2Utd2lkdGg9IjMuNSIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBkPSJNNDk0LjYgNDE3LjVjLjUtLjggMS0xLjYgMS40LTIuNWwyLjMtNC4xYzEuMS0yIDIuMS00LjEgMy4yLTYuMWwxLjUtMi45Yy44LTEuNyAxLjYtMy4zIDIuNC01IC4yLS41LjUtMSAuOC0xLjV2LS41YzYwLjUtMTMxLjkgMi42LTI4Ny44LTEyOS4zLTM0OC4zUzg5LjEgNDMuOSAyOC42IDE3NS44Yy01MC4xIDEwOS4zLTE5LjcgMjM4LjYgNzMuOSAzMTQuMWwuOS44aC4zYzIwLjMgMTYuMyA0Mi45IDI5LjQgNjcuMSAzOC45bDEuOS44IDMuNCAxLjMgMy4zIDEuMiAyIC43IDQuMyAxLjVoMWMxNC4zIDQuNiAyOSA4IDQzLjkgMTAuMWgxLjVsMy44LjUgMi4xLjMgMy41LjQgNS41LjRoMzUuM2MzOS4xLTEuOSA3Ny40LTEyLjUgMTExLjktMzEuMWwuOC0uNCA0LjctMi43IDIuMS0xLjMgMy4xLTEuOSAzLjItMiAxLjktMS4yIDQuMS0yLjcuNS0uM2MxOC43LTEyLjYgMzUuNi0yNy41IDUwLjQtNDQuNSA3LjMtOC4zIDE0LTE3IDIwLjEtMjYuMmwuMy0uM2MzLjItNC44IDYuMy05LjcgOS4yLTE0LjdtLTUuMS0yMS45Yy0uNC44LS44IDEuNy0xLjMgMi42LTEgMS45LTIgMy44LTMuMSA1LjgtMSAxLjktMS40IDIuNS0yLjEgMy43cy0uOSAxLjYtMS40IDIuNWMtMi43IDQuNi01LjUgOS4yLTguNSAxMy42bC0uNC43Yy0xNy43IDI2LTQwLjEgNDguNS02Ni4xIDY2LjJsLTEgLjctMy40IDIuMi0yLjIgMS40LTIuNSAxLjYtMy41IDIuMS0xLjUuOWMtMS42LjktMy4zIDEuOS01IDIuOC0zMi42IDE4LTY4LjggMjguNS0xMDYgMzAuNUgyNDhsLTUtLjQtMy4zLS40aC0ybC0zLjYtLjVoLTEuNHEtMjEuMTUtMy4zLTQxLjQtMTAuMmwtMS4zLS40LTMuNy0xLjQtMi0uNy0zLTEtMy4zLTEuMy0xLjctLjdjLTIzLjEtOS4yLTQ0LjctMjEuOC02NC0zNy40LTM3LTMxLjYtNTguMy03Ny42LTU4LjctMTI2LjEgMC0xMC4zIDEtMjAuNyAzLTMwLjggMjguMSA5My4zIDExNCAxNTcuMSAyMTEuNCAxNTcgNi40IDAgMTMgMCAyMC4xLS45bDktLjhjMS4zLS4xIDIuMy0xLjMgMi4yLTIuNiAwLS40LS4yLS44LS41LTEuMmwtNS41LTcuMmMtMTUuOC0yMS4yLTE5LjctNTEuOC0xMS04My45IDE4LjYtNjMuOSA4MS40LTExNi4zIDE0MC44LTExNi42aDEuMmMxOC42LS40IDM2LjggNS45IDUxLjEgMTcuOSAyMy45IDIwLjcgMzEuNyA1Ni4yIDIwLjggOTQuOC0xIDMuNy0yLjMgNy41LTMuNyAxMS4zbC0uNyAxLjVjLS44IDEuNS0xLjUgMy4xLTIuMyA0LjdNMjY4IDUyYzExMy42LjIgMjA1LjcgOTIuMiAyMDUuOCAyMDUuOXY1LjVsLTEuMi0uNi0uOC0uNWMtMi4xLTEuMi00LjMtMi4zLTYuNS0zLjNsLS45LS40LTIuNi0xLjItLjktLjNjLTEuMS0uNS0yLjMtLjktMy41LTEuM2gtLjRsLTMuMS0xLTEtLjMtMi45LS44aC0uOGwtMy43LS44aC0uOWwtMy0uNWgtMS4xbC0zLjQtMS4zaC0uNWwtMy44LS40aC0xcS0xLjUtLjE1LTMgMEg0MjdjLTE4LjgtMjcuMS00My44LTQ5LjItNzMtNjQuNS0zMi40LTE2LjktNjguNS0yNS43LTEwNS4xLTI1LjctNzkuMiAwLTE1MSA0MC43LTE4Ni40IDEwMy41di02Yy4xLTExMy42IDkyLTIwNS43IDIwNS41LTIwNm02LjMgNDExLjYtNi40LjFjLTEwMC4zIDAtMTg2LTcyLjMtMjAyLjktMTcxLjMgMjcuOS03MC4xIDEwMS41LTExNy4xIDE4NC0xMTcuMSAzNC4yLS4xIDY3LjggOC4xIDk4LjEgMjMuOSAyNC4yIDEyLjcgNDUuNCAzMC41IDYyLjIgNTItMjkuOSA0LjMtNTggMTcuMS04MC45IDM2LjgtMjguNiAyMy4yLTQ5LjUgNTQuNi02MCA4OS45LTkgMzEuNC02LjcgNjEuOSA1LjkgODUuN20yNDAuOS0xNzhjMCAxMS0uNyAyMi0yLjIgMzIuOS0zLjgtMTYuOS0xMi41LTMyLjMtMjUtNDQuMy40LTUuNS43LTExIC43LTE2LjUtLjItOTkuNS02Ni44LTE4Ni42LTE2Mi44LTIxMi45IDExMSAyNy4xIDE4OS4yIDEyNi41IDE4OS4zIDI0MC44TTIxNy42IDQyLjlDMTA1LjYgNjkuMiAzMi41IDE3NyA0OS43IDI5MC44Yy03LjMgMTkuNy0xMSA0MC41LTExIDYxLjQgMCAxMi41IDEuMyAyNSAzLjkgMzcuMy01Ny40LTEyNC4yLTMuMy0yNzEuNCAxMjAuOC0zMjguOCAxNy40LTggMzUuNi0xNCA1NC4yLTE3LjgiLz48L3N2Zz4=","pillGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjI4OS4xMTgwMTEgMjY2LjM0MDc3NyA0NzIuNzAxMTU2IDQ4My4xMjk2MjQiPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKDAuMDAwMDAwLDEwMjQuMDAwMDAwKSBzY2FsZSgwLjEwMDAwMCwtMC4xMDAwMDApIiBmaWxsPSIjZmZmIj48cGF0aCBkPSJNNjE5MiA3NTU2IGMtMTA5IC0yMSAtMjA3IC01MyAtMjk3IC05OCAtMjEyIC0xMDcgLTIzNyAtMTMxIC0xMzE4IC0xMjY2IGwtMzE3IC0zMzMgMzMyIC0zNDAgYzE4MiAtMTg2IDMzNSAtMzM5IDMzOSAtMzM5IDMgMCAxMTggMTE4IDI1NSAyNjIgNDE2IDQzOSA5OTEgMTAzOSAxMDcwIDExMTYgNDIgNDEgOTQgODIgMTE3IDkzIDYyIDI4IDE0NSAyNSAyMTIgLTcgNjMgLTI5IDk4IC02OCAxMzMgLTE0MyAyMSAtNDUgMjQgLTY0IDIwIC0xMjUgLTMgLTQwIC0xMyAtODggLTI0IC0xMTEgLTIyIC00NyAtMzUgLTYxIC03ODQgLTgzNSAtMzIyIC0zMzMgLTYyMSAtNjQyIC02NjQgLTY4NyAtNDMgLTQ1IC05NyAtOTEgLTEyMCAtMTAyIC01OSAtMjkgLTExOCAtMzYgLTE3NCAtMjEgLTQzIDEyIC04MyA0OCAtNDkyIDQ1NiAtNDgyIDQ3OSAtNDY2IDQ2NiAtNTU3IDQ1MCAtNDIgLTggLTgxIC00NSAtNTAxIC00ODYgLTE2OSAtMTc3IC0yNTMgLTI3MyAtMzAyIC0zNDcgLTExNCAtMTY5IC0xNzAgLTMwOCAtMjA1IC01MDYgLTE0IC03NyAtMTYgLTEzMCAtMTIgLTI0MCA0IC04NyAxNCAtMTc2IDI2IC0yMzAgMTAwIC00MzIgNDYzIC04MDQgOTAxIC05MjEgMzA0IC04MiA2MjggLTM5IDkwNSAxMTkgNDMgMjUgMTEyIDcwIDE1MiAxMDEgMzkgMzAgMjU1IDI0MyA0ODAgNDcyIDE4OTIgMTkzMiAxODc5IDE5MTggMjAwOSAyMDk4IDgyIDExNCAxNTkgMjc5IDE5NSA0MjEgMjUgOTcgMzIgMTQ1IDM2IDI2OCA2IDE4MCAtMTAgMjkyIC02MyA0NTIgLTkxIDI3NCAtMjU0IDQ4NSAtNTAzIDY1MyAtOTQgNjMgLTIzNyAxMjggLTM0OCAxNTcgLTgyIDIyIC0xMjUgMjYgLTI2MyAyOSAtMTExIDIgLTE4OSAtMSAtMjM4IC0xMHoiIGZpbGwtcnVsZT0iZXZlbm9kZCIvPjwvZz48L3N2Zz4=","lastfmGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9Ii04IC04IDUyOCAzMDAuNDk4Ij48cGF0aCBmaWxsPSIjZmZmIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiIGNsaXAtcnVsZT0iZXZlbm9kZCIgZD0iTTIyNS43NzkgMjUzLjQwM2wtMTguODIzLTUxLjAwN3MtMzAuNDkxIDM0LjAyNi03Ni4xODggMzQuMDI2Yy00MC41MjYgMC02OS4yMjQtMzUuMTk5LTY5LjIyNC05MS40OTcgMC03Mi4wOTYgMzYuMzc4LTk3Ljg5MSA3Mi4wOTctOTcuODkxIDUxLjU4NCAwIDY3Ljk5NyAzMy40MTEgODIuMTExIDc2LjIwNWwxOC44MjMgNTguNTkzYzE4LjgxMyA1Ni44OTQgNTQuMDIxIDEwMi42MDkgMTU1LjM5OSAxMDIuNjA5IDcyLjcwMSAwIDEyMi4wMjYtMjIuMzA5IDEyMi4wMjYtODAuOTE0IDAtNDcuNDktMjcuMDA3LTcyLjA5Ny03Ny40MTgtODMuODk4bC0zNy40OTQtOC4xOTFjLTI1LjgwNy01Ljg4Ny0zMy40MTEtMTYuNDEyLTMzLjQxMS0zNC4wMjggMC0xOS45MDggMTUuODA4LTMxLjcxMyA0MS42MTUtMzEuNzEzIDI4LjE4NiAwIDQzLjM5IDEwLjU5MiA0NS42ODcgMzUuODEzbDU4LjYwMi03LjAwM0M1MDQuODc3IDIxLjY5NSA0NjguNDk2IDAgNDA4LjY5MyAwYy01Mi44MTEgMC0xMDQuNDA0IDE5LjkwOC0xMDQuNDA0IDgzLjkwNyAwIDM5LjkwNCAxOS4zOTEgNjUuMDg1IDY4LjAwNSA3Ni44MDJsMzkuOTA0IDkuNGMyOS44NzcgNy4wMTMgMzkuODk0IDE5LjM5MSAzOS44OTQgMzYuMzkxIDAgMjEuNjk1LTIxLjA5OSAzMC40OTQtNjAuOTkzIDMwLjQ5NC01OS4yMSAwLTgzLjkwOC0zMS4xMDgtOTcuOTA0LTczLjg5M2wtMTkuMzkxLTU4LjU5M0MyNDkuMjk3IDI4LjIxMSAyMDkuOTk4LjA5MSAxMzIuMDE0LjA5MSA0NS43OTEuMDkxIDAgNTQuNTkzIDAgMTQ3LjI5NmMwIDg5LjA5NiA0NS42OTcgMTM3LjIwMiAxMjcuOTE0IDEzNy4yMDIgNjYuMjAxIDAgOTcuODkyLTMxLjEwNyA5Ny44OTItMzEuMTA3bC0uMDI3LjAwOXYuMDAzeiIvPjwvc3ZnPg==","transitGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjIxLjIgMTcuMyAyMDkuMyAxNjQuMiI+PHBhdGggZmlsbD0iI2ZmZiIgZD0iTTI4LjUgNDQuNUg5MkMxMDguNSA0NC41IDEyMSA1NyAxMjEgNzIuNUMxMjEgNzcgMTIwLjUgODEuOCAxMTkuNSA4Ni41TDEwMC4xIDE4MS41SDEwNy44QzExOS4yIDE4MS41IDEyOC43IDE3My41IDEzMS41IDE1OS42TDE0OC4xIDc4LjJIMTk3QzIxMC4yIDc4LjIgMjE5LjggNjguNiAyMjMgNTAuNUgxNTUuMkwxNDkuMiA3Ny40QzE0OC41IDc3IDE0OCA3Ni4yIDE0Ny45IDc0LjRDMTQ2LjMgNDIuMiAxMjUuNSAxNy41IDk3LjIgMTcuNUg0OUMzNy44IDE3LjUgMzAuMiAyNy40IDI4LjUgNDQuNVoiLz48cGF0aCBmaWxsPSIjZmZmIiBkPSJNMjcuNCA1MC4zSDkxLjhDMTA0LjcgNTAuMyAxMTQuOSA1OS44IDExNC45IDcyLjVDMTE0LjkgNzcuMiAxMTQuMSA4Mi4yIDExMy4yIDg2LjVMOTQuMiAxODEuNUg2Ni4zTDg3LjUgNzguN0gyMS4yTDI3LjQgNTAuM1oiLz48cGF0aCBmaWxsPSIjZmZmIiBkPSJNMTYyLjQgMTcuM0gyMzAuNUwyMjQuNiA0NC40SDE1Ni4yTDE2Mi40IDE3LjNaIi8+PC9zdmc+","calendarGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjE3OSAyMTcgNjU3IDU3MCIgZmlsbD0iI2ZmZiI+CiAgPHBhdGggZD0iTTczNC42MDM3NjAsNTMwLjQ0NTQzNSAgIEM3MzcuMjI1MjIwLDU0Mi4wNTQxOTkgNzM5LjQwNDU0MSw1NTMuMzEzNjYwIDc0MS40MzQ4NzUsNTY0LjYxNjI3MiAgIEM3NDYuNzA4NjE4LDU5My45NzM4MTYgNzI0LjczNDk4NSw2MjcuODY3MTI2IDY5Mi41ODYwNjAsNjM0LjYwODg4NyAgIEM2MzEuMzMxMjM4LDY0Ny40NTQyMjQgNTcwLjI1OTgyNyw2NjEuMTc2NTE0IDUwOC44NTYxNDAsNjczLjMyOTE2MyAgIEM0OTguNDEwNTIyLDY3NS4zOTY0ODQgNDg3Ljk4Mzk0OCw2NzcuNTg5NzIyIDQ3Ny42MTU3NTMsNjgwLjAxMjIwNyAgIEM0NTQuMTUzNzc4LDY4NS40OTQxNDEgNDMzLjgwNDc3OSw2ODAuMTI2ODMxIDQxNi41ODAyMDAsNjYzLjEyNTczMiAgIEM0MTEuNDY1NDI0LDY1OC4wNzcyMDkgNDA4LjA4NzQ2Myw2NTIuMDM0NDg1IDQwNS44NDIyMjQsNjQ1LjMyODE4NiAgIEM0MDMuNDY5MzYwLDYzOC4yNDA3ODQgNDA1LjkyMTkzNiw2MzMuMzQ0MzYwIDQxMy4yMTY0NjEsNjMxLjA2NTczNSAgIEM0MjQuMTYzNTQ0LDYyNy42NDU5OTYgNDMyLjU4NjM5NSw2MjEuMjQ5OTM5IDQzNy4wMDAwOTIsNjEwLjU2OTM5NyAgIEM0NDIuODQ0Njk2LDU5Ni40MjYzMzEgNDQxLjk2MjYxNiw1ODIuMTg3MzE3IDQzMy44ODkzNDMsNTY5LjM1Njg3MyAgIEM0MjAuOTcyMTY4LDU0OC44MjgzNjkgMzkxLjY3NTg0Miw1NDkuNjU5NjA3IDM3OC4yMjg2MzgsNTcwLjU2MzQ3NyAgIEMzNzUuODM3OTgyLDU3NC4yNzk4NDYgMzczLjM5OTU2Nyw1NzYuNzQ5MDg0IDM2OC44MDUyMzcsNTc3LjY1NTAyOSAgIEMzNjQuMjE3NzEyLDU3OC41NTk2OTIgMzYwLjI2MTYyNyw1ODEuMzMwMDE3IDM1Ny41OTQ3ODgsNTg1LjQ2MjcwOCAgIEMzNTYuMTg5NDg0LDU4Ny42NDA0NDIgMzU2LjAyMjk0OSw1ODkuODQwMDg4IDM1Ny43Njg5MjEsNTkxLjkyMzUyMyAgIEMzNTkuNTc2OTY1LDU5NC4wODExMTYgMzYxLjc1NTM0MSw1OTQuMDc5MTYzIDM2NC4wNTUyOTgsNTkyLjgwNTQ4MSAgIEMzNjkuNzQ5NTQyLDU4OS42NTIyODMgMzc1LjY4NTY5OSw1ODcuODQ3OTAwIDM4Mi4yNjQ5MjMsNTg5LjM2NDY4NSAgIEMzODQuNDUwNzE0LDU4OS44Njg1OTEgMzg1LjE5MTQwNiw1ODguMzc2OTUzIDM4NS43OTA1ODgsNTg2LjcwODA2OSAgIEMzODcuMTUyMTYxLDU4Mi45MTU1MjcgMzg5LjAzODcyNyw1NzkuNDA5OTEyIDM5MS41NjM0NDYsNTc2LjI1Nzg3NCAgIEM0MDAuMDg2MDkwLDU2NS42MTczNzEgNDE1LjA4MzIyMSw1NjYuNzcwODEzIDQyMS41MTU1MzMsNTc4Ljc2NzAyOSAgIEM0MjkuNDIxODE0LDU5My41MTIwMjQgNDI0LjUwODI0MCw2MTYuNTI2MzY3IDQwMy4yNzI5MTksNjE5LjI4ODM5MSAgIEM0MDAuMDEyMDU0LDYxOS43MTI0NjMgMzk4LjU2MDczMCw2MjIuMjUyMTk3IDM5Ni42NzU1MDcsNjI0LjI0NjMzOCAgIEMzODcuNzQyMjc5LDYzMy42OTU2NzkgMzc3LjAwOTY3NCw2MzkuMjkwNzEwIDM2My45NTA4MDYsNjQwLjYwODUyMSAgIEMzNDEuNTA1MTU3LDY0Mi44NzM3NzkgMzIzLjM2MDI5MSw2MjQuMDY1NDkxIDMyNy4yMzQ5ODUsNjAxLjg1NTI4NiAgIEMzMzIuMjc5MzU4LDU3Mi45NDA0OTEgMzQ4LjI0NDUwNyw1NTEuOTM3MDczIDM3NC41OTkzOTYsNTM5LjEwMDU4NiAgIEMzNzkuMDk5MDMwLDUzNi45MDg5MzYgMzgwLjIxNzgwNCw1MzQuNjY5ODAwIDM3OS4xNTE3OTQsNTI5Ljg4MTQwOSAgIEMzNzEuNDg2NDgxLDQ5NS40NTAyODcgMzY2LjAzMTA2Nyw0NjAuNjAxNTkzIDM1OS44MDA2MjksNDI1Ljg5ODQ2OCAgIEMzNTkuMDYyODk3LDQyMS43ODkzMDcgMzYwLjAzNjQwNyw0MjAuMDAxMzQzIDM2NC40NDY5NjAsNDE5LjIyNjE2NiAgIEM0MDQuNDQwNDMwLDQxMi4xOTcyMDUgNDQ0LjM5NTAyMCw0MDQuOTQ2ODY5IDQ4NC4zNjAxMzgsMzk3Ljc1NjY1MyAgIEM1MjAuMzkyMDkwLDM5MS4yNzQxMzkgNTU2LjQyMzA5NiwzODQuNzg2NDk5IDU5Mi40NTQyODUsMzc4LjI5OTc0NCAgIEM2MjYuNTMwMzM0LDM3Mi4xNjUwMDkgNjYwLjYwMjc4MywzNjYuMDEwNTI5IDY5NC42ODQzMjYsMzU5LjkwNjE4OSAgIEM3MDIuNDk4MTA4LDM1OC41MDY2ODMgNzAyLjc3MTk3MywzNTguNTM3MDc5IDcwNC4xNzkxOTksMzY2LjE5OTA2NiAgIEM3MDcuNjMzMTE4LDM4NS4wMDQ4NTIgNzEwLjgxMjI1Niw0MDMuODYwOTYyIDcxNC4xMjI4MDMsNDIyLjY5MzIwNyAgIEM3MjAuMTk4ODUzLDQ1Ny4yNTcwMTkgNzI2LjkzOTA4Nyw0OTEuNjk0MTIyIDczMy44NjkwMTksNTI2LjA5NTAzMiAgIEM3MzQuMTMxNDcwLDUyNy4zOTgwNzEgNzM0LjI4MzAyMCw1MjguNzIzNTcyIDczNC42MDM3NjAsNTMwLjQ0NTQzNSAgTTUyNS44MzQ1MzQsNDY4LjM5OTgxMSAgIEM1MjUuMjMwNDY5LDQ2NS40NjgxNDAgNTI0Ljc0NDYyOSw0NjIuNTA2MTM0IDUyNC4wMDA3MzIsNDU5LjYxMDM4MiAgIEM1MjEuNDg5MzgwLDQ0OS44MzQwNzYgNTEyLjk0NzkzNyw0NDMuNzE3NzczIDUwMy43MDM3MzUsNDQ0Ljk3MzgxNiAgIEM0OTUuMDI3MjUyLDQ0Ni4xNTI3NDAgNDg4LjU0Mzg1NCw0NTQuMDU3NzA5IDQ4OS4wMTk2ODQsNDY0LjEwOTYxOSAgIEM0ODkuNjQ5OTk0LDQ3Ny40MjQ1MzAgNDkyLjMwMjI0Niw0OTAuNTI4OTkyIDQ5NS40ODAzNDcsNTAzLjQyMjk0MyAgIEM0OTcuNzYwNzczLDUxMi42NzUwNDkgNTA4LjEzNTgzNCw1MTguNDY5OTEwIDUxNi40MjM4ODksNTE2LjM3MzIzMCAgIEM1MjUuNjAzOTQzLDUxNC4wNTA5MDMgNTMyLjUyOTQ4MCw1MDMuOTIzODI4IDUzMC45NTA0MzksNDk0LjY4MDAyMyAgIEM1MjkuNDk1MzAwLDQ4Ni4xNjE5NTcgNTI3LjY4Nzc0NCw0NzcuNzA0MTAyIDUyNS44MzQ1MzQsNDY4LjM5OTgxMSAgTTYzNy40NzY4NjgsNDIzLjgxMTczNyAgIEM2MjIuMzQwNTc2LDQxOC42MDk3NDEgNjEwLjQ3MzYzMyw0MjcuODc5NzYxIDYxMi40NTIzMzIsNDQzLjk5OTYwMyAgIEM2MTMuODg1NzQyLDQ1NS42Nzc1ODIgNjE2LjIwNTAxNyw0NjcuMjc1MzkxIDYxOC42OTc2OTMsNDc4Ljc4NTg1OCAgIEM2MjAuNzU5Mzk5LDQ4OC4zMDYwMzAgNjMwLjQ2MTMwNCw0OTQuMzkwMDQ1IDYzOS40MzI5MjIsNDkyLjU2MjQzOSAgIEM2NDkuMTA5Mzc1LDQ5MC41OTEzMDkgNjU1Ljc1NDIxMSw0ODAuNzQ3NDA2IDY1My45ODI4NDksNDcwLjg1OTIyMiAgIEM2NTEuOTYzMzc5LDQ1OS41ODU5NjggNjQ5LjcxNzA0MSw0NDguMzUxNjg1IDY0Ny44MzIyNzUsNDM3LjA1NjYxMCAgIEM2NDYuODEzODQzLDQzMC45NTM3NjYgNjQzLjg1MjcyMiw0MjYuNjEyODg1IDYzNy40NzY4NjgsNDIzLjgxMTczNyAgeiIvPgogIDxwYXRoIGQ9Ik00ODUuMjc3MjIyLDMwMi4xNTI4MzIgICBDNTE2LjY3ODU4OSwyOTYuNTU1NTczIDU0Ny42NTQ5NjgsMjkxLjA0MjU0MiA1NzguNjMzMjQwLDI4NS41NDAxNjEgICBDNTg0LjQyMDQxMCwyODQuNTEyMjM4IDU4NC43Mjg1NzcsMjg0LjgyMzIxMiA1ODUuMzIzMTIwLDI5MC43OTk5NTcgICBDNTg1LjkzMTk0NiwyOTYuOTIwNzE1IDU4Ni41ODYyNDMsMzAzLjAzOTMzNyA1ODcuMzY3MzEwLDMwOS4xNDAxMDYgICBDNTg4Ljk5NDE0MSwzMjEuODQ2NzQxIDYwMS41MzkwNjIsMzMzLjQwMTEyMyA2MTMuOTAxMDYyLDMzMy42ODU1MTYgICBDNjI3LjU5Njc0MSwzMzQuMDAwNjQxIDYzOS4xNTgzMjUsMzI0LjEyMzM4MyA2NDEuODA4MTA1LDMxMC41NDE5MzEgICBDNjQzLjE1NTIxMiwzMDMuNjM3MTQ2IDY0MC43NjkzNDgsMjk3LjQwMzk5MiA2MzkuNzgxNzM4LDI5MC44OTYwODggICBDNjM5LjM1NzA1NiwyODguMDk3MTY4IDYzOC45Mzk5NDEsMjg1LjI5NjUwOSA2MzguNDY0NzgzLDI4Mi41MDU5NTEgICBDNjM3LjQ0NTI1MSwyNzYuNTE4MDA1IDYzOC4xNjk2NzgsMjc1LjY5NDcwMiA2NDQuNDYzMDc0LDI3NS44MDkzMjYgICBDNjY2LjAyOTc4NSwyNzYuMjAxOTk2IDY4Ny4xMzMzMDEsMjkyLjA4ODM0OCA2OTQuMDYxNjQ2LDMxMy4yMTM3NDUgICBDNjk3LjAzMDM5NiwzMjIuMjY1OTYxIDY5OC4yNzIxNTYsMzMxLjY5Mjk5MyA2OTkuOTE5MTg5LDM0MS4wMzA5NDUgICBDNzAwLjQ5NTg1MCwzNDQuMzAwNTA3IDY5OC40OTczMTQsMzQ0Ljk5MDIwNCA2OTUuOTQxMjg0LDM0NS40NDE4OTUgICBDNjgzLjY1NTIxMiwzNDcuNjEyOTE1IDY3MS4zODEwNDIsMzQ5Ljg1MTEzNSA2NTkuMTAwNzY5LDM1Mi4wNTQ2ODggICBDNjE4Ljk3NzM1NiwzNTkuMjU0MjcyIDU3OC44NTMzMzMsMzY2LjQ0OTczOCA1MzguNzI5MzA5LDM3My42NDU2NjAgICBDNDg4LjEyMjQ2NywzODIuNzIxNjE5IDQzNy41MTUwNzYsMzkxLjc5NDY3OCAzODYuOTA4ODEzLDQwMC44NzM2NTcgICBDMzc4LjA2NjcxMSw0MDIuNDU5OTYxIDM2OS4yMjI5NjEsNDA0LjAzODMwMCAzNjAuMzkzMDA1LDQwNS42ODk4ODAgICBDMzU4LjE4ODc1MSw0MDYuMTAyMTczIDM1Ni4yNjk3NzUsNDA2LjE4NjY0NiAzNTUuOTMwNTQyLDQwMy4zNTYyNjIgICBDMzU0LjY0OTU2NywzOTIuNjY4MzY1IDM1MS4xMDcxNDcsMzgyLjIxMjYxNiAzNTEuODczMDc3LDM3MS4zMzI4NTUgICBDMzUzLjUxMzAwMCwzNDguMDM4Njk2IDM2NC44NzYyODIsMzMxLjI0MzU5MSAzODYuMjE4NzE5LDMyMS4zOTg3NDMgICBDMzkyLjY1NTM5NiwzMTguNDI5NjI2IDM5OS42NDc2MTQsMzE3LjU4NTY5MyA0MDYuNDU0NzEyLDMxNi4wMzIzNzkgICBDNDEwLjMyOTQ5OCwzMTUuMTQ4MTkzIDQxMC45NjE1MTcsMzE3LjcxNDQ3OCA0MTEuMjQ0OTA0LDMyMC42MDkzNzUgICBDNDExLjkwODE3MywzMjcuMzg1OTU2IDQxMi4xNDQxOTYsMzM0LjIzNjQ4MSA0MTMuMzA5OTA2LDM0MC45MjUyMzIgICBDNDE1Ljk2ODc1MCwzNTYuMTgxNTgwIDQzMC41MzE5MjEsMzY1Ljc1NzA1MCA0NDYuMDM4ODE4LDM2Mi44NjYwODkgICBDNDYwLjYzMTAxMiwzNjAuMTQ1NjkxIDQ3MC45ODA5MjcsMzQ1LjAwNzIzMyA0NjguMjQxNTQ3LDMzMC40Mjk2NTcgICBDNDY3LjA0MzczMiwzMjQuMDU1NzI1IDQ2Ni4xMzgyMTQsMzE3LjYxNTg0NSA0NjQuNjU2NTg2LDMxMS4zMTAwODkgICBDNDYzLjY2NjYyNiwzMDcuMDk2NzcxIDQ2NS4yNTkyNzcsMzA1LjYyMzM1MiA0NjkuMTI1MzA1LDMwNS4wNTczNDMgICBDNDc0LjM5MjM2NSwzMDQuMjg2MTk0IDQ3OS42MTI1NDksMzAzLjE5NDc5NCA0ODUuMjc3MjIyLDMwMi4xNTI4MzIgIHoiLz4KICA8cGF0aCBkPSJNNDIxLjUzNTcwNiw3MjcuNTM1ODI4ICAgQzQyMC45NTEwMTksNzM0LjgyOTg5NSA0MjEuNDc5MzcwLDc0MS42Mjc5MzAgNDIyLjQxMTgwNCw3NDguMzk2MTE4ICAgQzQyMi43MTgwNzksNzUwLjYxOTI2MyA0MjIuMjUzMDUyLDc1Mi4xNTg4NzUgNDIwLjE3MzczNyw3NTMuMzY3NjE1ICAgQzM5My4zMDEyMDgsNzY4Ljk5MDE3MyAzNjUuNTY3MzUyLDc3My45NDM3ODcgMzM2LjM4MjUzOCw3NjAuMDcyNTEwICAgQzMyNi4wNjYxOTMsNzU1LjE2OTI1MCAzMTguNjYzNzI3LDc0Ni42MzgzNjcgMzEwLjk2NTMzMiw3MzguNTkyNzczICAgQzMwMC42OTQ2NDEsNzI3Ljg1OTA3MCAyODguMzg1MjU0LDcyMi40NzA2NDIgMjczLjQ1OTgwOCw3MjMuMjc5MTE0ICAgQzI2NS40MjcwOTQsNzIzLjcxNDIzMyAyNTguMjIxMTkxLDcyNi40Mjg1ODkgMjUxLjYwMjkwNSw3MzAuOTEzNjk2ICAgQzI1MC4xNjEzMTYsNzMxLjg5MDY4NiAyNDkuMDQ0ODkxLDczMy42NTk5MTIgMjQ2LjI5MTkxNiw3MzMuMTA4ODI2ICAgQzI0Ny45MjQzOTMsNzIzLjU4OTIzMyAyNTAuNjUyMTkxLDcxNC40NTkwNDUgMjU0LjU4NjIxMiw3MDUuNjg5MTQ4ICAgQzI2MS43MDM2NDQsNjg5LjgyMjU3MSAyNzAuODE5NjQxLDY3NS4xMDEzNzkgMjgwLjk1NDIyNCw2NjEuMDMzNDQ3ICAgQzI5MS41MjgyNTksNjQ2LjM1NTQ2OSAzMDAuMTYyNzIwLDYzMC42NzUyOTMgMzA2LjgzOTYzMCw2MTMuODgxODk3ICAgQzMwNy41NDc1NDYsNjEyLjEwMTM3OSAzMDcuODE4OTA5LDYwOS45NTExMTEgMzEwLjA4MDM4Myw2MDguNzk1NTMyICAgQzMxMi4wMjY3OTQsNjEwLjMwMzg5NCAzMTEuNTk0MzYwLDYxMi43MTAwMjIgMzExLjk0MjYyNyw2MTQuNjQwMzgxICAgQzMxNS44Nzk4ODMsNjM2LjQ2MzkyOCAzMjcuNDQ3OTY4LDY1MS4wNzA2NzkgMzUwLjA1MDE0MCw2NTUuMzczMjMwICAgQzM1Mi42NzI0NTUsNjU1Ljg3MjQzNyAzNTQuMzY4NDA4LDY1Ni44NTQxMjYgMzU1LjQzMjE1OSw2NTkuNjkzOTA5ICAgQzM2NC4yNzI0NjEsNjgzLjI5NDA2NyAzODEuNDg0NDM2LDY5Ny4yODUzMzkgNDA1LjgzNTM1OCw3MDIuNzg4NDUyICAgQzQxMC42OTg3MDAsNzAzLjg4NzUxMiA0MTUuNjMwMDM1LDcwNC43MjM0NTAgNDIwLjU2MjQwOCw3MDUuNDY1MzkzICAgQzQyMy41ODQxOTgsNzA1LjkxOTkyMiA0MjQuNDAzMDE1LDcwNy40MzIzMTIgNDIzLjk1MzU4Myw3MTAuMjg3MzU0ICAgQzQyMy4wNzQ3OTksNzE1Ljg3MDM2MSA0MjIuMzgwMDA1LDcyMS40ODIyMzkgNDIxLjUzNTcwNiw3MjcuNTM1ODI4ICB6Ii8+CiAgPHBhdGggZD0iTTIwNC43NDUzOTIsNzUyLjE5MDkxOCAgIEMxODYuMzA2Mzk2LDcyMy4wODMxOTEgMTg0LjA4NzU4NSw2OTMuNTk1MDkzIDIwMS43MDQzMzAsNjYzLjc5MDU4OCAgIEMyMDcuNjUyMTQ1LDY1My43Mjc5NjYgMjE2LjUxMTkwMiw2NDUuODgwMzEwIDIyNC45NzE3ODYsNjM3LjgyNDQ2MyAgIEMyMzkuNTQ5NDg0LDYyMy45NDI5MzIgMjU0Ljk5ODkxNyw2MTAuOTIwOTU5IDI2Ny41NTE0MjIsNTk1LjAxMzczMyAgIEMyNzguNDIwNzE1LDU4MS4yMzk2ODUgMjg3LjAxMzMzNiw1NjUuOTg3ODU0IDI5Ni4yNjAwNDAsNTUxLjE1NjI1MCAgIEMzMDYuNjMzNzg5LDUzNC41MTY4NDYgMzIwLjkxMTA0MSw1MjMuNDExNDM4IDM0MC42NzI5MTMsNTIwLjIxNDYwMCAgIEMzNDguMDU4OTkwLDUxOS4wMTk3MTQgMzU4LjQ4NzI0NCw1MjIuMTI0MjY4IDM2My4zMjc5MTEsNTI3LjAxMTI5MiAgIEMzNjIuNjEyNzYyLDUyOS4yNDk2MzQgMzYwLjM3NTQ4OCw1MjkuNTAwMDYxIDM1OC42NzI4MjEsNTMwLjI0MTMzMyAgIEMzNDEuNDc4NTc3LDUzNy43MjYyNTcgMzI4LjQ1MjU3Niw1NDkuODYwMDQ2IDMxOC4xMzE0NzAsNTY1LjM3Mjk4NiAgIEMzMDcuMzAwNDE1LDU4MS42NTI0NjYgMjk4LjA0OTEwMyw1OTguNzQwODQ1IDI5MC4yOTU2NTQsNjE2LjY1NTE1MSAgIEMyODIuMDg3MjgwLDYzNS42MjA1NDQgMjcwLjA1MzI1Myw2NTIuMTkzNjA0IDI1OC4wNDMwNjAsNjY4Ljc5Njg3NSAgIEMyNDIuMjgyNjg0LDY5MC41ODQ0NzMgMjMzLjI3MjMyNCw3MTQuOTQ3NjMyIDIyOS45MTMyMzksNzQxLjU3NjkwNCAgIEMyMjguNzY5MDU4LDc1MC42NDc0MDAgMjI4LjYyNzM2NSw3NTkuNTg5ODQ0IDIyOS44OTAwNDUsNzY4LjYyNjY0OCAgIEMyMzAuMjE2OTM0LDc3MC45NjYxODcgMjMwLjY4OTg4MCw3NzMuNjAwNTI1IDIyOC4yNTkwMzMsNzc2LjYxMTA4NCAgIEMyMTkuMzI0NDkzLDc2OS41Nzc1NzYgMjExLjY4Mjc3MCw3NjEuNDk4NzE4IDIwNC43NDUzOTIsNzUyLjE5MDkxOCAgeiIvPgogIDxwYXRoIGQ9Ik00NDAuNjU0NjYzLDc0NS4wMDI0NDEgICBDNDM4LjA5OTYwOSw3MzEuOTg5NzQ2IDQ0MC44ODk4MzIsNzE5Ljc0Njc2NSA0NDMuMDk4ODQ2LDcwNy40Nzc0NzggICBDNDQzLjU5MTEyNSw3MDQuNzQzMzQ3IDQ0Ni4xNTkyNzEsNzA0LjY5MDI0NyA0NDguMjI1MDY3LDcwNC4yOTQxMjggICBDNDY2Ljg1NDQ5Miw3MDAuNzIyMTY4IDQ4NS40OTQwODAsNjk3LjIwMzA2NCA1MDQuMTMwMjQ5LDY5My42NjYyNjAgICBDNTA5LjQ0MzM1OSw2OTIuNjU3ODk4IDUxNC43NTUxODgsNjkxLjY0MjgyMiA1MTkuNjM0NDYwLDY5MC43MTMzMTggICBDNTIxLjI3NDQ3NSw2OTIuNjA5MzE0IDUyMC42MTAxNjgsNjkzLjkyNzczNCA1MjAuMzAxMDI1LDY5NS4xNjI0NzYgICBDNTE1LjY0NTM4Niw3MTMuNzU5MjE2IDUwOS44MTgxNDYsNzMxLjk3OTM3MCA1MDIuMjMyODQ5LDc0OS41OTQwNTUgICBDNDk4LjYwMTE2Niw3NTguMDI3NjQ5IDQ5My4zMTU1NTIsNzY1LjI3Njk3OCA0ODQuODg0ODU3LDc2OS41NTMxMDEgICBDNDY3LjQzMzcxNiw3NzguNDA0NDE5IDQ1Mi40MzU3MzAsNzY4LjUzOTMwNyA0NDUuMDM0NDI0LDc1Ni41NTg0MTEgICBDNDQyLjkyNTUzNyw3NTMuMTQ0NjUzIDQ0MS41NDQyMjAsNzQ5LjM3NjUyNiA0NDAuNjU0NjYzLDc0NS4wMDI0NDEgIHoiLz4KICA8cGF0aCBkPSJNNjQyLjI4ODU3NCw3MDIuMTYwNjQ1ICAgQzYzOC45OTczNzUsNzEwLjIxNDE3MiA2MzYuNDk1MzAwLDcxOC4xODU5NzQgNjMyLjQxMDIxNyw3MjUuNDc1MzQyICAgQzYyNi41NTc3MzksNzM1LjkxODE1MiA2MTguMzk3ODg4LDc0My40MjE1NzAgNjA1LjkwODQ0Nyw3NDQuMTc4NTI4ICAgQzU5MC4yNTQwODksNzQ1LjEyNzMxOSA1NzYuNDc2Mzc5LDczMi4yOTQ4MDAgNTc0LjAyNTg3OSw3MTQuOTQzNjA0ICAgQzU3Mi40MTg3NjIsNzAzLjU2NDQ1MyA1NzQuODExNzA3LDY5Mi40MDQ0MTkgNTc2LjE3MDM0OSw2ODEuMTk4NDI1ICAgQzU3Ni41MDU3MzcsNjc4LjQzMTc2MyA1NzguNjMwMDY2LDY3Ny40ODAxNjQgNTgxLjA2NjY1MCw2NzYuOTYzNjIzICAgQzYwMy4xNzUyMzIsNjcyLjI3NjI0NSA2MjUuMjc0MzUzLDY2Ny41NDM5NDUgNjQ3LjM5MTg0Niw2NjIuODk5MTcwICAgQzY1Mi4xMDE5OTAsNjYxLjkxMDAzNCA2NTMuMzQyNDY4LDY2My4xMzA3MzcgNjUyLjI0NjI3Nyw2NjcuNzgzODc1ICAgQzY0OS41Mzc5MDMsNjc5LjI3OTkwNyA2NDYuNTEyMDg1LDY5MC42OTM0ODEgNjQyLjI4ODU3NCw3MDIuMTYwNjQ1ICB6Ii8+CiAgPHBhdGggZD0iTTc1MC45Njc4OTYsNTAwLjA3NzIwOSAgIEM3NDYuODI5NzEyLDUwMS42MTcxMjYgNzQ3LjQxODk0NSw0OTguMTkwMjc3IDc0Ny4xMDI5MDUsNDk2LjU1NjAzMCAgIEM3NDQuNTE1OTMwLDQ4My4xNzY5MTAgNzQyLjAzMjk1OSw0NjkuNzc1MTE2IDczOS43NDc1NTksNDU2LjM0MTI0OCAgIEM3MzguNTUzODk0LDQ0OS4zMjQ2NDYgNzM1LjA2NDU3NSw0NDEuODcyMzQ1IDczNy4xMTY3NjAsNDM1LjM1MjExMiAgIEM3MzkuMDk4OTk5LDQyOS4wNTQzNTIgNzQ0LjY5MDczNSw0MjMuNzQ5NTQyIDc0OS4xOTcyNjYsNDE4LjQzMjY3OCAgIEM3NTMuMTI2ODkyLDQxMy43OTY1MDkgNzU0LjQ1MDE5NSw0MDkuNTAxNDA0IDc1MS44NzEwMzMsNDAzLjU1MjM5OSAgIEM3NDkuNzg5NTUxLDM5OC43NTE0OTUgNzQ4Ljg3Mzg0MCwzOTMuMzU3NjA1IDc1MC42Nzg0NjcsMzg3Ljk0ODQ4NiAgIEM3NTIuOTcxNzQxLDM4MS4wNzQ2NDYgNzU4LjkwNTE1MSwzNzcuNDIzMjE4IDc2NS4yOTI5NjksMzc5LjE2MTE2MyAgIEM3NzIuNjE1NzIzLDM4MS4xNTM0NDIgNzc4LjQ5OTgxNywzODkuMjY3NTE3IDc3Ny4xOTQwMzEsMzk1LjUwMDczMiAgIEM3NzYuNTk1NjQyLDM5OC4zNTY3ODEgNzc0LjY1Mjk1NCwzOTkuNzEzODk4IDc3Mi4wMjAzMjUsNDAwLjE5MzU3MyAgIEM3NzAuMDYyOTg4LDQwMC41NTAyOTMgNzY4LjA1MDExMCw0MDAuNTkxMDY0IDc2Ni4wNzA1NTcsNDAwLjg0MDc5MCAgIEM3NjQuNDQ3NjkzLDQwMS4wNDU1MDIgNzYyLjUxNjE3NCw0MDAuOTY0MDUwIDc2Mi4wMjE5NzMsNDAzLjAzODc4OCAgIEM3NjEuNDg5MTM2LDQwNS4yNzU3NTcgNzYyLjU3NTk4OSw0MDcuNDUxOTk2IDc2NC40OTc1NTksNDA3Ljk2MTczMSAgIEM3NzAuMzczOTAxLDQwOS41MjA1MDggNzcyLjM1MDQ2NCw0MTQuMjMzMDkzIDc3NS4wNzc4MjAsNDE4Ljg1ODc2NSAgIEM3NzguNDE4ODIzLDQyNC41MjUyOTkgNzgzLjY5MTE2Miw0MjguNDQ5MDA1IDc4OS45NzAzMzcsNDMwLjg2NzE4OCAgIEM3OTIuMjUzOTA2LDQzMS43NDY2NDMgNzk0LjgyNTM3OCw0MzIuMDE3MzAzIDc5Ny4zMzkwNTAsNDM0LjE5MTE2MiAgIEM3OTUuMjQyNDkzLDQzOC4xNjU2MTkgNzkzLjIyNjE5Niw0NDIuMjk4OTIwIDc5MC45MzY3MDcsNDQ2LjI3NDg3MiAgIEM3ODAuMjc0MTA5LDQ2NC43OTE0NzMgNzY3LjczNTg0MCw0ODEuOTYwNTcxIDc1My41MjQzNTMsNDk3LjkwOTYwNyAgIEM3NTIuODY2NDU1LDQ5OC42NDc5MTkgNzUyLjAyNzU4OCw0OTkuMjI0OTc2IDc1MC45Njc4OTYsNTAwLjA3NzIwOSAgeiIvPgogIDxwYXRoIGQ9Ik01NzEuNDYzOTg5LDIzNS40ODA1MzAgICBDNTg2Ljg2ODQwOCwyMjAuMjUzMjgxIDYwOC4wOTc1OTUsMjI3LjUzNTQ0NiA2MTYuNzc0NjU4LDI0Mi40Mjk0ODkgICBDNjIxLjMzNTE0NCwyNTAuMjU3Mzg1IDYyMi42Mzg2NzIsMjU4Ljk5NjAwMiA2MjQuMjUyNjg2LDI2Ny42NDM5NTEgICBDNjI2LjI5NTUzMiwyNzguNTg5ODc0IDYyOC4xNDMyNTAsMjg5LjU3MzA5MCA2MjkuOTc1MzQyLDMwMC41NTY5MTUgICBDNjMwLjQ5NjY0MywzMDMuNjgyMjIwIDYzMC41NTk5MzcsMzA2Ljg0MTQwMCA2MjkuNDkzMDQyLDMwOS45NDE0NjcgICBDNjI3LjM1MzI3MSwzMTYuMTU5MDU4IDYyMS43MTczNDYsMzIwLjQxMzExNiA2MTUuNjQ1NDQ3LDMyMC4yOTY2MzEgICBDNjA5LjU1MjAwMiwzMjAuMTc5NjU3IDYwMy40NzUyODEsMzE1LjUyODQ3MyA2MDIuMDY3MzgzLDMwOC45NTcyNzUgICBDNTk5Ljk3ODI3MSwyOTkuMjA2NDIxIDU5OC40NTYxNzcsMjg5LjMzMzU1NyA1OTYuNzIxNDk3LDI3OS41MDc1MzggICBDNTk1LjY4MDc4NiwyNzMuNjEyMjc0IDU5NC43NTQyNzIsMjY3LjY5NjI4OSA1OTMuNjYwNzA2LDI2MS44MTExMjcgICBDNTkzLjA1MDkwMywyNTguNTI5MzI3IDU5MS41MjI3MDUsMjU1LjczODYxNyA1ODguMDA1MzcxLDI1NC44NzkyNDIgICBDNTgzLjk5NDAxOSwyNTMuODk5MTg1IDU4MS4zMjgwMDMsMjU2LjcwNjg3OSA1ODEuMTQzMzcyLDI2MS44NTA2MTYgICBDNTgwLjgyMjYzMiwyNzAuNzg0NTE1IDU4MC44MjIzMjcsMjcwLjc4MjU5MyA1NzEuOTgzMTU0LDI3Mi4xOTYxMzYgICBDNTcwLjM0MDYzNywyNzIuNDU4ODMyIDU2OC43MDcwOTIsMjcyLjc4MjYyMyA1NjcuMDU5NzUzLDI3My4wMDg1MTQgICBDNTY1LjMxMTU4NCwyNzMuMjQ4MjYwIDU2NC4wNzE1OTQsMjcyLjU1NDQxMyA1NjMuODMzNDM1LDI3MC43Mjk5ODAgICBDNTYyLjE5OTIxOSwyNTguMjEwMDgzIDU2Mi40NTM3MzUsMjQ2LjA3NjEyNiA1NzEuNDYzOTg5LDIzNS40ODA1MzAgIHoiLz4KICA8cGF0aCBkPSJNNDQ5LjMxMTEyNywyOTguODcxMTU1ICAgQzQ1MS43NDYxNTUsMzEwLjM1NzUxMyA0NTMuMzU5NDM2LDMyMS41MzcwNDggNDU0LjY3MTM1NiwzMzIuNzc0OTk0ICAgQzQ1NS42MTEwODQsMzQwLjgyNDcwNyA0NTEuMDg4MzQ4LDM0OC4wNzk4OTUgNDQzLjU4MDA3OCwzNDkuOTQzMTE1ICAgQzQzNy4zOTc2MTQsMzUxLjQ3NzMyNSA0MzAuMjcyMDAzLDM0Ny4xODEzOTYgNDI3LjY4OTM5MiwzMzkuOTYxNzkyICAgQzQyNS4zODM0MjMsMzMzLjUxNTQ3MiA0MjUuMDE0NTI2LDMyNi42MjQyMzcgNDIzLjc4ODQ4MywzMTkuOTMyODkyICAgQzQyMi4yMDA2MjMsMzExLjI2NjkwNyA0MjAuOTU0NTkwLDMwMi41Mzg2MzUgNDE5LjQyNTE0MCwyOTMuODYxMjM3ICAgQzQxOS4wNTM4MzMsMjkxLjc1NDczMCA0MTguMzMyMzY3LDI4OS42NTg4NzUgNDE3LjQ0NDE4MywyODcuNzA3Mjc1ICAgQzQxNi4yOTk2NTIsMjg1LjE5MjI2MSA0MTQuMTg2MDM1LDI4My45OTMwNzMgNDExLjM0OTY0MCwyODQuMzg1ODM0ICAgQzQwOC42NzU5MzQsMjg0Ljc1NjEzNCA0MDcuMjQxMTE5LDI4Ni41NjY4NjQgNDA2LjY3NDU2MSwyODguOTY4MTQwICAgQzQwNi4zMDAzMjMsMjkwLjU1NDM1MiA0MDYuMjgxMzExLDI5Mi4yNzYwOTMgNDA2LjM5NTQ3NywyOTMuOTE2ODQwICAgQzQwNi44OTc2MTQsMzAxLjEzNDU4MyA0MDYuOTM2MDk2LDMwMS4xMzE4NjYgMzk5Ljk2ODY4OSwzMDIuNDM1OTQ0ICAgQzM4OS42OTAwOTQsMzA0LjM1OTc0MSAzODkuNjkwMDYzLDMwNC4zNTk3NDEgMzg4LjIxOTUxMywyOTMuODk3MDM0ICAgQzM4Ni4xNzQ1MzAsMjc5LjM0NzUwNCAzOTIuNTQzNTE4LDI2Ni4xOTc5NjggNDAzLjg3MTQyOSwyNTkuODA3OTgzICAgQzQxOS44MDMzNzUsMjUwLjgyMDkwOCA0MzcuNDMxODI0LDI2MS43NjMzNjcgNDQzLjExMzA2OCwyNzQuMzU3NDgzICAgQzQ0Ni41OTQ4MTgsMjgyLjA3NTc3NSA0NDcuNDk5Nzg2LDI5MC4zNDY5MjQgNDQ5LjMxMTEyNywyOTguODcxMTU1ICB6Ii8+CiAgPHBhdGggZD0iTTY3My45NDM5NzAsNzI3LjEzMzE3OSAgIEM2NzAuNTkxNDMxLDcyMi4zOTExNzQgNjY2Ljc0NDA4MCw3MTguNzY0NjQ4IDY2MS40OTI3MzcsNzE2Ljk5OTUxMiAgIEM2NTguNzIxNjgwLDcxNi4wNjgxNzYgNjU3Ljg2NDkyOSw3MTQuNTkwODIwIDY1OC43OTY2OTIsNzExLjc1MjE5NyAgIEM2NjQuMTk2Nzc3LDY5NS4zMDEyNzAgNjY4LjM4NDM5OSw2NzguNTI4MDc2IDY3MS45Mjg4OTQsNjYxLjU4ODUwMSAgIEM2NzIuMzM0Nzc4LDY1OS42NDg3NDMgNjczLjM3NTY3MSw2NTguMTg0MjY1IDY3NS4zNzEyNzcsNjU3LjcxNTc1OSAgIEM2ODEuMDM2MTk0LDY1Ni4zODU4NjQgNjg2LjcwMDY4NCw2NTUuMDQzNTc5IDY5Mi4zOTk1MzYsNjUzLjg3MjQzNyAgIEM2OTYuMTcwMDQ0LDY1My4wOTc1MzQgNjk2LjkwNzc3Niw2NTUuNjUzNTAzIDY5Ny4wODg2ODQsNjU4LjYwNzYwNSAgIEM2OTcuODExMTU3LDY3MC40MDI1ODggNjk3LjgxNDc1OCw2ODIuMjMxMTQwIDY5Ni43Nzk1NDEsNjkzLjk2OTYwNCAgIEM2OTUuMzQxNzk3LDcxMC4yNzE1NDUgNjkyLjA4MTcyNiw3MjYuMjI1MDk4IDY4NC4yNjE4NDEsNzQwLjg4MjU2OCAgIEM2ODMuNDM5ODE5LDc0Mi40MjMzNDAgNjgzLjE4NDU3MCw3NDQuNDM1NDI1IDY4MC4zMzU5OTksNzQ1LjQ2NTAyNyAgIEM2NzguNTc0NzY4LDczOS4zMTU2NzQgNjc3Ljc1MjU2Myw3MzIuODIyMzg4IDY3My45NDM5NzAsNzI3LjEzMzE3OSAgeiIvPgogIDxwYXRoIGQ9Ik04MTUuNTAwNjEwLDQxNC42MTU2NjIgICBDODA1LjQ1ODQ5Niw0MjEuMzY3MjQ5IDc5NC40MjQ1NjEsNDIwLjQ3MjAxNSA3ODcuNjc3MzY4LDQxMi41Nzk0MzcgICBDNzg1LjIzMDE2NCw0MDkuNzE2NzM2IDc4NC4zNTYzODQsNDA3LjA3MjQ3OSA3ODYuNTUzNTI4LDQwMy4xOTUyMjEgICBDNzkzLjA0NzI0MSwzOTEuNzM1NjI2IDc5MC4yOTYyNjUsMzgwLjE1Mzg3MCA3NzkuNjg2NTg0LDM3MS43NTQwNTkgICBDNzc1LjI4NDA1OCwzNjguMjY4NDk0IDc3NS4zMjA4MDEsMzY2LjQ5OTQ1MSA3ODAuMTUwMzMwLDM2My4zNjk3MjAgICBDNzg3Ljg4NzQ1MSwzNTguMzU1NzEzIDc5NS44OTI3NjEsMzU3Ljk1MTc1MiA4MDQuMzQ0MjM4LDM2MS43ODUwMzQgICBDODE4LjcxOTQ4MiwzNjguMzA1MTc2IDgyNy43NzE5MTIsMzgzLjg5MjUxNyA4MjUuMTgwODQ3LDM5OC41MTQ4NjIgICBDODI0LjA0NzU0Niw0MDQuOTEwNjE0IDgyMS4zODAxODgsNDEwLjU1Njk0NiA4MTUuNTAwNjEwLDQxNC42MTU2NjIgIHoiLz4KICA8cGF0aCBkPSJNMzI2LjAyMzE2Myw0MzYuNTE0OTg0ICAgQzMzMC43NTkwMDMsNDMyLjIyNDM2NSAzMzYuNDg0MzQ0LDQzMC4yMzQyODMgMzQxLjY0MjIxMiw0MjcuMjU4ODUwICAgQzM0NC40NDMyNjgsNDI1LjY0Mjk3NSAzNDQuOTE0ODI1LDQyNy43OTE2NTYgMzQ1LjM0MTEyNSw0MjkuOTg3ODg1ICAgQzM0OC42MzIxNDEsNDQ2Ljk0MTcxMSAzNTEuOTc4MDU4LDQ2My44ODQ5MTggMzU1LjMyMTQ0Miw0ODAuODI4NTUyICAgQzM1Ni41MTE0NDQsNDg2Ljg1OTA3MCAzNTcuNjY0Mzk4LDQ5Mi44OTg2NTEgMzU4Ljk3OTQ2Miw0OTguOTAyMjgzICAgQzM1OS45MTE2ODIsNTAzLjE1ODE3MyAzNTguMzE4MDU0LDUwNC42ODUxODEgMzU0LjA4MTYwNCw1MDQuMzQyMzQ2ICAgQzM1MC40NDE3MTEsNTA0LjA0Nzc5MSAzNDYuNzU0NzYxLDUwMy45NTg5MjMgMzQzLjExMjMzNSw1MDQuMTc2MjcwICAgQzMzOS4zMzc0MDIsNTA0LjQwMTUyMCAzMzcuODQ2NzEwLDUwMi43MTAxNDQgMzM3LjExMDY1Nyw0OTkuMTkwMDk0ICAgQzMzMi43OTA0OTcsNDc4LjUzMDA2MCAzMjkuMTI5NzkxLDQ1Ny43NTU0OTMgMzI2LjAyMzE2Myw0MzYuNTE0OTg0ICB6Ii8+CiAgPHBhdGggZD0iTTMzNy4xOTA2MTMsMzc4LjgwMTg4MCAgIEMzMzcuOTQ5MTg4LDM4OC4yMjAxODQgMzM5LjQxMDY0NSwzOTcuMDY1MjE2IDM0MS4xMDI2OTIsNDA1Ljg2Mjk0NiAgIEMzNDEuOTI4OTg2LDQxMC4xNTkzMzIgMzQxLjMyNzQyMyw0MTMuMDc4ODg4IDMzNi42ODg2MjksNDE0LjgyODA2NCAgIEMzMzIuMjI5NDYyLDQxNi41MDk1NTIgMzI4LjA5MDc5MCw0MTkuMDQxMTM4IDMyMy44MTEzMTAsNDIxLjE5OTE4OCAgIEMzMjAuODE4NDgxLDQxOC42MzM1NDUgMzIxLjc4NzUzNyw0MTUuMzM2ODIzIDMyMS4zNzY4NjIsNDEyLjU3MTE5OCAgIEMzMTkuODQ1OTE3LDQwMi4yNjEzMjIgMzE3LjIzNzIxMywzOTIuMDQ2Mzg3IDMxOC41OTkyMTMsMzgxLjQ0NTc0MCAgIEMzMjAuODg3MjA3LDM2My42Mzc2NjUgMzI5LjU2MDEyMCwzNDkuMjg4NTc0IDM0Mi4yNTY1MDAsMzM3LjA2MzM4NSAgIEMzNDMuMDc2MTQxLDMzNi4yNzQxMzkgMzQ0LjA1OTc4NCwzMzUuMjgwNjcwIDM0NS4yODc1MzcsMzM2LjE5Nzk5OCAgIEMzNDYuNzU1NzM3LDMzNy4yOTQ5MjIgMzQ1LjgzOTc1MiwzMzguNTY5NjExIDM0NS4yNDU3MjgsMzM5Ljc0NjI0NiAgIEMzMzkuMTE1OTM2LDM1MS44ODc1NDMgMzM2LjQ1MDk4OSwzNjQuNzUzNDQ4IDMzNy4xOTA2MTMsMzc4LjgwMTg4MCAgeiIvPgogIDxwYXRoIGQ9Ik01MzMuMjYyMDg1LDcxNy4zMzQ3NzggICBDNTM1LjU4ODE5Niw3MDguMDcwMTI5IDUzNy44NjE3NTUsNjk5LjIxNDg0NCA1MzkuOTc0NDg3LDY5MC4zMjE0NzIgICBDNTQwLjY3ODY1MCw2ODcuMzU3NjA1IDU0Mi4yMzc0MjcsNjg1LjY5ODMwMyA1NDUuMjgyMTA0LDY4NS4yNzczNDQgICBDNTQ5LjE2OTg2MSw2ODQuNzM5ODY4IDU1Mi44OTQyODcsNjgzLjA2NjIyMyA1NTcuMTcwMzQ5LDY4My40MTQ5MTcgICBDNTU4LjE2MjkwMyw2ODcuOTMyNDM0IDU1Ni40MjU0MTUsNjkyLjA2NDYzNiA1NTUuOTU4MDA4LDY5Ni4yMzk4MDcgICBDNTU0LjczOTk5MCw3MDcuMTE5NzUxIDU1NC45NTU4NzIsNzE3LjgwOTQ0OCA1NTguNDgyNzI3LDcyOC4yNjU2MjUgICBDNTU5LjEzNTM3Niw3MzAuMjAwNjg0IDU1OS4zODMzNjIsNzMxLjY4NDYzMSA1NTcuMTQxMzU3LDczMi44MDY3NjMgICBDNTQ3LjE4MDIzNyw3MzcuNzkyNDgwIDUzNy4yNzAzMjUsNzQyLjg4MDMxMCA1MjcuMzIzMzY0LDc0Ny44OTQ0NzAgICBDNTI2Ljc3Nzk1NCw3NDguMTY5MzEyIDUyNi4wNjUxMjUsNzQ4LjExMTg3NyA1MjQuMzkzNTU1LDc0OC4zNjc5MjAgICBDNTI2LjI0Mjc5OCw3MzcuNDgxMTQwIDUzMC44ODcyNjgsNzI3LjkxMDA5NSA1MzMuMjYyMDg1LDcxNy4zMzQ3NzggIHoiLz4KICA8cGF0aCBkPSJNMzkwLjg0OTI0Myw2NTMuMDA4NjY3ICAgQzM5OC41NDMxNTIsNjY2LjIwMTM1NSA0MDcuNzczODk1LDY3Ny42MTQ1NjMgNDE5LjQzMjM3Myw2ODYuODE5MzM2ICAgQzQxOS4xOTM5MDksNjg4LjQ2NTI3MSA0MTguMDA4MzkyLDY4OC4zNzUxMjIgNDE3LjEwODQ5MCw2ODguMjMwOTU3ICAgQzM5OS45MjY3MjcsNjg1LjQ3NzI5NSAzODUuNTQzMDYwLDY3OC4wMDUxMjcgMzc1Ljk5MTY2OSw2NjIuOTYyOTUyICAgQzM3Mi4zODA3NjgsNjU3LjI3NjI0NSAzNzMuMDY4MjA3LDY1NS45MTU1MjcgMzc5LjM4OTg5Myw2NTMuNjU5NjA3ICAgQzM4My4wMjA1OTksNjUyLjM2NDA3NSAzODYuNjIyNDk4LDY0OS43NTczODUgMzkwLjg0OTI0Myw2NTMuMDA4NjY3ICB6Ii8+Cjwvc3ZnPg==","mediaGlyph":"data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjI5MS4xNjY3ODkgMjg3LjUxMTUzOSAxNDcyLjcwMjU4OCAxNDc1Ljk2MzQxMSI+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMC4wMDAwMDAsMjA0OC4wMDAwMDApIHNjYWxlKDAuMTAwMDAwLC0wLjEwMDAwMCkiPjxwYXRoIGZpbGw9IiNmZmYiIGQ9Ik0xMDgzOCAxNzYwMCBjLTg0IC0xMCAtMTUzIC0zMiAtMjQzIC03OCAtOTkgLTUxIC0yMjcgLTE4MSAtMjc4IC0yODIgLTIwIC00MSAtNDcgLTExMyAtNTggLTE2MCAtMTggLTY5IC0yMSAtMTA0IC0xNiAtMTg1IDYgLTEyNiA0MiAtMjQwIDEwNyAtMzM5IDg0IC0xMjggMjIyIC0yMzQgMzYxIC0yNzcgMzIgLTEwIDE0OCAtMzAgMjU4IC00NCAxMTAgLTE0IDI2MiAtMzYgMzM4IC01MCAxMjIwIC0yMjEgMjMzMCAtODAyIDMyMDMgLTE2NzUgNDY5IC00NjkgODU1IC0xMDA4IDExNDQgLTE1OTUgMzMxIC02NzMgNTI1IC0xMzYwIDYwMyAtMjEzNSAyNCAtMjQ3IDI0IC04MzUgMCAtMTA4MCAtNDkgLTQ4MiAtMTM0IC04OTYgLTI3NCAtMTMyNSAtMjEyIC02NTAgLTUyMSAtMTI0MyAtOTMyIC0xNzg1IC0zODcgLTUxMiAtODU0IC05NTcgLTE0MDMgLTEzMzcgLTIyOSAtMTU4IC0zMzIgLTMyNSAtMzQ1IC01NTkgLTcgLTEyOCAxMyAtMjI3IDcwIC0zNDQgMzMgLTY4IDU4IC0xMDIgMTI3IC0xNzEgNjYgLTY2IDEwMyAtOTUgMTY3IC0xMjYgMTE1IC01OCAyMDcgLTc3IDM0NiAtNzAgODUgMyAxMzAgMTEgMTg1IDMwIDg2IDMwIDE1NSA3MSAzNTYgMjE1IDMyNSAyMzIgNjExIDQ3NiA5MTEgNzc3IDIyNSAyMjQgMzkzIDQxMSA1NzEgNjM1IDM0OSA0MzkgNjYzIDk0NSA5MDEgMTQ1NCAzODQgODIxIDYwOCAxNjcyIDY4MyAyNTk2IDYgNjkgMTMgMjUxIDE3IDQwNSA2IDI5MCAtNSA1OTcgLTMyIDg2NSAtMTE0IDExMjAgLTQ2NSAyMTcyIC0xMDQyIDMxMjMgLTM4OCA2NDAgLTg5NyAxMjQ0IC0xNDU4IDE3MzIgLTM0MyAyOTggLTY3MCA1MzYgLTEwNTYgNzY4IC02OTQgNDE3IC0xNDMwIDcxMCAtMjIzNCA4ODcgLTI1MSA1NiAtNjM4IDExNSAtODY1IDEzNCAtMjUgMiAtNzUgMCAtMTEyIC00eiBNNTkwMiAxNjE3MCBjLTE0MSAtMzcgLTI0MCAtMTA1IC01MzcgLTM2NCAtMjA5IC0xODMgLTMzNSAtMzExIC0zNzggLTM4NiAtMjEgLTM2IC00OSAtMTAxIC02NCAtMTQ1IC0yMSAtNjUgLTI2IC0xMDAgLTI3IC0xOTAgMCAtNjIgNSAtMTM0IDEzIC0xNjUgNyAtMzAgMzMgLTk3IDU4IC0xNDkgMzggLTc5IDU5IC0xMDggMTI3IC0xNzYgNjQgLTY0IDk5IC05MSAxNjYgLTEyMyAxMTMgLTU2IDE5NCAtNzUgMzEwIC03NSAxMTIgMCAyMDcgMjIgMzAwIDY4IDg3IDQ0IDEyNSA3MyAyNTUgMTk1IDYxIDU3IDE4NSAxNjYgMjc3IDI0MiAxMDEgODQgMTg1IDE2MyAyMTMgMjAwIDU3IDc2IDEwMCAxNjcgMTIwIDI1MyAyMCA4NSAyMCAyNDIgMCAzMjAgLTIxIDgyIC04NSAyMDkgLTEzNyAyNzIgLTkxIDEwOCAtMjE3IDE4OCAtMzU0IDIyMyAtMTAxIDI2IC0yNDIgMjYgLTM0MiAweiBNMTAxMDUgMTM4NjYgYy0xNDQgLTM0IC0yNTQgLTk0IC0zNTIgLTE5NCAtNzIgLTc0IC0xMjUgLTE2MiAtMTYxIC0yNjcgLTI3IC04MCAtMjcgLTgwIC0zMCAtMjI4MCAtMiAtMjE5OSAtMiAtMjE5OSAtOTM1IC0xMjY5IC0xMDA5IDEwMDcgLTk3NSA5NzUgLTExNDIgMTAzMSAtNjkgMjMgLTk2IDI3IC0yMDUgMjcgLTEwOSAwIC0xMzYgLTQgLTIwNSAtMjcgLTEwOCAtMzYgLTE5NSAtODkgLTI3NCAtMTY4IC03OSAtNzkgLTEzMiAtMTY2IC0xNjggLTI3NCAtMjMgLTY5IC0yNyAtOTYgLTI3IC0yMDUgMCAtMTA5IDQgLTEzNiAyNyAtMjA1IDU3IC0xNzIgLTMwIC03OSAxNjQ3IC0xNzU1IDE2NzYgLTE2NzcgMTU4MyAtMTU5MCAxNzU1IC0xNjQ3IDY5IC0yMyA5NiAtMjcgMjA1IC0yNyAxMDkgMCAxMzYgNCAyMDUgMjcgMTcyIDU3IDc5IC0zMCAxNzU1IDE2NDcgMTY3NyAxNjc2IDE1OTAgMTU4MyAxNjQ3IDE3NTUgMjMgNjkgMjcgOTYgMjcgMjA1IDAgMTA5IC00IDEzNiAtMjcgMjA1IC0zNiAxMDggLTg5IDE5NCAtMTY4IDI3NCAtNzggNzggLTE2NSAxMzIgLTI3NCAxNjggLTY5IDIzIC05NiAyNyAtMjA1IDI3IC0xMDkgMCAtMTM2IC00IC0yMDUgLTI3IC0xNjcgLTU1IC0xMzMgLTI1IC0xMTQyIC0xMDMxIC05MzMgLTkzMCAtOTMzIC05MzAgLTkzNSAxMjY5IC0zIDIyMDAgLTMgMjIwMCAtMzAgMjI4MCAtMzcgMTA5IC05MCAxOTUgLTE2OSAyNzQgLTEyMyAxMjQgLTI3NSAxOTAgLTQ1NCAxOTcgLTY0IDIgLTEyMiAtMSAtMTYwIC0xMHogTTM5NDkgMTM2NTUgYy0xMjcgLTMxIC0yNTcgLTExMCAtMzQ5IC0yMTIgLTYxIC02OSAtMTM4IC0yMjAgLTIzMyAtNDYzIC0yMDIgLTUxNCAtMzM5IC0xMDAzIC00MjggLTE1MzUgLTI1IC0xNTEgLTMwIC0yMDcgLTI2IC0yNzUgNyAtMTAzIDQzIC0yMjIgOTQgLTMwOSA0OSAtODIgMTUyIC0xODUgMjM0IC0yMzQgMTQyIC04MyAzMjIgLTExNSA0NzkgLTgzIDIwMyA0MiAzNzUgMTY4IDQ2OSAzNDYgNDIgNzkgNTkgMTM3IDgyIDI3NCA2MSAzNzEgMTM3IDY3OSAyNDcgMTAwNiA2MCAxNzcgOTYgMjcwIDE3OSA0NjcgMzUgODQgNzEgMTg1IDgwIDIyNSAxMCA1MCAxMyAxMDMgMTAgMTY4IC05IDE3OCAtODYgMzQyIC0yMTcgNDYzIC04MSA3NSAtMTc1IDEyNyAtMjgwIDE1NiAtNTUgMTYgLTEwMiAyMSAtMTgwIDIwIC01OCAwIC0xMzAgLTcgLTE2MSAtMTR6IE0zNTM5IDkzNTUgYy0xMjkgLTMyIC0yNTggLTExMSAtMzQ5IC0yMTIgLTY0IC03MyAtMTM2IC0yMTUgLTE1NiAtMzEzIC0yMiAtMTA0IC0xNCAtMjI1IDI1IC0zODAgMjM5IC05MzkgNjM1IC0xNzg3IDExOTUgLTI1NjAgMTkxIC0yNjMgNDMxIC01NTAgNTIzIC02MjUgNjkgLTU3IDE3OCAtMTA5IDI2OCAtMTMxIDUxIC0xMiAxMDEgLTE1IDE4NSAtMTIgOTAgNCAxMzEgMTAgMTg4IDMxIDEwNiAzNyAxNzYgODIgMjYzIDE2NyA2MSA2MSA4NSA5NCAxMjMgMTcwIDQ5IDEwMCA3NiAyMDggNzYgMzA1IDAgOTYgLTI3IDIwNSAtNzUgMzAwIC0zMyA2NyAtNzcgMTI5IC0xNzEgMjQwIC0zOTcgNDcxIC02OTQgOTUwIC05MzYgMTUwNSAtMTI3IDI5MyAtMjE5IDU2MiAtMzA4IDkwMyAtNjMgMjM5IC0xMDMgMzIwIC0yMTAgNDI3IC04NSA4NSAtMTU1IDEzMCAtMjYyIDE2NyAtNjEgMjIgLTk0IDI3IC0xOTggMjkgLTg0IDIgLTE0MyAtMSAtMTgxIC0xMXogTTcxMDUgNDkwNSBjLTEzOCAtMjYgLTI0NCAtNzkgLTM0NSAtMTcyIC0xNzIgLTE2MCAtMjUxIC0zOTggLTIwNiAtNjIzIDMzIC0xNjIgMTI4IC0zMTggMjUwIC00MTAgODYgLTY1IDQyMCAtMjI0IDcyMSAtMzQzIDI4NyAtMTEzIDY4MCAtMjM4IDk2NSAtMzA2IDQxMiAtOTggNzY4IC0xNTQgMTIyNSAtMTkzIDIyNSAtMTkgODc0IC0xNiAxMTEyIDUgMzA2IDI3IDU5MiA2NyA4NjMgMTIxIDk2IDE5IDE0NiAzNiAyMTUgNzAgMTA5IDU0IDE5NiAxMzAgMjYxIDIyOCAyNiAzOSA2MSAxMDkgNzcgMTU3IDI0IDY4IDMxIDEwOCAzNSAxOTIgNCA4NSAwIDEyMSAtMTYgMTg4IC0yOCAxMDcgLTcxIDE5NCAtMTM4IDI3NCAtMTMzIDE2MiAtMzU2IDI1OSAtNTU5IDI0NCAtNDQgLTQgLTEzNiAtMTggLTIwNiAtMzEgLTE1MiAtMzEgLTQ0MiAtNzAgLTYzOSAtODggLTgzIC03IC0yODggLTEzIC00ODAgLTEzIC0zNTMgMCAtNTAxIDEwIC04MTAgNTEgLTYwNSA4MSAtMTIwOSAyNjAgLTE3ODAgNTI5IC05NyA0NiAtMTk4IDkwIC0yMjQgOTkgLTU0IDE4IC0xNjMgMzYgLTIxNiAzNSAtMTkgMCAtNjYgLTcgLTEwNSAtMTR6Ii8+PC9nPjwvc3ZnPg=="};
const SAQUETTO_COMPATIBILITY = {"clube-uol":{"sha256":["e89d676713543aa015c2661bd8c1a9a46e40012271ca93111404626bb6010a51","119f05461336dab2d7f68924b67c36cd3073c0c2c772c6ceb962641fcb12f766","cabfcef72ca0f2838f13017bb4d7d4eaf0933046d1c9676d87acbfc22f95ad21"]},"bilhete-unico":{"sha256":["b2e6b8483fd78fa1644aac280ba9ade30c2ea2e6a6663ea915914365d9baabbc"]},"super-calendario":{"sha256":["c0573f79698b4cfd00377c5a4f83b8c26f736f93382a36eee027d7c4863d02d6"]},"media-downloader":{"sha256":["d5f5f06b7da62f2d863c486b12a7270a336c435abb1cf770b3f191604dcfe118"]},"pilltracker":{"sha256":["f3c9673004fd5fb5ecf76ffcd99ae335daa60fd435937e8918d179fe998114cc"]},"lastfm-small":{"sha256":["855a730e783566a0ce876f33855e4c4c324f7d6c12a16c056cdea94325bb0bd9"]}};
const SAQUETTO_SELF = { id: 'saquettoos', name: 'saquettoOS', scriptName: 'saquettoOS',
  manifestBase: 'https://raw.githubusercontent.com/leosaquetto/uol-bot/main/saquettoos/releases/',
  artifactPath: 'saquettoos/dist/saquettoOS.js', envelope: 'saquettoos' };

// Code-only OTA for saquettoOS and Clube UOL Premium. No module side effects.
const SaquettoUpdater = (() => {
  const BASE = 'https://raw.githubusercontent.com/leosaquetto/uol-bot/';
  const SOURCES = Object.freeze({
    saquettoos: Object.freeze({ scriptName: 'saquettoOS', manifestBase: BASE + 'main/saquettoos/releases/', artifactPath: 'saquettoos/dist/saquettoOS.js', envelope: 'saquettoos', maxBytes: 8 * 1024 * 1024 }),
    'clube-uol': Object.freeze({ scriptName: 'Clube UOL Premium', manifestBase: BASE + 'main/stories-hunter/releases/', artifactPath: 'stories-hunter/dist/Hunter.js', envelope: 'uol', maxBytes: 2 * 1024 * 1024 })
  });
  const versionParts = value => typeof value === 'string' && /^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/.test(value) && value.split('.').map(Number).every(Number.isSafeInteger) ? value.split('.').map(Number) : null;
  const newer = (a, b) => { const x = versionParts(a), y = versionParts(b); for (let i = 0; i < 3; i++) { if (x[i] !== y[i]) return x[i] > y[i]; } return false; };
  const utf8 = text => Array.from(unescape(encodeURIComponent(text)), c => c.charCodeAt(0));
  // SHA-256 adapted from stories-hunter/native/updater.js (same repository).
  function sha256(bytes) {
    const constants = [
      0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
      0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
      0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
      0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
      0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
      0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
      0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
      0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
    ];
    const hash = [0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19];
    const length = bytes.length, padded = Math.ceil((length + 9) / 64) * 64;
    const words = new Array(64);
    const rotate = (value, count) => value >>> count | value << (32 - count);
    const high = Math.floor(length / 0x20000000), low = length * 8 >>> 0;
    const byteAt = index => index < length ? bytes[index] : index === length ? 128
      : index >= padded - 8 ? ((index < padded - 4 ? high : low) >>> ((padded - 1 - index) % 4 * 8)) & 255 : 0;
    for (let offset = 0; offset < padded; offset += 64) {
      for (let i = 0; i < 16; i++) {
        const index = offset + i * 4;
        words[i] = byteAt(index) << 24 | byteAt(index + 1) << 16 | byteAt(index + 2) << 8 | byteAt(index + 3);
      }
      for (let i = 16; i < 64; i++) {
        const x = words[i - 15], y = words[i - 2];
        words[i] = (words[i - 16] + (rotate(x,7) ^ rotate(x,18) ^ x >>> 3) + words[i - 7] + (rotate(y,17) ^ rotate(y,19) ^ y >>> 10)) | 0;
      }
      let [a,b,c,d,e,f,g,h] = hash;
      for (let i = 0; i < 64; i++) {
        const t1 = (h + (rotate(e,6) ^ rotate(e,11) ^ rotate(e,25)) + (e & f ^ ~e & g) + constants[i] + words[i]) | 0;
        const t2 = ((rotate(a,2) ^ rotate(a,13) ^ rotate(a,22)) + (a & b ^ a & c ^ b & c)) | 0;
        h=g; g=f; f=e; e=(d+t1)|0; d=c; c=b; b=a; a=(t1+t2)|0;
      }
      [a,b,c,d,e,f,g,h].forEach((value, i) => { hash[i] = (hash[i] + value) | 0; });
    }
    return hash.map(value => (value >>> 0).toString(16).padStart(8, '0')).join('');
  }
  const hashCode = code => sha256(utf8(code));
  function source(app) {
    if (!app || !Object.prototype.hasOwnProperty.call(SOURCES, app.id)) return { status: 'not_configured' };
    const known = SOURCES[app.id];
    if (['scriptName', 'manifestBase', 'artifactPath', 'envelope'].some(key => app[key] !== known[key])) return { status: 'invalid_source' };
    return { ...known, id: app.id, status: 'configured' };
  }
  function codeVersion(code, spec, bounded = false) {
    if (typeof code !== 'string') return null;
    try {
      if (bounded && utf8(code).length > spec.maxBytes) return null;
      let matches;
      if (spec.envelope === 'saquettoos') {
        if (!/^\/\/ saquettoOS Scriptable launcher\r?$/m.test(code) || !/^async function saquettoMain\(\)\s*\{/m.test(code) ||
            !/\nawait saquettoMain\(\);\r?\nScript.complete\(\);\s*$/.test(code)) return null;
        matches = [...code.matchAll(/^const SAQUETTO_VERSION = ['"]([^'"]+)['"];\r?$/gm)];
      } else {
        // Mirrors the UOL envelope. Private configuration stays only in the local backup.
        if (!code.startsWith('// Variables used by Scriptable.\n// These must be at the very top of the file.') ||
            !/\/\/ Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\r?\n/.test(code) ||
            !/\nawait main\(\);\r?\nScript.complete\(\);\s*$/.test(code) ||
            !/^async function main\(\) \{/m.test(code) || !/^function buildDashboardHTML\(/m.test(code) ||
            !/^function hunterCollectionPausedForUpdate\(action\) \{/m.test(code) || !/^const HunterCollector = \(\(\) => \{/m.test(code)) return null;
        matches = [...code.matchAll(/^const SCRIPT_VERSION = "([^"]+)";\r?$/gm)];
      }
      if (matches.length !== 1 || !versionParts(matches[0][1])) return null;
      // Parse, never execute the candidate, including during recovery.
      const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
      new AsyncFunction(code);
      return matches[0][1];
    } catch (_) { return null; }
  }
  function manifest(value, spec, channel) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return { status: 'invalid_manifest' };
    if (value.schema !== 1 || value.updaterProtocol !== 1) return { status: 'incompatible' };
    const suffix = typeof value.artifact === 'string' && value.artifact.startsWith(BASE) ? value.artifact.slice(BASE.length) : '';
    const commit = suffix.slice(0, 40);
    if (value.channel !== channel || !versionParts(value.version) || !/^[a-f0-9]{40}$/.test(commit) || suffix !== commit + '/' + spec.artifactPath ||
        !Number.isSafeInteger(value.size) || value.size < 1 || value.size > spec.maxBytes || !/^[a-f0-9]{64}$/.test(value.sha256 || '') ||
        typeof value.publishedAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value.publishedAt) ||
        !Number.isFinite(Date.parse(value.publishedAt)) || new Date(value.publishedAt).toISOString() !== (value.publishedAt.length === 20 ? value.publishedAt.replace('Z', '.000Z') : value.publishedAt)) return { status: 'invalid_manifest' };
    return { status: 'valid', manifest: { schema: 1, updaterProtocol: 1, version: value.version, channel, artifact: value.artifact, size: value.size, sha256: value.sha256, publishedAt: value.publishedAt } };
  }
  function create({ fm, local, request, uuid, scriptName }) {
    const busy = new Set();
    function paths(spec) {
      // Never derive another app's destination from the running Script.name().
      if (spec.id === 'saquettoos' && scriptName && scriptName.normalize('NFC') !== spec.scriptName) throw new Error('script_name_mismatch');
      const names = fm.listContents(fm.documentsDirectory()).filter(name => name.normalize('NFC') === spec.scriptName + '.js');
      if (names.length > 1) throw new Error('ambiguous_installation');
      const scriptPath = fm.joinPath(fm.documentsDirectory(), names[0] || spec.scriptName + '.js');
      if (fm.fileExists(scriptPath) && fm.isDirectory && fm.isDirectory(scriptPath)) throw new Error('invalid_target');
      const key = hashCode(spec.id + ':' + scriptPath).slice(0, 16);
      return { scriptPath, key, journalPath: local.joinPath(local.documentsDirectory(), 'saquettoos_ota_' + key + '.json') };
    }
    async function downloadLocal(path) {
      if (fm.fileExists(path) && fm.isFileDownloaded && !fm.isFileDownloaded(path)) await fm.downloadFileFromiCloud(path);
    }
    function read(spec, p) {
      // Re-resolve on every read: detect newly introduced NFC duplicates too.
      if (paths(spec).scriptPath !== p.scriptPath) throw new Error('concurrent_change');
      if (!fm.fileExists(p.scriptPath)) return { exists: false, code: '', hash: '', version: null };
      const code = fm.readString(p.scriptPath);
      if (typeof code !== 'string') throw new Error('installed_script_unreadable');
      return { exists: true, code, hash: hashCode(code), version: codeVersion(code, spec) };
    }
    function failure(error, fallback = 'unreadable') {
      return { status: ['ambiguous_installation', 'script_name_mismatch', 'invalid_target', 'concurrent_change'].includes(error.message) ? error.message : fallback };
    }
    async function inspect(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      try {
        const p = paths(spec); await downloadLocal(p.scriptPath); const current = read(spec, p);
        return { status: !current.exists ? 'not_installed' : current.version ? 'installed' : 'unrecognized', version: current.version, scriptName: spec.scriptName };
      } catch (error) { return failure(error); }
    }
    async function fetchBytes(url, maxBytes, isManifest) {
      const invalid = isManifest ? 'invalid_manifest' : 'invalid_artifact';
      let req, data;
      try {
        req = request(url); req.timeoutInterval = 20; req.onRedirect = () => null;
        req.headers = { Accept: isManifest ? 'application/json, text/plain' : 'text/plain, application/javascript', 'Cache-Control': 'no-cache' };
        data = await req.load();
      } catch (_) { return { status: 'network_error' }; }
      const response = req.response || {}, header = name => Object.entries(response.headers || {}).find(([key]) => key.toLowerCase() === name)?.[1] || '';
      const type = header('content-type') || response.mimeType || '';
      const accepted = isManifest ? /^(?:application\/json|text\/plain)(?:;|$)/i : /^(?:text\/(?:plain|javascript)|application\/(?:javascript|x-javascript))(?:;|$)/i;
      if (response.statusCode >= 500 || response.statusCode === 429) return { status: 'network_error' };
      if (response.statusCode !== 200 || response.url !== url || !accepted.test(type)) return { status: invalid, reason: 'response_invalid' };
      if (Number(header('content-length')) > maxBytes) return { status: invalid, reason: 'size_limit' };
      try {
        // Scriptable exposes no streamed cancellation: the byte cap is post-download.
        const bytes = data.getBytes(), code = data.toRawString();
        if (!Array.isArray(bytes) || bytes.length > maxBytes || bytes.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) return { status: invalid, reason: 'size_limit' };
        if (typeof code !== 'string') return { status: invalid, reason: 'invalid_utf8' };
        const encoded = utf8(code);
        if (encoded.length !== bytes.length || encoded.some((byte, index) => byte !== bytes[index])) return { status: invalid, reason: 'invalid_utf8' };
        return { status: 'downloaded', code, bytes };
      } catch (_) { return { status: invalid, reason: 'invalid_utf8' }; }
    }
    async function check(app, channel = 'stable') {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (!['stable', 'beta'].includes(channel)) return { status: 'invalid_channel' };
      const current = await inspect(app); if (current.status !== 'installed') return current;
      const response = await fetchBytes(spec.manifestBase + channel + '.json', 8192, true);
      if (response.status !== 'downloaded') return { ...response, version: current.version, channel };
      let value; try { value = JSON.parse(response.code); } catch (_) { return { status: 'invalid_manifest', version: current.version, channel }; }
      const checked = manifest(value, spec, channel);
      if (checked.status !== 'valid') return { ...checked, version: current.version, channel };
      return { status: newer(checked.manifest.version, current.version) ? 'available' : 'up_to_date', version: current.version, latestVersion: checked.manifest.version, manifest: checked.manifest, channel };
    }
    const backupPath = (p, operationId) => local.joinPath(local.documentsDirectory(), 'saquettoos_ota_backup-' + p.key + '-' + operationId + '.js');
    function journalWrite(p, journal) { const encoded = JSON.stringify(journal); local.writeString(p.journalPath, encoded); if (local.readString(p.journalPath) !== encoded) throw new Error('journal_unverified'); }
    function own(p, journal) { try { return JSON.parse(local.readString(p.journalPath)).operationId === journal.operationId; } catch (_) { return false; } }
    function finish(p, journal, phase) { if (!own(p, journal)) return false; try { journalWrite(p, { ...journal, phase }); return true; } catch (_) { return false; } }
    function validJournal(j, p, spec) {
      return j && j.schema === 1 && j.appId === spec.id && typeof j.operationId === 'string' && /^[a-f0-9-]{8,80}$/.test(j.operationId) &&
        j.scriptPath === p.scriptPath && j.backupPath === backupPath(p, j.operationId) && /^[a-f0-9]{64}$/.test(j.oldHash) && /^[a-f0-9]{64}$/.test(j.newHash) &&
        versionParts(j.oldVersion) && versionParts(j.newVersion) && ['prepared', 'replacing', 'installed', 'rolled_back', 'superseded'].includes(j.phase);
    }
    function restore(spec, p, journal) {
      if (!own(p, journal)) return { status: 'concurrent_change' };
      try {
        const current = read(spec, p);
        if (current.hash === journal.newHash && current.version === journal.newVersion) {
          if (!finish(p, journal, 'installed')) return { status: 'recovery_required' };
          return { status: 'installed', version: current.version };
        }
        if (current.hash === journal.oldHash && current.version === journal.oldVersion) {
          if (!finish(p, journal, 'rolled_back')) return { status: 'recovery_required' };
          return { status: 'rolled_back', version: current.version };
        }
        // Never overwrite an unrelated file, including one with an unknown envelope.
        if (current.exists) return { status: current.version ? 'concurrent_change' : 'recovery_required' };
        const backup = local.readString(journal.backupPath);
        if (typeof backup !== 'string' || hashCode(backup) !== journal.oldHash || codeVersion(backup, spec) !== journal.oldVersion) return { status: 'recovery_required' };
        const staging = fm.joinPath(fm.documentsDirectory(), '.' + spec.scriptName + '.restore-' + journal.operationId + '.tmp');
        fm.writeString(staging, backup);
        if (fm.readString(staging) !== backup || !own(p, journal)) return { status: 'recovery_required' };
        if (read(spec, p).exists) return { status: 'concurrent_change' };
        fm.move(staging, p.scriptPath);
        const restored = read(spec, p);
        if (restored.hash !== journal.oldHash || restored.version !== journal.oldVersion || !finish(p, journal, 'rolled_back')) return { status: 'recovery_required' };
        return { status: 'rolled_back', version: restored.version };
      } catch (error) { return failure(error, 'recovery_required'); }
    }
    async function recoverInternal(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      try {
        const p = paths(spec); if (!local.fileExists(p.journalPath)) return { status: 'no_recovery' };
        const journal = JSON.parse(local.readString(p.journalPath));
        if (!validJournal(journal, p, spec)) return { status: 'recovery_required' };
        if (!['prepared', 'replacing'].includes(journal.phase)) return { status: 'no_recovery' };
        await downloadLocal(p.scriptPath);
        return restore(spec, p, journal);
      } catch (error) { return failure(error, 'recovery_required'); }
    }
    async function recover(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (busy.has(spec.id)) return { status: 'busy' }; busy.add(spec.id);
      try { return await recoverInternal(app); } finally { busy.delete(spec.id); }
    }
    async function install(app, channel = 'stable') {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (!['stable', 'beta'].includes(channel)) return { status: 'invalid_channel' };
      if (busy.has(spec.id)) return { status: 'busy' }; busy.add(spec.id);
      try {
        const recovery = await recoverInternal(app);
        if (!['no_recovery', 'rolled_back', 'installed'].includes(recovery.status)) return recovery;
        const checked = await check(app, channel); if (checked.status !== 'available') return checked;
        const p = paths(spec), previous = read(spec, p);
        if (!previous.version || previous.version !== checked.version) return { status: 'concurrent_change' };
        const downloaded = await fetchBytes(checked.manifest.artifact, spec.maxBytes, false);
        if (downloaded.status !== 'downloaded') return downloaded;
        if (downloaded.bytes.length !== checked.manifest.size) return { status: 'invalid_artifact', reason: 'size_mismatch' };
        if (sha256(downloaded.bytes) !== checked.manifest.sha256) return { status: 'invalid_artifact', reason: 'hash_mismatch' };
        if (codeVersion(downloaded.code, spec, true) !== checked.manifest.version) return { status: 'invalid_artifact', reason: 'version_or_envelope' };
        const operationId = String(uuid()).toLowerCase();
        if (!/^[a-f0-9-]{8,80}$/.test(operationId)) return { status: 'installation_failed', reason: 'invalid_operation_id' };
        const backup = backupPath(p, operationId), staging = fm.joinPath(fm.documentsDirectory(), '.' + spec.scriptName + '.ota-' + operationId + '.tmp');
        if (local.fileExists(backup) || fm.fileExists(staging)) return { status: 'concurrent_change' };
        const journal = { schema: 1, appId: spec.id, operationId, phase: 'prepared', scriptPath: p.scriptPath, backupPath: backup, oldVersion: previous.version, oldHash: previous.hash, newVersion: checked.manifest.version, newHash: checked.manifest.sha256 };
        let replacing = false;
        try {
          local.writeString(backup, previous.code);
          if (local.readString(backup) !== previous.code) return { status: 'installation_failed', reason: 'backup_unverified' };
          fm.writeString(staging, downloaded.code);
          if (fm.readString(staging) !== downloaded.code) return { status: 'installation_failed', reason: 'staging_unverified' };
          // A different operation may have become active while the download awaited.
          if (local.fileExists(p.journalPath)) {
            const prior = JSON.parse(local.readString(p.journalPath));
            if (!validJournal(prior, p, spec) || ['prepared', 'replacing'].includes(prior.phase)) return { status: 'concurrent_change' };
          }
          if (read(spec, p).hash !== previous.hash) return { status: 'concurrent_change' };
          journalWrite(p, journal);
          if (read(spec, p).hash !== previous.hash || !own(p, journal)) return { status: 'concurrent_change' };
          journal.phase = 'replacing'; journalWrite(p, journal);
          if (read(spec, p).hash !== previous.hash || !own(p, journal)) return { status: 'concurrent_change' };
          replacing = true;
          // Same-directory move replaces the target. Scriptable documents no CAS or
          // atomicity guarantee; another process can still race after this last read.
          fm.move(staging, p.scriptPath);
          const installed = read(spec, p);
          if (installed.hash !== journal.newHash || installed.version !== journal.newVersion) throw new Error('readback_unverified');
          if (!finish(p, journal, 'installed')) return { status: 'recovery_required' };
          return { status: 'installed', version: installed.version, latestVersion: installed.version };
        } catch (error) { return replacing ? restore(spec, p, journal) : failure(error, 'installation_failed'); }
      } catch (error) { return failure(error, 'installation_failed'); }
      finally { busy.delete(spec.id); }
    }
    return Object.freeze({ check, install, recover, inspect });
  }
  return Object.freeze({ create, fingerprint: hashCode });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoUpdater;

// Native-app adapters load only the user's installed, hash-approved source.
// No private script, setting, medication data or API key is bundled here.
const SaquettoNativeApps = (() => {
  const ids = Object.freeze({ calendar: 'super-calendario', pill: 'pilltracker', lastfm: 'lastfm-small' });
  const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const json = value => JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  function once(source, text, replacement) {
    const at = source.indexOf(text);
    if (at < 0 || at !== source.lastIndexOf(text)) throw new Error('adapter_source_changed');
    return source.slice(0, at) + replacement + source.slice(at + text.length);
  }
  function withoutTail(source, anchor, expected) {
    const at = source.indexOf(anchor);
    if (at < 0 || at !== source.lastIndexOf(anchor) || !expected.test(source.slice(at))) throw new Error('adapter_entrypoint_changed');
    return source.slice(0, at);
  }
  function calendarTransform(source) {
    // The host supplies empty queryParameters, so the standalone URL-toggle block is inert.
    return withoutTail(source, '\nif (config.runsInApp && !args.queryParameters.toggle) {', /await presentTable\("all"\);[\s\S]*Script\.complete\(\);\s*$/);
  }
  function pillTransform(source) {
    return withoutTail(source, '\ntry {\n  const data = await loadData()', /await showMainMenu\(data\)[\s\S]*Script\.complete\(\)\s*\}\s*$/);
  }
  function lastfmTransform(source) {
    source = withoutTail(source, '\nif (config.runsInWidget) {\n  let widget = await createSmall();', /await presentRecentTracksWebView\(\);\s*\}\s*Script\.complete\(\);\s*$/);
    source = once(source, '    api_key: API_KEY,\n', '');
    source = once(source, 'console.log("❌ Falha ao buscar dados da API: " + e.message);', '/* Do not log native request URLs or private API configuration. */');
    const start = '    async function refreshData(isManual = false) {', end = '    function manualRefresh() {';
    const a = source.indexOf(start), b = source.indexOf(end);
    if (a < 0 || a !== source.lastIndexOf(start) || b <= a || b !== source.lastIndexOf(end)) throw new Error('adapter_refresh_changed');
    const replacement = `    async function refreshData(isManual = false) {
      const btn = document.getElementById("btnRefresh");
      if (btn) btn.classList.add("spinning");
      try {
        const response = await window.SaquettoApp.request("lastfm.refresh", {});
        if (!response || response.status !== "ok") throw new Error("refresh_unavailable");
        const data = response.state;
        currentUser = data.user; allTracks = data.tracks; playbackDevice = data.playbackDevice;
        renderHeader(currentUser);
        renderHero(allTracks.find(t => t["@attr"] && t["@attr"].nowplaying === "true"), playbackDevice);
        renderTrackList();
        const note = document.getElementById("saquettoLastfmStatus"); if (note) note.textContent = "Atualizado agora";
      } catch (_) {
        const note = document.getElementById("saquettoLastfmStatus"); if (note) note.textContent = "Não foi possível atualizar. Os dados anteriores foram mantidos.";
      } finally { if (btn) btn.classList.remove("spinning"); }
    }

`;
    source = source.slice(0, a) + replacement + source.slice(b);
    // JSON inside a script element must not be able to terminate the element.
    source = once(source, '    username: USER\n  });', "    username: USER\n  }).replace(/</g, '\\\\u003c').replace(/\\u2028/g, '\\\\u2028').replace(/\\u2029/g, '\\\\u2029');");
    return source;
  }
  const calendarExports = `({
    async snapshot() { return { agenda: showAgenda, birthdays: showBirthdays, events: await getUnifiedTimeline(100, true) }; },
    async toggle(kind) {
      if (kind === 'agenda') showAgenda = !showAgenda;
      else if (kind === 'birthdays') showBirthdays = !showBirthdays;
      else throw new Error('invalid_toggle');
      if (!showAgenda && !showBirthdays) { showAgenda = true; showBirthdays = true; }
      Keychain.set(KEY_AGENDA, String(showAgenda)); Keychain.set(KEY_BDAYS, String(showBirthdays));
      return this.snapshot();
    }
  })`;
  const pillExports = `(() => {
    let baseline;
    async function readDisk() {
      const path = getICloudPath();
      if (!fm.fileExists(path)) throw new Error('pill_data_unavailable');
      if (fm.isFileStoredIniCloud(path)) await fm.downloadFileFromiCloud(path);
      return fm.readString(path);
    }
    async function assertCurrent() {
      if (await readDisk() !== baseline) throw new Error('pill_data_changed');
    }
    return {
    async loadData() {
      baseline = await readDisk();
      const existing = JSON.parse(baseline);
      if (!existing || !existing.medicamentos || typeof existing.medicamentos !== 'object' || Array.isArray(existing.medicamentos) || !Object.keys(existing.medicamentos).length || Object.values(existing.medicamentos).some(m => !m || typeof m !== 'object' || Array.isArray(m))) throw new Error('pill_data_unrecognized');
      return loadData();
    },
    assertCurrent,
    async saveData(data) {
      if (!_dataDirty) return;
      await assertCurrent(); await saveData(data);
      const saved = await readDisk();
      if (JSON.stringify(JSON.parse(saved)) !== JSON.stringify(data)) throw new Error('pill_save_unverified');
      baseline = saved;
    },
    getActiveMed, todayISO, getDoseState, getStockState,
    commands: { dose: markTodayTaken, bottle: addBottle, stock: editStock, 'history-add': addHistoryDay,
      'history-remove': removeHistoryDay, 'history-clear': clearHistory, pause: pauseMenu,
      add: addMedication, select: switchActiveMedication, remove: removeMedication, 'clear-icons': limparCacheImagens,
      async edit(data) {
        const active = getActiveMed(data), med = active.data;
        const time = await askTime('Horário ideal', 'Horário registrado para ' + active.name, med.horaIdeal);
        if (time === null) return;
        const minimum = await askNumber('Alerta de estoque', 'Quantidade mínima para o alerta visual.', med.alertaMinimo);
        if (minimum === null) return;
        const maximum = await askNumber('Estoque máximo', 'Máximo exibido no indicador de estoque.', med.maxEstoque);
        if (maximum === null) return;
        med.horaIdeal = time; med.alertaMinimo = minimum; med.maxEstoque = Math.max(maximum, Number(med.estoque) || 0);
        markDirty(); await showAlert('Salvo', 'Preferências do medicamento atualizadas.');
      }
    }
  }; })()`;
  const lastfmExports = `({
    cached: loadCachedState, build: buildRecentTracksHTML,
    async refresh() {
      const recentUrl = 'https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=' + encodeURIComponent(USER) + '&api_key=' + API_KEY + '&format=json&limit=30&extended=1';
      const userUrl = 'https://ws.audioscrobbler.com/2.0/?method=user.getinfo&user=' + encodeURIComponent(USER) + '&api_key=' + API_KEY + '&format=json';
      const [recent, profile, playbackDevice] = await Promise.all([fetchJSON(recentUrl), fetchJSON(userUrl), detectPlaybackSource()]);
      if (!recent || !recent.recenttracks || !Array.isArray(recent.recenttracks.track) || !profile || !profile.user) throw new Error('lastfm_unavailable');
      return { tracks: recent.recenttracks.track, user: profile.user, playbackDevice };
    }
  })`;
  const styles = `
    :root{color-scheme:dark;--bg:#101118;--card:rgba(255,255,255,.055);--ink:#f5f5fa;--muted:#9396a9;--line:rgba(255,255,255,.09);--accent:#9c91ff;--tint:rgba(141,121,255,.14)}
    :root[data-theme="light"]{color-scheme:light;--bg:#f7f7fb;--card:#fff;--ink:#20212d;--muted:#777a8c;--line:#e8e8f0;--accent:#6854dc;--tint:#eeebff}
    :root[data-theme="glass"]{--bg:transparent;--card:rgba(255,255,255,.08);--line:rgba(255,255,255,.14)}
    @media(prefers-color-scheme:light){:root[data-theme="system"]{color-scheme:light;--bg:#f7f7fb;--card:#fff;--ink:#20212d;--muted:#777a8c;--line:#e8e8f0;--accent:#6854dc;--tint:#eeebff}}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px -apple-system,BlinkMacSystemFont,sans-serif;padding:22px 20px calc(30px + env(safe-area-inset-bottom));-webkit-tap-highlight-color:transparent}button,input{font:inherit}button{cursor:pointer;color:inherit;border:0}button:disabled{opacity:.45;cursor:wait}button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:3px}header{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:24px}.eyebrow{font-size:10px;letter-spacing:2px;color:var(--accent);font-weight:700;text-transform:uppercase}h1{font-size:29px;letter-spacing:-1px;margin:6px 0 0}h2{font-size:17px;margin:0}h3{font-size:14px;margin:0}p{line-height:1.5}.muted,.sub{color:var(--muted);font-size:12px}.sub{margin-top:5px;line-height:1.4}.card{border:1px solid var(--line);border-radius:24px;background:var(--card);padding:20px;margin-bottom:14px;backdrop-filter:blur(18px)}.hero{background:linear-gradient(145deg,var(--tint),var(--card));position:relative;overflow:hidden}.hero h2{font-size:26px;letter-spacing:-.8px;margin:8px 0}.row{display:flex;justify-content:space-between;align-items:center;gap:12px}.metric{font-size:48px;font-weight:700;letter-spacing:-2px;font-variant-numeric:tabular-nums}.caps{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase}.round{width:42px;height:42px;border-radius:50%;background:var(--card);border:1px solid var(--line);font-size:22px}.primary,.secondary{border-radius:15px;padding:13px 16px;min-height:46px;font-size:13px;font-weight:600}.primary{background:var(--accent);color:#fff}.secondary{background:var(--card);border:1px solid var(--line)}.wide{width:100%;margin-top:16px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.action{padding:17px;text-align:left;min-height:81px;border-radius:19px;background:var(--card);border:1px solid var(--line)}.action strong{display:block;font-size:13px;margin-top:9px}.action span{font-size:19px}.label{margin:25px 0 12px}.pill{display:inline-block;border-radius:20px;padding:6px 10px;font-size:10px;font-weight:600;background:var(--tint);color:var(--accent)}.event{width:100%;display:flex;gap:14px;text-align:left;background:none;padding:15px 0;border-bottom:1px solid var(--line)}.event:last-child{border:0}.day{width:46px;flex:none;text-align:center;background:var(--tint);color:var(--accent);border-radius:13px;padding:9px 4px}.day strong{font-size:21px;display:block;line-height:1.1}.day small{font-size:9px;text-transform:uppercase}.event .content{flex:1;align-self:center}.event h3{line-height:1.45}.filters{display:flex;gap:7px;margin-bottom:16px;overflow:auto}.filter{border-radius:22px;padding:9px 13px;white-space:nowrap;background:var(--card);color:var(--muted);font-size:12px}.filter.active{background:var(--tint);color:var(--accent)}.switches{display:flex;gap:8px;margin:15px 0}.switches button{flex:1;padding:10px;border-radius:12px;background:var(--card);font-size:11px;color:var(--muted)}.switches button[aria-pressed="true"]{color:var(--accent);background:var(--tint)}.history{display:flex;gap:7px;flex-wrap:wrap}.history span{font-size:11px;border-radius:9px;background:var(--card);border:1px solid var(--line);padding:8px}.notice{font-size:12px;color:var(--muted);margin:12px 0;min-height:16px}.empty{padding:28px;text-align:center;color:var(--muted);font-size:13px}.modal{position:fixed;inset:0;background:rgba(0,0,0,.4);backdrop-filter:blur(8px);display:flex;align-items:flex-end;z-index:20;padding:14px}.modal[hidden]{display:none}.sheet{background:var(--bg);color:var(--ink);border:1px solid var(--line);border-radius:28px;width:100%;max-height:88vh;overflow:auto;padding:25px 20px}.sheet h2{font-size:21px}.sheet p{white-space:pre-line;color:var(--muted);font-size:13px}.sheet input{display:block;width:100%;padding:15px;background:var(--card);border:1px solid var(--line);color:var(--ink);border-radius:13px;margin:10px 0;font-size:16px}.sheet .choices{display:grid;gap:9px;margin-top:20px}.danger{color:#ee6d89}.section-spacer{height:10px}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}`;
  function page(title, accent, content, script, theme) {
    const chosen = ['dark', 'light', 'glass', 'system'].includes(theme) ? theme : 'system';
    return `<!doctype html><html lang="pt-BR" data-theme="${chosen}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>${escape(title)}</title><style>${styles}:root{--accent:${accent}}</style></head><body>${content}<div id="notice" class="notice" role="status" aria-live="polite"></div><div class="modal" id="modal" hidden><section class="sheet" role="dialog" aria-modal="true" aria-labelledby="dialogTitle" tabindex="-1"><h2 id="dialogTitle"></h2><p id="dialogMessage"></p><form id="dialogForm"><div id="dialogFields"></div><div id="dialogChoices" class="choices"></div></form></section></div><script>${script}</script></body></html>`;
  }
  const clientBase = `
    const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const node = id => document.getElementById(id);
    let working = false;
    async function call(action,payload={}) {
      if (working) return; working=true;
      document.querySelectorAll('button').forEach(b=>b.disabled=true);
      try { const response = await window.SaquettoApp.request(action,payload); if (!response || response.status === 'error') throw new Error('failed'); apply(response); }
      catch (_) { node('notice').textContent='Não foi possível concluir. Verifique antes de tentar novamente.'; }
      finally { working=false; document.querySelectorAll('button').forEach(b=>b.disabled=false); }
    }
    function detail(title,message) {
      node('dialogTitle').textContent=title;node('dialogMessage').textContent=message;node('dialogFields').replaceChildren();
      node('dialogChoices').innerHTML='<button type="button" class="primary" id="closeDetail">Fechar</button>';
      node('modal').hidden=false;node('closeDetail').onclick=()=>{node('modal').hidden=true;};
      document.querySelector('.sheet').focus();
    }
  `;
  function calendarHTML(state, theme) {
    return page('Super Calendário', '#d69d46', `<header><div><div class="eyebrow">Seu tempo, reunido</div><h1>Calendário</h1></div><button class="round" id="refresh" aria-label="Atualizar calendário">↻</button></header><section class="card hero"><div class="caps" id="today"></div><h2 id="nextTitle">Sua próxima história</h2><div class="sub" id="nextDate"></div><div class="switches"><button id="agendaToggle">Eventos</button><button id="birthdayToggle">Aniversários</button></div></section><div class="filters" role="group" aria-label="Filtrar calendário"><button class="filter active" data-filter="all">Todos</button><button class="filter" data-filter="agenda">Eventos</button><button class="filter" data-filter="birthday">Aniversários</button></div><main id="events"></main>`, clientBase + `
      let state=${json(state)}, filter='all';
      function apply(response){if(response.state)state=response.state;render();}
      function render(){
        node('today').textContent=new Date().toLocaleDateString('pt-BR',{weekday:'long',day:'numeric',month:'long'});
        const first=state.events[0];node('nextTitle').textContent=first?first.title:'Um espaço para você';
        node('nextDate').textContent=first?new Date(first.date).toLocaleDateString('pt-BR',{day:'numeric',month:'long'})+' · '+first.calendar:'Nenhum compromisso encontrado nos próximos 12 meses.';
        node('agendaToggle').setAttribute('aria-pressed',String(state.agenda));node('birthdayToggle').setAttribute('aria-pressed',String(state.birthdays));
        const events=state.events.filter(e=>filter==='all'||e.type===filter);let lastMonth='';
        node('events').innerHTML=events.map(e=>{const d=new Date(e.date),month=d.toLocaleDateString('pt-BR',{month:'long',year:'numeric'});const label=month!==lastMonth?'<div class="caps label">'+escape(month)+'</div>':'';lastMonth=month;return label+'<button class="event" data-event="'+e.id+'"><span class="day"><strong>'+d.getDate()+'</strong><small>'+escape(d.toLocaleDateString('pt-BR',{month:'short'}).replace('.',''))+'</small></span><span class="content"><h3>'+escape(e.title)+'</h3><div class="sub">'+escape(e.calendar)+(e.allDay?' · Dia inteiro':' · '+escape(d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'})))+'</div></span><span class="sub">'+(e.type==='birthday'?'🎈':'↗')+'</span></button>';}).join('')||'<div class="card empty">Nada por aqui neste filtro.</div>';
        document.querySelectorAll('[data-event]').forEach(b=>b.onclick=()=>{const e=state.events.find(e=>e.id===Number(b.dataset.event));detail(e.title,new Date(e.date).toLocaleString('pt-BR')+'\\n'+e.calendar+(e.allDay?'\\nDia inteiro':''));});
      }
      document.querySelectorAll('[data-filter]').forEach(b=>b.onclick=()=>{filter=b.dataset.filter;document.querySelectorAll('[data-filter]').forEach(x=>x.classList.toggle('active',x===b));render();});
      node('refresh').onclick=()=>call('calendar.refresh');node('agendaToggle').onclick=()=>call('calendar.toggle',{kind:'agenda'});node('birthdayToggle').onclick=()=>call('calendar.toggle',{kind:'birthdays'});render();
    `, theme);
  }
  const pillActions = [['dose', '✓', 'Registrar dose'], ['bottle', '＋', 'Novo pote'], ['stock', '▤', 'Editar estoque'], ['edit', '◷', 'Editar medicamento'], ['add', '✦', 'Adicionar medicamento'], ['select', '⇄', 'Medicamento do widget'], ['pause', 'Ⅱ', 'Pausar ou retomar'], ['history-add', '▦', 'Adicionar dia'], ['history-remove', '−', 'Remover dia'], ['history-clear', '⌫', 'Limpar histórico'], ['remove', '×', 'Remover medicamento'], ['clear-icons', '↻', 'Limpar cache de ícones']];
  function pillHTML(state, theme) {
    const buttons = pillActions.map(([id, icon, title]) => `<button class="action" data-action="${id}"><span aria-hidden="true">${icon}</span><strong>${title}</strong></button>`).join('');
    return page('PillTracker', '#8b80df', `<header><div><div class="eyebrow">Um cuidado de cada vez</div><h1>PillTracker</h1></div><span class="pill" id="medCount"></span></header><section class="card hero"><div class="row"><span class="caps">Medicamento do widget</span><span class="pill" id="doseStatus"></span></div><h2 id="medName"></h2><div class="row"><div><div class="metric" id="stock"></div><div class="caps">unidades em estoque</div></div><div><h2 id="doseTime"></h2><div class="sub">horário registrado</div></div></div><div class="sub" id="stockStatus"></div></section><div class="grid">${buttons}</div><div class="row label"><h2>Histórico registrado</h2><span class="sub" id="historyCount"></span></div><div class="history" id="history"></div>`, clientBase + `
      let state=${json(state)}, dialog=null;
      function apply(response){if(response.state)state=response.state;dialog=response.dialog||null;render();renderDialog();}
      function render(){const med=state.active;node('medName').textContent=med.name;node('stock').textContent=med.stock;node('doseTime').textContent=med.time;node('medCount').textContent=state.count+' medicamento'+(state.count===1?'':'s');node('doseStatus').textContent=med.paused?'Pausado':med.taken?'Registrada hoje':'A registrar';node('stockStatus').textContent='Alerta visual: '+med.minimum+' unidades · Máximo: '+med.maximum;node('historyCount').textContent=med.history.length+' registro'+(med.history.length===1?'':'s');node('history').innerHTML=med.history.slice().reverse().map(date=>'<span>'+escape(date.split('-').reverse().join('/'))+'</span>').join('')||'<p class="muted">Nenhuma dose registrada.</p>';}
      function renderDialog(){
        node('modal').hidden=!dialog;if(!dialog)return;
        node('dialogTitle').textContent=dialog.title;node('dialogMessage').textContent=dialog.message;
        node('dialogFields').innerHTML=dialog.fields.map((f,i)=>'<label class="sub">'+escape(f.placeholder||'Valor')+'<input data-field="'+i+'" value="'+escape(f.value)+'" placeholder="'+escape(f.placeholder)+'" autocomplete="off" autocapitalize="off" maxlength="300"></label>').join('');
        node('dialogChoices').innerHTML=dialog.actions.map((a,i)=>'<button type="button" class="'+(i===0?'primary':'secondary')+'" data-choice="'+i+'">'+escape(a)+'</button>').join('')+(dialog.cancel?'<button type="button" class="secondary" data-choice="-1">'+escape(dialog.cancel)+'</button>':'');
        document.querySelectorAll('[data-choice]').forEach(b=>b.onclick=()=>submit(Number(b.dataset.choice)));
        const input=node('dialogFields').querySelector('input');if(input)input.focus();else document.querySelector('.sheet').focus();
      }
      function submit(choice){call('dialog.submit',{id:dialog.id,choice,fields:[...document.querySelectorAll('[data-field]')].map(e=>e.value)});}
      node('dialogForm').onsubmit=e=>{e.preventDefault();if(dialog&&dialog.actions.length)submit(0);};
      document.querySelectorAll('[data-action]').forEach(b=>b.onclick=()=>call('pill.command',{command:b.dataset.action}));
      document.addEventListener('keydown',e=>{if(e.key==='Escape'&&dialog&&dialog.cancel){e.preventDefault();submit(-1);}});render();
    `, theme);
  }
  function calendarState(value) {
    return { agenda: value.agenda === true, birthdays: value.birthdays === true, events: (value.events || []).slice(0, 100).map((event, id) => ({ id, type: event.type === 'birthday' ? 'birthday' : 'agenda', title: String(event.title || ''), date: new Date(event.date).toISOString(), calendar: String(event.calName || ''), allDay: !!event.isAllDay })) };
  }
  function safeURL(value) {
    // Scriptable's JavaScriptCore does not promise the browser URL global.
    return typeof value === 'string' && /^https:\/\/[a-z0-9.-]+(?::[0-9]+)?(?:[/?#][^\s"'<>\\]*)?$/i.test(value) ? value : '';
  }
  function lastfmState(value) {
    const text = v => escape(String(v == null ? '' : v).slice(0, 1000));
    const rawUser = value && value.user || {};
    const image = list => Array.isArray(list) ? list.slice(0, 6).map(i => ({ size: ['small', 'medium', 'large', 'extralarge', 'mega'].includes(i.size) ? i.size : 'small', '#text': safeURL(i['#text']) })) : [];
    return { user: { name: text(rawUser.name), realname: text(rawUser.realname), playcount: String(Number(rawUser.playcount) || 0), image: image(rawUser.image), url: safeURL(rawUser.url) },
      tracks: (value && Array.isArray(value.tracks) ? value.tracks : []).slice(0, 30).map(t => ({ name: text(t.name), artist: { '#text': text(t.artist && (t.artist['#text'] || t.artist.name)) }, album: { '#text': text(t.album && t.album['#text']) }, url: safeURL(t.url), image: image(t.image), date: t.date ? { uts: String(Math.max(0, Number(t.date.uts) || 0)) } : undefined, '@attr': { nowplaying: t['@attr'] && t['@attr'].nowplaying === 'true' ? 'true' : 'false' } })),
      playbackDevice: text(value && value.playbackDevice) || null };
  }
  function enrichLastfm(html, theme) {
    const chosen = ['dark', 'light', 'glass', 'system'].includes(theme) ? theme : 'system';
    const css = `<style>:root[data-saquetto-theme="light"]{color-scheme:light;--bg:#f7f7fb;--card-bg:#fff;--card-border:#e8e8f0;--card-hover:#f0eff6;--text-main:#20212d;--text-muted:#777a8c;--text-sub:#777a8c}:root[data-saquetto-theme="glass"]{--bg:transparent;--card-bg:rgba(255,255,255,.08)}@media(prefers-color-scheme:light){:root[data-saquetto-theme="system"]{color-scheme:light;--bg:#f7f7fb;--card-bg:#fff;--card-border:#e8e8f0;--text-main:#20212d;--text-muted:#777a8c;--text-sub:#777a8c}}#saquettoLastfmStatus{font:11px -apple-system,sans-serif;color:var(--text-muted);padding:10px 22px}#saquettoTrackDetail{position:fixed;inset:0;z-index:100;background:var(--bg);color:var(--text-main);padding:30px 24px;overflow:auto}#saquettoTrackDetail[hidden]{display:none}#saquettoTrackDetail button{padding:12px 18px;background:var(--card-bg);border:1px solid var(--card-border);border-radius:20px;color:var(--text-main);font:14px -apple-system,sans-serif}#saquettoTrackDetail h2{font:700 30px -apple-system,sans-serif;margin:36px 0 16px}#saquettoTrackDetail p{font:15px/1.7 -apple-system,sans-serif;color:var(--text-muted)}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}</style>`;
    html = html.replace('<html lang="pt-BR">', `<html lang="pt-BR" data-saquetto-theme="${chosen}">`).replace('</head>', css + '</head>');
    return html.replace('</body>', `<div id="saquettoLastfmStatus" role="status" aria-live="polite">Dados locais; atualizando…</div><section id="saquettoTrackDetail" hidden><button id="saquettoCloseTrack">‹ Voltar</button><h2 id="saquettoTrackTitle"></h2><p id="saquettoTrackInfo"></p></section><script>
      document.addEventListener('click',function(event){const link=event.target.closest('a');if(!link)return;event.preventDefault();event.stopPropagation();const track=allTracks.find(t=>t.url===link.href);const title=document.getElementById('saquettoTrackTitle'),info=document.getElementById('saquettoTrackInfo');
        const decode=value=>{const t=document.createElement('textarea');t.innerHTML=value||'';return t.value;};
        title.textContent=track?decode(track.name):decode(currentUser.realname||currentUser.name);
        info.textContent=track?[decode(track.artist&&track.artist['#text']),decode(track.album&&track.album['#text']),track['@attr']&&track['@attr'].nowplaying==='true'?'Tocando agora':track.date?new Date(Number(track.date.uts)*1000).toLocaleString('pt-BR'):''].filter(Boolean).join(' · '):String(currentUser.playcount||0)+' scrobbles';
        document.getElementById('saquettoTrackDetail').hidden=false;},true);
      document.getElementById('saquettoCloseTrack').onclick=()=>{document.getElementById('saquettoTrackDetail').hidden=true;};
    </script></body>`);
  }
  function create({ loader, render, theme = 'system' }) {
    const selectedTheme = typeof theme === 'function' ? theme() : theme;
    async function openCalendar() {
      const core = await loader.load(ids.calendar, calendarExports, calendarTransform);
      let disposed = false, busy = false;
      const state = calendarState(await core.snapshot());
      return { html: calendarHTML(state, selectedTheme), async handle(action, payload = {}) {
        if (disposed || busy) return { status: 'error', reason: disposed ? 'disposed' : 'busy' };
        busy = true;
        try {
          if (action === 'calendar.refresh') return { status: 'ok', state: calendarState(await core.snapshot()) };
          if (action === 'calendar.toggle' && ['agenda', 'birthdays'].includes(payload.kind)) return { status: 'ok', state: calendarState(await core.toggle(payload.kind)) };
          return { status: 'error', reason: 'invalid_action' };
        } catch (_) { return { status: 'error', reason: 'calendar_unavailable' }; } finally { busy = false; }
      }, dispose() { disposed = true; } };
    }
    async function openPill() {
      let disposed = false, operation = null, pending = null, waiter = null, serial = 0, lastError = false;
      const notify = () => { if (waiter) { const wake = waiter; waiter = null; wake(); } };
      const next = () => new Promise(resolve => { waiter = resolve; });
      class WebAlert {
        constructor() { this.title = ''; this.message = ''; this.fields = []; this.actions = []; this.cancel = ''; }
        addTextField(placeholder = '', value = '') { this.fields.push({ placeholder: String(placeholder), value: String(value) }); }
        addAction(label) { this.actions.push(String(label)); }
        addDestructiveAction(label) { this.addAction(label); }
        addCancelAction(label) { this.cancel = String(label); }
        textFieldValue(index) { return this.fields[index] ? this.fields[index].value : ''; }
        presentAlert() { return this.presentSheet(); }
        presentSheet() {
          if (disposed) return Promise.resolve(-1);
          return new Promise(resolve => { pending = { id: ++serial, alert: this, resolve }; notify(); });
        }
      }
      const core = await loader.load(ids.pill, pillExports, pillTransform, { Alert: WebAlert });
      const data = await core.loadData();
      function snapshot() {
        const active = core.getActiveMed(data), med = active.data;
        return { count: Object.keys(data.medicamentos).length, active: { name: active.name, stock: Number(med.estoque) || 0, maximum: Number(med.maxEstoque) || 0, minimum: Number(med.alertaMinimo) || 0, time: String(med.horaIdeal || ''), paused: !!med.pausado, taken: Array.isArray(med.historico) && med.historico.includes(core.todayISO()), history: Array.isArray(med.historico) ? med.historico.map(String) : [] } };
      }
      function response() {
        const dialog = pending ? { id: pending.id, title: pending.alert.title, message: pending.alert.message, fields: pending.alert.fields.map(f => ({ ...f })), actions: pending.alert.actions.slice(), cancel: pending.alert.cancel } : null;
        return { status: lastError ? 'error' : 'ok', state: snapshot(), dialog, ...(lastError ? { reason: 'pill_operation_failed' } : {}) };
      }
      return { html: pillHTML(snapshot(), selectedTheme), async handle(action, payload = {}) {
        if (disposed) return { status: 'error', reason: 'disposed' };
        if (action === 'pill.command') {
          if (lastError) return { status: 'error', reason: 'session_requires_reload' };
          if (operation || !Object.prototype.hasOwnProperty.call(core.commands, payload.command)) return { status: 'error', reason: operation ? 'busy' : 'invalid_action' };
          lastError = false; const waiting = next();
          operation = Promise.resolve().then(async () => { try { await core.assertCurrent(); await core.commands[payload.command](data); await core.saveData(data); } catch (_) { lastError = true; } finally { operation = null; notify(); } });
          await waiting; return response();
        }
        if (action === 'dialog.submit') {
          if (!pending || payload.id !== pending.id || !Number.isInteger(payload.choice) || payload.choice < -1 || payload.choice >= pending.alert.actions.length || payload.choice === -1 && !pending.alert.cancel || !Array.isArray(payload.fields) || payload.fields.length !== pending.alert.fields.length || payload.fields.some(v => typeof v !== 'string' || v.length > 300)) return { status: 'error', reason: 'invalid_dialog' };
          const dialog = pending, waiting = next(); pending = null;
          payload.fields.forEach((value, i) => { dialog.alert.fields[i].value = value; }); dialog.resolve(payload.choice);
          await waiting; return response();
        }
        return { status: 'error', reason: 'invalid_action' };
      }, async dispose() {
        disposed = true;
        if (pending) { const dialog = pending; pending = null; dialog.resolve(-1); }
        if (operation) await operation;
      } };
    }
    async function openLastfm() {
      const core = await loader.load(ids.lastfm, lastfmExports, lastfmTransform);
      let disposed = false, busy = false;
      const state = lastfmState(core.cached());
      const html = enrichLastfm(core.build(state), selectedTheme);
      return { html, async handle(action) {
        if (disposed || busy || action !== 'lastfm.refresh') return { status: 'error', reason: disposed ? 'disposed' : busy ? 'busy' : 'invalid_action' };
        busy = true; try { return { status: 'ok', state: lastfmState(await core.refresh()) }; } catch (_) { return { status: 'error', reason: 'lastfm_unavailable' }; } finally { busy = false; }
      }, dispose() { disposed = true; } };
    }
    return Object.freeze({ async open(appId) {
      if (appId === ids.calendar) return openCalendar();
      if (appId === ids.pill) return openPill();
      if (appId === ids.lastfm) return openLastfm();
      throw new Error('unsupported_native_app');
    } });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoNativeApps;

// Menus live in the saquettoOS shell. Extraction and downloads remain in the
// user's hash-approved local script; no private source or configuration is bundled.
const SaquettoMediaApp = (() => {
  const APP_ID = 'media-downloader';
  const ENTRYPOINT = '\n// ── DISPARO AUTOMÁTICO SE EM AMBIENTE SCRIPTABLE ──\n';
  const TAIL = /^\n\/\/ ── DISPARO AUTOMÁTICO SE EM AMBIENTE SCRIPTABLE ──\nif \(typeof Script !== "undefined"\) \{\n  \(async function \(\) \{\n    try \{\n      await main\(\);\n    \} catch \(err\) \{[\s\S]*?\n  \}\)\(\);\n\}\n\n\/\/ ── EXPORTAÇÃO PARA TESTES UNITÁRIOS EM NODE\.JS ──\nif \(typeof module !== "undefined" && module\.exports\) \{\n  module\.exports = \{[\s\S]*?\n    main: main\n  \};\n\}\s*$/;
  function transform(source) {
    const at = source.indexOf(ENTRYPOINT);
    if (at < 0 || at !== source.lastIndexOf(ENTRYPOINT) || !TAIL.test(source.slice(at))) throw new Error('media_entrypoint_changed');
    return source.slice(0, at);
  }
  const text = (value, max = 1200) => String(value == null ? '' : value).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').slice(0, max);
  function validUrl(value) {
    if (typeof value !== 'string' || value.length > 4096) return '';
    const clean = value.trim();
    if (/[\u0000-\u0020\\]/.test(clean)) return '';
    const match = /^https?:\/\/([^/?#]+)(?:[/?#].*)?$/i.exec(clean);
    return match && /^(?:[a-z0-9.-]+|\[[a-f0-9:]+\])(?::\d{1,5})?$/i.test(match[1]) ? clean : '';
  }
  const safeTheme = value => ['dark', 'light', 'glass', 'system'].includes(value) ? value : 'system';
  const styles = `:root{color-scheme:dark;--bg:#101118;--card:rgba(255,255,255,.055);--ink:#f4f5fa;--muted:#9599aa;--line:rgba(255,255,255,.1);--accent:#48cbd0;--tint:rgba(72,203,208,.12)}:root[data-theme=light]{color-scheme:light;--bg:#f7f8fb;--card:#fff;--ink:#202533;--muted:#697183;--line:#e5e9ef;--accent:#07858d;--tint:#e5f5f5}:root[data-theme=glass]{--bg:transparent;--card:rgba(255,255,255,.085);--line:rgba(255,255,255,.14)}@media(prefers-color-scheme:light){:root[data-theme=system]{color-scheme:light;--bg:#f7f8fb;--card:#fff;--ink:#202533;--muted:#697183;--line:#e5e9ef;--accent:#07858d;--tint:#e5f5f5}}*{box-sizing:border-box}body{margin:0;padding:24px 20px calc(30px + env(safe-area-inset-bottom));background:var(--bg);color:var(--ink);font:15px -apple-system,BlinkMacSystemFont,sans-serif;-webkit-tap-highlight-color:transparent}button,input{font:inherit}button{cursor:pointer;color:inherit;border:0;min-height:46px}button:disabled{opacity:.45;cursor:wait}button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:3px}.eyebrow{color:var(--accent);font-size:10px;letter-spacing:2px;text-transform:uppercase;font-weight:700}h1{font-size:30px;letter-spacing:-1px;margin:8px 0 10px}h2{font-size:20px;margin:0 0 8px}p{font-size:13px;line-height:1.55;color:var(--muted)}.card{border:1px solid var(--line);background:var(--card);border-radius:24px;padding:21px;margin:20px 0;backdrop-filter:blur(18px)}.hero{background:linear-gradient(145deg,var(--tint),var(--card))}.hero-mark{font-size:32px;color:var(--accent);margin-bottom:20px}.hero strong{display:block;font-size:21px;letter-spacing:-.5px}.platforms{display:flex;gap:7px;flex-wrap:wrap;margin:17px 0 0}.platforms span{font-size:10px;border:1px solid var(--line);padding:6px 9px;border-radius:15px;color:var(--muted)}label{display:block;font-size:12px;font-weight:600;margin-bottom:10px}input{width:100%;border:1px solid var(--line);background:var(--bg);color:var(--ink);border-radius:15px;padding:15px;font-size:16px;margin-bottom:12px}.primary{width:100%;background:var(--accent);color:#062a2c;border-radius:15px;padding:14px;font-weight:700;font-size:13px}.secondary{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px 15px;font-size:12px}.state{display:flex;gap:12px;align-items:center;min-height:50px;font-size:13px;line-height:1.5;color:var(--muted)}.spinner{height:18px;width:18px;flex:none;border:2px solid var(--line);border-top-color:var(--accent);border-radius:50%;animation:spin .9s linear infinite}.spinner[hidden]{display:none}.rows{display:grid;gap:10px}.row{display:flex;gap:15px;align-items:center;width:100%;text-align:left;border:1px solid var(--line);background:var(--card);border-radius:19px;padding:16px}.row.header{background:var(--tint);border-color:transparent}.cell-text{min-width:0;flex:1}.cell-text strong{display:block;font-size:13px;line-height:1.45;overflow-wrap:anywhere}.cell-text small{display:block;color:var(--muted);font-size:11px;line-height:1.45;margin-top:5px;overflow-wrap:anywhere}.thumb{width:58px;height:65px;object-fit:contain;border-radius:12px;background:var(--tint)}.row.header .thumb{width:42px;height:42px}.cell-button{padding:8px;border-radius:12px;background:var(--tint);color:var(--accent);font-size:12px}.modal{position:fixed;inset:0;background:rgba(0,0,0,.4);backdrop-filter:blur(8px);padding:14px;display:flex;align-items:flex-end;z-index:20}.modal[hidden]{display:none}.sheet{background:var(--bg);border:1px solid var(--line);border-radius:27px;padding:24px 20px;width:100%;max-height:88vh;overflow:auto}.sheet p{white-space:pre-line}.choices{display:grid;gap:9px}.danger{color:#eb7490}.footer{margin:22px 0;color:var(--muted);font-size:11px;line-height:1.5}@keyframes spin{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}`;
  function html(theme) {
    return `<!doctype html><html lang="pt-BR" data-theme="${safeTheme(theme)}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="referrer" content="no-referrer"><title>Media Downloader</title><style>${styles}</style></head><body><header><div class="eyebrow">Sua biblioteca, do seu jeito</div><h1>Media Downloader</h1></header><section class="card hero"><div class="hero-mark" aria-hidden="true">↙</div><strong>Guarde o que vale rever.</strong><p>Fotos, vídeos, perfis e cards de posts. Cole um link para escolher o que salvar.</p><div class="platforms"><span>Instagram</span><span>X</span><span>Threads</span><span>TikTok</span><span>YouTube</span></div></section><form id="urlForm" class="card"><label for="mediaUrl">Link do post ou perfil</label><input id="mediaUrl" type="url" inputmode="url" placeholder="https://…" autocomplete="off" autocapitalize="off" spellcheck="false" required><button class="primary" id="start">Encontrar mídia</button></form><div class="state" aria-live="polite" role="status"><span class="spinner" id="spinner" hidden></span><span id="status">Pronto para receber um link.</span></div><main id="rows" class="rows"></main><button class="secondary" id="cancel" hidden>Cancelar seleção</button><p class="footer">Fotos e compartilhamento usam os controles do iOS quando você escolher salvar.</p><div class="modal" id="modal" hidden><section class="sheet" role="dialog" aria-modal="true" aria-labelledby="dialogTitle" tabindex="-1"><h2 id="dialogTitle"></h2><p id="dialogMessage"></p><form id="dialogForm"><div id="fields"></div><div id="choices" class="choices"></div></form></section></div><script>
      const byId=id=>document.getElementById(id);let state={phase:'idle'},requesting=false,timer=null,dialogId=null;
      const label=(tag,value,cls)=>{const n=document.createElement(tag);n.textContent=value||'';if(cls)n.className=cls;return n;};
      function schedule(){clearTimeout(timer);if(state.phase==='working')timer=setTimeout(()=>call('media.state'),800);}
      async function call(action,payload={}){if(requesting)return;requesting=true;byId('start').disabled=true;try{const result=await window.SaquettoApp.request(action,payload);if(!result||result.status==='error'){byId('status').textContent='Não foi possível concluir. Verifique o link e tente novamente.';}else{state=result.state;draw();}}catch(_){byId('status').textContent='Não foi possível continuar agora.';}finally{requesting=false;byId('start').disabled=['working','table','dialog'].includes(state.phase);schedule();}}
      function draw(){byId('status').textContent=state.message;byId('spinner').hidden=state.phase!=='working';byId('mediaUrl').disabled=['working','table','dialog'].includes(state.phase);byId('rows').replaceChildren();const table=state.table;byId('cancel').hidden=!table;
        if(table)table.rows.forEach((row,index)=>{const container=document.createElement(row.selectable?'button':'div');container.className='row'+(row.header?' header':'');if(row.selectable){container.type='button';container.onclick=()=>call('media.row',{id:table.id,row:index});}row.cells.forEach((cell,cellIndex)=>{if(cell.kind==='image'){if(cell.image){const img=document.createElement('img');img.className='thumb';img.src=cell.image;img.alt='';container.append(img);}return;}const box=document.createElement('span');box.className='cell-text';if(cell.kind==='button'){const b=label('button',cell.title,'cell-button');b.type='button';b.onclick=e=>{e.stopPropagation();call('media.cell',{id:table.id,row:index,cell:cellIndex});};box.append(b);}else{box.append(label('strong',cell.title));if(cell.subtitle)box.append(label('small',cell.subtitle));}container.append(box);});byId('rows').append(container);});
        const dialog=state.dialog;byId('modal').hidden=!dialog;if(!dialog){dialogId=null;byId('fields').replaceChildren();byId('choices').replaceChildren();return;}if(dialogId===dialog.id)return;dialogId=dialog.id;byId('dialogTitle').textContent=dialog.title;byId('dialogMessage').textContent=dialog.message;byId('fields').replaceChildren();dialog.fields.forEach((field,index)=>{const input=document.createElement('input');input.type=field.secure?'password':'text';input.placeholder=field.placeholder;input.value=field.value||'';input.autocomplete='off';input.dataset.field=String(index);input.setAttribute('aria-label',field.placeholder||'Campo '+(index+1));byId('fields').append(input);});byId('choices').replaceChildren();dialog.actions.forEach((action,index)=>{const button=label('button',action.title,action.destructive?'secondary danger':'primary');button.type='button';button.onclick=()=>submit(index);byId('choices').append(button);});if(dialog.cancel){const button=label('button',dialog.cancel,'secondary');button.type='button';button.onclick=()=>submit(-1);byId('choices').append(button);}document.querySelector('.sheet').focus();}
      function submit(choice){const fields=Array.from(byId('fields').querySelectorAll('input')).map(input=>input.value);call('media.dialog',{id:dialogId,choice,fields});}
      byId('urlForm').onsubmit=e=>{e.preventDefault();call('media.start',{url:byId('mediaUrl').value});};byId('dialogForm').onsubmit=e=>{e.preventDefault();submit(0);};byId('cancel').onclick=()=>call('media.cancel',{id:state.table.id});document.addEventListener('keydown',e=>{if(e.key==='Escape'&&state.dialog&&state.dialog.cancel)submit(-1);});window.addEventListener('pagehide',()=>clearTimeout(timer));draw();
    </script></body></html>`;
  }

  function create({ loader, render, theme = 'system' }) {
    void render;
    return Object.freeze({ async open() {
      let disposed = false, operation = null, pending = null, serial = 0, callbackBusy = false;
      let phase = 'idle', message = 'Pronto para receber um link.', imageBudget = 1500000;
      const effects = { photos: 0, videos: 0, shared: false, previewed: false };
      const guard = () => { if (disposed) throw new Error('media_disposed'); };
      function imageData(image) {
        if (!image || typeof Data === 'undefined' || typeof Data.fromPNG !== 'function') return '';
        try { const encoded = Data.fromPNG(image).toBase64String();
          if (typeof encoded !== 'string' || encoded.length > 400000 || encoded.length > imageBudget || !/^[A-Za-z0-9+/=]+$/.test(encoded)) return '';
          imageBudget -= encoded.length; return 'data:image/png;base64,' + encoded;
        } catch (_) { return ''; }
      }
      class WebCell {
        constructor(kind, title = '', subtitle = '', image = '') { this.kind = kind; this.title = title; this.subtitle = subtitle; this.image = image; this.dismissOnTap = false; }
        static text(title, subtitle) { return new WebCell('text', title, subtitle); }
        static button(title) { return new WebCell('button', title); }
        static image(image) { return new WebCell('image', '', '', imageData(image)); }
        static imageAtURL(url) { const cell = new WebCell('image'); cell.nativeUrl = String(url); return cell; }
        leftAligned() {} centerAligned() {} rightAligned() {}
      }
      class WebRow {
        constructor() { this.cells = []; this.isHeader = false; this.dismissOnSelect = true; }
        addCell(cell) { this.cells.push(cell); return cell; }
        addText(title, subtitle = '') { return this.addCell(WebCell.text(title, subtitle)); }
        addButton(title) { return this.addCell(WebCell.button(title)); }
        addImage(image) { return this.addCell(WebCell.image(image)); }
        addImageAtURL(url) { return this.addCell(WebCell.imageAtURL(url)); }
      }
      class WebTable {
        constructor() { this.rows = []; }
        addRow(row) { this.rows.push(row); }
        removeRow(row) { this.rows = this.rows.filter(item => item !== row); }
        removeAllRows() { this.rows = []; }
        reload() { guard(); }
        async present() {
          guard();
          if (pending) throw new Error('media_dialog_busy');
          // URLs stay native; the shell receives only raster bytes, never signed URLs.
          if (typeof Request !== 'undefined') await Promise.all(this.rows.flatMap(row => row.cells).filter(cell => cell.nativeUrl).map(async cell => {
            try { const url = validUrl(cell.nativeUrl); if (!/^https:\/\//i.test(url)) return;
              const request = new Request(url); request.timeoutInterval = 8;
              const image = await request.loadImage(); if (!disposed) cell.image = imageData(image);
            } catch (_) {} finally { delete cell.nativeUrl; }
          }));
          guard(); phase = 'table'; message = 'Escolha o que deseja salvar.';
          return new Promise(resolve => { pending = { kind: 'table', id: ++serial, table: this, resolve }; });
        }
      }
      class WebAlert {
        constructor() { this.title = ''; this.message = ''; this.fields = []; this.actions = []; this.cancel = ''; }
        addTextField(placeholder = '', value = '') { const field = { placeholder: text(placeholder), value: String(value), secure: false }; this.fields.push(field); return field; }
        addSecureTextField(placeholder = '', value = '') { const field = this.addTextField(placeholder, value); field.secure = true; return field; }
        addAction(title) { this.actions.push({ title: text(title), destructive: false }); }
        addDestructiveAction(title) { this.actions.push({ title: text(title), destructive: true }); }
        addCancelAction(title) { this.cancel = text(title); }
        textFieldValue(index) { return this.fields[index]?.value || ''; }
        presentAlert() { return this.presentSheet(); }
        present() { return this.presentSheet(); }
        presentSheet() {
          if (disposed) return Promise.resolve(-1);
          if (pending) return Promise.reject(new Error('media_dialog_busy'));
          phase = 'dialog'; message = 'Responda para continuar.';
          return new Promise(resolve => { pending = { kind: 'dialog', id: ++serial, alert: this, resolve }; });
        }
      }
      function snapshot() {
        const state = { phase, message, table: null, dialog: null };
        if (pending?.kind === 'table') state.table = { id: pending.id, rows: pending.table.rows.map(row => ({ header: !!row.isHeader, selectable: typeof row.onSelect === 'function', cells: row.cells.map(cell => ({ kind: cell.kind, title: text(cell.title), subtitle: text(cell.subtitle), image: cell.image || '', tappable: typeof cell.onTap === 'function' })) })) };
        if (pending?.kind === 'dialog') state.dialog = { id: pending.id, title: text(pending.alert.title), message: text(pending.alert.message), actions: pending.alert.actions.map(action => ({ ...action })), cancel: pending.alert.cancel, fields: pending.alert.fields.map(field => ({ placeholder: field.placeholder, secure: field.secure, value: field.secure ? '' : text(field.value, 4096) })) };
        return state;
      }
      const response = () => ({ status: 'ok', state: snapshot() });
      function guardedObject(native, after) {
        return new Proxy(native, { get(target, key) {
          const value = target[key];
          if (typeof value !== 'function') return value;
          return async (...args) => { guard(); const result = await value.apply(target, args); guard(); if (after) after(key, result); return result; };
        } });
      }
      function overrides(url) {
        const result = { args: { shortcutParameter: url, queryParameters: {}, urls: [], plainTexts: [], images: [], fileURLs: [], notification: null },
          UITable: WebTable, UITableRow: WebRow, UITableCell: WebCell, Alert: WebAlert,
          console: { log() {}, warn() {}, error() {}, debug() {} } };
        if (typeof Photos !== 'undefined') result.Photos = guardedObject(Photos, key => { if (key === 'saveVideo') effects.videos++; else if (key === 'save' || key === 'saveImage') effects.photos++; });
        if (typeof ShareSheet !== 'undefined') result.ShareSheet = guardedObject(ShareSheet, key => { if (key === 'present') effects.shared = true; });
        if (typeof QuickLook !== 'undefined') result.QuickLook = guardedObject(QuickLook, key => { if (key === 'present') effects.previewed = true; });
        if (typeof Notification !== 'undefined') result.Notification = new Proxy(Notification, { construct(Target, args) {
          guard(); const notification = new Target(...args);
          return new Proxy(notification, { get(target, key) {
            if (key === 'schedule') return async () => {
              guard();
              // The standalone script has optimistic success messages. Only announce
              // native operations observed here, without claiming a shared video was saved.
              if (!effects.photos && !effects.videos && !effects.shared && !effects.previewed) return;
              target.body = outcomeMessage();
              return await target.schedule();
            };
            const value = target[key]; return typeof value === 'function' ? value.bind(target) : value;
          } });
        } });
        if (typeof Request !== 'undefined') result.Request = new Proxy(Request, { construct(Target, args) {
          guard(); const request = new Target(...args);
          return new Proxy(request, { get(target, key) { const value = target[key]; if (typeof value !== 'function') return value;
            return (...values) => { guard(); const output = value.apply(target, values); return output && typeof output.then === 'function' ? output.then(resolved => { guard(); return resolved; }) : output; };
          } });
        } });
        return result;
      }
      async function start(url) {
        const core = await loader.load(APP_ID, '({ main, extractPostTarget })', transform, overrides(url));
        guard();
        const result = await core.main();
        guard();
        if (result?.cancelled) { phase = 'cancelled'; message = 'Seleção cancelada.'; }
        else if (effects.photos || effects.videos || effects.shared || effects.previewed) { phase = 'done'; message = outcomeMessage(); }
        else { phase = 'error'; message = 'Nenhum salvamento confirmado. Verifique o link e tente novamente.'; }
      }
      function outcomeMessage() {
        if (effects.photos || effects.videos) return [effects.photos ? effects.photos + ' foto(s)' : '', effects.videos ? effects.videos + ' vídeo(s)' : ''].filter(Boolean).join(' e ') + ' salvo(s) na galeria.';
        return effects.shared ? 'Menu de compartilhamento encerrado.' : 'Visualização encerrada.';
      }
      function release(value) {
        const active = pending; pending = null; phase = 'working'; message = 'Processando a mídia…'; active.resolve(value);
      }
      return {
        html: html(typeof theme === 'function' ? theme() : theme),
        async handle(action, payload = {}) {
          if (disposed) return { status: 'error', reason: 'disposed' };
          if (action === 'media.state') return response();
          if (action === 'media.start') {
            if (operation || pending || callbackBusy) return { status: 'error', reason: 'busy' };
            const url = validUrl(payload.url);
            if (!url) return { status: 'error', reason: 'invalid_url' };
            effects.photos = effects.videos = 0; effects.shared = effects.previewed = false;
            imageBudget = 1500000;
            phase = 'working'; message = 'Buscando o post e preparando suas opções…';
            operation = Promise.resolve().then(() => start(url)).catch(() => { if (!disposed) { phase = 'error'; message = 'Não foi possível obter ou salvar essa mídia. Tente novamente.'; } }).finally(() => { operation = null; });
            return response();
          }
          if (action === 'media.cancel' && pending?.kind === 'table' && payload.id === pending.id && !callbackBusy) { release(); return response(); }
          if (action === 'media.dialog') {
            if (pending?.kind !== 'dialog' || payload.id !== pending.id || !Number.isInteger(payload.choice) || payload.choice < -1 || payload.choice >= pending.alert.actions.length || payload.choice === -1 && !pending.alert.cancel || !Array.isArray(payload.fields) || payload.fields.length !== pending.alert.fields.length || payload.fields.some(value => typeof value !== 'string' || value.length > 4096)) return { status: 'error', reason: 'invalid_dialog' };
            payload.fields.forEach((value, i) => { if (!pending.alert.fields[i].secure || value) pending.alert.fields[i].value = value; });
            release(payload.choice); return response();
          }
          if (action === 'media.row' || action === 'media.cell') {
            if (pending?.kind !== 'table' || payload.id !== pending.id || callbackBusy || !Number.isInteger(payload.row)) return { status: 'error', reason: 'invalid_selection' };
            const active = pending, row = active.table.rows[payload.row];
            const cell = action === 'media.cell' && Number.isInteger(payload.cell) ? row?.cells[payload.cell] : null;
            const callback = action === 'media.row' ? row?.onSelect : cell?.onTap;
            if (typeof callback !== 'function') return { status: 'error', reason: 'invalid_selection' };
            callbackBusy = true;
            try {
              await callback();
              if (pending === active && (action === 'media.row' ? row.dismissOnSelect : cell.dismissOnTap)) release();
              return response();
            } catch (_) { return { status: 'error', reason: 'selection_failed' }; } finally { callbackBusy = false; }
          }
          return { status: 'error', reason: 'invalid_action' };
        },
        dispose() {
          disposed = true;
          if (pending) { const active = pending; pending = null; active.resolve(active.kind === 'dialog' ? -1 : undefined); }
          phase = 'disposed'; message = '';
        },
      };
    } });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoMediaApp;

// Reuse the installed app's HTML and native business logic inside saquettoOS.
// Source code, credentials and account state remain on the user's device.
const SaquettoWebviewApps = (() => {
  const IDS = new Set(['clube-uol', 'bilhete-unico']);
  const bilheteModes = new Set(['painel', 'metro', 'extrato', 'tarifas', 'viagem', 'recarga', 'corrigir', 'desfazer', 'limite', 'auth', 'atualizar', 'checkFares', 'saveRouteCard', 'openMap', 'manageFavorites', 'favoriteShortcuts', 'routeStats', 'structuredHealth', 'icloudAssistant', 'widgetPreview', 'auditCapture', 'exportReports', 'cleanupCaptures', 'structuredSync', 'manageLogin', 'reconcile', 'close', 'destino']);
  function uolTransform(source) {
    const tail = /\nawait main\(\);\r?\nScript\.complete\(\);\s*$/;
    if (!tail.test(source) || !/^async function main\(\)\s*\{/m.test(source)) throw new Error('adapter_entrypoint_changed');
    const header = 'function buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions, collectionPausedForUpdate = false, collectionResult = null) {';
    const start = source.indexOf(header);
    if (start < 0 || start !== source.lastIndexOf(header)) throw new Error('adapter_dashboard_changed');
    let builder = source.slice(start).replace(tail, '\n');
    const replaceOnce = (from, to) => {
      if (builder.split(from).length !== 2) throw new Error('adapter_dashboard_changed');
      builder = builder.replace(from, to);
    };
    // Only the presentation DTO changes. Native account functions retain their
    // original arguments, credential files and isolated authenticated sessions.
    replaceOnce(header, header + `
  const uiAccounts = {};
  for (const account of ["leosaquetto", "gaoriel"]) {
    const entry = credentials && credentials.accounts && credentials.accounts[account] || {};
    const login = typeof entry.login === "string" ? entry.login : "";
    const masked = login.length > 5 ? login.slice(0, 3) + "••••" + login.slice(-2) : login ? "••••••" : "";
    uiAccounts[account] = { login: masked.replace(/[^a-zA-Z0-9@._•-]/g, "•"), hasCredentials: Boolean(login && entry.senha) };
  }
  const uiSettings = {};
  for (const key of ["currentUser", "currentTab", "priorizarWords", "despriorizarWords", "groupedView", "myRedemptions", "realRedemptions", "navigationBarTheme"]) {
    if (Object.prototype.hasOwnProperty.call(settings, key)) uiSettings[key] = settings[key];
  }`);
    replaceOnce('    settings: settings,', '    settings: uiSettings,');
    replaceOnce('    credentials: credentials,', '    credentials: { accounts: uiAccounts, source: "Login nativo" },');
    replaceOnce('    sessionCookies: (sessionCookies && sessionCookies.length > 0) ? sessionCookies : cookiesForActiveUser,', '    sessionCookies: [],');
    replaceOnce('    sessionCookiesLeo: settings.userSessions.leosaquetto,', '    sessionCookiesLeo: [],');
    replaceOnce('    sessionCookiesGaoriel: settings.userSessions.gaoriel,', '    sessionCookiesGaoriel: [],');
    replaceOnce('  return resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr);', `  const uiHtml = resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr)
    .replaceAll("Boolean(targetCreds.login && targetCreds.senha)", "Boolean(targetCreds.hasCredentials)")
    .replaceAll("Boolean(userCreds.login && userCreds.senha)", "Boolean(userCreds.hasCredentials)");
  // Legacy credential helpers now enter the existing native reconnect flow.
  // No raw login, password or cookie is available to these iframe functions.
  const nativeLogin = '<script>openAuthAssistantModal = reconnectUolSession; copyAuthLogin = reconnectUolSession; copyAuthPassword = reconnectUolSession; openUolLogoutAndLogin = reconnectUolSession; openUolDirectLogin = reconnectUolSession; toggleAuthPassVisibility = reconnectUolSession;</script>';
  if (!uiHtml.includes("</body>")) throw new Error("adapter_dashboard_changed");
  return uiHtml.replace("</body>", nativeLogin + "</body>");`);
    return source.slice(0, start) + builder;
  }
  function bilheteTransform(source) {
    const anchor = '\ntry{\n  cleanupRetiredInfrastructureKeys();';
    const tail = '\ntry{\n  cleanupRetiredInfrastructureKeys();\n  if(config.runsInWidget)Script.setWidget(await widget());\n  else await panel(args.queryParameters?.mode||\'painel\');\n}catch(e){\n  if(config.runsInWidget){recordAutomaticError(e);const w=new ListWidget();w.addText(\'Abra Bilhete Único no Scriptable\');w.url=open(\'painel\');Script.setWidget(w);}\n  else await errorDialog(e);\n}\nScript.complete();';
    const at = source.indexOf(anchor);
    if (at < 0 || at !== source.lastIndexOf(anchor) || source.slice(at).trimEnd() !== tail || !/^async function panel\(mode\)\s*\{/m.test(source)) throw new Error('adapter_entrypoint_changed');
    return source.slice(0, at);
  }
  function bilheteRoute(raw) {
    if (typeof raw !== 'string' || raw.length > 8192 || !/^scriptable:\/\/\/run\?/.test(raw) || /[\x00-\x20]/.test(raw) || raw.includes('#')) return null;
    const values = Object.create(null);
    try {
      for (const part of raw.slice('scriptable:///run?'.length).split('&')) {
        const at = part.indexOf('='); if (at < 1) return null;
        const key = decodeURIComponent(part.slice(0, at)), value = decodeURIComponent(part.slice(at + 1));
        if (!['scriptName', 'mode', 'origem', 'destino', 'favorito', 'tab'].includes(key) || Object.prototype.hasOwnProperty.call(values, key) || value.length > 1000 || /[\x00-\x1f]/.test(value)) return null;
        values[key] = value;
      }
      if (!values.scriptName || values.scriptName.normalize('NFC') !== 'Bilhete Único' || !bilheteModes.has(values.mode || 'painel')) return null;
      if (values.tab && !['home', 'metro', 'history', 'settings'].includes(values.tab)) return null;
      const mode = values.mode || 'painel'; delete values.scriptName; values.mode = mode;
      return { mode, params: values };
    } catch (_) { return null; }
  }
  function themedHTML(html, theme) {
    if (typeof html !== 'string') throw new Error('invalid_app_html');
    const value = typeof theme === 'function' ? theme() : theme;
    const light = '--bg:#f7f7fb;--card-bg:#fff;--card-elevated:#f0f0f7;--card-border:#e0e2ec;--text-primary:#212331;--text-main:#212331;--text-muted:#606779;--text-dim:#70778a;--code-bg:#eceef5;color-scheme:light';
    const css = value === 'light' ? ':root{' + light + '}' : value === 'glass' ? ':root{--bg:transparent;--card-bg:rgba(22,24,37,.65);--card-elevated:rgba(30,33,48,.75);--card-border:rgba(255,255,255,.14)}' : value === 'system' ? '@media(prefers-color-scheme:light){:root{' + light + '}}' : '';
    const style = '<style data-saquetto-app-theme>' + css + '@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto!important;animation-duration:.01ms!important;transition-duration:.01ms!important}}</style>';
    return html.includes('</head>') ? html.replace('</head>', style + '</head>') : style + html;
  }
  function create({ loader, WebView: NativeWebView, render, evalFrame, openURL, theme = 'system', closeRequested = () => {} }) {
    if (!loader || typeof loader.load !== 'function' || typeof render !== 'function' || typeof evalFrame !== 'function') throw new Error('webview_adapter_host_required');
    async function open(appId) {
      if (!IDS.has(appId)) throw new Error('unsupported_webview_app');
      let disposed = false, disposal = null, closeScheduled = false, current = null, revision = 0, renderQueue = Promise.resolve(), lastHTML = null, busy = false;
      const nativeViews = new Set(), embeddedViews = new Set(), tasks = new Set(), listeners = new Set();
      const args = { queryParameters: {}, shortcutParameter: null, widgetParameter: null, urls: [], plainTexts: [], images: [], fileURLs: [] };
      const emit = () => { for (const listener of [...listeners]) listener(revision); };
      function waitForPresentation(after = revision) {
        let cancel;
        const promise = new Promise(resolve => { const listener = next => { if (next > after || disposed) { listeners.delete(listener); resolve({ shown: next > after, disposed }); } }; listeners.add(listener); cancel = () => listeners.delete(listener); });
        return { promise, cancel };
      }
      function track(promise) { tasks.add(promise); promise.then(() => tasks.delete(promise), () => tasks.delete(promise)); return promise; }
      class EmbeddedWebView {
        constructor() { this.native = null; this.nativeOnly = false; this.html = null; this.baseURL = undefined; this.active = false; this.closed = false; this.navigation = null; this.presentation = null; this.resolvePresentation = null; }
        set shouldAllowRequest(handler) { this.navigation = handler; if (this.native) this.native.shouldAllowRequest = handler; }
        get shouldAllowRequest() { return this.navigation; }
        set hunterActionTask(task) { this.actionTask = task && typeof task.then === 'function' ? track(Promise.resolve(task)) : task; }
        get hunterActionTask() { return this.actionTask; }
        async nativeView(loadPending = true) {
          if (!this.native) {
            if (typeof NativeWebView !== 'function') throw new Error('native_webview_unavailable');
            this.native = new NativeWebView(); nativeViews.add(this.native);
            if (this.navigation) this.native.shouldAllowRequest = this.navigation;
            if (loadPending && this.html !== null) await this.native.loadHTML(this.html, this.baseURL);
          }
          return this.native;
        }
        async loadURL(url) {
          if (disposed || this.closed) throw new Error('app_closed');
          this.nativeOnly = true; const native = await this.nativeView(false); return track(Promise.resolve(native.loadURL(url)));
        }
        async loadRequest(request) {
          if (disposed || this.closed) throw new Error('app_closed');
          this.nativeOnly = true; const native = await this.nativeView(false); return track(Promise.resolve(native.loadRequest(request)));
        }
        async loadHTML(html, baseURL) {
          if (disposed || this.closed) return;
          this.html = html; this.baseURL = baseURL;
          if (this.nativeOnly || this.native && !this.active) { const native = await this.nativeView(false); return native.loadHTML(html, baseURL); }
          if (this.active) return this.renderHTML();
        }
        async renderHTML() {
          const html = themedHTML(this.html, theme), view = this;
          renderQueue = renderQueue.catch(() => {}).then(async () => {
            if (disposed || view.closed) return;
            await render(html, view.baseURL);
            if (disposed || view.closed) return;
            lastHTML = html; revision++; emit();
          });
          return renderQueue;
        }
        async evaluateJavaScript(code, useCallback = false) {
          if (disposed || this.closed) throw new Error('app_closed');
          if (this.active && current === this) { await renderQueue; if (disposed || this.closed || current !== this) throw new Error('stale_app_view'); return evalFrame(code, useCallback); }
          const native = await this.nativeView(); return track(Promise.resolve(native.evaluateJavaScript(code, useCallback)));
        }
        async getHTML() {
          if (this.active && current === this) return evalFrame('document.documentElement.outerHTML', false);
          const native = await this.nativeView(); return native.getHTML();
        }
        async present(fullscreen = true) {
          if (disposed || this.closed) return;
          if (this.nativeOnly || this.native) { const native = await this.nativeView(); return track(Promise.resolve(native.present(fullscreen))); }
          if (this.html === null) throw new Error('app_html_missing');
          if (this.presentation) return this.presentation;
          if (current && current !== this) current.dismiss();
          current = this; this.active = true; embeddedViews.add(this);
          this.presentation = new Promise(resolve => { this.resolvePresentation = resolve; });
          await this.renderHTML();
          return this.presentation;
        }
        dismiss() {
          this.closed = true; this.active = false;
          if (this.resolvePresentation) this.resolvePresentation();
          embeddedViews.delete(this);
        }
      }
      const overrides = { WebView: EmbeddedWebView, args };
      // UOL's offer navigation starts detached Safari promises. Keep those in
      // the same drain as core actions without closing the user's native sheet.
      if (typeof Safari !== 'undefined') {
        overrides.Safari = {};
        for (const method of ['open', 'openInApp']) overrides.Safari[method] = (...values) => {
          if (disposed) return Promise.reject(new Error('app_closed'));
          return track(Promise.resolve().then(() => Safari[method](...values)));
        };
      }
      const core = await loader.load(appId, appId === 'clube-uol' ? '({main})' : '({panel})', appId === 'clube-uol' ? uolTransform : bilheteTransform, overrides);
      async function runUntilPresented(run) {
        const waiting = waitForPresentation(), task = track(Promise.resolve().then(run));
        // No error detail is returned: native errors may contain private URLs or payloads.
        const outcome = await Promise.race([waiting.promise, task.then(() => ({ finished: true }), () => ({ failed: true }))]);
        waiting.cancel();
        return outcome;
      }
      const first = await runUntilPresented(() => appId === 'clube-uol' ? core.main() : core.panel('painel'));
      if (first.failed || first.finished && !first.shown || disposed) {
        disposed = true; for (const view of embeddedViews) view.dismiss(); emit();
        throw new Error('app_open_failed');
      }
      async function currentTab() {
        try { const tab = await evalFrame("(document.querySelector('.tab-content.active')?.id || 'tab-home').replace(/^tab-/, '')", false); return ['home', 'metro', 'history', 'settings'].includes(tab) ? tab : 'home'; } catch (_) { return 'home'; }
      }
      async function navigateBilhete(route) {
        if (disposed || busy) return { status: 'error', reason: disposed ? 'disposed' : 'busy', allow: false };
        if (route.mode === 'close') {
          // The host closes by awaiting dispose(). Do not include that callback
          // in our tracked navigation promise, which dispose itself must drain.
          if (!closeScheduled) { closeScheduled = true; Promise.resolve().then(() => closeRequested()).catch(() => {}); }
          return { status: 'ok', allow: false };
        }
        busy = true;
        try {
          const tab = await currentTab();
          args.queryParameters = { ...route.params };
          const outcome = await runUntilPresented(() => core.panel(route.mode));
          if (disposed) return { status: 'error', reason: 'disposed', allow: false };
          if (outcome.failed) return { status: 'error', reason: 'native_action_failed', allow: false };
          if (!outcome.shown) {
            args.queryParameters = { mode: 'painel', tab };
            const refreshed = await runUntilPresented(() => core.panel('painel'));
            if (!refreshed.shown || refreshed.failed) return { status: 'error', reason: 'refresh_failed', allow: false };
          }
          return { status: 'ok', allow: false };
        } finally { busy = false; }
      }
      function allowOriginal(request) {
        if (disposed || !current || current.closed) return false;
        try { return typeof current.navigation === 'function' ? current.navigation(request) !== false : true; } catch (_) { return false; }
      }
      async function navigateOriginal(request) {
        const allow = allowOriginal(request);
        if (!allow) return { status: 'ok', allow: false };
        if (!/^https?:\/\//i.test(request.url) || typeof openURL !== 'function') return { status: 'error', reason: 'external_navigation_unavailable', allow: false };
        try { await openURL(request.url); return { status: 'ok', allow: false }; }
        catch (_) { return { status: 'error', reason: 'navigation_failed', allow: false }; }
      }
      function shouldAllowRequest(request) {
        const url = request && request.url;
        if (disposed || typeof url !== 'string' || url.length > 8192) return false;
        if (appId === 'bilhete-unico' && /^scriptable:/i.test(url)) {
          const route = bilheteRoute(url); if (route) track(navigateBilhete(route).catch(() => ({ status: 'error' })));
          return false;
        }
        // Never replace the host document. Native auth/offer handlers keep their
        // own URL checks; other web navigations use the host's authorized opener.
        track(navigateOriginal(request).catch(() => ({ status: 'error' })));
        return false;
      }
      return {
        get html() { return lastHTML; },
        shouldAllowRequest,
        async handle(action, payload = {}) {
          if (disposed) return { status: 'error', reason: 'disposed' };
          if (action !== 'webview.navigate' || typeof payload.url !== 'string' || payload.url.length > 8192) return { status: 'error', reason: 'invalid_action' };
          if (appId === 'bilhete-unico' && /^scriptable:/i.test(payload.url)) {
            const route = bilheteRoute(payload.url);
            return route ? track(navigateBilhete(route)) : { status: 'error', reason: 'invalid_route', allow: false };
          }
          return track(navigateOriginal({ url: payload.url }));
        },
        dispose() {
          if (disposal) return disposal;
          disposed = true; for (const view of [...embeddedViews]) view.dismiss(); emit();
          disposal = (async () => {
            await renderQueue.catch(() => {});
            // Native login sheets retain their own Close control. Waiting here
            // prevents another app instance from writing while one is still open.
            while (tasks.size) await Promise.allSettled([...tasks]);
          })();
          return disposal;
        }
      };
    }
    return Object.freeze({ open });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoWebviewApps;

// Installed scripts remain local. Both execution modes require reviewed fingerprints.
const SaquettoRuntime = (() => {
  const json = value => JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  function createLoader({ fm, catalog, compatibility, hash, script, globals = {} }) {
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    return { async load(id, exports, transform, overrides = {}) {
      const app = catalog.find(item => item.id === id);
      if (!app || !compatibility[id]) throw new Error('app_invalid');
      const names = fm.listContents(fm.documentsDirectory()).filter(name => name.normalize('NFC') === (app.scriptName + '.js').normalize('NFC'));
      if (names.length !== 1) throw new Error(names.length ? 'script_name_conflict' : 'app_not_installed');
      const filename = fm.joinPath(fm.documentsDirectory(), names[0]);
      if (fm.isFileDownloaded && !fm.isFileDownloaded(filename)) await fm.downloadFileFromiCloud(filename);
      const original = fm.readString(filename);
      if (!compatibility[id].sha256.includes(hash(original))) throw new Error('adapter_source_changed');
      const source = transform(original);
      if (typeof source !== 'string') throw new Error('adapter_source_changed');
      const name = names[0].slice(0, -3);
      const bindings = { ...globals, Script: { name: () => name, complete() {}, setShortcutOutput() {},
          setWidget(widget) { if (typeof script.setWidget === 'function') script.setWidget(widget); } },
        URLScheme: { forRunningScript: () => 'scriptable:///run?scriptName=' + encodeURIComponent(name),
          forOpeningScript: () => 'scriptable:///open?scriptName=' + encodeURIComponent(name),
          forOpeningScriptSettings: () => 'scriptable:///open?scriptName=' + encodeURIComponent(name) + '&openSettings=true',
          allParameters: () => ({}), parameter: () => undefined },
        config: { runsInApp: true, runsInWidget: false, runsInAccessoryWidget: false, runsFromHomeScreen: false,
          runsInActionExtension: false, runsWithSiri: false, runsInNotification: false, widgetFamily: null },
        args: { queryParameters: {}, shortcutParameter: null, widgetParameter: null, urls: [], plainTexts: [], images: [], fileURLs: [] },
        module: { filename, exports: {} }, ...overrides };
      return new AsyncFunction(...Object.keys(bindings), source + '\nreturn (' + exports + ');')(...Object.values(bindings));
    } };
  }
  function nativeSource(id, source) {
    // These two reviewed scripts otherwise return before their native UI finishes.
    // Normalize await ownership in memory only; never rewrite an installed app.
    const entry = id === 'super-calendario' ? '  t.present();' : id === 'media-downloader' ? '(async function () {' : null;
    if (!entry) return source;
    if (source.split(entry).length !== 2) throw new Error('adapter_entrypoint_changed');
    return source.replace(entry, id === 'super-calendario' ? '  await t.present();' : 'await (async function () {');
  }
  // This function is serialized into the sandboxed child, before app scripts.
  function childBridge(initial) {
    const waiting = new Map(); let serial = 0;
    const request = (action, payload = {}) => new Promise((resolve, reject) => {
      const id = 'child-' + (++serial);
      waiting.set(id, { resolve, reject });
      parent.postMessage({ saquetto: 1, kind: 'request', id, instanceId: initial.instanceId, action, payload }, '*');
    });
    window.SaquettoApp = Object.freeze({ request });
    window.addEventListener('message', event => {
      if (event.source !== parent || event.data?.saquetto !== 1 || event.data.instanceId !== initial.instanceId) return;
      const message = event.data;
      if (message.kind === 'response') {
        const pending = waiting.get(message.id); if (!pending) return;
        waiting.delete(message.id);
        message.ok ? pending.resolve(message.data) : pending.reject(new Error(message.error || 'operation_failed'));
      } else if (message.kind === 'eval') {
        let done = false;
        const finish = (value, error) => {
          if (done) return; done = true;
          try { value = value === undefined ? null : JSON.parse(JSON.stringify(value)); }
          catch (_) { value = null; error = 'evaluation_result_invalid'; }
          parent.postMessage({ saquetto: 1, kind: 'evalResult', id: message.id, instanceId: message.instanceId, value, error }, '*');
        };
        try {
          if (message.useCallback) new Function('completion', message.code)(value => finish(value));
          else Promise.resolve((0, eval)(message.code)).then(value => finish(value), () => finish(null, 'evaluation_failed'));
        } catch (_) { finish(null, 'evaluation_failed'); }
      }
    });
    let values = { ...initial.storage };
    const storage = { get length() { return Object.keys(values).length; }, key: index => Object.keys(values)[index] || null,
      getItem: key => Object.prototype.hasOwnProperty.call(values, key) ? values[key] : null,
      setItem(key, value) { key = String(key); value = String(value); values[key] = value; request('storage.set', { key, value }).catch(() => storageError()); },
      removeItem(key) { key = String(key); delete values[key]; request('storage.remove', { key }).catch(() => storageError()); },
      clear() { values = {}; request('storage.clear').catch(() => storageError()); } };
    function storageError() {
      let notice = document.getElementById('saquetto-storage-error');
      if (!notice) { notice = document.createElement('div'); notice.id = 'saquetto-storage-error'; notice.setAttribute('role', 'alert'); notice.style.cssText = 'position:fixed;bottom:0;left:0;right:0;z-index:2147483647;padding:16px;background:#762c42;color:white;font:14px -apple-system'; (document.body || document.documentElement).appendChild(notice); }
      notice.textContent = 'Não foi possível salvar os ajustes desta tela. Feche e reabra o app.';
    }
    Object.defineProperty(window, 'localStorage', { value: storage });
    if (initial.proxyFetch) window.fetch = async (input, options = {}) => {
      const url = new URL(typeof input === 'string' ? input : input.url, initial.baseURL).href;
      if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const headers = {}; new Headers(options.headers || {}).forEach((value, key) => { headers[key] = value; });
      const result = await request('web.fetch', { url, method: options.method || 'GET', headers });
      if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const bytes = Uint8Array.from(atob(result.body), c => c.charCodeAt(0));
      return new Response([204,205,304].includes(result.status) ? null : bytes, { status: result.status, headers: result.headers });
    };
    document.addEventListener('click', event => {
      const anchor = event.target.closest?.('a[href]'); if (!anchor) return;
      const raw = anchor.getAttribute('href'); if (!raw || raw[0] === '#') return;
      if (/^(https?:|scriptable:|hunter-)/i.test(raw)) {
        event.preventDefault(); request('webview.navigate', { url: new URL(raw, initial.baseURL).href }).catch(() => {});
      }
    }, true);
  }
  function inject(html, initial) {
    const bridge = '<script>(' + childBridge.toString() + ')(' + json(initial) + ');</script>';
    return /<head(?:\s[^>]*)?>/i.test(html) ? html.replace(/<head(?:\s[^>]*)?>/i, match => match + bridge) : bridge + html;
  }
  function create(deps) {
    const { fm, local, catalog, compatibility, hash, script, shell, uuid, timer, request, nativeWebView, openURL, theme, globals } = deps;
    const loader = createLoader({ fm, catalog, compatibility, hash, script, globals });
    let active = null, opening = false, evalSequence = 0, shutdownRequested = false, openingDone = Promise.resolve(), finishOpening;
    const evaluations = new Map();
    function cancelEvaluations() { for (const item of evaluations.values()) { item.timer.invalidate(); item.reject(new Error('app_closed')); } evaluations.clear(); }
    async function render(html, baseURL = 'https://clube.uol.com.br') {
      const app = active; if (!app || app.closing || shutdownRequested) throw new Error('app_closed');
      cancelEvaluations();
      app.instanceId = uuid(); // Register before child scripts can post their first request.
      const prepared = inject(html, { instanceId: app.instanceId, storage: app.storage, baseURL, proxyFetch: app.id === 'clube-uol' });
      const result = await shell('window.saquettoShowApp(' + json({ id: app.id, name: app.name, html: prepared, instanceId: app.instanceId }) + ');');
      if (!result || app !== active) throw new Error('app_render_failed');
      app.instanceId = result;
      return result;
    }
    async function evalFrame(code, useCallback = false) {
      const app = active; if (!app?.instanceId || shutdownRequested) throw new Error('app_not_ready');
      const id = 'eval-' + (++evalSequence), instanceId = app.instanceId;
      return new Promise((resolve, reject) => {
        const timeout = timer(30000, () => { evaluations.delete(id); reject(new Error('evaluation_timeout')); });
        evaluations.set(id, { resolve, reject, timer: timeout, instanceId });
        shell('window.saquettoFrameMessage(' + json({ kind: 'eval', id, code, useCallback, instanceId }) + ');').then(ok => {
          if (!ok && evaluations.has(id)) { timeout.invalidate(); evaluations.delete(id); reject(new Error('app_not_ready')); }
        }, () => { timeout.invalidate(); evaluations.delete(id); reject(new Error('evaluation_failed')); });
      });
    }
    function matches(payload) { return active && payload.appId === active.id && payload.instanceId === active.instanceId; }
    async function proxyFetch(app, payload) {
      const match = typeof payload.url === 'string' && payload.url.match(/^https:\/\/([^/?#]+)(\/[^#]*)?$/i);
      const hosts = ['clube.uol.com.br', 'uol-telegram-shadow-pilot.leosaquetto.workers.dev'];
      if (app.id !== 'clube-uol' || !match || !hosts.includes(match[1].toLowerCase()) || !['GET','HEAD'].includes(payload.method)) throw new Error('network_not_allowed');
      const req = request(payload.url); req.method = payload.method; req.timeoutInterval = 30; req.onRedirect = () => null;
      req.headers = {};
      for (const [name, value] of Object.entries(payload.headers || {})) {
        if (!['accept', 'if-none-match', 'if-modified-since'].includes(name.toLowerCase()) || typeof value !== 'string' || value.length > 1024 || /[\r\n]/.test(value)) throw new Error('network_not_allowed');
        req.headers[name] = value;
      }
      const data = await req.load();
      const response = req.response;
      if (!response || response.statusCode >= 300 && response.statusCode !== 304 && response.statusCode < 400) throw new Error('redirect_not_allowed');
      const body = data.toBase64String();
      if (body.length > 12 * 1024 * 1024) throw new Error('response_too_large');
      const headers = {};
      for (const [name, value] of Object.entries(response.headers || {})) if (['content-type','etag','last-modified'].includes(name.toLowerCase())) headers[name] = value;
      return { body, status: response.statusCode, headers };
    }
    async function open(id) {
      if (shutdownRequested) throw new Error('app_closed');
      if (opening || active) throw new Error('app_busy');
      opening = true;
      openingDone = new Promise(resolve => { finishOpening = resolve; });
      try {
        const app = catalog.find(item => item.id === id); if (!app) throw new Error('app_invalid');
        if (app.mode === 'native') {
          active = { ...app, panel: null };
          // Use the actual WebView, UITable, Alert, storage and networking APIs.
          // Child Script.complete is scoped by the loader; the central stays alive.
          await loader.load(id, 'null', source => {
            if (shutdownRequested) throw new Error('app_closed');
            return nativeSource(id, source);
          });
          active = null;
          return { status: 'returned', appId: id, mode: 'native' };
        }
        const storagePath = local.joinPath(local.documentsDirectory(), 'saquettoos.appstate.' + id + '.json');
        let storage = {};
        if (local.fileExists(storagePath)) {
          try { storage = JSON.parse(local.readString(storagePath)); } catch (_) { throw new Error('app_storage_unreadable'); }
          if (!storage || Array.isArray(storage) || Object.values(storage).some(value => typeof value !== 'string')) throw new Error('app_storage_unreadable');
        }
        const session = active = { ...app, storagePath, storage, instanceId: '', panel: null, closing: false, storageFailed: false };
        const common = { loader, theme, render, WebView: nativeWebView, evalFrame, openURL, closeRequested: () => shell('window.saquettoHome();') };
        const factory = ['clube-uol','bilhete-unico'].includes(id) ? SaquettoWebviewApps : id === 'media-downloader' ? SaquettoMediaApp : SaquettoNativeApps;
        session.panel = await factory.create(common).open(id);
        if (shutdownRequested) throw new Error('app_closed');
        if (session.panel.html && !session.instanceId) await render(session.panel.html);
        return { status: 'opened', appId: id, mode: 'integrated' };
      } catch (error) { if (active?.panel) await active.panel.dispose(); active = null; cancelEvaluations(); throw error; }
      finally { opening = false; finishOpening(); }
    }
    async function close(payload) {
      if (!active) return { status: 'closed' };
      if (payload && !matches(payload)) throw new Error('app_instance_changed');
      if (opening || active.closing) throw new Error('app_busy');
      const app = active; app.closing = true;
      cancelEvaluations();
      try { await app.panel?.dispose(); active = null; return { status: 'closed' }; }
      catch (_) { app.closing = false; throw new Error('app_close_failed'); }
    }
    async function dispatch(action, payload) {
      if (shutdownRequested) throw new Error('app_closed');
      if (!matches(payload)) throw new Error('app_instance_changed');
      if (action === 'appEvalResult') {
        const pending = evaluations.get(payload.id);
        if (!pending || pending.instanceId !== active.instanceId) throw new Error('evaluation_invalid');
        pending.timer.invalidate(); evaluations.delete(payload.id);
        payload.error ? pending.reject(new Error('evaluation_failed')) : pending.resolve(payload.value);
        return { status: 'received' };
      }
      if (action === 'closeApp') return close(payload);
      if (action !== 'appAction' || active.closing) throw new Error('action_invalid');
      const app = active, message = payload.request;
      if (!message || typeof message.action !== 'string' || !message.payload || typeof message.payload !== 'object' || Array.isArray(message.payload)) throw new Error('payload_invalid');
      if (message.action.startsWith('storage.')) {
        if (app.storageFailed) throw new Error('app_storage_unverified');
        const data = message.payload, next = { ...app.storage };
        if (message.action === 'storage.clear') Object.keys(next).forEach(key => delete next[key]);
        else {
          if (typeof data.key !== 'string' || data.key.length > 160 || ['__proto__','constructor','prototype'].includes(data.key)) throw new Error('storage_key_invalid');
          if (message.action === 'storage.remove') delete next[data.key];
          else if (message.action === 'storage.set' && typeof data.value === 'string') next[data.key] = data.value;
          else throw new Error('payload_invalid');
        }
        const text = JSON.stringify(next); if (text.length > 2 * 1024 * 1024) throw new Error('storage_full');
        try { local.writeString(app.storagePath, text); if (local.readString(app.storagePath) !== text) throw new Error('readback'); app.storage = next; }
        catch (_) { app.storageFailed = true; throw new Error('app_storage_unverified'); }
        return { status: 'saved' };
      }
      if (message.action === 'web.fetch') return proxyFetch(app, message.payload);
      return app.panel.handle(message.action, message.payload);
    }
    function shouldAllowRequest(req) {
      if (['about:blank','about:srcdoc'].includes(req.url)) return true;
      if (active?.panel?.shouldAllowRequest) return active.panel.shouldAllowRequest(req);
      return false;
    }
    async function shutdown() {
      shutdownRequested = true; cancelEvaluations();
      await openingDone;
      return close();
    }
    return { open, close, shutdown, dispatch, shouldAllowRequest,
      waitForNative: () => openingDone,
      get nativeActive() { return active?.mode === 'native'; },
      get activeId() { return active?.id || null; } };
  }
  return { create, createLoader, inject, nativeSource };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoRuntime;


const SaquettoHost = (() => {
  const key = 'saquettoos.identity.v1';
  const stateFile = 'saquettoos.settings.v1.json';
  const bootstrapFile = 'saquettoos-owner-bootstrap.json';
  const defaults = { appearance: 'system', material: 'glass', channel: 'stable', showAppNames: false,
    favorites: ['clube-uol', 'bilhete-unico', 'super-calendario', 'media-downloader'],
    notifications: { enabled: false, updates: true, access: true } };
  function preferences(value = {}) {
    return { appearance: ['system', 'light', 'dark'].includes(value.appearance) ? value.appearance : defaults.appearance,
      material: ['glass', 'solid'].includes(value.material) ? value.material : defaults.material,
      showAppNames: typeof value.showAppNames === 'boolean' ? value.showAppNames : defaults.showAppNames,
      channel: ['stable', 'beta'].includes(value.channel) ? value.channel : defaults.channel,
      favorites: Array.isArray(value.favorites) ? [...new Set(value.favorites.filter(id => SAQUETTO_CATALOG.some(app => app.id === id)))].slice(0, 4) : defaults.favorites.slice(),
      notifications: Object.fromEntries(['enabled', 'updates', 'access'].map(name => [name,
        typeof value.notifications?.[name] === 'boolean' ? value.notifications[name] : defaults.notifications[name]])) };
  }
  function identityValid(value) {
    return value && /^[a-f0-9-]{36}$/i.test(value.deviceId) && /^[a-f0-9]{64}$/.test(value.token);
  }
  function appFor(id) { return id === 'saquettoos' ? SAQUETTO_SELF : SAQUETTO_CATALOG.find(app => app.id === id); }
  function scriptFile(fm, app) {
    const matches = fm.listContents(fm.documentsDirectory()).filter(name =>
      name.normalize('NFC') === (app.scriptName + '.js').normalize('NFC'));
    if (matches.length > 1) throw new Error('script_name_conflict');
    return matches.length ? { name: matches[0].slice(0, -3), path: fm.joinPath(fm.documentsDirectory(), matches[0]) } : null;
  }
  function safeJSON(value) { return JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029'); }
  function create(deps) {
    const { fm, local, keychain, request, uuid, launch, notify, now = Date.now, scriptName = 'saquettoOS' } = deps;
    const disk = local.joinPath(local.documentsDirectory(), stateFile);
    let saved = {}, identity = null, update = { items: [] }, runtime = null;
    if (local.fileExists(disk)) {
      try { saved = JSON.parse(local.readString(disk)); } catch (_) { throw new Error('settings_unreadable'); }
    }
    let prefs = preferences(saved.preferences);
    let notifications = Array.isArray(saved.notifications) ? saved.notifications.slice(-50) : [];
    let lastAccess = saved.lastAccess || '', noticeKeys = Array.isArray(saved.noticeKeys) ? saved.noticeKeys.slice(-100) : [];
    let profile = { name: 'Seu espaço', role: 'member', status: 'not_registered', appIds: [] };
    const updater = SaquettoUpdater.create({ fm, local, request, uuid, scriptName });
    function persist() {
      const data = JSON.stringify({ preferences: prefs, notifications, lastAccess, noticeKeys });
      local.writeString(disk, data);
      if (local.readString(disk) !== data) throw new Error('settings_unverified');
    }
    function loadIdentity() {
      if (!keychain.contains(key)) return null;
      let value;
      try { value = JSON.parse(keychain.get(key)); } catch (_) { throw new Error('identity_unreadable'); }
      if (!identityValid(value)) throw new Error('identity_invalid');
      return value;
    }
    async function consumeBootstrap() {
      identity = loadIdentity();
      const file = fm.joinPath(fm.documentsDirectory(), bootstrapFile);
      if (!fm.fileExists(file)) return;
      if (fm.isFileDownloaded && !fm.isFileDownloaded(file)) await fm.downloadFileFromiCloud(file);
      const value = JSON.parse(fm.readString(file));
      if (!identityValid(value) || value.api !== SAQUETTO_API) throw new Error('bootstrap_invalid');
      if (identity && (identity.deviceId !== value.deviceId || identity.token !== value.token)) throw new Error('identity_conflict');
      keychain.set(key, JSON.stringify({ deviceId: value.deviceId, token: value.token }));
      identity = loadIdentity();
      if (!identity || identity.token !== value.token) throw new Error('identity_unverified');
      fm.remove(file); // This one-time credential has been verified in Keychain.
    }
    async function api(path, method = 'GET', body) {
      const req = request(SAQUETTO_API + path);
      req.method = method; req.timeoutInterval = 15; req.onRedirect = () => null;
      req.headers = { Accept: 'application/json', ...(identity ? { Authorization: 'Bearer ' + identity.token } : {}) };
      if (body !== undefined) { req.headers['Content-Type'] = 'application/json'; req.body = JSON.stringify(body); }
      let data;
      try { data = await req.loadJSON(); } catch (_) { throw new Error('access_unavailable'); }
      const status = req.response?.statusCode;
      if (status < 200 || status >= 300 || !Number.isInteger(status)) {
        const code = typeof data?.error === 'string' && /^[a-z_]{1,60}$/.test(data.error) ? data.error : 'access_unavailable';
        throw new Error(code);
      }
      return data;
    }
    async function addNotice(id, title, body, appId, category) {
      if (noticeKeys.includes(id)) return;
      const item = { id, title, body, appId: appFor(appId)?.id || '', date: new Date(now()).toISOString(), read: false };
      notifications = [...notifications, item].slice(-50); noticeKeys = [...noticeKeys, id].slice(-100);
      persist();
      if (prefs.notifications.enabled && prefs.notifications[category]) {
        try { await notify(item); } catch (_) { /* Inbox remains available if iOS denies delivery. */ }
      }
    }
    async function refreshProfile() {
      if (!identity) return profile;
      let next;
      try {
        next = (await api('/v1/me')).profile;
        if (!next || !['pending', 'approved', 'revoked', 'expired'].includes(next.status) ||
            !['admin', 'member'].includes(next.role)) throw new Error('access_invalid');
        next = { id: next.id, name: String(next.name || '').slice(0, 60), role: next.role, status: next.status,
          code: String(next.code || ''), appIds: Array.isArray(next.appIds) ? next.appIds.filter(id => appFor(id) && id !== 'saquettoos') : [] };
      } catch (_) { profile = { ...profile, role: 'member', status: 'unavailable', appIds: [] }; return profile; }
      profile = next;
      if (lastAccess && lastAccess !== next.status) {
        await addNotice('access-' + next.status + '-' + now(), 'Acesso ao saquettoOS',
          next.status === 'approved' ? 'Seu acesso foi aprovado. Abra para conferir seus apps.' : 'Seu acesso mudou. Consulte seu perfil.', '', 'access');
      }
      lastAccess = next.status; persist();
      return profile;
    }
    async function state(refresh = false) {
      if (refresh) await refreshProfile();
      const apps = [];
      for (const app of SAQUETTO_CATALOG) {
        let found = null, issue = '';
        try { found = scriptFile(fm, app); } catch (_) { issue = 'script_name_conflict'; }
        apps.push({ id: app.id, name: app.name, scriptName: app.scriptName, icon: app.icon, color: app.color,
          mode: app.mode, installed: !!found, issue, allowed: profile.status === 'approved' && profile.appIds.includes(app.id),
          version: update.items.find(item => item.id === app.id)?.version || null,
          updateAvailable: update.items.find(item => item.id === app.id)?.available || false });
      }
      return { version: SAQUETTO_VERSION, profile, apps, preferences: prefs, notifications,
        accessConfigured: true, assets: SAQUETTO_ASSETS, update };
    }
    async function authorize(app) {
      if (!app || app.id === 'saquettoos') throw new Error('app_invalid');
      if (!identity) throw new Error('approval_required');
      const result = await api('/v1/access/' + app.id);
      if (result.allowed !== true) throw new Error('approval_required');
    }
    async function openApp(id, standalone = false) {
      const app = appFor(id);
      await authorize(app);
      const file = scriptFile(fm, app);
      if (!file) throw new Error('app_not_installed');
      if (!standalone) {
        if (!runtime) throw new Error('runtime_unavailable');
        return runtime.open(id);
      }
      if (runtime?.activeId) await runtime.close();
      await launch('scriptable:///run?scriptName=' + encodeURIComponent(file.name));
      return { status: 'dispatched', appId: id, mode: 'external' };
    }
    async function checkUpdates() {
      const items = [];
      for (const app of [SAQUETTO_SELF, ...SAQUETTO_CATALOG]) {
        const result = await updater.check(app, prefs.channel);
        items.push({ id: app.id, name: app.name, status: result.status, version: result.version || null,
          latestVersion: result.latestVersion || result.manifest?.version || null, available: result.status === 'available' });
        if (result.status === 'available') await addNotice('update-' + app.id + '-' + result.manifest.version,
          'Atualização disponível', app.name + ' · ' + result.manifest.version, app.id, 'updates');
      }
      update = { items, checkedAt: new Date(now()).toISOString() };
      return update;
    }
    async function installUpdate(id) {
      const app = appFor(id);
      if (!app) throw new Error('app_invalid');
      if (runtime?.activeId) throw new Error('close_app_before_update');
      if (id !== 'saquettoos') await authorize(app);
      const result = await updater.install(app, prefs.channel);
      if (result.status === 'installed') {
        await addNotice('installed-' + id + '-' + result.version, 'Atualização concluída',
          app.name + ' foi verificado. Abra novamente para usar a versão ' + result.version + '.', id, 'updates');
      }
      // A dispatch, attempted write or ambiguous result is never called installed.
      return result;
    }
    async function dispatch(action, payload = {}) {
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('payload_invalid');
      if (action === 'getState') return { state: await state(true) };
      if (action === 'openApp' || action === 'launchStandalone') return openApp(payload.id || payload.appId, action === 'launchStandalone');
      if (['appAction', 'appEvalResult', 'closeApp'].includes(action)) {
        if (!runtime) throw new Error('runtime_unavailable');
        return runtime.dispatch(action, payload);
      }
      if (action === 'returnHome') return { state: await state(true) };
      if (action === 'setPreferences') {
        if (payload.channel !== undefined && !['stable', 'beta'].includes(payload.channel)) throw new Error('channel_invalid');
        prefs = preferences({ ...prefs, ...payload }); persist(); return { state: await state() };
      }
      if (action === 'setNotificationPreferences') {
        prefs = preferences({ ...prefs, notifications: { ...prefs.notifications, ...payload } }); persist();
        return { state: await state() };
      }
      if (action === 'testNotification') {
        if (!prefs.notifications.enabled) throw new Error('notifications_disabled');
        await notify({ id: 'test-' + uuid(), title: 'saquettoOS', body: 'Notificações ativadas neste aparelho.', appId: '' });
        return { status: 'scheduled' };
      }
      if (action === 'requestAccess') {
        const name = typeof payload.name === 'string' ? payload.name.trim() : '';
        if (!name || name.length > 60 || /[\u0000-\u001f\u007f]/.test(name)) throw new Error('name_invalid');
        if (!identity) {
          const next = { deviceId: uuid().toLowerCase(), token: (uuid() + uuid()).replace(/-/g, '').toLowerCase() };
          if (!identityValid(next)) throw new Error('identity_invalid');
          keychain.set(key, JSON.stringify(next)); identity = loadIdentity();
          if (!identity || identity.token !== next.token) throw new Error('identity_unverified');
        }
        await api('/v1/enroll', 'POST', { name, ...identity });
        return { state: await state(true) };
      }
      if (action === 'listUsers') return api('/v1/users');
      if (action === 'decideUser') {
        const result = await api('/v1/decision', 'POST', { id: payload.id, status: payload.status, appIds: payload.appIds, code: payload.code });
        return { ...result, ...(await api('/v1/users')) };
      }
      if (action === 'checkUpdates') return checkUpdates();
      if (action === 'installUpdate') return installUpdate(payload.id || payload.appId || 'saquettoos');
      throw new Error('action_invalid');
    }
    async function start() {
      await consumeBootstrap();
      const recoveries = [];
      for (const app of [SAQUETTO_SELF, SAQUETTO_CATALOG[0]]) recoveries.push({ id: app.id, ...(await updater.recover(app)) });
      const unresolved = recoveries.filter(item => ['recovery_required', 'concurrent_change'].includes(item.status));
      if (unresolved.length) update = { items: unresolved.map(item => ({ ...item, name: appFor(item.id).name })) };
      return state(true);
    }
    return { start, state, dispatch, preferences, updater, attachRuntime(value) { runtime = value; } };
  }
  return { create, preferences, safeJSON, scriptFile, identityValid };
})();

async function saquettoMain() {
  if (Script.name() !== 'saquettoOS') {
    const alert = new Alert(); alert.title = 'Nome do script';
    alert.message = 'Salve este script com o nome saquettoOS para usar sua instalação e suas atualizações.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  const local = FileManager.local();
  // module.filename is documented by Scriptable; a local run must not update an iCloud namesake.
  const filename = typeof module !== 'undefined' ? module.filename : '';
  const fm = filename && local.isFileStoredIniCloud(filename) ? FileManager.iCloud() : local;
  const parameters = typeof args.shortcutParameter === 'object' && args.shortcutParameter ? args.shortcutParameter : {};
  const query = args.queryParameters || {};
  const action = String(query.action || parameters.action || 'open');
  const allowedApp = SAQUETTO_CATALOG.find(app => app.id === (query.appId || parameters.appId || args.notification?.userInfo?.appId));
  if (config.runsInNotification) {
    await QuickLook.present('saquettoOS\n' + (allowedApp?.name || 'Sua central de apps') + '\nAbra a notificação para continuar.');
    return;
  }
  async function notify(item) {
    const notification = new Notification();
    notification.identifier = 'saquettoos.' + item.id;
    notification.threadIdentifier = 'saquettoos.' + (item.appId || 'system');
    notification.title = item.title; notification.body = item.body; notification.sound = null;
    notification.scriptName = 'saquettoOS'; notification.userInfo = { appId: item.appId || '' };
    notification.openURL = 'scriptable:///run?scriptName=saquettoOS' + (item.appId && item.appId !== 'saquettoos' ? '&appId=' + encodeURIComponent(item.appId) : '');
    notification.addAction('Abrir saquettoOS', notification.openURL);
    await notification.schedule();
  }
  let host;
  try {
    host = SaquettoHost.create({ fm, local, keychain: Keychain, request: url => new Request(url),
      uuid: () => UUID.string(), launch: url => Safari.open(url), notify, scriptName: Script.name() });
  } catch (_) {
    const alert = new Alert(); alert.title = 'Configuração indisponível';
    alert.message = 'Não foi possível ler os ajustes do saquettoOS. O arquivo foi preservado para recuperação.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  if (action === 'update' || action === 'update-auto') {
    await host.start();
    if (query.channel || parameters.channel) await host.dispatch('setPreferences', { channel: query.channel || parameters.channel });
    const result = await host.dispatch('installUpdate', { id: query.appId || parameters.appId || 'saquettoos' });
    Script.setShortcutOutput(JSON.stringify(result)); return;
  }
  const view = new WebView();
  let dismissed = false;
  let evaluationTail = Promise.resolve();
  let cancelEvaluation = null;
  const evaluate = code => {
    const next = evaluationTail.then(() => {
      if (dismissed) throw new Error('app_closed');
      return new Promise((resolve, reject) => {
        let settled = false;
        const finish = (error, value) => {
          if (settled) return;
          settled = true;
          if (cancelEvaluation === cancel) cancelEvaluation = null;
          if (error) reject(error); else resolve(value);
        };
        // Scriptable has no WebView evaluation cancellation API. Release only our wait
        // on dismissal; ignore late native results and refuse all queued evaluations.
        const cancel = () => finish(new Error('app_closed'));
        cancelEvaluation = cancel;
        try { Promise.resolve(view.evaluateJavaScript(code)).then(value => finish(null, value), error => finish(error)); }
        catch (error) { finish(error); }
      });
    });
    evaluationTail = next.catch(() => {});
    return next;
  };
  let cancelIdle = null;
  const pause = ms => dismissed ? Promise.resolve() : new Promise(resolve => {
    const timer = Timer.schedule(ms, false, () => { cancelIdle = null; resolve(); });
    cancelIdle = () => { timer.invalidate(); cancelIdle = null; resolve(); };
  });
  // Only fixed stage names and layout measurements; never identity, app data or errors containing URLs.
  const diagnostic = { version: SAQUETTO_VERSION, stages: [] };
  function trace(stage, layout) {
    diagnostic.stages.push({ stage, at: new Date().toISOString(), ...(layout ? { layout } : {}) });
    try { fm.writeString(fm.joinPath(fm.documentsDirectory(), 'saquettoos.startup.v1.json'), JSON.stringify(diagnostic)); } catch (_) {}
  }
  const failureHTML = () => '<!doctype html><html lang="pt-BR"><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>saquettoOS</title></head>' +
    '<body style="margin:0;background:#101225;color:#f6f7ff;font:17px -apple-system,sans-serif;padding:80px 28px"><h1 style="font-size:30px">saquettoOS</h1>' +
    '<p>A interface não terminou de carregar. Feche esta janela para ver o diagnóstico.' +
    '</p><p style="color:#afb7d0;font-size:13px">Versão ' + SAQUETTO_VERSION + '</p></body></html>';
  const baseURL = SAQUETTO_API + '/app/';
  let currentPreferences = (await host.state()).preferences;
  const runtime = SaquettoRuntime.create({ fm, local, catalog: SAQUETTO_CATALOG, compatibility: SAQUETTO_COMPATIBILITY,
    hash: SaquettoUpdater.fingerprint, script: Script, shell: evaluate,
    uuid: () => UUID.string(), timer: (ms, callback) => Timer.schedule(ms, false, callback),
    request: url => new Request(url), nativeWebView: WebView, openURL: url => Safari.open(url),
    theme: () => {
      const appearance = currentPreferences.appearance === 'system' ? (Device.isUsingDarkAppearance() ? 'dark' : 'light') : currentPreferences.appearance;
      return currentPreferences.material === 'glass' && appearance === 'dark' ? 'glass' : appearance;
    } });
  host.attachRuntime(runtime);
  const initial = await host.state();
  initial.profile = { ...initial.profile, status: 'loading' };
  trace('starting');
  try {
    // The first document already contains the home screen. No splash or second load.
    // Keep native presentation before any evaluateJavaScript call.
    await view.loadHTML(SAQUETTO_HTML.replace('__SAQUETTO_INITIAL_JSON__', () => SaquettoHost.safeJSON(initial)), baseURL);
    trace('startup_loaded');
  } catch (_) {
    trace('startup_load_failed');
    const alert = new Alert(); alert.title = 'Não foi possível abrir o saquettoOS';
    alert.message = 'A interface não terminou de carregar. Feche e abra o script novamente. Versão ' + SAQUETTO_VERSION + '.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  const presentation = view.present(true).finally(() => {
    dismissed = true;
    if (cancelIdle) cancelIdle();
    if (cancelEvaluation) cancelEvaluation();
    trace('dismissed');
  });
  trace('presentation_requested');
  const tasks = new Set();
  let initialized = false, startupFailure = false, synchronized = Promise.resolve();
  const ready = host.start().then(state => {
    initialized = true; return state;
  }).catch(() => {
    trace('access_initialization_failed');
    return { ...initial, profile: { name: 'Seu espaço', role: 'member', status: 'unavailable', appIds: [] } };
  });
  try {
    // present() resolves on dismissal, not on appearance. Give the native transition a turn.
    await pause(800);
    if (!dismissed) {
      view.shouldAllowRequest = req => runtime.shouldAllowRequest(req);
      if (!dismissed) {
        const layout = await evaluate(`(() => {
          const shell = document.getElementById('shell');
          const rect = shell && shell.getBoundingClientRect();
          return { ready: !!(window.__SAQUETTO_RENDERED && typeof window.saquettoNative === 'function' && Array.isArray(window.__SAQUETTO_QUEUE)),
            width: Math.round(rect ? rect.width : 0), height: Math.round(rect ? rect.height : 0) };
        })()`);
        if (!layout?.ready || layout.width <= 0 || layout.height <= 0) throw new Error('interface_unavailable');
        trace('dashboard_ready', { width: layout.width, height: layout.height });
        synchronized = ready.then(async state => {
          if (dismissed) return;
          currentPreferences = state.preferences;
          // Assets are already loaded. Only the small verified access/state patch crosses the bridge.
          const { assets, ...patch } = state;
          await evaluate('window.saquettoSetState(' + SaquettoHost.safeJSON(patch) + ');');
          if (!dismissed && initialized && allowedApp && action === 'open') {
            const opening = host.dispatch('openApp', { id: allowedApp.id }).catch(() => {});
            tasks.add(opening); opening.finally(() => tasks.delete(opening));
          }
        }).catch(() => { if (!dismissed) trace('access_state_failed'); });
        tasks.add(synchronized); synchronized.finally(() => tasks.delete(synchronized));
      }
    }
  } catch (_) {
    if (!dismissed) {
      startupFailure = true; trace('dashboard_failed');
      // No app has opened on this failure path. Permit only the host's trusted replacement.
      try {
        view.shouldAllowRequest = () => true;
        await view.loadHTML(failureHTML(), baseURL);
      } catch (_) {}
      view.shouldAllowRequest = req => runtime.shouldAllowRequest(req);
    }
  }
  async function process(item) {
    let result;
    try {
      await synchronized;
      if (!initialized) throw new Error('initialization_failed');
      if (dismissed) throw new Error('app_closed');
      const data = await host.dispatch(item.action, item.payload);
      if (data?.state?.preferences) currentPreferences = data.state.preferences;
      result = { ok: true, data };
    } catch (error) { result = { ok: false, error: /^[a-z_]{1,60}$/.test(error?.message || '') ? error.message : 'operation_failed' }; }
    if (!dismissed) {
      try { await evaluate('window.saquettoResolve(' + JSON.stringify(item.id) + ',' + SaquettoHost.safeJSON(result) + ');'); } catch (_) {}
    }
  }
  while (!dismissed && !startupFailure) {
    // The child owns native presentation/evaluation until its own UI closes.
    // Do not poll the covered central while a child WebView callback is pending.
    if (runtime.nativeActive) { await runtime.waitForNative(); continue; }
    let items;
    try {
      // Return the queue directly; release our wait if the native view closes mid-evaluation.
      items = await evaluate('window.__SAQUETTO_QUEUE.splice(0, 20)');
    } catch (_) { if (!dismissed) trace('bridge_failed'); break; }
    if (dismissed || !Array.isArray(items)) break;
    for (const item of items) {
      if (!item || typeof item.id !== 'string' || typeof item.action !== 'string') continue;
      // Callback results must drain while an app action is awaiting its WebView evaluation.
      const task = process(item); tasks.add(task); task.finally(() => tasks.delete(task));
    }
    if (!items.length) await pause(100);
  }
  await ready;
  try { await runtime.shutdown(); } catch (_) {}
  await Promise.allSettled([...tasks]);
  await presentation;
  trace('complete');
  if (startupFailure) {
    const alert = new Alert(); alert.title = 'Diagnóstico saquettoOS';
    alert.message = 'Versão ' + SAQUETTO_VERSION + ': dashboard_failed. O registro de abertura foi salvo na pasta Scriptable como saquettoos.startup.v1.json.';
    alert.addAction('OK'); await alert.presentAlert();
  }
}

try { await saquettoMain(); }
finally { Script.complete(); }
