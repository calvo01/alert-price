# Price Alert Bot

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/platform-Telegram-26A5E4?logo=telegram)](https://core.telegram.org/bots)
[![MongoDB](https://img.shields.io/badge/database-MongoDB-47A248?logo=mongodb)](https://www.mongodb.com/)
[![Status](https://img.shields.io/badge/status-24%2F7%20em%20produção-success)]()

Bot de Telegram que **descobre sozinho** os produtos mais vendidos da Amazon BR e do Mercado Livre, monitora preços em tempo real e dispara alertas quando encontra desconto de verdade.

Rodando 24/7 em produção no canal [@achadosmlsp](https://t.me/achadosmlsp).

Projeto pessoal de estudo — foco em **async, MongoDB, scraping resiliente, filas com backpressure e integração de APIs (oficiais e não-tão-oficiais)**.

---

## O que faz

1. **Descoberta automática (1x/dia):** varre bestsellers de várias categorias em Amazon e Mercado Livre.
2. **Monitoramento contínuo:**
   - Mercado Livre a cada **15 min** (scraping HTML + cookie de sessão)
   - Amazon a cada **1 h** (scraping — mais devagar por causa do anti-bot)
3. **Detecção de desconto real** — considera oferta quando o preço cai 15%+ contra:
   - Média dos últimos 30 dias
   - Preço "de/por" original do anúncio
   - Mínimo histórico
4. **Alerta em cadência constante:** ao detectar desconto, entra na fila. Dispatcher manda **1 mensagem a cada 5 min** (com foto) das **09h às 23h BRT** — fora dessa janela a fila continua acumulando, mas nada é postado. Maior desconto pendente sai primeiro.
5. **Dedupe anti-spam:** cooldown de 8h por produto (48h + queda +10% pra realertar antes).
6. **Resumo diário 20h:** top 10 dos melhores descontos das últimas 24h.

---

## Arquitetura

```
┌──────────────────┐    ┌───────────────────┐    ┌─────────────────┐
│  Seeder (1×/dia) │───▶│  MongoDB Atlas    │◀───│  Checker loop   │
│  Amazon + ML     │    │  produtos + hist  │    │  ML: 15min      │
└──────────────────┘    └─────────┬─────────┘    │  Amazon: 1h     │
                                  │              └────────┬────────┘
                                  ▼                       │
                          ┌───────────────┐               │
                          │  Discount     │◀──────────────┘
                          │  detector     │  compara com média 30d
                          └───────┬───────┘
                                  │
                                  ▼
                          ┌───────────────┐    ┌─────────────────┐
                          │  Fila (Mongo) │───▶│  Dispatcher     │
                          │  ordenada por │    │  1 msg/5min     │
                          │  maior %OFF   │    │  09h–23h BRT    │
                          └───────────────┘    └────────┬────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────┐
                                              │  Canal Telegram │
                                              └─────────────────┘
```

**Componente separado:** [`ml_cookie_refresher/`](./ml_cookie_refresher/) — watcher local com Playwright que renova o cookie de sessão do painel de afiliado ML automaticamente quando expira (a cada ~30 dias).

---

## Stack

- **Python 3.10+**
- [`python-telegram-bot`](https://github.com/python-telegram-bot/python-telegram-bot) `[job-queue]` — bot + APScheduler
- [`pymongo`](https://pymongo.readthedocs.io/) — MongoDB (Atlas ou local)
- `requests` + `beautifulsoup4` — scraping Amazon/ML
- `python-dotenv` — variáveis de ambiente
- `playwright` (no refresher) — headless Chromium com perfil persistente

---

## Highlights técnicos

- **Fila com backpressure e quiet hours** — dispatcher desacopla detecção de envio; garante ritmo constante no canal (evita rajadas + silêncio).
- **Detecção de desconto multi-referência** — combina média histórica, preço "de/por" e mínimo histórico pra filtrar oferta inflada.
- **Scraping resiliente** — rotação de User-Agent, retry com backoff, headers Sec-Fetch pra mitigar bloqueio da Amazon (503).
- **Dedupe idempotente** — cooldown por produto + regra de re-alerta em queda adicional (evita spam sem perder ofertas relevantes).
- **Auto-refresh de sessão** — quando o cookie de afiliado ML expira, o bot cria uma flag remota; watcher local com Playwright detecta, renova e faz deploy do novo `.env` via SSH.
- **Notificação de admin em falha crítica** — cookie expirado, scraping quebrado etc. dispara mensagem privada no Telegram.

---

## Comandos do bot

**Uso normal**
- `/start` — mensagem inicial
- `/help` — como funciona
- `/list_products` — produtos rastreados
- `/stats` — estatísticas gerais

**Cadastro manual (opcional — o bot já descobre sozinho)**
- `/add_product` — cadastra produto pra monitorar (fluxo em 6 etapas)
- `/remove_product` — remove produto pelo nome
- `/cancel` — cancela conversa

**Admin (rodar jobs na hora, útil pra testar)**
- `/force_seed` — dispara descoberta de bestsellers agora
- `/force_check_ml` — checa produtos ML agora
- `/force_check_amazon` — checa produtos Amazon agora
- `/force_top10` — envia resumo top 10 agora
- `/queue` — quantos alertas estão na fila
- `/force_dispatch` — envia 1 alerta da fila agora (pula a espera)

---

## Setup local

```bash
git clone https://github.com/calvo01/alert-price.git
cd alert-price

python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# preencher .env — ver seção abaixo

python bot.py
```

## Variáveis de ambiente

Ver [`.env.example`](.env.example) pra lista completa. Mínimo pra rodar:

| Variável | Obrigatória | Descrição |
|---|---|---|
| `TELEGRAM_TOKEN` | sim | Token do bot (via [@BotFather](https://t.me/BotFather)) |
| `MONGO_URL` | sim | Connection string do MongoDB (local ou Atlas) |
| `CHANNEL_ID` | não | `@usuario` ou chat_id do canal. Se vazio, roda 1:1 pra quem deu `/start` |
| `AMAZON_ASSOCIATE_TAG` | não | Tag de afiliado Amazon |
| `MERCADOLIVRE_AFFILIATE_TAG` | não | Tag de afiliado Mercado Livre |
| `MERCADOLIVRE_COOKIE` | não | Cookie de sessão do painel de afiliado ML (pra gerar shortlinks meli.la) |
| `ADMIN_CHAT_ID` | não | Chat pra receber notificações privadas de falha |

---

## Deploy (VM + Atlas)

Setup validado em produção rodando 24/7:

- **VM:** Oracle Cloud Free Tier (`VM.Standard.E2.1.Micro`, Ubuntu 22.04, ~44 MB RAM ocioso)
- **MongoDB:** [MongoDB Atlas Free (M0)](https://www.mongodb.com/cloud/atlas) — cluster shared, 512 MB
- **Processo:** `systemd` unit com `Restart=always`

Exemplo de unit `/etc/systemd/system/price-alert-bot.service`:

```ini
[Unit]
Description=Price Alert Bot (Telegram)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/alerta_bot
EnvironmentFile=/home/ubuntu/alerta_bot/.env
ExecStart=/home/ubuntu/alerta_bot/venv/bin/python /home/ubuntu/alerta_bot/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Comandos úteis:

```bash
sudo systemctl enable --now price-alert-bot   # start + start-on-boot
sudo systemctl status price-alert-bot         # ver status
sudo journalctl -u price-alert-bot -f         # logs em tempo real
sudo systemctl restart price-alert-bot        # reiniciar (após atualizar código)
```

---

## Estrutura do repo

```
alerta_bot/
├── bot.py                    # bot principal (scheduling, scraping, dispatcher)
├── requirements.txt
├── .env.example
├── README.md
└── ml_cookie_refresher/      # watcher opcional pra renovar cookie ML
    ├── refresh_cookie.py     # Playwright — extrai cookie do painel de afiliado
    ├── watcher.py            # polling SSH, dispara refresh quando bot flagga
    ├── requirements.txt
    └── watcher.vbs           # launcher silencioso Windows (Task Scheduler)
```

---

## Roadmap

- [x] Descoberta automática de bestsellers (Amazon + ML)
- [x] Streaming item-por-item (não mais batch)
- [x] Dedupe de alertas + cooldown por produto
- [x] Resumo top 10 diário
- [x] Retry + rotação de User-Agent no scraping
- [x] Deploy 24/7 (Oracle Cloud + Atlas + systemd)
- [x] Fila com dispatcher espaçado + quiet hours
- [x] Auto-refresh do cookie ML via Playwright
- [ ] Suporte a Shopee (aguardando cadastro na Open API)
- [ ] LLM classificador (Claude/GPT) pra distinguir oferta genuína de "de/por" inflado
- [ ] Testes E2E (pytest)
- [ ] CI/CD via GitHub Actions
- [ ] Dockerfile + compose
- [ ] Comando `/preferencias` — usuário escolhe categorias favoritas

---

## Licença

Uso pessoal / estudo. Sinta-se à vontade pra abrir issue ou PR.
