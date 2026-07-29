# Price Alert Bot

Bot de Telegram que **descobre sozinho** os produtos mais vendidos da Amazon BR e do Mercado Livre, monitora preços em tempo real e dispara alertas quando encontra desconto de verdade.

Projeto pessoal de estudo — foco em async, MongoDB, scraping e API do Telegram.

## Como funciona

1. **Descoberta automática (1x/dia):** varre bestsellers de várias categorias (Amazon e ML)
2. **Monitoramento contínuo:**
   - Mercado Livre a cada **15min** (API oficial)
   - Amazon a cada **1h** (scraping — mais devagar por causa do anti-bot)
3. **Detecção de desconto:** considera oferta quando o preço cai 15%+ contra:
   - Média dos últimos 30 dias
   - Preço "de/por" original
   - Mínimo histórico
4. **Alerta em tempo real (item por item):** assim que detecta desconto, manda mensagem com foto pra todos os usuários
5. **Dedupe anti-spam:** cooldown de 7 dias por produto (ou queda +5% pra realertar antes)
6. **Resumo diário 20h:** top 10 dos melhores descontos das últimas 24h

## Stack

- Python 3.10+
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) `[job-queue]` — bot + apscheduler
- [pymongo](https://pymongo.readthedocs.io/) — MongoDB (Atlas ou local)
- `requests` + `beautifulsoup4` — scraping Amazon
- API pública Mercado Livre (`/sites/MLB/search`)
- `python-dotenv` — variáveis de ambiente

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

Ver [`.env.example`](.env.example).

| Variável | Obrigatória | Descrição |
|---|---|---|
| `TELEGRAM_TOKEN` | sim | Token do bot (via @BotFather) |
| `MONGO_URL` | sim | Connection string do MongoDB (local ou Atlas) |
| `AMAZON_ASSOCIATE_TAG` | não | Tag de afiliado Amazon |
| `MERCADOLIVRE_AFFILIATE_TAG` | não | Tag de afiliado Mercado Livre |

## Deploy (VM + Atlas)

Setup validado em produção rodando 24/7:

- **VM:** Oracle Cloud Free Tier (`VM.Standard.E2.1.Micro`, Ubuntu 22.04, ~44MB RAM ocioso)
- **MongoDB:** [MongoDB Atlas Free (M0)](https://www.mongodb.com/cloud/atlas) — cluster shared, 512MB
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

## Status / roadmap

- [x] Descoberta automática de bestsellers (Amazon + ML)
- [x] Streaming item-por-item (não mais batch)
- [x] Dedupe de alertas (cooldown 7d ou queda +5%)
- [x] Resumo top 10 diário
- [x] `/remove_product`
- [x] Retry + rotação de User-Agent no scraping
- [x] Deploy 24/7 (Oracle Cloud + Atlas + systemd)
- [ ] **Mercado Livre via OAuth2** — API pública passou a exigir credenciais em 2026-07
- [ ] Programa de afiliados Mercado Livre
- [ ] Suporte a Shopee
- [ ] Testes E2E
- [ ] Integração WhatsApp (opcional)
