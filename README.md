# Price Alert Bot

Bot de Telegram que rastreia preços em **Amazon**, **Mercado Livre** e **Shopee** e dispara alertas quando encontra bons descontos.

Projeto pessoal de estudo — foco em async, MongoDB, scraping e API do Telegram.

## Stack

- Python 3.10+
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) — bot do Telegram
- [pymongo](https://pymongo.readthedocs.io/) — MongoDB
- `requests` + `beautifulsoup4` — scraping Amazon / Mercado Livre
- API interna Shopee (`/api/v2/item/get`)
- `python-dotenv` — variáveis de ambiente

## Comandos do bot

- `/start` — mensagem inicial
- `/help` — lista de comandos
- `/add_product` — cadastra um produto pra monitorar (fluxo em etapas)
- `/list_products` — lista os produtos rastreados
- `/stats` — estatísticas gerais

## Setup

```bash
# 1. Clonar o repo
git clone <url-do-repo>
cd alerta_bot

# 2. Criar virtualenv
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Copiar .env.example para .env e preencher
cp .env.example .env
# Editar .env com token do Telegram, URL do MongoDB, etc.

# 5. Rodar
python bot.py
```

## Variáveis de ambiente

Ver [`.env.example`](.env.example). As principais:

| Variável | Obrigatória | Descrição |
|---|---|---|
| `TELEGRAM_TOKEN` | sim | Token do bot (via @BotFather) |
| `MONGO_URL` | sim | Connection string do MongoDB |
| `AMAZON_ASSOCIATE_TAG` | não | Tag de afiliado Amazon |
| `MERCADOLIVRE_CLIENT_ID` | não | Client ID da API do ML |
| `SHOPEE_PARTNER_ID` | não | Partner ID Shopee |

## Jobs agendados

- Verificação de preços: a cada 6h
- Envio de alertas diários: 08h / 12h / 18h

## Status / roadmap

- [x] Comandos básicos e cadastro de produtos
- [x] Scraping Amazon, ML e Shopee
- [x] Jobs agendados
- [ ] `/remove_product`
- [ ] `send_daily_alerts` enviando de fato (hoje só loga)
- [ ] Retry + rotação de User-Agent no scraping
- [ ] Testes E2E
- [ ] Integração WhatsApp (opcional)
