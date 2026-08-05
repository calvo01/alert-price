"""
PRICE ALERT BOT
Descobre produtos mais vendidos da Amazon BR, monitora preços
e envia alertas quando detecta desconto.
"""

import os
import random
import logging
import statistics
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson import ObjectId
import requests
from bs4 import BeautifulSoup
import asyncio
from typing import Optional, Dict, List, Tuple

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/price_alert_bot")
AWS_ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "")
# CHANNEL_ID pode ser @username (ex: '@achadosmlsp') ou chat_id numerico ('-1001234567890').
# Se setado, o bot posta APENAS no canal (ignora users individuais). Se vazio, itera users que deram /start.
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip() or None

# Config do detector de desconto
DISCOUNT_THRESHOLD = 0.85       # preço atual < 85% da referência = desconto (queda de 15%+)
MIN_HISTORY_POINTS = 5          # mínimo de leituras pra usar média histórica
BESTSELLERS_LIMIT = 20          # top N por categoria da Amazon
SCRAPE_DELAY_RANGE = (2, 4)     # delay aleatório entre requests (segundos)

# Cadência do canal: dispatcher manda 1 alerta enfileirado a cada X segundos.
# Evita rajadas (24 msgs em 2min → silêncio o dia inteiro) e mantém canal ativo.
ALERT_INTERVAL_SECONDS = int(os.getenv('ALERT_INTERVAL_SECONDS', '300'))
# TTL de alerta na fila — se ficou mais que isso pendente, descarta (oferta velha).
# Precisa cobrir a janela silenciosa (10h) + tempo de esvaziar fila (~4h) com folga.
ALERT_QUEUE_TTL_HOURS = int(os.getenv('ALERT_QUEUE_TTL_HOURS', '24'))
# Quiet hours (BRT, UTC-3): fora dessa janela dispatcher segura alertas na fila.
# Default: silencia 23h-09h BRT (canal só posta 09h-23h). Aceita janela cruzando meia-noite.
QUIET_HOURS_START_BRT = int(os.getenv('QUIET_HOURS_START_BRT', '23'))
QUIET_HOURS_END_BRT = int(os.getenv('QUIET_HOURS_END_BRT', '9'))
BRT_TZ = timezone(timedelta(hours=-3))


def _is_quiet_hours() -> bool:
    """True quando estamos dentro da janela silenciosa (BRT). Dispatcher pausa envios,
    mas o check continua enfileirando normalmente pra reserva."""
    hour = datetime.now(BRT_TZ).hour
    if QUIET_HOURS_START_BRT == QUIET_HOURS_END_BRT:
        return False  # desativado
    if QUIET_HOURS_START_BRT < QUIET_HOURS_END_BRT:
        # janela dentro do mesmo dia (ex: 1h-6h)
        return QUIET_HOURS_START_BRT <= hour < QUIET_HOURS_END_BRT
    # janela cruzando meia-noite (default 23h-9h): silencia se hora >= start OU hora < end
    return hour >= QUIET_HOURS_START_BRT or hour < QUIET_HOURS_END_BRT

# Amazon: URLs de bestsellers por categoria (mais variedade que a home)
AMAZON_BESTSELLER_URLS = [
    "https://www.amazon.com.br/gp/bestsellers/electronics/",
    "https://www.amazon.com.br/gp/bestsellers/computers/",
    "https://www.amazon.com.br/gp/bestsellers/home/",
    "https://www.amazon.com.br/gp/bestsellers/beauty/",
    "https://www.amazon.com.br/gp/bestsellers/sports/",
]

MERCADOLIVRE_AFFILIATE_TAG = os.getenv("MERCADOLIVRE_AFFILIATE_TAG", "")
MERCADOLIVRE_CLIENT_ID = os.getenv("MERCADOLIVRE_CLIENT_ID", "")
MERCADOLIVRE_CLIENT_SECRET = os.getenv("MERCADOLIVRE_CLIENT_SECRET", "")
# Cookie de sessão do painel de afiliado (renovar quando expirar — normalmente ~30 dias)
MERCADOLIVRE_COOKIE = os.getenv("MERCADOLIVRE_COOKIE", "")
# Chat_id do admin — pra receber alertas do bot em privado (ex: cookie ML expirado)
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip() or None

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:132.0) Gecko/20100101 Firefox/132.0",
]

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Estados de conversas
ADD_PRODUCT_NAME, ADD_PRODUCT_CATEGORY, ADD_PRODUCT_AMAZON, ADD_PRODUCT_ML, ADD_PRODUCT_SHOPEE, ADD_PRODUCT_DISCOUNT, REMOVE_PRODUCT_NAME = range(7)


# ==================== DATABASE ====================
class Database:
    def __init__(self, url):
        try:
            self.client = MongoClient(url, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client['price_alert_bot']
            logger.info("✅ Conectado ao MongoDB")
        except ServerSelectionTimeoutError:
            logger.warning("⚠️ MongoDB offline")
            self.db = None

    # ---------- Produtos ----------
    def upsert_product(self, product: dict) -> Optional[ObjectId]:
        """Insere produto novo ou atualiza last_seen se já existe (usa URL como chave)."""
        if self.db is None:
            return None
        try:
            now = datetime.now()
            set_fields = {
                'name': product['name'],
                'marketplace': product['marketplace'],
                'category': product.get('category'),
                'last_seen': now,
                'active': True,
            }
            # Campos opcionais que só entram se vieram
            if product.get('ml_item_id'):
                set_fields['ml_item_id'] = product['ml_item_id']
            if product.get('image_url'):
                set_fields['image_url'] = product['image_url']

            result = self.db.products.update_one(
                {'url': product['url']},
                {
                    '$set': set_fields,
                    '$setOnInsert': {
                        'url': product['url'],
                        'source': product.get('source', 'bestseller'),
                        'first_seen': now,
                    }
                },
                upsert=True
            )
            if result.upserted_id:
                return result.upserted_id
            doc = self.db.products.find_one({'url': product['url']}, {'_id': 1})
            return doc['_id'] if doc else None
        except Exception as e:
            logger.error(f"Erro em upsert_product: {e}")
            return None

    def get_active_products(self) -> List[dict]:
        if self.db is None:
            return []
        try:
            return list(self.db.products.find({'active': True}))
        except Exception as e:
            logger.error(f"Erro get_active_products: {e}")
            return []

    def get_products(self) -> List[dict]:
        """Alias mantido pra compatibilidade com /list_products e /stats."""
        return self.get_active_products()

    def delete_product_by_name(self, name: str) -> bool:
        if self.db is None:
            return False
        try:
            result = self.db.products.delete_one({
                'name': {'$regex': f'^{name}$', '$options': 'i'}
            })
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Erro delete_product_by_name: {e}")
            return False

    def add_product(self, product: dict) -> bool:
        """Insere produto manual (via /add_product) — mantém compatibilidade."""
        if self.db is None:
            return False
        try:
            self.db.products.insert_one({
                **product,
                'source': 'manual',
                'active': True,
                'first_seen': datetime.now(),
                'last_seen': datetime.now(),
            })
            return True
        except Exception as e:
            logger.error(f"Erro add_product: {e}")
            return False

    # ---------- Histórico de preços ----------
    def add_price_history(self, product_id: ObjectId, marketplace: str, price: float, list_price: Optional[float] = None, image_url: Optional[str] = None):
        if self.db is None:
            return
        try:
            self.db.price_history.insert_one({
                'product_id': product_id,
                'marketplace': marketplace,
                'price': price,
                'list_price': list_price,
                'checked_at': datetime.now(),
            })
            update = {'current_price': price, 'last_checked': datetime.now()}
            if list_price:
                update['list_price'] = list_price
            if image_url:
                update['image_url'] = image_url
            self.db.products.update_one({'_id': product_id}, {'$set': update})
        except Exception as e:
            logger.error(f"Erro add_price_history: {e}")

    def get_price_history(self, product_id: ObjectId, days: int = 30) -> List[float]:
        if self.db is None:
            return []
        try:
            cutoff = datetime.now() - timedelta(days=days)
            docs = self.db.price_history.find(
                {'product_id': product_id, 'checked_at': {'$gte': cutoff}},
                {'price': 1}
            )
            return [d['price'] for d in docs if d.get('price')]
        except Exception as e:
            logger.error(f"Erro get_price_history: {e}")
            return []

    # ---------- Usuários ----------
    def add_user(self, user_id: int, username: str = None):
        if self.db is None:
            return
        try:
            self.db.users.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'username': username,
                        'last_active': datetime.now(),
                        'is_active': True
                    },
                    '$setOnInsert': {'joined_at': datetime.now()}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Erro add_user: {e}")

    def get_users_count(self) -> int:
        if self.db is None:
            return 0
        try:
            return self.db.users.count_documents({'is_active': True})
        except Exception:
            return 0

    def get_all_active_user_ids(self) -> List[int]:
        if self.db is None:
            return []
        try:
            return [u['user_id'] for u in self.db.users.find({'is_active': True}, {'user_id': 1})]
        except Exception as e:
            logger.error(f"Erro get_all_active_user_ids: {e}")
            return []

    def deactivate_user(self, user_id: int):
        if self.db is None:
            return
        try:
            self.db.users.update_one({'user_id': user_id}, {'$set': {'is_active': False}})
        except Exception as e:
            logger.error(f"Erro deactivate_user: {e}")

    # ---------- Dedupe de alertas ----------
    def get_products_by_marketplace(self, marketplace: str) -> List[dict]:
        if self.db is None:
            return []
        try:
            return list(self.db.products.find({'active': True, 'marketplace': marketplace}))
        except Exception as e:
            logger.error(f"Erro get_products_by_marketplace: {e}")
            return []

    def should_alert(self, product: dict, current_price: float,
                     min_drop_pct: float = 0.10, days_cooldown: int = 1,
                     min_hours_between: int = 8) -> bool:
        """True se pode enviar alerta novo. Le last_alerted_at FRESCO do DB pra evitar race
        condition entre execucoes paralelas (ex: dois check_amazon_task simultaneos).

        Regras:
        - nunca alertou → alerta
        - dentro de min_hours_between (8h) desde ultimo alerta → NUNCA realerta
        - passou days_cooldown (1 dia) → pode realertar mesmo com mesmo preco
        - entre 8h e 1 dia → so realerta se preco caiu +10% desde ultimo alerta
        """
        if self.db is None:
            return True
        try:
            doc = self.db.products.find_one(
                {'_id': product['_id']},
                {'last_alerted_at': 1, 'last_alerted_price': 1}
            ) or {}
        except Exception as e:
            logger.error(f"Erro should_alert (fresh read): {e}")
            doc = product  # fallback pro dict cached
        last_at = doc.get('last_alerted_at')
        if not last_at:
            return True
        hours_since = (datetime.now() - last_at).total_seconds() / 3600
        if hours_since < min_hours_between:
            return False
        if hours_since > days_cooldown * 24:
            return True
        last_price = doc.get('last_alerted_price')
        if last_price and current_price < last_price * (1 - min_drop_pct):
            return True
        return False

    def mark_alerted(self, product_id: ObjectId, price: float, percent_off: int, reason: str):
        if self.db is None:
            return
        try:
            self.db.products.update_one(
                {'_id': product_id},
                {'$set': {
                    'last_alerted_at': datetime.now(),
                    'last_alerted_price': price,
                    'last_alerted_percent_off': percent_off,
                    'last_alerted_reason': reason,
                }}
            )
        except Exception as e:
            logger.error(f"Erro mark_alerted: {e}")

    def get_recently_alerted(self, hours: int = 24) -> List[dict]:
        if self.db is None:
            return []
        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            return list(self.db.products.find({'last_alerted_at': {'$gte': cutoff}}))
        except Exception as e:
            logger.error(f"Erro get_recently_alerted: {e}")
            return []

    def log_click(self, user_id: int, product_id, marketplace: str):
        if self.db is None:
            return
        try:
            self.db.clicks.insert_one({
                'user_id': user_id,
                'product_id': product_id,
                'marketplace': marketplace,
                'clicked_at': datetime.now()
            })
        except Exception as e:
            logger.error(f"Erro log_click: {e}")

    # ---------- Fila de alertas (dispatcher espaçado) ----------
    def enqueue_alert(self, product_id: ObjectId, data: dict, percent_off: int, reason: str) -> bool:
        """Marca produto como pending_alert com o snapshot do desconto. Se ja tem pending,
        atualiza somente se o desconto novo for maior (evita perder ofertas melhores)."""
        if self.db is None:
            return False
        try:
            existing = self.db.products.find_one(
                {'_id': product_id, 'pending_alert': True},
                {'pending_percent_off': 1}
            )
            if existing and (existing.get('pending_percent_off') or 0) >= percent_off:
                return False
            self.db.products.update_one(
                {'_id': product_id},
                {'$set': {
                    'pending_alert': True,
                    'pending_price': data['price'],
                    'pending_list_price': data.get('list_price'),
                    'pending_image_url': data.get('image_url'),
                    'pending_marketplace': data.get('marketplace'),
                    'pending_percent_off': percent_off,
                    'pending_reason': reason,
                    'pending_since': datetime.now(),
                }}
            )
            return True
        except Exception as e:
            logger.error(f"Erro enqueue_alert: {e}")
            return False

    def pop_next_pending_alert(self, max_age_hours: int = 12) -> Optional[dict]:
        """Retorna proximo alerta da fila (maior desconto primeiro) e desmarca pending.
        Descarta silenciosamente alertas mais velhos que max_age_hours."""
        if self.db is None:
            return None
        try:
            cutoff = datetime.now() - timedelta(hours=max_age_hours)
            expired = self.db.products.update_many(
                {'pending_alert': True, 'pending_since': {'$lt': cutoff}},
                {'$set': {'pending_alert': False}}
            )
            if expired.modified_count:
                logger.info(f"🗑️ Descartados {expired.modified_count} alertas velhos (>{max_age_hours}h) da fila")
            return self.db.products.find_one_and_update(
                {'pending_alert': True},
                {'$set': {'pending_alert': False}},
                sort=[('pending_percent_off', -1)],
            )
        except Exception as e:
            logger.error(f"Erro pop_next_pending_alert: {e}")
            return None

    def get_pending_count(self) -> int:
        if self.db is None:
            return 0
        try:
            return self.db.products.count_documents({'pending_alert': True})
        except Exception:
            return 0

    def has_pending_alert(self, product_id: ObjectId) -> bool:
        if self.db is None:
            return False
        try:
            return self.db.products.count_documents(
                {'_id': product_id, 'pending_alert': True}, limit=1
            ) > 0
        except Exception:
            return False

    # ---------- Cache shortlink ML afiliado ----------
    def get_ml_shortlink(self, origin_url: str) -> Optional[str]:
        if self.db is None:
            return None
        try:
            doc = self.db.affiliate_shortlinks.find_one({'origin_url': origin_url})
            return doc['short_url'] if doc else None
        except Exception:
            return None

    def save_ml_shortlink(self, origin_url: str, short_url: str, tag: str):
        if self.db is None:
            return
        try:
            self.db.affiliate_shortlinks.update_one(
                {'origin_url': origin_url},
                {'$set': {'short_url': short_url, 'tag': tag, 'created_at': datetime.now()}},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Erro save_ml_shortlink: {e}")


# ==================== SCRAPING ====================
def _random_headers(url: str = "") -> dict:
    """Headers rotativos que imitam browser real. Adiciona Referer contextual quando aplicavel."""
    h = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
    }
    if 'amazon.com.br' in url:
        h['Referer'] = 'https://www.amazon.com.br/'
    elif 'mercadolivre' in url or 'mercadolibre' in url:
        h['Referer'] = 'https://www.mercadolivre.com.br/'
    return h


_ml_token_cache = {'token': None, 'expires_at': None}


def _get_ml_token() -> Optional[str]:
    """Devolve access_token OAuth2 do ML, renovando quando faltar <10min pro vencimento."""
    if not MERCADOLIVRE_CLIENT_ID or not MERCADOLIVRE_CLIENT_SECRET:
        logger.warning("⚠️ MERCADOLIVRE_CLIENT_ID/SECRET não configurados — pulando ML")
        return None

    now = datetime.now()
    cached = _ml_token_cache['token']
    expires_at = _ml_token_cache['expires_at']
    if cached and expires_at and (expires_at - now).total_seconds() > 600:
        return cached

    try:
        resp = requests.post(
            'https://api.mercadolibre.com/oauth/token',
            data={
                'grant_type': 'client_credentials',
                'client_id': MERCADOLIVRE_CLIENT_ID,
                'client_secret': MERCADOLIVRE_CLIENT_SECRET,
            },
            timeout=15,
        )
    except requests.RequestException as e:
        logger.error(f"❌ Erro ao pegar token ML: {e}")
        return None

    if resp.status_code != 200:
        logger.error(f"❌ Token ML falhou: HTTP {resp.status_code} — {resp.text[:200]}")
        return None

    data = resp.json()
    token = data.get('access_token')
    expires_in = data.get('expires_in', 21600)
    if not token:
        return None

    _ml_token_cache['token'] = token
    _ml_token_cache['expires_at'] = now + timedelta(seconds=expires_in)
    logger.info(f"🔑 Token ML renovado (expira em {expires_in}s)")
    return token


def _http_get_ml(url: str, tries: int = 3, timeout: int = 15) -> Optional[requests.Response]:
    """GET autenticado na API do ML. Renova token em caso de 401."""
    token = _get_ml_token()
    if not token:
        return None

    for attempt in range(tries):
        try:
            resp = requests.get(
                url,
                headers={'Authorization': f'Bearer {token}'},
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.warning(f"⚠️ Erro request ML {url}: {e} (tentativa {attempt + 1}/{tries})")
            asyncio_safe_sleep(2 ** attempt + random.random())
            continue

        if resp.status_code == 200:
            return resp
        if resp.status_code == 401 and attempt == 0:
            logger.info("🔄 Token ML expirado (401) — renovando e tentando de novo")
            _ml_token_cache['token'] = None
            token = _get_ml_token()
            if not token:
                return None
            continue
        logger.warning(f"⚠️ HTTP {resp.status_code} em {url} (tentativa {attempt + 1}/{tries})")
        asyncio_safe_sleep(2 ** attempt + random.random())

    logger.warning(f"❌ _http_get_ml desistiu de {url} após {tries} tentativas")
    return None


def _http_get(url: str, tries: int = 3, timeout: int = 15) -> Optional[requests.Response]:
    """GET com retry, headers rotativos e delay. Loga status HTTP quando falha."""
    last_status = None
    last_error = None
    for attempt in range(tries):
        try:
            resp = requests.get(url, headers=_random_headers(url), timeout=timeout)
            if resp.status_code == 200:
                return resp
            last_status = resp.status_code
            logger.warning(f"⚠️ HTTP {resp.status_code} em {url} (tentativa {attempt + 1}/{tries})")
        except requests.RequestException as e:
            last_error = str(e)
            logger.warning(f"⚠️ Erro request em {url}: {e} (tentativa {attempt + 1}/{tries})")
        asyncio_safe_sleep(2 ** attempt + random.random())
    reason = f"status {last_status}" if last_status else f"erro {last_error}"
    logger.warning(f"❌ _http_get desistiu de {url} após {tries} tentativas ({reason})")
    return None


def asyncio_safe_sleep(seconds: float):
    """Sleep síncrono usado em código chamado de async — usar time.sleep normal."""
    import time
    time.sleep(seconds)


import re as _re

_AMAZON_ASIN_RE = _re.compile(r'/(?:dp|gp/product)/([A-Z0-9]{10})')


def _normalize_amazon_url(href: str) -> Optional[str]:
    """Extrai o ASIN e reconstroi URL canonica: https://www.amazon.com.br/dp/ASIN.
    Evita duplicatas quando a Amazon devolve variantes (/ref=..., /gp/product/, com categoria no path)."""
    if not href:
        return None
    m = _AMAZON_ASIN_RE.search(href)
    if not m:
        return None
    return f"https://www.amazon.com.br/dp/{m.group(1)}"


def _parse_price(text: str) -> Optional[float]:
    """Extrai float de string tipo 'R$ 1.299,90' ou '1.299,90'."""
    if not text:
        return None
    cleaned = text.replace('R$', '').replace('\xa0', '').strip()
    # Remove todos os pontos (separador de milhar), troca vírgula por ponto
    cleaned = cleaned.replace('.', '').replace(',', '.')
    # Se sobrou algum caractere estranho, pega só dígitos e ponto
    keep = ''
    for c in cleaned:
        if c.isdigit() or c == '.':
            keep += c
    try:
        return float(keep) if keep else None
    except ValueError:
        return None


class BestSellersScraper:
    """Puxa lista de produtos populares da Amazon BR e do Mercado Livre."""

    def get_amazon_bestsellers(self, limit: int = BESTSELLERS_LIMIT) -> List[dict]:
        """Percorre categorias e junta os top produtos da Amazon."""
        all_products = []
        for category_url in AMAZON_BESTSELLER_URLS:
            category = category_url.rstrip('/').split('/')[-1]
            logger.info(f"🔎 Buscando bestsellers Amazon: {category}")
            products = self._scrape_amazon_category(category_url, category, limit)
            all_products.extend(products)
            import time
            time.sleep(random.uniform(*SCRAPE_DELAY_RANGE))
        logger.info(f"📦 Total Amazon: {len(all_products)}")
        return all_products

    def get_ml_bestsellers(self, limit: int = BESTSELLERS_LIMIT * 4, pages: int = 3) -> List[dict]:
        """Raspa /ofertas do site do ML (API pública foi bloqueada por PolicyAgent em 2025).
        Percorre `pages` páginas pra ampliar o universo (~3x mais produtos)."""
        all_products = []
        seen_urls = set()
        for page in range(1, pages + 1):
            products = self._scrape_ml_ofertas(limit=limit, page=page)
            new = 0
            for p in products:
                if p['url'] in seen_urls:
                    continue
                seen_urls.add(p['url'])
                all_products.append(p)
                new += 1
            logger.info(f"🟡 ML /ofertas página {page}: {len(products)} cards, {new} novos (total {len(all_products)})")
            if not products:
                break
            import time
            time.sleep(random.uniform(*SCRAPE_DELAY_RANGE))
        return all_products

    def _scrape_ml_ofertas(self, limit: int, page: int = 1) -> List[dict]:
        url = "https://www.mercadolivre.com.br/ofertas" if page == 1 else f"https://www.mercadolivre.com.br/ofertas?page={page}"
        resp = _http_get(url)
        if not resp:
            logger.warning(f"❌ Falha ao buscar {url}")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        cards = soup.select('div.poly-card')
        products = []

        for card in cards[:limit]:
            title_el = card.select_one('a.poly-component__title')
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            href = title_el.get('href', '')
            if not name or not href:
                continue

            item_id = None
            m = _re.search(r'MLB-?(\d+)', href)
            if m:
                item_id = f"MLB{m.group(1)}"

            price_el = card.select_one('.poly-price__current .andes-money-amount__fraction')
            price = _parse_price(price_el.get_text(strip=True)) if price_el else None

            list_price_el = card.select_one('s.andes-money-amount--previous .andes-money-amount__fraction')
            list_price = _parse_price(list_price_el.get_text(strip=True)) if list_price_el else None

            img_el = card.select_one('img.poly-component__picture')
            image_url = img_el.get('src') if img_el else None

            products.append({
                'name': name[:200],
                'url': href.split('?')[0],
                'image_url': image_url,
                'marketplace': 'mercadolivre',
                'category': 'ofertas',
                'source': 'bestseller',
                'ml_item_id': item_id,
                'initial_price': price,
                'initial_list_price': list_price,
            })

        return products

    def _scrape_amazon_category(self, url: str, category: str, limit: int) -> List[dict]:
        resp = _http_get(url)
        if not resp:
            logger.warning(f"❌ Falha ao buscar {url}")
            return []

        html_text = resp.text
        # Detecta bloqueio anti-bot
        if 'captcha' in html_text.lower() or 'api-services-support' in html_text.lower():
            logger.warning(f"🤖 Amazon retornou CAPTCHA/bloqueio em {category} (HTML {len(html_text)} bytes)")
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        products = []

        # Cards de bestseller — Amazon usa várias estruturas, tento múltiplos seletores
        cards = soup.select('div[id^="p13n-asin-index-"]')
        selector_used = 'p13n-asin-index'
        if not cards:
            cards = soup.select('.zg-grid-general-faceout')
            selector_used = 'zg-grid-general-faceout'
        if not cards:
            cards = soup.select('[data-testid="zg-card-body"]')
            selector_used = 'zg-card-body'
        if not cards:
            cards = soup.select('div.p13n-sc-uncoverable-faceout')
            selector_used = 'p13n-sc-uncoverable-faceout'

        for card in cards[:limit]:
            # Link — qualquer <a> com href de produto (dp ou gp/product)
            link_el = (
                card.select_one('a[href*="/dp/"]') or
                card.select_one('a[href*="/gp/product/"]')
            )
            if not link_el:
                continue

            href = link_el.get('href', '')
            product_url = _normalize_amazon_url(href)
            if not product_url:
                continue

            # Nome — tenta em cascata: aria-label, alt de img, seletores de texto
            name = link_el.get('aria-label') or link_el.get('title')
            if not name:
                for sel in [
                    'div[data-testid="zg-card-title"]',
                    '._cDEzb_p13n-sc-css-line-clamp-3_g3dy1',
                    '._cDEzb_p13n-sc-css-line-clamp-4_2q2cc',
                    '.p13n-sc-truncate-desktop-type2',
                    '.p13n-sc-truncate',
                    'div.a-row.a-size-small',
                ]:
                    el = card.select_one(sel)
                    if el:
                        name = el.get_text(strip=True)
                        if name:
                            break
            img_el = card.select_one('img')
            # Último recurso: alt do <img>
            if not name and img_el:
                name = img_el.get('alt', '').strip()

            if not name or len(name) < 5:
                continue

            image_url = img_el.get('src') if img_el else None

            products.append({
                'name': name[:200],
                'url': product_url,
                'image_url': image_url,
                'marketplace': 'amazon',
                'category': category,
                'source': 'bestseller',
            })

        logger.info(f"📄 {category}: HTML {len(html_text)} bytes, {len(cards)} cards ({selector_used}), {len(products)} extraídos")
        return products


class PriceScraper:
    """Busca preço atual de um produto por URL/ID."""

    def get_price(self, product: dict) -> Optional[Dict]:
        """Roteia pro scraper certo baseado no marketplace."""
        marketplace = product.get('marketplace')
        if marketplace == 'amazon':
            return self.get_amazon_price(product['url'])
        if marketplace == 'mercadolivre':
            return self.get_ml_price(product)
        return None

    def get_ml_price(self, product: dict) -> Optional[Dict]:
        """Scrapa a página do produto ML pra pegar preço atualizado. Requer MERCADOLIVRE_COOKIE
        (sem cookie o ML redireciona pra verificação anti-bot).

        Retorna dict com 'blocked_by_verification' quando ML redireciona pra
        /gz/account-verification — o caller usa isso pra abrir circuit breaker."""
        url = product.get('url')
        if not url or not MERCADOLIVRE_COOKIE:
            return None

        try:
            # UA fixo (mesmo do cookie) — rotacionar UA com cookie fixo é padrao
            # bot obvio pro antifraude do ML e faz a conta ser flagada.
            resp = requests.get(
                url,
                headers={
                    'User-Agent': ML_SESSION_UA,
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'pt-BR,pt;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Cookie': MERCADOLIVRE_COOKIE,
                    'Referer': 'https://www.mercadolivre.com.br/ofertas',
                },
                timeout=15,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            logger.warning(f"⚠️ Erro get_ml_price {url[:60]}: {e}")
            return None

        if 'account-verification' in resp.url:
            logger.warning(f"⚠️ ML exigindo verificacao ({url[:60]}) — sinaliza circuit breaker")
            return {'blocked_by_verification': True}
        if resp.status_code != 200:
            logger.warning(f"⚠️ ML bloqueou {url[:60]} (status {resp.status_code}, final={resp.url[:60]})")
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # meta itemprop=price vem em formato inglês (ex: "1279.90"), não usar _parse_price
        meta_price = soup.select_one('meta[itemprop="price"]')
        price = None
        if meta_price:
            try:
                price = float(meta_price.get('content'))
            except (ValueError, TypeError):
                pass

        prev_el = soup.select_one('s.andes-money-amount--previous .andes-money-amount__fraction')
        list_price = _parse_price(prev_el.get_text(strip=True)) if prev_el else None

        og_image = soup.select_one('meta[property="og:image"]')
        image_url = og_image.get('content') if og_image else None

        if not price:
            return None

        return {
            'price': float(price),
            'list_price': float(list_price) if list_price else None,
            'image_url': image_url,
            'marketplace': 'mercadolivre',
        }

    def get_amazon_price(self, url: str) -> Optional[Dict]:
        """Retorna preço atual + preço 'de/por' (list_price) da Amazon."""
        resp = _http_get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # Isola o container do preco PRINCIPAL (evita widgets tipo "preco melhor
        # encontrado", "cashback", "1a compra", que sao promos condicionais e
        # nao refletem o preco de checkout de verdade).
        price_root = (
            soup.select_one('#corePriceDisplay_desktop_feature_div')
            or soup.select_one('#corePrice_feature_div')
            or soup.select_one('#apex_desktop')
            or soup.select_one('#apex_desktop_newAccordionRow')
            or soup
        )

        # Preco atual — busca DENTRO do price_root apenas
        price = None
        for selector in [
            'span.a-price[data-a-color="base"] span.a-offscreen',
            'span.a-price[data-a-color="price"] span.a-offscreen',
            'span.priceToPay span.a-offscreen',
            'span.a-price:not(.a-text-price) span.a-offscreen',
            'span.a-price-whole',
        ]:
            el = price_root.select_one(selector)
            if el:
                price = _parse_price(el.get_text())
                if price:
                    break

        # Preco "de/por" (list price, riscado) — tambem dentro do price_root
        list_price = None
        for selector in [
            'span.a-price.a-text-price[data-a-strike="true"] span.a-offscreen',
            'span.a-price.a-text-price span.a-offscreen',
            '.basisPrice .a-offscreen',
        ]:
            el = price_root.select_one(selector)
            if el:
                list_price = _parse_price(el.get_text())
                if list_price and list_price > (price or 0):
                    break
                list_price = None

        if not price:
            return None

        # Imagem principal (alta resolução) — data-old-hires quando existe, senão src
        image_url = None
        for selector in ['img#landingImage', 'img#imgBlkFront', 'img[data-old-hires]']:
            img_el = soup.select_one(selector)
            if img_el:
                image_url = img_el.get('data-old-hires') or img_el.get('src')
                if image_url:
                    break

        return {'price': price, 'list_price': list_price, 'image_url': image_url, 'marketplace': 'amazon'}


# ==================== DETECTOR DE DESCONTO ====================
class DiscountDetector:
    """Decide se o preço atual é desconto relevante."""

    def check(self, product: dict, current_price: float, history: List[float]) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Retorna (é_desconto, motivo, percent_off).
        Aplica 3 critérios em OR: média histórica, list_price (de/por), mínimo histórico.
        """
        # (a) Média histórica — só se tiver histórico suficiente
        if len(history) >= MIN_HISTORY_POINTS:
            avg = statistics.mean(history)
            if current_price < avg * DISCOUNT_THRESHOLD:
                percent_off = round((1 - current_price / avg) * 100)
                return True, 'média histórica', percent_off

        # (b) List price (de/por da Amazon)
        list_price = product.get('list_price')
        if list_price and current_price < list_price * DISCOUNT_THRESHOLD:
            percent_off = round((1 - current_price / list_price) * 100)
            return True, 'preço de/por', percent_off

        # (c) Mínimo histórico — precisa de pelo menos 3 leituras pra fazer sentido
        if len(history) >= 3:
            min_hist = min(history)
            if current_price <= min_hist and current_price < statistics.mean(history) * 0.95:
                percent_off = round((1 - current_price / statistics.mean(history)) * 100)
                return True, 'mínimo histórico', percent_off

        return False, None, None


# ==================== INSTÂNCIAS GLOBAIS ====================
db = Database(MONGO_URL)
bestseller_scraper = BestSellersScraper()
price_scraper = PriceScraper()
detector = DiscountDetector()


# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user(user.id, user.username)

    welcome_text = """
🎯 <b>Bem-vindo ao Price Alert Bot!</b>

O bot descobre sozinho os produtos mais populares do Brasil e te avisa quando estão com desconto de verdade.

<b>Comandos:</b>
/start - Reinicia
/list_products - Ver produtos rastreados
/stats - Estatísticas
/queue - Alertas na fila
/help - Ajuda

Você vai receber alertas <b>em tempo real</b> assim que o bot detectar uma boa oferta, e um resumo top 10 do dia todo dia às <b>20h</b>.
    """
    await update.message.reply_text(welcome_text, parse_mode='HTML')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
<b>📚 COMO FUNCIONA</b>

1️⃣ <b>DESCOBERTA</b>
O bot puxa 1x/dia os produtos mais vendidos da Amazon BR (várias categorias).

2️⃣ <b>MONITORAMENTO CONTÍNUO</b>
Mercado Livre a cada 15min, Amazon a cada 1h — checa preço, salva no histórico e alerta na hora.

3️⃣ <b>DETECÇÃO DE DESCONTO</b>
Considera desconto quando o preço cai 15%+ em relação a:
- Média dos últimos 30 dias
- Preço "de/por" original
- Mínimo histórico

4️⃣ <b>ALERTAS EM CADÊNCIA CONSTANTE</b>
Quando detecta desconto, entra na fila. O canal recebe 1 oferta a cada 5min (com foto) das <b>09h às 23h</b>. Fora dessa janela a fila continua acumulando, mas nada é postado — pra não incomodar de madrugada. Melhor desconto pendente sai primeiro. Cooldown de 8h por produto.

5️⃣ <b>RESUMO DIÁRIO</b>
Todo dia às 20h, top 10 das melhores ofertas das últimas 24h.

<b>Dúvidas?</b> É só mandar mensagem.
    """
    await update.message.reply_text(help_text, parse_mode='HTML')


async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"🆔 Seu chat_id: <code>{chat_id}</code>\n\n"
        f"Coloca em <code>ADMIN_CHAT_ID</code> no <code>.env</code> pra receber alertas do bot em privado.",
        parse_mode='HTML',
    )


async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    products = db.get_active_products()
    if not products:
        await update.message.reply_text("📭 Ainda não tem produtos rastreados. O bot vai popular em breve.")
        return

    text = f"📊 <b>{len(products)} produtos rastreados</b>\n\n"
    # Mostra só os 20 primeiros pra não estourar o limite de mensagem
    for i, p in enumerate(products[:20], 1):
        price = p.get('current_price')
        price_txt = f" — R$ {price:.2f}" if price else ""
        text += f"{i}. {p['name'][:60]}{price_txt}\n"
    if len(products) > 20:
        text += f"\n... e mais {len(products) - 20} produtos"

    await update.message.reply_text(text, parse_mode='HTML')


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    products = db.get_active_products()
    users = db.get_users_count()
    with_price = sum(1 for p in products if p.get('current_price'))
    pending = db.get_pending_count()
    interval_min = ALERT_INTERVAL_SECONDS // 60

    stats_text = f"""
📈 <b>ESTATÍSTICAS</b>

👥 Usuários ativos: {users}
📦 Produtos rastreados: {len(products)}
💰 Com preço coletado: {with_price}
📬 Fila de alertas: {pending}
⏰ Agora: {datetime.now().strftime('%d/%m %H:%M')}

🎯 Cadência do canal: 1 alerta a cada {interval_min} min
🕒 Janela ativa: {QUIET_HOURS_END_BRT:02d}h-{QUIET_HOURS_START_BRT:02d}h BRT
    """
    await update.message.reply_text(stats_text, parse_mode='HTML')


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Operação cancelada.")
    return ConversationHandler.END


# --- Comandos admin (rodar jobs manualmente pra testar) ---
async def force_seed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🌱 Rodando seed de bestsellers agora...")
    await seed_bestsellers_task(context)
    await update.message.reply_text("✅ Seed concluído. Confere com /list_products.")


async def force_check_ml(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🟡 Rodando check ML agora (alerta na hora se achar desconto)...")
    await check_ml_task(context)
    await update.message.reply_text("✅ Check ML concluído.")


async def force_check_amazon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🟠 Rodando check Amazon agora (pode demorar alguns min)...")
    await check_amazon_task(context)
    await update.message.reply_text("✅ Check Amazon concluído.")


async def force_top10(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏆 Enviando resumo top 10 do dia...")
    await send_daily_top_10(context)
    await update.message.reply_text("✅ Resumo enviado.")


async def queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra quantos alertas estão pendentes na fila do dispatcher."""
    pending = db.get_pending_count()
    interval_min = ALERT_INTERVAL_SECONDS // 60
    quiet = _is_quiet_hours()
    quiet_line = f"\n🌙 <b>Janela silenciosa ativa</b> ({QUIET_HOURS_START_BRT:02d}h-{QUIET_HOURS_END_BRT:02d}h BRT) — fila segurando" if quiet else ""
    if pending == 0:
        await update.message.reply_text(f"📭 Fila vazia — nenhum alerta pendente.{quiet_line}", parse_mode='HTML')
        return
    eta_min = pending * interval_min
    await update.message.reply_text(
        f"📬 <b>{pending}</b> alerta(s) na fila\n"
        f"⏱️ Cadência: 1 msg a cada {interval_min} min\n"
        f"⏳ ETA pra esvaziar: ~{eta_min} min"
        f"{quiet_line}",
        parse_mode='HTML'
    )


async def force_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Força envio de 1 alerta da fila agora (útil pra testar)."""
    pending = db.get_pending_count()
    if pending == 0:
        await update.message.reply_text("📭 Fila vazia.")
        return
    await update.message.reply_text(f"📤 Disparando 1 da fila (restam {pending})...")
    await dispatch_pending_alerts_task(context)
    await update.message.reply_text(f"✅ Enviado. Restam {db.get_pending_count()} na fila.")


# --- /add_product mantido como admin (útil pra testar / adicionar produtos fora do bestseller) ---
async def add_product_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📦 Qual o <b>nome do produto</b>?", parse_mode='HTML')
    return ADD_PRODUCT_NAME


async def add_product_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['product_name'] = update.message.text
    keyboard = [
        [InlineKeyboardButton("Eletrônicos", callback_data='cat_eletronicos')],
        [InlineKeyboardButton("Casa", callback_data='cat_casa')],
        [InlineKeyboardButton("Outro", callback_data='cat_outro')],
    ]
    await update.message.reply_text(
        "Qual a <b>categoria</b>?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    return ADD_PRODUCT_CATEGORY


async def add_product_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    context.user_data['category'] = query.data.replace('cat_', '')
    await query.answer()
    await query.edit_message_text("Link da <b>Amazon</b>? (ou 'skip')", parse_mode='HTML')
    return ADD_PRODUCT_AMAZON


async def add_product_amazon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    context.user_data['amazon_url'] = url if url.lower() != 'skip' else None
    await update.message.reply_text("Link do <b>Mercado Livre</b>? (ou 'skip')", parse_mode='HTML')
    return ADD_PRODUCT_ML


async def add_product_ml(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    context.user_data['ml_url'] = url if url.lower() != 'skip' else None
    await update.message.reply_text("Link da <b>Shopee</b>? (ou 'skip')", parse_mode='HTML')
    return ADD_PRODUCT_SHOPEE


async def add_product_shopee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    context.user_data['shopee_url'] = url if url.lower() != 'skip' else None
    await update.message.reply_text("Desconto mínimo em %? (ex: 15)")
    return ADD_PRODUCT_DISCOUNT


async def add_product_discount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        discount = int(update.message.text)
    except ValueError:
        await update.message.reply_text("❌ Digite um número válido")
        return ADD_PRODUCT_DISCOUNT

    # Salva com a URL Amazon como url principal (se tiver)
    amazon_url = context.user_data.get('amazon_url')
    if not amazon_url:
        await update.message.reply_text("❌ Produto manual precisa ao menos da URL Amazon nessa versão.")
        return ConversationHandler.END

    product = {
        'name': context.user_data['product_name'],
        'url': amazon_url,
        'marketplace': 'amazon',
        'category': context.user_data['category'],
        'min_discount': discount,
    }
    if db.add_product(product):
        await update.message.reply_text(f"✅ Produto '{product['name']}' adicionado.")
    else:
        await update.message.reply_text("❌ Erro ao adicionar produto")
    return ConversationHandler.END


async def remove_product_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    products = db.get_active_products()
    if not products:
        await update.message.reply_text("📭 Nenhum produto pra remover.")
        return ConversationHandler.END

    text = "🗑️ <b>Produtos cadastrados:</b>\n\n"
    for i, product in enumerate(products[:20], 1):
        text += f"{i}. {product['name'][:60]}\n"
    text += "\nDigite o <b>nome exato</b> do produto que quer remover (ou /cancel):"
    await update.message.reply_text(text, parse_mode='HTML')
    return REMOVE_PRODUCT_NAME


async def remove_product_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if db.delete_product_by_name(name):
        await update.message.reply_text(f"✅ Produto '{name}' removido.")
    else:
        await update.message.reply_text(f"❌ Não achei '{name}'. Confere com /list_products.")
    return ConversationHandler.END


async def error_handler(update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Erro: {context.error}")
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("❌ Ocorreu um erro. Tente novamente.")
        except Exception:
            pass


# ==================== JOBS AGENDADOS ====================
async def seed_bestsellers_task(context: ContextTypes.DEFAULT_TYPE):
    """1x/dia: descobre novos bestsellers e adiciona/atualiza na DB."""
    logger.info("🌱 Iniciando seed de bestsellers...")

    amazon_products = await asyncio.to_thread(bestseller_scraper.get_amazon_bestsellers)
    ml_products = await asyncio.to_thread(bestseller_scraper.get_ml_bestsellers)

    all_products = amazon_products + ml_products
    added = 0
    for p in all_products:
        product_id = db.upsert_product(p)
        if not product_id:
            continue
        added += 1
        # ML já vem com preço no bestseller — grava direto pra ganhar tempo
        if p.get('initial_price') and p.get('marketplace') == 'mercadolivre':
            db.add_price_history(
                product_id,
                'mercadolivre',
                float(p['initial_price']),
                float(p['initial_list_price']) if p.get('initial_list_price') else None
            )
    logger.info(f"🌱 Seed concluído — {added} produtos processados (Amazon {len(amazon_products)} + ML {len(ml_products)})")


async def _check_and_alert_product(context: ContextTypes.DEFAULT_TYPE, product: dict) -> Optional[dict]:
    """Checa preço de 1 produto, salva no histórico, e se for desconto novo enfileira alerta.
    O dispatcher (dispatch_pending_alerts_task) envia da fila em cadência controlada.
    Retorna dict {name, percent_off} se enfileirou, senão None."""
    global _ml_verification_blocked
    try:
        data = await asyncio.to_thread(price_scraper.get_price, product)
        if not data:
            return None
        # ML antifraude sinalizou verificacao — acende breaker e sai.
        if data.get('blocked_by_verification'):
            _ml_verification_blocked = True
            return None
        db.add_price_history(
            product['_id'],
            data['marketplace'],
            data['price'],
            data.get('list_price'),
            data.get('image_url'),
        )
        history = db.get_price_history(product['_id'], days=30)
        # Reintroduz list_price no produto pro detector considerar (add_price_history atualizou o doc)
        product = {**product, 'list_price': data.get('list_price') or product.get('list_price')}
        is_disc, reason, percent_off = detector.check(product, data['price'], history)
        if not is_disc:
            return None
        if not db.should_alert(product, data['price']):
            return None
        if db.enqueue_alert(product['_id'], data, percent_off, reason):
            logger.info(f"➕ Enfileirado: {product['name'][:60]} (-{percent_off}%, {reason})")
            return {'name': product['name'], 'percent_off': percent_off}
        return None
    except Exception as e:
        logger.error(f"Erro _check_and_alert_product {product.get('name')}: {e}")
        return None


_dispatch_lock = asyncio.Lock()


async def dispatch_pending_alerts_task(context: ContextTypes.DEFAULT_TYPE):
    """A cada ALERT_INTERVAL_SECONDS: puxa 1 alerta da fila (maior desconto primeiro),
    envia e marca como alertado. Mantem cadencia constante no canal.
    Durante quiet hours (default 23h-09h BRT), pula envio — fila continua acumulando."""
    if _is_quiet_hours():
        return
    if _dispatch_lock.locked():
        return
    async with _dispatch_lock:
        product = db.pop_next_pending_alert(max_age_hours=ALERT_QUEUE_TTL_HOURS)
        if not product:
            return
        data = {
            'price': product.get('pending_price'),
            'list_price': product.get('pending_list_price'),
            'image_url': product.get('pending_image_url'),
            'marketplace': product.get('pending_marketplace') or product.get('marketplace'),
        }
        percent_off = product.get('pending_percent_off', 0)
        reason = product.get('pending_reason', '')
        try:
            await _send_discount_alert(context, product, data, reason, percent_off)
            db.mark_alerted(product['_id'], data['price'], percent_off, reason)
            pending = db.get_pending_count()
            logger.info(f"📤 Alerta enviado (restam {pending} na fila): {product['name'][:60]} (-{percent_off}%)")
        except Exception as e:
            logger.error(f"Erro dispatch_pending_alerts_task {product.get('name')}: {e}")


async def _send_discount_alert(context: ContextTypes.DEFAULT_TYPE, product: dict, data: dict,
                                reason: str, percent_off: int):
    """Envia alerta individual (com foto se tiver) pra todos usuários ativos."""
    price = data['price']
    list_price = data.get('list_price') or product.get('list_price')
    image_url = data.get('image_url') or product.get('image_url')
    marketplace = data.get('marketplace', product.get('marketplace', 'amazon'))
    affiliate_url = _build_affiliate_url(product['url'], marketplace)
    emoji = _marketplace_emoji(marketplace)
    marketplace_name = {'amazon': 'Amazon', 'mercadolivre': 'Mercado Livre', 'shopee': 'Shopee'}.get(marketplace, 'Loja')

    if list_price and list_price > price:
        price_block = f"De <s>R$ {list_price:.2f}</s> | Por <b>R$ {price:.2f}</b> 💰"
    else:
        price_block = f"<b>R$ {price:.2f}</b> 💰"

    caption = (
        f"{emoji} <b>{product['name'][:100]}</b>\n\n"
        f"{price_block}\n"
        f"🔻 <b>{percent_off}% OFF</b> <i>({reason})</i>\n\n"
        f"🛒 Achado na {marketplace_name}\n"
        f"👉 <a href=\"{affiliate_url}\">Ver oferta</a>"
    )

    # Se CHANNEL_ID setado, posta so no canal (broadcast). Senao, itera users individuais.
    targets = [CHANNEL_ID] if CHANNEL_ID else db.get_all_active_user_ids()
    for target in targets:
        try:
            if image_url:
                await context.bot.send_photo(chat_id=target, photo=image_url,
                                             caption=caption, parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id=target, text=caption,
                                               parse_mode='HTML', disable_web_page_preview=False)
        except Exception as e:
            err = str(e).lower()
            logger.warning(f"Falha ao alertar {target}: {e}")
            if isinstance(target, int) and ('blocked' in err or 'forbidden' in err):
                db.deactivate_user(target)


_ml_check_lock = asyncio.Lock()
_amazon_check_lock = asyncio.Lock()

# Circuit breaker do ML: quando o antifraude comeca a redirecionar pra
# /gz/account-verification, continuar batendo so reforca o flag da conta.
# _get_ml_price seta essa flag quando detecta o redirect; o check aborta
# assim que ela liga e pausa novos ciclos ate MLCOOLDOWN_UNTIL.
_ml_verification_blocked = False
_ml_cooldown_until: Optional[datetime] = None
ML_VERIFICATION_COOLDOWN_HOURS = 2


async def check_ml_task(context: ContextTypes.DEFAULT_TYPE):
    """A cada 30min: checa produtos ML e alerta na hora quando acha desconto.
    Se o circuit breaker do antifraude estiver aberto, pula o ciclo."""
    global _ml_cooldown_until, _ml_verification_blocked
    if _ml_cooldown_until and datetime.now() < _ml_cooldown_until:
        remaining = int((_ml_cooldown_until - datetime.now()).total_seconds() / 60)
        logger.info(f"🟡 ML check pulado — cooldown do antifraude ({remaining}min restantes)")
        return
    # cooldown expirou: reseta breaker e tenta de novo
    if _ml_cooldown_until:
        logger.info("🟡 Cooldown do antifraude expirou, retomando checks ML")
        _ml_cooldown_until = None
        _ml_verification_blocked = False
    if _ml_check_lock.locked():
        logger.warning("🟡 check_ml_task ja rodando — pulando execucao paralela")
        return
    async with _ml_check_lock:
        await _check_ml_impl(context)


async def _check_ml_impl(context: ContextTypes.DEFAULT_TYPE):
    global _ml_verification_blocked, _ml_cooldown_until
    _ml_verification_blocked = False
    products = db.get_products_by_marketplace('mercadolivre')
    logger.info(f"🟡 ML check — {len(products)} produtos")
    enqueued = 0
    for product in products:
        result = await _check_and_alert_product(context, product)
        if result:
            enqueued += 1
        # Se qualquer request detectou redirect pra /account-verification,
        # aborta o ciclo inteiro. Continuar batendo so piora o flag da conta.
        if _ml_verification_blocked:
            _ml_cooldown_until = datetime.now() + timedelta(hours=ML_VERIFICATION_COOLDOWN_HOURS)
            logger.error(
                f"🛑 Circuit breaker ML ativado — antifraude exigindo verificacao. "
                f"Cooldown ate {_ml_cooldown_until:%H:%M}"
            )
            _notify_admin(
                f"🛑 <b>ML antifraude detectado</b>\n\n"
                f"Ciclo abortado no produto {product.get('name', '?')[:60]}.\n"
                f"Checks pausados por {ML_VERIFICATION_COOLDOWN_HOURS}h "
                f"(retomam apos {_ml_cooldown_until:%H:%M})."
            )
            break
        # Delay maior e mais variavel — 0.3-0.8s era muito uniforme e triggou
        # o antifraude. 2-5s reduz volume de req/h e parece mais humano.
        await asyncio.sleep(random.uniform(2.0, 5.0))
    pending = db.get_pending_count()
    logger.info(f"🟡 ML check concluído — {enqueued} novos enfileirados (fila total: {pending})")


async def check_amazon_task(context: ContextTypes.DEFAULT_TYPE):
    """A cada 1h: checa produtos Amazon (mais devagar por causa do anti-bot) e alerta item-por-item."""
    if _amazon_check_lock.locked():
        logger.warning("🟠 check_amazon_task ja rodando — pulando execucao paralela")
        return
    async with _amazon_check_lock:
        await _check_amazon_impl(context)


async def _check_amazon_impl(context: ContextTypes.DEFAULT_TYPE):
    products = db.get_products_by_marketplace('amazon')
    logger.info(f"🟠 Amazon check — {len(products)} produtos")
    enqueued = 0
    for product in products:
        result = await _check_and_alert_product(context, product)
        if result:
            enqueued += 1
        await asyncio.sleep(random.uniform(*SCRAPE_DELAY_RANGE))
    pending = db.get_pending_count()
    logger.info(f"🟠 Amazon check concluído — {enqueued} novos enfileirados (fila total: {pending})")


_ml_cookie_warned_expired = False

# Mesmo UA usado por ml_cookie_refresher/refresh_cookie.py ao extrair o cookie.
# Fingerprint precisa ser estavel entre login e uso, senao ML invalida sessao rapido.
ML_SESSION_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _notify_admin(msg: str):
    """Envia mensagem privada pro admin via API HTTP do Telegram — funciona em código sync."""
    if not ADMIN_CHAT_ID or not TELEGRAM_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
            data={'chat_id': ADMIN_CHAT_ID, 'text': msg, 'parse_mode': 'HTML'},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"⚠️ Falha ao notificar admin: {e}")


def _ml_affiliate_shortlink(long_url: str) -> Optional[str]:
    """Chama API do painel de afiliado ML pra gerar meli.la/xxx. Cacheia no Mongo (links são permanentes)."""
    global _ml_cookie_warned_expired
    if not MERCADOLIVRE_COOKIE or not MERCADOLIVRE_AFFILIATE_TAG:
        return None

    cached = db.get_ml_shortlink(long_url)
    if cached:
        return cached

    m = _re.search(r'_csrf=([^;]+)', MERCADOLIVRE_COOKIE)
    if not m:
        logger.warning("⚠️ MERCADOLIVRE_COOKIE não contém _csrf token")
        return None
    csrf = m.group(1)

    try:
        resp = requests.post(
            'https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink',
            json={'urls': [long_url], 'tag': MERCADOLIVRE_AFFILIATE_TAG},
            headers={
                'User-Agent': ML_SESSION_UA,
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json;charset=UTF-8',
                'Cookie': MERCADOLIVRE_COOKIE,
                'Referer': 'https://www.mercadolivre.com.br/afiliados/link-generator',
                'Origin': 'https://www.mercadolivre.com.br',
                'x-csrf-token': csrf,
                'x-requested-with': 'XMLHttpRequest',
            },
            timeout=15,
        )
    except requests.RequestException as e:
        logger.warning(f"⚠️ Erro ao chamar ML createLink: {e}")
        return None

    if resp.status_code in (401, 403):
        # Sinaliza pro watcher local rodar o refresh automatico.
        try:
            open("/home/ubuntu/alerta_bot/COOKIE_NEEDS_REFRESH", "w").close()
        except OSError:
            pass
        if not _ml_cookie_warned_expired:
            logger.error(f"❌ Cookie ML expirado (HTTP {resp.status_code}) — refresh automatico disparado")
            _notify_admin(
                f"⚠️ <b>Cookie ML expirado</b> (HTTP {resp.status_code})\n\n"
                f"Refresh automatico foi disparado. Aguardando reconexao...\n"
                f"Enquanto isso, links do ML vão sair sem afiliado."
            )
            _ml_cookie_warned_expired = True
        return None

    if resp.status_code != 200:
        logger.warning(f"⚠️ ML createLink HTTP {resp.status_code}: {resp.text[:150]}")
        return None

    try:
        url_obj = resp.json()['urls'][0]
    except (KeyError, IndexError, ValueError):
        return None

    if 'short_url' not in url_obj:
        logger.info(f"ℹ️ ML rejeitou URL ({url_obj.get('message', '?')}): {long_url[:80]}")
        return None

    short = url_obj['short_url']
    db.save_ml_shortlink(long_url, short, MERCADOLIVRE_AFFILIATE_TAG)
    _ml_cookie_warned_expired = False
    return short


def _build_affiliate_url(url: str, marketplace: str = 'amazon') -> str:
    if marketplace == 'amazon' and AWS_ASSOCIATE_TAG and 'amazon.com.br' in url:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}tag={AWS_ASSOCIATE_TAG}"
    if marketplace == 'mercadolivre':
        short = _ml_affiliate_shortlink(url)
        if short:
            return short
    return url


def _marketplace_emoji(marketplace: str) -> str:
    return {'amazon': '🟠', 'mercadolivre': '🟡', 'shopee': '🔴'}.get(marketplace, '🛒')


async def send_daily_top_10(context: ContextTypes.DEFAULT_TYPE):
    """1x/dia às 20h: manda resumo com top 10 descontos das últimas 24h (dos produtos já alertados)."""
    logger.info("🏆 Rodando resumo diário top 10...")
    products = db.get_recently_alerted(hours=24)
    if not products:
        logger.info("🏆 Sem descontos nas últimas 24h, resumo pulado")
        return

    products.sort(key=lambda p: p.get('last_alerted_percent_off', 0) or 0, reverse=True)
    top = products[:10]

    lines = [f"🏆 <b>TOP {len(top)} DO DIA</b>",
             "<i>Melhores descontos das últimas 24h</i>\n"]
    for i, p in enumerate(top, 1):
        price = p.get('last_alerted_price') or p.get('current_price', 0)
        percent = p.get('last_alerted_percent_off', 0)
        marketplace = p.get('marketplace', 'amazon')
        emoji = _marketplace_emoji(marketplace)
        affiliate_url = _build_affiliate_url(p['url'], marketplace)
        list_price = p.get('list_price')
        if list_price and list_price > price:
            price_line = f"   💰 De <s>R$ {list_price:.2f}</s> | Por <b>R$ {price:.2f}</b> (-{percent}%)"
        else:
            price_line = f"   💰 <b>R$ {price:.2f}</b> (-{percent}%)"
        lines.append(f"{i}. {emoji} <b>{p['name'][:70]}</b>")
        lines.append(price_line)
        lines.append(f"   🛒 <a href=\"{affiliate_url}\">Ver oferta</a>\n")
    text = "\n".join(lines)

    targets = [CHANNEL_ID] if CHANNEL_ID else db.get_all_active_user_ids()
    sent = 0
    for target in targets:
        try:
            await context.bot.send_message(chat_id=target, text=text,
                                           parse_mode='HTML', disable_web_page_preview=True)
            sent += 1
        except Exception as e:
            logger.warning(f"Falha resumo pra {target}: {e}")
            if isinstance(target, int) and ('blocked' in str(e).lower() or 'forbidden' in str(e).lower()):
                db.deactivate_user(target)
    logger.info(f"🏆 Resumo diário enviado: {sent}/{len(targets)}")


# ==================== MAIN ====================
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("list_products", list_products))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("myid", myid_command))
    app.add_handler(CommandHandler("force_seed", force_seed))
    app.add_handler(CommandHandler("force_check_ml", force_check_ml))
    app.add_handler(CommandHandler("force_check_amazon", force_check_amazon))
    app.add_handler(CommandHandler("force_top10", force_top10))
    app.add_handler(CommandHandler("queue", queue_status))
    app.add_handler(CommandHandler("force_dispatch", force_dispatch))

    add_product_handler = ConversationHandler(
        entry_points=[CommandHandler("add_product", add_product_start)],
        states={
            ADD_PRODUCT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_name)],
            ADD_PRODUCT_CATEGORY: [CallbackQueryHandler(add_product_category, pattern=r'^cat_')],
            ADD_PRODUCT_AMAZON: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_amazon)],
            ADD_PRODUCT_ML: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_ml)],
            ADD_PRODUCT_SHOPEE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_shopee)],
            ADD_PRODUCT_DISCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_discount)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(add_product_handler)

    remove_product_handler = ConversationHandler(
        entry_points=[CommandHandler("remove_product", remove_product_start)],
        states={
            REMOVE_PRODUCT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, remove_product_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(remove_product_handler)

    app.add_error_handler(error_handler)

    # Jobs
    job_queue = app.job_queue
    # Seed inicial 30s depois de subir + a cada 6h (mais novidades ao longo do dia)
    job_queue.run_repeating(seed_bestsellers_task, interval=21600, first=30)
    # ML: scraping HTML com cookie. 30min pra reduzir volume de req/h e evitar
    # trigger do antifraude (dispatcher do canal já limita a 1 msg/5min).
    job_queue.run_repeating(check_ml_task, interval=1800, first=180)
    # Amazon: scraping frágil, 1 em 1h (primeira 5min após subir, escalonado do ML)
    job_queue.run_repeating(check_amazon_task, interval=3600, first=300)
    # Dispatcher: manda 1 alerta da fila a cada ALERT_INTERVAL_SECONDS (default 5min)
    job_queue.run_repeating(dispatch_pending_alerts_task, interval=ALERT_INTERVAL_SECONDS, first=90)
    # Resumo diário top 10 às 20h
    job_queue.run_daily(send_daily_top_10, time=datetime.strptime("20:00", "%H:%M").time())

    logger.info("🚀 Bot iniciado com sucesso!")
    app.run_polling()


if __name__ == '__main__':
    main()
