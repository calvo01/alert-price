"""
PRICE ALERT BOT
Descobre produtos mais vendidos da Amazon BR, monitora preços
e envia alertas quando detecta desconto.
"""

import os
import random
import logging
import statistics
from datetime import datetime, timedelta
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

# Amazon: URLs de bestsellers por categoria (mais variedade que a home)
AMAZON_BESTSELLER_URLS = [
    "https://www.amazon.com.br/gp/bestsellers/electronics/",
    "https://www.amazon.com.br/gp/bestsellers/computers/",
    "https://www.amazon.com.br/gp/bestsellers/home/",
    "https://www.amazon.com.br/gp/bestsellers/beauty/",
    "https://www.amazon.com.br/gp/bestsellers/sports/",
]

# Mercado Livre: IDs de categorias oficiais (site MLB = Brasil)
# Referência: https://api.mercadolibre.com/sites/MLB/categories
MERCADOLIVRE_CATEGORIES = {
    'MLB1051': 'celulares',
    'MLB1648': 'informatica',
    'MLB1000': 'eletronicos',
    'MLB1574': 'casa',
    'MLB1246': 'beleza',
    'MLB1276': 'esportes',
}
MERCADOLIVRE_AFFILIATE_TAG = os.getenv("MERCADOLIVRE_AFFILIATE_TAG", "")
MERCADOLIVRE_CLIENT_ID = os.getenv("MERCADOLIVRE_CLIENT_ID", "")
MERCADOLIVRE_CLIENT_SECRET = os.getenv("MERCADOLIVRE_CLIENT_SECRET", "")

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
                     min_drop_pct: float = 0.10, days_cooldown: int = 7,
                     min_hours_between: int = 24) -> bool:
        """True se pode enviar alerta novo. Le last_alerted_at FRESCO do DB pra evitar race
        condition entre execucoes paralelas (ex: dois check_amazon_task simultaneos).

        Regras:
        - nunca alertou → alerta
        - dentro de min_hours_between (24h) desde ultimo alerta → NUNCA realerta
        - passou days_cooldown (7 dias) → pode realertar mesmo com mesmo preco
        - entre 24h e 7 dias → so realerta se preco caiu +10% desde ultimo alerta
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


# ==================== SCRAPING ====================
def _random_headers(url: str = "") -> dict:
    """Headers rotativos que imitam browser real. Adiciona Referer contextual quando aplicavel."""
    h = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
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

    def get_ml_bestsellers(self, limit: int = BESTSELLERS_LIMIT) -> List[dict]:
        """Usa API pública do ML pra pegar mais vendidos por categoria."""
        all_products = []
        for cat_id, cat_name in MERCADOLIVRE_CATEGORIES.items():
            logger.info(f"🔎 Buscando bestsellers ML: {cat_name}")
            products = self._fetch_ml_category(cat_id, cat_name, limit)
            all_products.extend(products)
            import time
            time.sleep(random.uniform(1, 2))
        logger.info(f"📦 Total ML: {len(all_products)}")
        return all_products

    def _fetch_ml_category(self, category_id: str, category_name: str, limit: int) -> List[dict]:
        url = f"https://api.mercadolibre.com/sites/MLB/search?category={category_id}&sort=sold_quantity_desc&limit={limit}&condition=new"
        resp = _http_get_ml(url)
        if not resp:
            logger.warning(f"❌ Falha API ML: {category_name}")
            return []
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"Erro parse JSON ML {category_name}: {e}")
            return []

        results = data.get('results', [])
        products = []
        for item in results:
            title = item.get('title')
            permalink = item.get('permalink')
            if not title or not permalink:
                continue
            products.append({
                'name': title[:200],
                'url': permalink.split('?')[0],
                'image_url': item.get('thumbnail'),
                'marketplace': 'mercadolivre',
                'category': category_name,
                'source': 'bestseller',
                'ml_item_id': item.get('id'),
                'initial_price': item.get('price'),
                'initial_list_price': item.get('original_price'),
            })
        logger.info(f"📄 ML {category_name}: {len(products)} extraídos")
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
        """Usa API do ML pra pegar preço atualizado do item."""
        item_id = product.get('ml_item_id')
        if not item_id:
            # Fallback: tenta extrair "MLB-NNN" ou "MLBNNN" da URL
            import re
            match = re.search(r'MLB-?(\d+)', product.get('url', ''))
            if match:
                item_id = f"MLB{match.group(1)}"

        if not item_id:
            return None

        url = f"https://api.mercadolibre.com/items/{item_id}"
        resp = _http_get_ml(url)
        if not resp:
            return None
        try:
            data = resp.json()
        except Exception:
            return None

        price = data.get('price')
        list_price = data.get('original_price')
        if not price:
            return None

        # Pega imagem em alta resolução do array 'pictures'
        pictures = data.get('pictures') or []
        image_url = None
        if pictures:
            image_url = pictures[0].get('secure_url') or pictures[0].get('url')
        # Fallback: thumbnail se não achou nada
        if not image_url:
            image_url = data.get('thumbnail')

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

4️⃣ <b>ALERTAS EM TEMPO REAL</b>
Assim que detecta desconto, manda a oferta direto pra você (com foto). Sem spam: cooldown mínimo de 24h por produto; só realerta antes de 7 dias se o preço cair mais 10%.

5️⃣ <b>RESUMO DIÁRIO</b>
Todo dia às 20h, top 10 das melhores ofertas das últimas 24h.

<b>Dúvidas?</b> É só mandar mensagem.
    """
    await update.message.reply_text(help_text, parse_mode='HTML')


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

    stats_text = f"""
📈 <b>ESTATÍSTICAS</b>

👥 Usuários ativos: {users}
📦 Produtos rastreados: {len(products)}
💰 Com preço coletado: {with_price}
⏰ Agora: {datetime.now().strftime('%d/%m %H:%M')}

🎯 Próximos alertas: 08h, 12h, 18h
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
    """Checa preço de 1 produto, salva no histórico, e se for desconto novo envia alerta na hora.
    Retorna dict {name, percent_off} se alertou, senão None."""
    try:
        data = await asyncio.to_thread(price_scraper.get_price, product)
        if not data:
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
            logger.info(f"⏭️ Skip alerta (dedupe): {product['name'][:60]}")
            return None
        await _send_discount_alert(context, product, data, reason, percent_off)
        db.mark_alerted(product['_id'], data['price'], percent_off, reason)
        return {'name': product['name'], 'percent_off': percent_off}
    except Exception as e:
        logger.error(f"Erro _check_and_alert_product {product.get('name')}: {e}")
        return None


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


async def check_ml_task(context: ContextTypes.DEFAULT_TYPE):
    """A cada 15min: checa produtos ML e alerta na hora quando acha desconto."""
    if _ml_check_lock.locked():
        logger.warning("🟡 check_ml_task ja rodando — pulando execucao paralela")
        return
    async with _ml_check_lock:
        await _check_ml_impl(context)


async def _check_ml_impl(context: ContextTypes.DEFAULT_TYPE):
    products = db.get_products_by_marketplace('mercadolivre')
    logger.info(f"🟡 ML check — {len(products)} produtos")
    alerted = 0
    for product in products:
        result = await _check_and_alert_product(context, product)
        if result:
            alerted += 1
            logger.info(f"✅ Alerta ML: {result['name'][:60]} (-{result['percent_off']}%)")
        await asyncio.sleep(random.uniform(0.3, 0.8))
    logger.info(f"🟡 ML check concluído — {alerted} alertas enviados")


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
    alerted = 0
    for product in products:
        result = await _check_and_alert_product(context, product)
        if result:
            alerted += 1
            logger.info(f"✅ Alerta Amazon: {result['name'][:60]} (-{result['percent_off']}%)")
        await asyncio.sleep(random.uniform(*SCRAPE_DELAY_RANGE))
    logger.info(f"🟠 Amazon check concluído — {alerted} alertas enviados")


def _build_affiliate_url(url: str, marketplace: str = 'amazon') -> str:
    if marketplace == 'amazon' and AWS_ASSOCIATE_TAG and 'amazon.com.br' in url:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}tag={AWS_ASSOCIATE_TAG}"
    if marketplace == 'mercadolivre' and MERCADOLIVRE_AFFILIATE_TAG:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}matt_tool={MERCADOLIVRE_AFFILIATE_TAG}"
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
    app.add_handler(CommandHandler("force_seed", force_seed))
    app.add_handler(CommandHandler("force_check_ml", force_check_ml))
    app.add_handler(CommandHandler("force_check_amazon", force_check_amazon))
    app.add_handler(CommandHandler("force_top10", force_top10))

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
    # Seed inicial 30s depois de subir + 1x/dia (a cada 24h)
    job_queue.run_repeating(seed_bestsellers_task, interval=86400, first=30)
    # ML: API oficial, aguenta rodar de 15 em 15min (primeira 3min após subir)
    job_queue.run_repeating(check_ml_task, interval=900, first=180)
    # Amazon: scraping frágil, 1 em 1h (primeira 5min após subir, escalonado do ML)
    job_queue.run_repeating(check_amazon_task, interval=3600, first=300)
    # Resumo diário top 10 às 20h
    job_queue.run_daily(send_daily_top_10, time=datetime.strptime("20:00", "%H:%M").time())

    logger.info("🚀 Bot iniciado com sucesso!")
    app.run_polling()


if __name__ == '__main__':
    main()
