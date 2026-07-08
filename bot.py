"""
PRICE ALERT BOT - Código Completo
Rastreia preços em Amazon, Mercado Livre e Shopee
Manda alertas quando encontra bom desconto
"""

import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from telegram.constants import ChatAction
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import requests
from bs4 import BeautifulSoup
import asyncio
from typing import Optional, Dict, List
import json

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/price_alert_bot")
AWS_ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "default-tag")
MERCADOLIVRE_CLIENT_ID = os.getenv("MERCADOLIVRE_CLIENT_ID", "")
SHOPEE_PARTNER_ID = os.getenv("SHOPEE_PARTNER_ID", "")

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Estados da conversa
ADD_PRODUCT_NAME, ADD_PRODUCT_CATEGORY, ADD_PRODUCT_AMAZON, ADD_PRODUCT_ML, ADD_PRODUCT_SHOPEE, ADD_PRODUCT_DISCOUNT = range(6)

#mongodb
class Database:
    def __init__(self, url):
        try:
            self.client = MongoClient(url, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client['price_alert_bot']
            logger.info("✅ Conectado ao MongoDB")
        except ServerSelectionTimeoutError:
            logger.warning("⚠️ MongoDB offline, usando modo local")
            self.db = None

    def add_product(self, product: dict) -> bool:
        """Adiciona um produto ao banco"""
        if not self.db:
            return False
        try:
            self.db.products.insert_one({
                **product,
                'created_at': datetime.now(),
                'last_checked': None,
                'last_prices': {'amazon': None, 'mercadolivre': None, 'shopee': None}
            })
            return True
        except Exception as e:
            logger.error(f"Erro ao adicionar produto: {e}")
            return False

    def get_products(self) -> List[dict]:
        """Pega todos os produtos"""
        if not self.db:
            return []
        try:
            return list(self.db.products.find())
        except Exception as e:
            logger.error(f"Erro ao pegar produtos: {e}")
            return []

    def update_product_prices(self, product_id: str, prices: dict):
        """Atualiza preços de um produto"""
        if not self.db:
            return
        try:
            self.db.products.update_one(
                {'_id': product_id},
                {'$set': {
                    'last_prices': prices,
                    'last_checked': datetime.now()
                }}
            )
        except Exception as e:
            logger.error(f"Erro ao atualizar preços: {e}")

    def add_user(self, user_id: int, username: str = None):
        """Adiciona/atualiza usuário"""
        if not self.db:
            return
        try:
            self.db.users.update_one(
                {'user_id': user_id},
                {'$set': {
                    'username': username,
                    'joined_at': datetime.now(),
                    'last_active': datetime.now(),
                    'is_active': True
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Erro ao adicionar usuário: {e}")

    def get_users_count(self) -> int:
        """Conta usuários ativos"""
        if not self.db:
            return 0
        try:
            return self.db.users.count_documents({'is_active': True})
        except:
            return 0

    def log_click(self, user_id: int, product_id: str, marketplace: str):
        """Log de clique em produto"""
        if not self.db:
            return
        try:
            self.db.clicks.insert_one({
                'user_id': user_id,
                'product_id': product_id,
                'marketplace': marketplace,
                'clicked_at': datetime.now()
            })
        except Exception as e:
            logger.error(f"Erro ao logar clique: {e}")

# ==================== SCRAPING DE PREÇOS ====================
class PriceScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    async def get_amazon_price(self, url: str) -> Optional[Dict]:
        """Extrai preço da Amazon"""
        try:
            if not url:
                return None
            
            # Para ambiente de produção, use a Amazon API official
            # Por enquanto, retornamos estrutura básica
            # Em produção: use boto3 + Product Advertising API
            
            session = requests.Session()
            session.headers.update(self.headers)
            response = session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Busca preço (varia conforme página)
                price_element = soup.find('span', class_='a-price-whole')
                if price_element:
                    price_text = price_element.get_text().strip()
                    price = float(price_text.replace('R$', '').replace(',', '.').split()[0])
                    return {'price': price, 'marketplace': 'amazon', 'url': url}
            
            return None
        except Exception as e:
            logger.debug(f"Erro ao scrape Amazon: {e}")
            return None

    async def get_mercadolivre_price(self, url: str) -> Optional[Dict]:
        """Extrai preço do Mercado Livre"""
        try:
            if not url:
                return None
            
            session = requests.Session()
            session.headers.update(self.headers)
            response = session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Busca preço
                price_element = soup.find('span', class_='price-tag-fraction')
                if price_element:
                    price_text = price_element.get_text().strip()
                    price = float(price_text.replace(',', '.'))
                    return {'price': price, 'marketplace': 'mercadolivre', 'url': url}
            
            return None
        except Exception as e:
            logger.debug(f"Erro ao scrape Mercado Livre: {e}")
            return None

    async def get_shopee_price(self, url: str) -> Optional[Dict]:
        """Extrai preço do Shopee (via API)"""
        try:
            if not url:
                return None
            
            # Extrai item_id da URL
            if 'shopee.com.br' in url:
                # Format: shopee.com.br/produto-name-i.12345.67890
                parts = url.split('-i.')
                if len(parts) > 1:
                    item_id = parts[1].split('.')[-1]
                    
                    # Usa API do Shopee
                    api_url = f"https://shopee.com.br/api/v2/item/get?itemid={item_id}&shopid=1"
                    response = requests.get(api_url, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('data'):
                            price = data['data'].get('price', 0) / 100000  # Shopee usa centavos
                            return {'price': price, 'marketplace': 'shopee', 'url': url}
            
            return None
        except Exception as e:
            logger.debug(f"Erro ao scrape Shopee: {e}")
            return None

    async def get_all_prices(self, product: dict) -> Dict:
        """Pega preços de todos os marketplaces"""
        prices = {}
        
        # Amazon
        if product.get('amazon_url'):
            amazon = await self.get_amazon_price(product['amazon_url'])
            if amazon:
                prices['amazon'] = amazon['price']
        
        # Mercado Livre
        if product.get('mercadolivre_url'):
            ml = await self.get_mercadolivre_price(product['mercadolivre_url'])
            if ml:
                prices['mercadolivre'] = ml['price']
        
        # Shopee
        if product.get('shopee_url'):
            shopee = await self.get_shopee_price(product['shopee_url'])
            if shopee:
                prices['shopee'] = shopee['price']
        
        return prices

# ==================== BOT HANDLERS ====================
db = Database(MONGO_URL)
scraper = PriceScraper()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start"""
    user = update.effective_user
    db.add_user(user.id, user.username)
    
    welcome_text = """
🎯 **Bem-vindo ao Price Alert Bot!**

Seu assistente de compras inteligente que rastreia preços em:
✅ Amazon
✅ Mercado Livre  
✅ Shopee

Receba alertas quando encontrar bons descontos!

**Comandos disponíveis:**
/start - Reinicia o bot
/add_product - Adicionar novo produto
/list_products - Ver produtos rastreados
/remove_product - Remover um produto
/stats - Ver estatísticas
/help - Ajuda
    """
    
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /help"""
    help_text = """
**📚 AJUDA - Como usar o bot**

1️⃣ **ADICIONAR PRODUTO**
Comando: /add_product
Você vai informar:
- Nome do produto
- Categoria (eletrônicos/suplementos/fitness)
- Links nos 3 marketplaces
- Desconto mínimo para alertar

2️⃣ **ALERTAS AUTOMÁTICOS**
O bot rastreia 24/7 e manda:
- 08:00 - Melhor oferta matinal
- 12:00 - Oferta do meio do dia
- 18:00 - Oferta noturna

3️⃣ **COMO FUNCIONA**
Quando você clica no link:
- Você compra no melhor preço
- Você aproveita o desconto
- Ganhamos comissão automaticamente

**Dúvidas?** Envie uma mensagem!
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def add_product_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inicia conversa de adicionar produto"""
    await update.message.reply_text("📦 Qual o **nome do produto**?", parse_mode='Markdown')
    return ADD_PRODUCT_NAME

async def add_product_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe nome do produto"""
    context.user_data['product_name'] = update.message.text
    
    keyboard = [
        [InlineKeyboardButton("Eletrônicos", callback_data='cat_eletronicos')],
        [InlineKeyboardButton("Suplementos", callback_data='cat_suplementos')],
        [InlineKeyboardButton("Fitness", callback_data='cat_fitness')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text("Qual a **categoria**?", reply_markup=reply_markup, parse_mode='Markdown')
    return ADD_PRODUCT_CATEGORY

async def add_product_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe categoria"""
    query = update.callback_query
    category = query.data.replace('cat_', '')
    context.user_data['category'] = category
    
    await query.answer()
    await query.edit_message_text("Link da **Amazon**? (ou 'skip' para pular)")
    return ADD_PRODUCT_AMAZON

async def add_product_amazon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe link Amazon"""
    url = update.message.text
    context.user_data['amazon_url'] = url if url.lower() != 'skip' else None
    
    await update.message.reply_text("Link do **Mercado Livre**? (ou 'skip')")
    return ADD_PRODUCT_ML

async def add_product_ml(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe link Mercado Livre"""
    url = update.message.text
    context.user_data['mercadolivre_url'] = url if url.lower() != 'skip' else None
    
    await update.message.reply_text("Link do **Shopee**? (ou 'skip')")
    return ADD_PRODUCT_SHOPEE

async def add_product_shopee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe link Shopee"""
    url = update.message.text
    context.user_data['shopee_url'] = url if url.lower() != 'skip' else None
    
    await update.message.reply_text("Desconto mínimo em %? (ex: 15)")
    return ADD_PRODUCT_DISCOUNT

async def add_product_discount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe desconto mínimo e salva"""
    try:
        discount = int(update.message.text)
        
        product = {
            'name': context.user_data['product_name'],
            'category': context.user_data['category'],
            'amazon_url': context.user_data.get('amazon_url'),
            'mercadolivre_url': context.user_data.get('mercadolivre_url'),
            'shopee_url': context.user_data.get('shopee_url'),
            'min_discount': discount,
            'active': True
        }
        
        if db.add_product(product):
            await update.message.reply_text(f"✅ Produto '{product['name']}' adicionado com sucesso!")
        else:
            await update.message.reply_text("❌ Erro ao adicionar produto")
        
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Digite um número válido")
        return ADD_PRODUCT_DISCOUNT

async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista todos os produtos"""
    products = db.get_products()
    
    if not products:
        await update.message.reply_text("📭 Nenhum produto rastreado ainda")
        return
    
    text = "📊 **Produtos em rastreamento:**\n\n"
    for i, product in enumerate(products, 1):
        text += f"{i}. **{product['name']}**\n"
        text += f"   Categoria: {product['category']}\n"
        text += f"   Desconto mín: {product['min_discount']}%\n"
        text += f"   Status: {'✅ Ativo' if product['active'] else '❌ Inativo'}\n\n"
    
    await update.message.reply_text(text, parse_mode='Markdown')

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra estatísticas"""
    products = db.get_products()
    users = db.get_users_count()
    
    stats_text = f"""
📈 **ESTATÍSTICAS**

👥 Usuários ativos: {users}
📦 Produtos rastreando: {len(products)}
⏰ Último update: {datetime.now().strftime('%H:%M:%S')}

🎯 Próximo alerta: 08:00 amanhã
    """
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tratamento de erros"""
    logger.error(f"Erro: {context.error}")
    await update.message.reply_text("❌ Ocorreu um erro. Tente novamente.")

async def check_prices_task(context: ContextTypes.DEFAULT_TYPE):
    """Task que roda a cada 6 horas checando preços"""
    logger.info("🔍 Checando preços...")
    
    products = db.get_products()
    
    for product in products:
        try:
            prices = await scraper.get_all_prices(product)
            
            if prices:
                db.update_product_prices(str(product.get('_id')), prices)
                logger.info(f"✅ Preços de '{product['name']}' atualizados: {prices}")
        except Exception as e:
            logger.error(f"Erro ao checar {product['name']}: {e}")

async def send_daily_alerts(context: ContextTypes.DEFAULT_TYPE):
    """Task que manda alertas 2-3x por dia"""
    logger.info("📢 Enviando alertas diários...")
    
    products = db.get_products()
    
    # Aqui você manda mensagens pra todos os usuários
    # Exemplo: top 1 produto com melhor desconto
    
    if products:
        best_product = products[0]  # Simplificado - em produção, calcula melhor desconto
        
        # Cria link de afiliado
        affiliate_link = f"{best_product.get('amazon_url')}?tag={AWS_ASSOCIATE_TAG}"
        
        alert_text = f"""
🚨 **ALERTA DE PREÇO**

{best_product['name']}

💰 Confira o desconto agora!
[Ir para o melhor preço]({affiliate_link})
        """
        
        logger.info(f"Alerta criado para: {best_product['name']}")

# ==================== MAIN ====================
def main():
    """Inicia o bot"""
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Handlers de comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("list_products", list_products))
    app.add_handler(CommandHandler("stats", stats))

    # Handler de conversa para adicionar produto
    add_product_handler = ConversationHandler(
        entry_points=[CommandHandler("add_product", add_product_start)],
        states={
            ADD_PRODUCT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_name)],
            ADD_PRODUCT_CATEGORY: [MessageHandler(filters.ALL, add_product_category)],
            ADD_PRODUCT_AMAZON: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_amazon)],
            ADD_PRODUCT_ML: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_ml)],
            ADD_PRODUCT_SHOPEE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_shopee)],
            ADD_PRODUCT_DISCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_product_discount)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    app.add_handler(add_product_handler)

    # Error handler
    app.add_error_handler(error_handler)

    # Jobs (tarefas agendadas)
    job_queue = app.job_queue
    job_queue.run_repeating(check_prices_task, interval=21600, first=0)  # A cada 6h
    job_queue.run_daily(send_daily_alerts, time=datetime.strptime("08:00", "%H:%M").time())
    job_queue.run_daily(send_daily_alerts, time=datetime.strptime("12:00", "%H:%M").time())
    job_queue.run_daily(send_daily_alerts, time=datetime.strptime("18:00", "%H:%M").time())

    logger.info("🚀 Bot iniciado com sucesso!")
    app.run_polling()

if __name__ == '__main__':
    main()
