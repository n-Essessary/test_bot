import sqlite3
import requests
import logging
from itertools import permutations
import time
import datetime as dt
from datetime import datetime
import json
import websockets
import threading
import ssl
import certifi
import asyncio
import hmac
import base64
import traceback
import os
import math
import sys



# Настройка логирования
logging.basicConfig(
    filename="debug_log.txt",
    filemode="w",
    level=logging.ERROR,  # Оставляем DEBUG, но фильтруем лишнее
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Константы
OKX_API_TRADE = "https://www.okx.com"
OKX_PRIVAT_URL = "wss://ws.okx.com:8443/ws/v5/private"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_API_URL = "https://www.okx.com/api/v5"
INITIAL_BALANCE = 1000  # Стартовый баланс для проверки
UPDATE_INTERVAL = 1  # Интервал обновления (в секундах)
TARGET_CURRENCIES = {"USDT", "USDC"}  # Только финальные конвертации
TARGET_CURRENCIES2 = {"BTC", "ETH", "SOL", "OKB", "BCH", "BSV", "LTC"}
TARGET_CURRENCIES_DEF = {"USDT", "USDC", "BTC", "ETH"}
EXCLUDED_CURRENCIES = {"USD", "UAH", "EUR", "JPY", "CNY", "GBP", "CHF", "AUD", "CAD", "BRL", "SGD", "HKD", "KRW", "RUB", "INR", "MXN", "TRY", "AED"}  # Исключаем фиатные валюты
PROFIT_PERCENT = 1010
ORDER_SIZE = 5
MIN_TRADE_SIZE = 0.0001  # Минимальный размер ордера
TICK_SIZE = 0.0001  # Минимальный шаг округления

# 🔹 Округляем размер ордера по `TICK_SIZE`
def round_to_tick_size(amount):
    return math.floor(amount / TICK_SIZE) * TICK_SIZE

# Загружаем АПИ ключ из txt файла
def load_api_keys():
    with open("api_keys.txt", "r") as f:
        api_key = f.readline().strip()
        api_secret = f.readline().strip()
        passphrase = f.readline().strip()  
    return api_key, api_secret, passphrase

def generate_signature(timestamp, method, path, body, secret_key):
    """ Создает подпись HMAC для WebSocket-аутентификации OKX. """
    message = f"{timestamp}{method}{path}{body}"
    signature = hmac.new(secret_key.encode(), message.encode(), digestmod="sha256").digest()
    return base64.b64encode(signature).decode()

# Создаем соединение с SQLite
conn = sqlite3.connect("arbitrage.db", check_same_thread=False)
cursor = conn.cursor()

# Оптимизация SQLite
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA synchronous=NORMAL;")

# Пересоздание базы данных, если файла нет
cursor.execute("""
CREATE TABLE IF NOT EXISTS tab_1 (
    pair TEXT PRIMARY KEY,
    bid_price REAL DEFAULT NULL,
    ask_price REAL DEFAULT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tab_2 (
    pair1 TEXT,
    pair2 TEXT,
    pair3 TEXT,
    bid1 REAL DEFAULT NULL,
    ask1 REAL DEFAULT NULL,
    bid1_volume REAL DEFAULT NULL,
    ask1_volume REAL DEFAULT NULL,
    bid2 REAL DEFAULT NULL,
    ask2 REAL DEFAULT NULL,
    bid2_volume REAL DEFAULT NULL,
    ask2_volume REAL DEFAULT NULL,
    bid3 REAL DEFAULT NULL,
    ask3 REAL DEFAULT NULL,
    bid3_volume REAL DEFAULT NULL,
    ask3_volume REAL DEFAULT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tab_3 (
    pair1 TEXT,
    bid1 REAL,
    ask1 REAL,
    pair2 TEXT,
    bid2 REAL,
    ask2 REAL,
    pair3 TEXT,
    bid3 REAL,
    ask3 REAL,
    final_balance REAL
);
""")

# Таблица для баланса
cursor.execute("""
CREATE TABLE IF NOT EXISTS balances (
    currency TEXT PRIMARY KEY,
    available REAL,
    reserved REAL
);
""")

# Таблица для ордеров
cursor.execute("""
CREATE TABLE IF NOT EXISTS open_orders (
    order_id TEXT PRIMARY KEY,
    pair TEXT,
    type TEXT,
    price REAL,
    quantity REAL,
    created_at TIMESTAMP
);
""")
cursor.execute("DELETE FROM open_orders")
conn.commit()
conn.close()

# Функция для запроса торговых пар у OKX
def fetch_trading_pairs():
    """Запрашивает торговые пары у OKX и загружает их в `tab_1`."""
    logging.info("Запрос торговых пар у OKX")
    response = requests.get(f"{OKX_API_URL}/public/instruments?instType=SPOT")
    data = response.json()
    
    if "data" not in data:
        logging.error("Ошибка при получении данных о торговых парах")
        return []
    
    pairs = [(item["instId"], None, None) for item in data["data"]]

    with sqlite3.connect("arbitrage.db") as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tab_1")
        cursor.executemany("INSERT INTO tab_1 (pair, bid_price, ask_price) VALUES (?, ?, ?)", pairs)
        conn.commit()

    print("✅ Валютные пары загружены в tab_1.")
    return [p[0] for p in pairs]

def find_triangular_arbitrage():
    """Находит возможные треугольники для арбитража и загружает их в tab_2."""
    conn = sqlite3.connect("arbitrage.db")  # ✅ Открываем соединение
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tab_2")
    cursor.execute("SELECT pair FROM tab_1")
    pairs = [row[0] for row in cursor.fetchall()]

    currencies = set()
    pair_map = {}

    # Заполняем связи валют
    for pair in pairs:
        base, quote = pair.split("-")
        if base in EXCLUDED_CURRENCIES or quote in EXCLUDED_CURRENCIES:
            continue  # Пропускаем запрещённые валюты
        currencies.update([base, quote])
        pair_map.setdefault(base, []).append(quote)
        pair_map.setdefault(quote, []).append(base)

    triangles = []

    # Поиск треугольников
    for a, b, c in permutations(currencies, 3):
        if b in pair_map[a] and c in pair_map[b] and a in pair_map[c] and c in TARGET_CURRENCIES_DEF:
            pair1 = f"{a}-{b}" if f"{a}-{b}" in pairs else f"{b}-{a}"
            pair2 = f"{b}-{c}" if f"{b}-{c}" in pairs else f"{c}-{b}"
            pair3 = f"{c}-{a}" if f"{c}-{a}" in pairs else f"{a}-{c}"
            
            if not (pair1 in pairs and pair2 in pairs and pair3 in pairs):
                continue
            
            triangles.append((pair1, pair2, pair3))

    # Записываем треугольники в базу
    cursor.executemany("INSERT INTO tab_2 (pair1, pair2, pair3) VALUES (?, ?, ?)", triangles)
    conn.commit()
    conn.close()    # ✅ Закрываем соединение
    print(f"Найдено {len(triangles)} треугольников и загружено в tab_2.")


def filter_triangles():
    #Принудительно чистим лог арбитражей
    with open("arbitrage_log.txt", "a") as log_file:
        log_file.truncate(0)
    logging.info("Фильтрация треугольников на соответствие правилам")
    conn = sqlite3.connect("arbitrage.db")  # ✅ Открываем соединение
    cursor = conn.cursor()
    cursor.execute("SELECT pair1, pair2, pair3 FROM tab_2")
    triangles = cursor.fetchall()
    valid_triangles = []

    for pair1, pair2, pair3 in triangles:
        base1, quote1 = pair1.split("-")
        base2, quote2 = pair2.split("-")
        base3, quote3 = pair3.split("-")

        # Условие 1: каждая пара должна содержать хотя бы одну валюту конвертации
        contains_conversion_currency = any(currency in TARGET_CURRENCIES for currency in [base1, quote1, base2, quote2, base3, quote3])

        # Условие 2: последняя пара должна состоять только из валют конвертации
        last_pair_valid = base3 in TARGET_CURRENCIES and quote3 in TARGET_CURRENCIES
        
        # Условие 3: добавляем пары конвертации BTC
        second_valid_pair = base2 in TARGET_CURRENCIES2 and quote2 in TARGET_CURRENCIES2

        # Условие 4: удаление исключенных валют
        if any(currency in EXCLUDED_CURRENCIES for currency in [base1, quote1, base2, quote2, base3, quote3]):
            continue  # Пропускаем этот треугольник

        # Если треугольник соответствует условиям, сохраняем его
        if (contains_conversion_currency and last_pair_valid) or (second_valid_pair):
            valid_triangles.append((pair1, pair2, pair3))

    # Очищаем старую таблицу и записываем только валидные треугольники
    cursor.execute("DELETE FROM tab_2")
    cursor.executemany("INSERT INTO tab_2 (pair1, pair2, pair3) VALUES (?, ?, ?)", valid_triangles)
    conn.commit()
    conn.close()    # ✅ Закрываем соединение

    print(f"После фильтрации осталось {len(valid_triangles)} треугольников.")

# Восстановление списка пар
def get_unique_pairs():
    """Получает список уникальных пар из `tab_2`."""
    with sqlite3.connect("arbitrage.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT pair1 FROM tab_2 UNION SELECT DISTINCT pair2 FROM tab_2 UNION SELECT DISTINCT pair3 FROM tab_2")
        pairs = [row[0] for row in cursor.fetchall()]
    return pairs  # ✅ Возвращаем список пар после закрытия соединения
pairs = get_unique_pairs()  # ✅ Теперь список пар берется безопасно
#print(pairs)

# Формирование JSON-запроса для подписки
pairs = get_unique_pairs()
subscribe_message = {
    "op": "subscribe",
    "args": [{"channel": "books", "instId": pair} for pair in pairs]
}

def split_into_chunks(lst, chunk_size):
    """Разбивает список на части по `chunk_size`."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# Подключение Websockets
async def websocket_handler(subscribe_message):
    retry_delay = 5  # Начальная задержка на переподключение
    max_delay = 60   # Максимальная задержка между попытками

    while True:
        try:
            async with websockets.connect(
                OKX_WS_URL,
                ssl=ssl.create_default_context(cafile=certifi.where()),
                ping_interval=15,
                ping_timeout=10
            ) as ws:
                logging.error("✅ Подключено к WebSocket обновления цен")
                await ws.send(json.dumps(subscribe_message))

                async for message in ws:
                    await process_ws_message(message)

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"⚠️ WebSocket обновления цен закрыт (код {e.code}): {e.reason}")
            if e.code == 1000:
                logging.error("✅ Нормальное завершение соединения, не переподключаемся")
                break  # Если сервер закрыл соединение штатно, выходим

        except Exception as e:
            logging.error(f"🚨 Ошибка WebSocket обновления цен: {e}")
            print("❌ СКРИПТ ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ ❌")  # Огромными буквами с пиктограммой
            break  # Полностью останавливаем скрипт при критической ошибке

        logging.error(f"🔄 Переподключение обновления цен через {retry_delay} сек...")
        await asyncio.sleep(retry_delay)

        # Увеличиваем задержку при частых ошибках, но не более max_delay
        retry_delay = min(retry_delay * 2, max_delay)

async def process_ws_message(message):
    try:
        data = json.loads(message)
        if "arg" in data and "data" in data:
            pair = data["arg"]["instId"]
            bids = data["data"][0].get("bids", [])
            asks = data["data"][0].get("asks", [])

            bid_price = float(bids[0][0]) if bids else None
            bid_volume = float(bids[0][1]) if bids else None
            ask_price = float(asks[0][0]) if asks else None
            ask_volume = float(asks[0][1]) if asks else None

            if bid_price is not None and ask_price is not None:
                conn = sqlite3.connect("arbitrage.db")
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE tab_2 
                    SET 
                        bid1 = CASE WHEN pair1 = ? THEN ? ELSE bid1 END,
                        ask1 = CASE WHEN pair1 = ? THEN ? ELSE ask1 END,
                        bid1_volume = CASE WHEN pair1 = ? THEN ? ELSE bid1_volume END,
                        ask1_volume = CASE WHEN pair1 = ? THEN ? ELSE ask1_volume END,

                        bid2 = CASE WHEN pair2 = ? THEN ? ELSE bid2 END,
                        ask2 = CASE WHEN pair2 = ? THEN ? ELSE ask2 END,
                        bid2_volume = CASE WHEN pair2 = ? THEN ? ELSE bid2_volume END,
                        ask2_volume = CASE WHEN pair2 = ? THEN ? ELSE ask2_volume END,

                        bid3 = CASE WHEN pair3 = ? THEN ? ELSE bid3 END,
                        ask3 = CASE WHEN pair3 = ? THEN ? ELSE ask3 END,
                        bid3_volume = CASE WHEN pair3 = ? THEN ? ELSE bid3_volume END,
                        ask3_volume = CASE WHEN pair3 = ? THEN ? ELSE ask3_volume END
                    WHERE pair1 = ? OR pair2 = ? OR pair3 = ?
                    """,
                    (
                        pair, bid_price, pair, ask_price, pair, bid_volume, pair, ask_volume, 
                        pair, bid_price, pair, ask_price, pair, bid_volume, pair, ask_volume, 
                        pair, bid_price, pair, ask_price, pair, bid_volume, pair, ask_volume,
                        pair, pair, pair
                    )
                )
                updated_rows = cursor.rowcount
                conn.commit()
                conn.close()


    except Exception as e:
        logging.error(f"Ошибка обработки WebSocket-сообщения: {e}")
        print("\n" + "="*50)
        print("🚨 СКРИПТ ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ 🚨".center(50))
        print("="*50 + "\n")
        sys.exit(1)  # Завершаем выполнение скрипта с кодом ошибки 1

async def main():
    pairs = get_unique_pairs()
    chunk_size = max(1, len(pairs) // 3)  # ✅ Разбиваем список на 3 части
    tasks = []
    
    for chunk in split_into_chunks(pairs, chunk_size):
        subscribe_message = {
            "op": "subscribe",
            "args": [{"channel": "books", "instId": pair} for pair in chunk]
        }
        tasks.append(asyncio.create_task(websocket_handler(subscribe_message)))
    
    await asyncio.gather(*tasks)  # ✅ Запускаем все WebSocket-потоки

# Функция для анализа арбитража в потоке
def analyze_triangles_thread():
    while True:
        analyze_triangles()
        time.sleep(UPDATE_INTERVAL)

# Находит пары для треугольного арбитража, сравнивая цены и обьемы
def analyze_triangles():
    logging.info("Начат анализ треугольников")
    conn = sqlite3.connect("arbitrage.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT pair1, bid1, ask1, ask1_volume, pair2, bid2, ask2, bid2_volume, pair3, bid3, ask3 FROM tab_2")
    triangles = cursor.fetchall()
    
    results = []
    log_entries = []
    successful_arbitrages = []
    
    for pair1, bid1, ask1, ask1_volume, pair2, bid2, ask2, bid2_volume, pair3, bid3, ask3 in triangles:
        if None in (bid1, ask1, ask1_volume, bid2, ask2, bid2_volume, bid3, ask3):
            logging.warning(f"Пропущен треугольник {pair1} → {pair2} → {pair3} (нет данных bid/ask/volume)")
            continue
        
        required_ask1_volume = 200 / ask1
        required_bid2_volume = 200 / bid2

        if ask1_volume == 0 or bid2_volume == 0:
            continue
        
        if ask1_volume < required_ask1_volume or bid2_volume < required_bid2_volume:
            continue
        
        step1 = INITIAL_BALANCE / ask1
        step2 = step1 * bid2
        final_balance = step2 * bid3
        profit_percent = (final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
        
        status = "✅" if final_balance > INITIAL_BALANCE else "❌"
        
        if final_balance > PROFIT_PERCENT:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = (f"[{current_time}]\n"
                         f"🔹 {pair1} → {pair2} → {pair3}\n"
                         f"   Итоговый баланс: {final_balance:.2f} ({profit_percent:.2f}%) {status}🔥\n")
            log_entries.append(log_entry)
            
            successful_arbitrages.append(log_entry)
            results.append((pair1, bid1, ask1, pair2, bid2, ask2, pair3, bid3, ask3, final_balance))
    
    cursor.execute("DELETE FROM tab_3")
    if results:
        cursor.executemany("INSERT INTO tab_3 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", results)
    else:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entries.append(f"[{current_time}] ❌ Нет арбитражей, работаем дальше.\n")
        logging.info("Нет арбитражей для записи в tab_3.")
    
    with open("arbitrage_log.txt", "a") as log_file:
        log_file.writelines(log_entries)
    
    if successful_arbitrages:
        with open("analise_arrbitrage.txt", "a") as arb_file:
            arb_file.writelines(successful_arbitrages)
    conn.commit()
    conn.close()
    


def generate_signature(timestamp, method, path, body, secret_key):
    message = f"{timestamp}{method}{path}{body}"
    signature = hmac.new(secret_key.encode(), message.encode(), digestmod="sha256").digest()
    return base64.b64encode(signature).decode()

os.environ["SSL_CERT_FILE"] = certifi.where()

# Подписка на Websocket для доступа к балансу и списку ордеров
async def subscribe_private_ws():
    api_key, api_secret, passphrase = load_api_keys()
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    retry_delay = 5  # Начальная задержка на переподключение
    max_delay = 60   # Максимальная задержка между попытками

    while True:
        try:
            async with websockets.connect(
                OKX_PRIVAT_URL, ssl=ssl_context, timeout=10, ping_interval=15, ping_timeout=10
            ) as ws:
                logging.error("✅ Подключение к WebSocket баланса и ордеров установлено")

                # Генерируем подпись
                timestamp = str(int(time.time()))
                signature = generate_signature(timestamp, "GET", "/users/self/verify", "", api_secret)

                # 🔹 1. Аутентификация
                auth_msg = {
                    "op": "login",
                    "args": [{
                        "apiKey": api_key,
                        "passphrase": passphrase,
                        "timestamp": timestamp,
                        "sign": signature
                    }]
                }
                logging.info(f"📤 Отправка аутентификации: {auth_msg}")
                await ws.send(json.dumps(auth_msg))

                # Получаем ответ на аутентификацию
                response = await ws.recv()
                logging.info(f"📥 Ответ от сервера (аутентификация): {response}")

                # 🔹 2. Подписка на баланс и ордера
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [
                        {"channel": "account"},  # Баланс
                        {"channel": "orders", "instType": "SPOT"}  # Открытые ордера
                    ]
                }
                logging.info(f"📤 Отправка подписки: {subscribe_msg}")
                await ws.send(json.dumps(subscribe_msg))

                # Бесконечный цикл получения данных
                async for message in ws:
                    logging.info(f"📥 Получено сообщение: {message}")
                    await process_private_ws_message(message)

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"⚠️ WebSocket баланса и ордеров закрыт (код {e.code}): {e.reason}")
            if e.code == 1000:
                logging.error("✅ Нормальное завершение соединения, не переподключаемся")
                break  # Если сервер закрыл соединение штатно, выходим

        except asyncio.TimeoutError:
            logging.error("⏳ Ошибка: WebSocket(баланса и ордеров)-соединение не установлено (таймаут)")

        except Exception as e:
            logging.error(f"🚨 Критическая ошибка WebSocket баланса и ордеров: {e}")
            print("❌ СКРИПТ ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ ❌")  # Вывод в консоль
            traceback.print_exc()
            break  # Полностью останавливаем скрипт

        logging.error(f"🔄 Переподключение баланса и ордеров через {retry_delay} сек...")
        await asyncio.sleep(retry_delay)

        # Увеличиваем задержку при частых ошибках, но не более max_delay
        retry_delay = min(retry_delay * 2, max_delay)

async def process_private_ws_message(message):
    """ Обрабатывает входящие сообщения от приватного WebSocket-а """
    try:
        data = json.loads(message)
        if "arg" in data and "data" in data:
            channel = data["arg"]["channel"]
            
            conn = sqlite3.connect("arbitrage.db")
            cursor = conn.cursor()

            if channel == "account":
                logging.info("🔄 Обновление баланса...")

                for account_data in data.get("data", []):  # ✅ Избегаем KeyError
                    for balance in account_data.get("details", []):  # ✅ Избегаем KeyError
                        currency = balance["ccy"] 
                        available = float(balance.get("availBal", 0))
                        reserved = float(balance.get("frozenBal", 0))

                        logging.info(f"💰 Баланс обновлён: {currency} | Доступно: {available} | Зарезервировано: {reserved}")

                        cursor.execute("""
                            INSERT INTO balances (currency, available, reserved) 
                            VALUES (?, ?, ?)
                            ON CONFLICT(currency) DO UPDATE 
                            SET available = excluded.available, reserved = excluded.reserved
                        """, (currency, available, reserved))

            elif channel == "orders":
                logging.info("🔄 Обновление списка ордеров...")
                for order in data.get("data", []):  # ✅ Избегаем KeyError
                    order_id = order["ordId"]
                    pair = order["instId"]
                    order_type = order["side"]
                    price = float(order["px"]) if order["px"] else None
                    quantity = float(order["sz"])
                    created_at = order["cTime"]

                    logging.info(f"📜 Ордер обновлён: {order_id} | {pair} | {order_type} | Цена: {price} | Кол-во: {quantity}")

                    cursor.execute("""
                        INSERT INTO open_orders (order_id, pair, type, price, quantity, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(order_id) DO UPDATE 
                        SET price = excluded.price, quantity = excluded.quantity
                    """, (order_id, pair, order_type, price, quantity, created_at))
            
            conn.commit()
            conn.close()
    except Exception as e:
        logging.error(f"❌ Ошибка обработки WebSocket-сообщения: {e}")
        traceback.print_exc()

def run_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(subscribe_private_ws())


balances = {}  # Глобальный кеш балансов
API_KEY, API_SECRET, PASSPHRASE = load_api_keys()


async def get_balance(currency):
    """Возвращает актуальный баланс для указанной валюты из локальной БД"""
    conn = sqlite3.connect("arbitrage.db")
    cursor = conn.cursor()
    cursor.execute("SELECT available FROM balances WHERE currency = ?", (currency,))
    result = cursor.fetchone()
    conn.close()
    return float(result[0]) if result else 0.0  # Если валюты нет, возвращаем 0

async def wait_for_order(order_id):
    """Ждёт выполнения ордера по `order_id`, проверяя таблицу `open_orders`"""
    while True:
        conn = sqlite3.connect("arbitrage.db")
        cursor = conn.cursor()
        cursor.execute("SELECT order_id FROM open_orders WHERE order_id = ?", (order_id,))
        open_order = cursor.fetchone()
        conn.close()

        if not open_order:
            logging.info(f"✅ Ордер {order_id} исполнен!")
            return True  # Ордер выполнен
        await asyncio.sleep(0.2)  # Проверяем статус каждую секунду


balances = {}  # Глобальный кеш балансов
API_KEY, API_SECRET, PASSPHRASE = load_api_keys()

# ✅ Функция отправки ордера на OKX
def generate_signature(timestamp, method, request_path, body, api_secret):
    message = f"{timestamp}{method}{request_path}{body}"
    signature = hmac.new(api_secret.encode(), message.encode(), digestmod="sha256").digest()
    return base64.b64encode(signature).decode()


async def place_order(pair, side, quantity):
    """
    Подключается по WebSocket, аутентифицируется и отправляет ордер.
    """
    try:
        # Настройка SSL для WebSocket
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())

        # Используем правильный формат времени
        timestamp = str(int(time.time()))  # ✅ Исправлено: теперь в секундах

        request_path = "/users/self/verify"  # ✅ Исправлено
        method = "GET"

        # Генерируем подпись по рабочему алгоритму
        sign = generate_signature(timestamp, method, request_path, "", API_SECRET)

        # Формируем сообщение для логина (аутентификации)
        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": API_KEY,
                "passphrase": PASSPHRASE,
                "timestamp": timestamp,
                "sign": sign
            }]
        }

        # Формируем сообщение ордера
        order_msg = {
            "id": "order1",  # Идентификатор запроса
            "op": "order",
            "args": [{
                "instId": pair,
                "tdMode": "cash",   # Спотовая торговля
                "side": side,       # "buy" или "sell"
                "ordType": "market", # Лимитный ордер,
                "sz": str(quantity)
            }]
        }

        async with websockets.connect(OKX_PRIVAT_URL, ssl=ssl_context) as ws:
            # Отправляем логин
            await ws.send(json.dumps(login_msg, separators=(",", ":")))
            login_response = await ws.recv()
            logging.info(f"🔒 Ответ логина: {login_response}")

            if '"event":"error"' in login_response:
                logging.error("🚨 Ошибка логина! Проверь API-ключи или временную метку.")
                return None
            
            # После успешного логина отправляем ордер
            await ws.send(json.dumps(order_msg, separators=(",", ":")))
            order_response = await ws.recv()
            logging.info(f"📩 Ответ ордера: {order_response}")

            return order_response

    except Exception as e:
        print(f"🚨 Ошибка в WebSocket ордере: {e}")
        return None
    

# ✅ Основная логика треугольного арбитража
async def triangular_arbitrage():
    """Основная логика треугольного арбитража"""
    while True:
        try:
            conn = sqlite3.connect("arbitrage.db")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tab_3 ORDER BY final_balance DESC LIMIT 1")
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                logging.info("ℹ️ Нет доступных треугольников для арбитража")
                await asyncio.sleep(2)
                continue
            
            pair1, bid1, ask1, pair2, bid2, ask2, pair3, bid3, ask3, final_balance = row
            logging.info(f"🔍 Выбран треугольник: {pair1}, {pair2}, {pair3} с финальным балансом {final_balance}")

            # 🔄 Получаем актуальные цены из `tab_2`
            conn = sqlite3.connect("arbitrage.db")
            cursor = conn.cursor()
            cursor.execute("SELECT ask1 FROM tab_2 WHERE pair1 = ?", (pair1,))
            ask1 = float(cursor.fetchone()[0])

            cursor.execute("SELECT bid2 FROM tab_2 WHERE pair2 = ?", (pair2,))
            bid2 = float(cursor.fetchone()[0])

            cursor.execute("SELECT bid3 FROM tab_2 WHERE pair3 = ?", (pair3,))
            bid3 = float(cursor.fetchone()[0])
            conn.close()

            # 🔄 Получаем актуальные балансы из БД
            base1, quote1 = pair1.split("-")
            base2, quote2 = pair2.split("-")
            base3, quote3 = pair3.split("-")

            # ✅ **Первая сделка: BUY `pair1`**
            balance_quote1 = await get_balance(quote1)
            logging.info(f"💰 Баланс {quote1}: {balance_quote1}, требуется: {ORDER_SIZE}")

            if quote1 in TARGET_CURRENCIES and balance_quote1 > ORDER_SIZE:
                amount1 = ORDER_SIZE 
                logging.info(f"📊 Рассчитанный объём первой сделки: {amount1} {base1} по цене {ask1}")

                if amount1 < MIN_TRADE_SIZE:
                    logging.warning(f"⚠️ Слишком малый ордер: {amount1} < {MIN_TRADE_SIZE}")
                    continue

                order_id = await place_order(pair1, "buy", amount1)
    
                if order_id:
                    logging.info(f"✅ Ордер на покупку {pair1} создан: {order_id}")
                else:
                    logging.error(f"🚨 Ошибка! Биржа отклонила ордер на {pair1}.")
                    continue

                if not await wait_for_order(order_id):
                    logging.error(f"🚨 Ордер {order_id} на {pair1} не исполнился!")
                    continue
            else:
                logging.error(f"⚠️ Недостаточно {quote1} для первой сделки")
                continue

            # ✅ **Вторая сделка: SELL `pair2`**
            balance_base1 = balance_quote1 / bid2 * (1 - 0.0011)
            if balance_base1 < MIN_TRADE_SIZE:
                logging.warning(f"⚠️ Недостаточный баланс {base1} для продажи: {balance_base1}")
                continue

            order_id = await place_order(pair2, "sell", balance_base1)
            if not await wait_for_order(order_id):
                continue

            # ✅ **Третья сделка: SELL `pair3`**
            balance_base3 = balance_base1 * bid2 * (1 - 0.0011)
            if balance_base3 < MIN_TRADE_SIZE:
                logging.warning(f"⚠️ Недостаточный баланс {base2} для продажи: {balance_base3}")
                continue

            order_id = await place_order(pair3, "sell", balance_base3)
            if not await wait_for_order(order_id):
                continue

            logging.info("🏆 ✅ Треугольный арбитраж завершен!")
            print("🏆 ✅ Треугольный арбитраж завершен!")

        except Exception as e:
            logging.error(f"🚨 Ошибка в арбитраже: {e}")
            await asyncio.sleep(2)  # ✅ Если ошибка — подождать 


if __name__ == "__main__":
    pairs = fetch_trading_pairs()
    find_triangular_arbitrage()
    filter_triangles()
    get_unique_pairs()
    threading.Thread(target=lambda: asyncio.run(main()), daemon=True).start()
    threading.Thread(target=lambda: asyncio.run(subscribe_private_ws()), daemon=True).start()
    threading.Thread(target=analyze_triangles_thread, daemon=True).start()
    threading.Thread(target=run_ws, daemon=True).start()
    try:
        print("💡 Инициализация треугольного арбитража... 🟡")
        asyncio.run(triangular_arbitrage())
    except KeyboardInterrupt:
        print("\n🛑 Скрипт арбитража остановлен пользователем.")
    finally:
        print("✨ Завершение программы. До новых сделок! 🚀")
    while True:
        time.sleep(10)
