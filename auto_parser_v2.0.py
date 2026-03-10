import sys
import subprocess
import os
import time
import json
import csv
import logging
from datetime import datetime, timedelta

# Автоустановка базовых библиотек
def install(package):
    try:
        __import__(package)
    except ImportError:
        print(f"Установка {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("requests")
install("selenium")
install("seleniumbase")

from selenium import webdriver
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# ==========================================
# 1. СБОРЩИК ДАННЫХ
# ==========================================
class DataAggregator:
    def __init__(self):
        self.all_data = []

    def add_data(self, data):
        if data: self.all_data.extend(data)

    def save_to_csv(self, filename="family_shopping_total.csv"):
        if not self.all_data:
            logging.warning("Данные не найдены.")
            return
        keys = self.all_data[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8-sig') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(self.all_data)
        logging.info(f"💾 ГОТОВО! Сохранено в файл: {os.path.abspath(filename)}")

# ==========================================
# 2. МОДУЛЬ X5 (ПЯТЁРОЧКА)
# ==========================================
class X5AutoParser:
    def __init__(self, owner_name):
        self.owner = owner_name
        self.session_file = "x5_temp_session.json"
        self.session = requests.Session()
        self.all_items = []

    def get_auth(self):
        logging.info(f"🤖 X5: Входим в аккаунт ({self.owner})...")
        driver = Driver(uc=True, headless=False)
        try:
            driver.get("https://x5club.ru/lk/")
            WebDriverWait(driver, 90).until(lambda d: "auth" not in d.current_url)
            time.sleep(5)
            cookies = {c['name']: c['value'] for c in driver.get_cookies()}
            self.session.cookies.update(cookies)
            return True
        finally: driver.quit()

    def decode_graph(self, raw_text: str):
        """Декодирует 'сплющенный граф' X5 в список товарных позиций."""
        try:
            data = json.loads(raw_text)
        except Exception:
            return []

        if not data:
            return []

        flat_array = data[0] if isinstance(data[0], list) else data
        if isinstance(flat_array, list) and len(flat_array) > 0 and isinstance(flat_array[0], list):
            flat_array = flat_array[0]

        # Детектор протухшего токена
        for item in flat_array:
            if isinstance(item, str) and "Token expired" in item:
                return "EXPIRED"

        def resolve(index, visited=None):
            if visited is None:
                visited = set()
            if index in visited or type(index) is not int or index < 0 or index >= len(flat_array):
                return None
            visited.add(index)
            item = flat_array[index]
            if isinstance(item, dict):
                res = {}
                for k, v in item.items():
                    if k.startswith('_'):
                        key_idx = int(k[1:])
                        key_str = flat_array[key_idx] if key_idx < len(flat_array) else k
                        if isinstance(v, int):
                            res[key_str] = resolve(v, visited.copy()) if v >= 0 else None
                        elif isinstance(v, list):
                            res[key_str] = [resolve(i, visited.copy()) for i in v if isinstance(i, int)]
                        else:
                            res[key_str] = v
                return res
            elif isinstance(item, list):
                return [resolve(i, visited.copy()) for i in item if isinstance(i, int)]
            return item

        tree = resolve(0)

        parsed_items = []

        def find_receipts(node):
            if isinstance(node, dict):
                # Узел чека: есть ID транзакции и список items
                if 'rtlTxnId' in node and 'items' in node and isinstance(node['items'], list):
                    r_date = (node.get('created') or node.get('date') or '')[:10]
                    r_store = node.get('title', 'Пятёрочка')
                    address = node.get('storeAddress') or node.get('retailPlaceAddress') or node.get('address') or ""
                    for product in node['items']:
                        if isinstance(product, dict) and 'priceRegular' in product:
                            try:
                                qty = float(product.get('quantity', 1))
                                price = float(product.get('priceItem', product.get('priceRegular', 0)))
                                parsed_items.append({
                                    "Дата": r_date,
                                    "Магазин": r_store or "Пятёрочка",
                                    "Адрес": address,
                                    "Товар": product.get('name', 'Неизвестный товар'),
                                    "Количество": qty,
                                    "Цена_за_шт": price,
                                    "Сумма": round(qty * price, 2),
                                    "Владелец": self.owner,
                                })
                            except (ValueError, TypeError):
                                pass
                else:
                    for v in node.values():
                        find_receipts(v)
            elif isinstance(node, list):
                for item in node:
                    find_receipts(item)

        find_receipts(tree)
        return parsed_items

    def fetch_history(self, months=12):
        """Запрашивает историю чеков X5 и парсит товары через decode_graph."""
        self.get_auth()

        today = datetime.now()
        for i in range(months):
            start = (today - timedelta(days=30 * (i + 1))).strftime("%Y-%m-%d")
            end = (today - timedelta(days=30 * i)).strftime("%Y-%m-%d")
            logging.info(f"📅 X5 ({self.owner}): Сбор за период {start} — {end}")

            params = {
                "type": "receipts",
                "startDate": start,
                "endDate": end,
                "page": "0",
                "codeTc": "all",
            }

            try:
                res = self.session.get(
                    "https://x5club.ru/popup/receiptsAndPointsDetailsPopup.data",
                    params=params,
                    timeout=30,
                )
            except Exception as e:
                logging.error(f"❌ X5: Ошибка сети за период {start} — {end}: {e}")
                continue

            if res.status_code in (401, 403):
                logging.warning("⚠️ X5: Сессия истекла, попробую переавторизоваться.")
                self.get_auth()
                continue

            if res.status_code != 200:
                logging.warning(f"⚠️ X5: HTTP {res.status_code} за период {start} — {end}, пропускаем.")
                continue

            result = self.decode_graph(res.text)

            if result == "EXPIRED":
                logging.warning("⚠️ X5: Токен протух (Token expired в ответе), переавторизация.")
                self.get_auth()
                continue

            if isinstance(result, list) and result:
                self.all_items.extend(result)
                logging.info(f"✅ X5: Найдено {len(result)} товарных позиций за период {start} — {end}.")
            else:
                logging.info(f"ℹ️ X5: За период {start} — {end} товары не найдены.")

        logging.info(f"✅ X5 ({self.owner}): Всего собрано {len(self.all_items)} товарных позиций.")
        return self.all_items

# ==========================================
# 3. МОДУЛЬ МАГНИТА (ПЕРЕЗАГРУЗКА)
# ==========================================
class MagnitAutoParser:
    def __init__(self, owner_name):
        self.owner = owner_name
        self.all_items = []

    def run_sync(self):
        logging.info(f"🍓 МАГНИТ: Входим в аккаунт ({self.owner})...")

        driver = Driver(uc=True, headless=False)

        try:
            # --- 1. ГОТОВИМ СКРИПТ ПЕРЕХВАТА СПИСКА И ЗАГОЛОВКОВ ---
            inject_script = r"""
                (function () {
                    if (window.__MAGNIT_HOOK_INSTALLED__) { return; }
                    window.__MAGNIT_HOOK_INSTALLED__ = true;
                    window.__MAGNIT_TRANSACTIONS__ = [];
                    window.__MAGNIT_HEADERS__ = window.__MAGNIT_HEADERS__ || {};

                    function pushResponse(url, body) {
                        try {
                            if (!url || !body) { return; }
                            var data;
                            try { data = JSON.parse(body); } catch (e) { return; }

                            // сохраняем ВСЕ ответы, дальше в Python отфильтруем monthlyTotals
                            window.__MAGNIT_TRANSACTIONS__.push({
                                url: url,
                                data: data
                            });
                        } catch (e) {}
                    }

                    function captureHeaders(url, reqHeaders) {
                        try {
                            if (!url || !reqHeaders) { return; }
                            if (url.indexOf('/webgate/') === -1) { return; }
                            if (Object.keys(reqHeaders).length === 0) { return; }
                            window.__MAGNIT_HEADERS__ = reqHeaders;
                        } catch (e) {}
                    }

                    // Перехватываем fetch
                    try {
                        if (window.fetch) {
                            var origFetch = window.fetch;
                            window.fetch = function () {
                                var args = arguments;
                                var url = (args[0] && args[0].url) ? args[0].url : args[0];
                                var reqHeaders = {};
                                try {
                                    if (args[1] && args[1].headers) {
                                        var h = args[1].headers;
                                        if (typeof h.forEach === 'function') {
                                            h.forEach(function (v, k) { reqHeaders[k] = v; });
                                        } else {
                                            reqHeaders = JSON.parse(JSON.stringify(h));
                                        }
                                    }
                                } catch (e) {}

                                captureHeaders(url, reqHeaders);

                                return origFetch.apply(this, args).then(function (res) {
                                    try {
                                        var clone = res.clone();
                                        clone.text().then(function (text) {
                                            pushResponse(url, text);
                                        });
                                    } catch (e) {}
                                    return res;
                                });
                            };
                        }
                    } catch (e) {}

                    // Перехватываем XHR (только ради body, заголовки тут не трогаем)
                    try {
                        var origOpen = XMLHttpRequest.prototype.open;
                        var origSend = XMLHttpRequest.prototype.send;
                        XMLHttpRequest.prototype.open = function (method, url) {
                            try { this.__mag_url = url; } catch (e) {}
                            return origOpen.apply(this, arguments);
                        };
                        XMLHttpRequest.prototype.send = function (body) {
                            try {
                                var self = this;
                                this.addEventListener("load", function () {
                                    try {
                                        var url = self.__mag_url || "";
                                        var bodyText = "";
                                        if (self.responseType === "" || self.responseType === "text" || self.responseType === "json") {
                                            bodyText = self.responseText || "";
                                        }
                                        pushResponse(url, bodyText);
                                    } catch (e) {}
                                });
                            } catch (e) {}
                            return origSend.apply(this, arguments);
                        };
                    } catch (e) {}
                })();
            """

            # --- 2. ОТКРЫВАЕМ СТРАНИЦУ С ЧЕКАМИ И ВНЕДРЯЕМ ХУК ---
            driver.get("https://magnit.ru/profile/transactions/")
            time.sleep(3)
            driver.execute_script(inject_script)

            print("\n" + "=" * 60)
            print("⏳ МАГНИТ: Пожалуйста, введите код из СМС в окне браузера.")
            print("Остальное скрипт сделает сам (пролистает чеки до самого низа).")
            print("=" * 60 + "\n")

            # Ждем появления секретных заголовков (означает, что юзер вошел и пошли API запросы)
            wait_time = 0
            while wait_time < 120:
                has_headers = driver.execute_script("return !!window.__MAGNIT_HEADERS__;")
                if has_headers:
                    break
                time.sleep(1)
                wait_time += 1

            if wait_time >= 120:
                logging.error("❌ МАГНИТ: Время ожидания авторизации истекло.")
                return self.all_items

            logging.info("✅ Авторизация обнаружена! Начинаю автоматическую прокрутку истории...")

            # Авто-скроллинг до конца страницы
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.5)  # Ждем подгрузки новых чеков
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    time.sleep(2)  # Двойная проверка на случай медленного интернета
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                last_height = new_height

            logging.info("✅ Прокрутка завершена. Начинаю сбор данных...")

            # --- 3. ЗАБИРАЕМ ПЕРЕХВАЧЕННЫЕ ОТВЕТЫ И ЗАГОЛОВКИ ---
            driver.set_script_timeout(10)
            raw_payloads = driver.execute_script("return window.__MAGNIT_TRANSACTIONS__ || [];")
            stolen_headers = driver.execute_script("return window.__MAGNIT_HEADERS__ || {};")

            if not raw_payloads:
                logging.error("❌ МАГНИТ: Не удалось перехватить сетевые ответы. "
                              "Возможно, история пуста или формат API изменился.")
                return self.all_items

            # --- 4. ИЗВЛЕКАЕМ ID ПОКУПОК ИЗ monthlyTotals ---
            transaction_ids = []
            seen_ids = set()

            for entry in raw_payloads:
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if not isinstance(data, dict):
                    continue

                monthly_totals = data.get("monthlyTotals")
                if not isinstance(monthly_totals, list):
                    continue

                for month_block in monthly_totals:
                    if not isinstance(month_block, dict):
                        continue
                    transactions = month_block.get("transactions") or []
                    if not isinstance(transactions, list):
                        continue

                    for tr in transactions:
                        if not isinstance(tr, dict):
                            continue

                        tr_type = tr.get("type")
                        header = tr.get("header")

                        if tr_type != "purchase" and header != "Покупка":
                            continue

                        tr_id = tr.get("id")
                        if tr_id is None:
                            continue

                        if tr_id in seen_ids:
                            continue
                        seen_ids.add(tr_id)
                        transaction_ids.append(tr_id)

            self.transaction_ids = transaction_ids

            if not transaction_ids:
                logging.error("❌ МАГНИТ: Не найдено ни одной транзакции типа 'purchase' в monthlyTotals.")
                return self.all_items

            if not isinstance(stolen_headers, dict) or not stolen_headers:
                logging.warning("⚠️ МАГНИТ: Не удалось перехватить заголовки /webgate/. "
                                "Запросы детализации могут вернуть 401/403.")

            logging.info(f"🔎 МАГНИТ ({self.owner}): Найдено {len(transaction_ids)} покупок, загружаю детализацию чеков...")

            def to_float(val):
                if val is None:
                    return 0.0
                try:
                    num = float(val)
                    if num > 1000:
                        return num / 100.0
                    return num
                except Exception:
                    s = str(val)
                    cleaned = ""
                    for ch in s:
                        if ch.isdigit() or ch in ",.":
                            cleaned += ch
                    if not cleaned:
                        return 0.0
                    cleaned = cleaned.replace(" ", "").replace(",", ".")
                    try:
                        return float(cleaned)
                    except Exception:
                        return 0.0

            # --- 5. ДЕТАЛИЗАЦИЯ КАЖДОЙ ПОКУПКИ ЧЕРЕЗ /webgate/v2/... В КОНТЕКСТЕ БРАУЗЕРА ---
            driver.set_script_timeout(30)

            fetch_script = """
                var tr_id = arguments[0];
                var callback = arguments[arguments.length - 1];
                var headers = window.__MAGNIT_HEADERS__ || {};
                fetch('https://magnit.ru/webgate/v2/user/transactions/' + tr_id, {
                    headers: headers
                })
                .then(function(r) { return r.json(); })
                .then(function(data) { callback(data); })
                .catch(function(e) {
                    callback({error: e && e.toString ? e.toString() : 'unknown error'});
                });
            """

            for tr_id in transaction_ids:
                try:
                    data = driver.execute_async_script(fetch_script, tr_id)
                except Exception as e:
                    logging.error(f"❌ МАГНИТ: Ошибка JS-fetch для транзакции {tr_id}: {e}")
                    self.all_items.append({
                        "Дата": "",
                        "Магазин": "Магнит",
                        "Адрес": "",
                        "Товар": f"Покупка #{tr_id} (без детализации)",
                        "Количество": 1.0,
                        "Цена_за_шт": 0.0,
                        "Сумма": 0.0,
                        "Владелец": self.owner,
                    })
                    time.sleep(1)
                    continue

                if not isinstance(data, dict) or data.get("error"):
                    logging.warning(f"⚠️ МАГНИТ: Ошибка или пустой ответ для транзакции {tr_id}: "
                                    f"{data.get('error') if isinstance(data, dict) else 'некорректный формат'}")
                    self.all_items.append({
                        "Дата": "",
                        "Магазин": "Магнит",
                        "Адрес": "",
                        "Товар": f"Покупка #{tr_id} (без детализации)",
                        "Количество": 1.0,
                        "Цена_за_шт": 0.0,
                        "Сумма": 0.0,
                        "Владелец": self.owner,
                    })
                    time.sleep(1)
                    continue

                payload = data.get("data", data)

                items = []
                if isinstance(payload, dict):
                    items = payload.get("products") or payload.get("items") or payload.get("positions") or []
                elif isinstance(payload, list):
                    items = payload

                # Извлекаем дату и адрес
                date_raw = None
                address = ""
                if isinstance(payload, dict):
                    date_raw = payload.get("dateTime") or payload.get("date") or payload.get("operationDate")
                    address = payload.get("storeAddress") or ""
                if not date_raw:
                    date_raw = data.get("date")
                date = str(date_raw)[:10] if date_raw else ""

                if not items:
                    self.all_items.append({
                        "Дата": date,
                        "Магазин": "Магнит",
                        "Адрес": address,
                        "Товар": f"Покупка #{tr_id} (без детализации)",
                        "Количество": 1.0,
                        "Цена_за_шт": 0.0,
                        "Сумма": 0.0,
                        "Владелец": self.owner,
                    })
                    time.sleep(1)
                    continue

                for it in items:
                    if not isinstance(it, dict):
                        continue

                    name = it.get("name") or it.get("title") or "Товар"
                    qty = it.get("quantity") or it.get("count") or it.get("qty") or 1
                    price = it.get("price") or it.get("itemPrice") or it.get("pricePerItem") or 0
                    total = it.get("amount") or it.get("sum") or it.get("totalPrice") or (price or 0) * (qty or 1)

                    qty_f = to_float(qty)
                    price_f = to_float(price)
                    total_f = to_float(total)

                    self.all_items.append({
                        "Дата": date,
                        "Магазин": "Магнит",
                        "Адрес": address,
                        "Товар": name,
                        "Количество": qty_f,
                        "Цена_за_шт": price_f,
                        "Сумма": total_f,
                        "Владелец": self.owner,
                    })

                time.sleep(1)

            logging.info(
                f"✅ МАГНИТ ({self.owner}): Загружено {len(transaction_ids)} чеков, "
                f"итого {len(self.all_items)} товарных позиций."
            )

        except Exception as e:
            logging.error(f"Ошибка Магнита: {e}")
        finally:
            driver.quit()

        return self.all_items

# ==========================================
# ГЛАВНЫЙ ЦИКЛ (СЕМЕЙНЫЙ МЕНЕДЖЕР)
# ==========================================
if __name__ == "__main__":
    aggregator = DataAggregator()

    def family_loop(parser_class, name):
        while True:
            owner = input(f"\nИмя владельца карты {name} (или Enter для пропуска): ").strip()
            if not owner: break
            
            parser = parser_class(owner)
            if name == "X5 Club": items = parser.fetch_history()
            else: items = parser.run_sync()
            
            aggregator.add_data(items)
            
            cont = input(f"Добавить еще одну карту {name}? (д/н): ").lower()
            if cont != 'д': break

    print("=== НЕЙРОПАРСЕР: СЕМЕЙНАЯ ВЕРСИЯ ===")
    
    # 1. Собираем Пятёрочки
    family_loop(X5AutoParser, "X5 Club")
    
    # 2. Собираем Магниты
    family_loop(MagnitAutoParser, "Магнит")

    # 3. Сохранение
    aggregator.save_to_csv()
