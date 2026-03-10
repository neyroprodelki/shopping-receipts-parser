import time
import json
import csv
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class X5SessionManager:
    def __init__(self, session_file="x5_session.json"):
        self.session_file = session_file
        self.cookies = {}

    def load_cookies(self) -> str:
        if os.path.exists(self.session_file):
            with open(self.session_file, 'r', encoding='utf-8') as f:
                self.cookies = json.load(f)
                return self._build_cookie_string()
        return ""

    def save_cookies(self, selenium_cookies):
        for cookie in selenium_cookies:
            self.cookies[cookie['name']] = cookie['value']
        with open(self.session_file, 'w', encoding='utf-8') as f:
            json.dump(self.cookies, f)

    def _build_cookie_string(self) -> str:
        return "; ".join([f"{k}={v}" for k, v in self.cookies.items()])

    def get_fresh_token(self, force_login=False):
        logging.info("🤖 Запускаю браузер для получения/обновления ключей...")
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        driver = webdriver.Chrome(options=options)
        
        try:
            driver.get("https://x5club.ru/lk/")
            if not force_login and self.cookies:
                for name, value in self.cookies.items():
                    driver.add_cookie({'name': name, 'value': value})
                driver.refresh()
                time.sleep(3)

            logging.info("⏳ Ожидание авторизации... (введите СМС, если нужно)")
            WebDriverWait(driver, 60).until(lambda d: "auth" not in d.current_url)
            time.sleep(5) 
            self.save_cookies(driver.get_cookies())
            logging.info("✅ Ключи успешно перехвачены!")
            return self._build_cookie_string()
        finally:
            driver.quit()

class X5AutoParser:
    def __init__(self):
        self.session_manager = X5SessionManager()
        self.session = requests.Session()
        self.all_items = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }

    def update_auth(self, force_login=False):
        cookie_str = self.session_manager.get_fresh_token(force_login)
        self.headers["Cookie"] = cookie_str
        self.session.headers.update(self.headers)

    def decode_graph(self, raw_text: str):
        try:
            data = json.loads(raw_text)
        except Exception:
            return []

        if not data: return []
        flat_array = data[0] if isinstance(data[0], list) else data
        if isinstance(flat_array, list) and len(flat_array) > 0 and isinstance(flat_array[0], list):
            flat_array = flat_array[0]
            
        # Детектор протухшего токена
        for item in flat_array:
            if isinstance(item, str) and "Token expired" in item:
                return "EXPIRED"

        def resolve(index, visited=None):
            if visited is None: visited = set()
            if index in visited or type(index) is not int or index < 0 or index >= len(flat_array): return None
            visited.add(index)
            item = flat_array[index]
            if isinstance(item, dict):
                res = {}
                for k, v in item.items():
                    if k.startswith('_'):
                        key_idx = int(k[1:])
                        key_str = flat_array[key_idx] if key_idx < len(flat_array) else k
                        if isinstance(v, int): res[key_str] = resolve(v, visited.copy()) if v >= 0 else None
                        elif isinstance(v, list): res[key_str] = [resolve(i, visited.copy()) for i in v if isinstance(i, int)]
                        else: res[key_str] = v
                return res
            elif isinstance(item, list):
                return [resolve(i, visited.copy()) for i in item if isinstance(i, int)]
            return item

        tree = resolve(0)
        
        # Ищем чеки и товары внутри графа
        parsed_items = []
        def find_receipts(node):
            if isinstance(node, dict):
                if 'rtlTxnId' in node and 'items' in node and isinstance(node['items'], list):
                    r_date = node.get('created', 'n/a')[:10]
                    r_store = node.get('title', 'X5 Club')
                    for product in node['items']:
                        if isinstance(product, dict) and 'priceRegular' in product:
                            try:
                                qty = float(product.get('quantity', 1))
                                price = float(product.get('priceItem', product.get('priceRegular', 0)))
                                parsed_items.append({
                                    'Дата': r_date,
                                    'Магазин': r_store,
                                    'Товар': product.get('name', 'Неизвестный товар'),
                                    'Количество': qty,
                                    'Цена_за_шт': price,
                                    'Сумма': round(qty * price, 2)
                                })
                            except (ValueError, TypeError): pass
                else:
                    for v in node.values(): find_receipts(v)
            elif isinstance(node, list):
                for item in node: find_receipts(item)
                
        find_receipts(tree)
        return parsed_items

    def fetch_month(self, start_date, end_date) -> bool:
        logging.info(f"📅 Скачиваю чеки за: {start_date} - {end_date}")
        params = {"type": "receipts", "startDate": start_date, "endDate": end_date, "page": "0", "codeTc": "all"}
        response = self.session.get("https://x5club.ru/popup/receiptsAndPointsDetailsPopup.data", params=params)
        
        result = self.decode_graph(response.text)
        
        if result == "EXPIRED" or response.status_code in [401, 403]:
            logging.warning("⚠️ Токен протух! Запускаю авто-продление...")
            self.update_auth(force_login=False)
            return False 
            
        if isinstance(result, list):
            self.all_items.extend(result)
            logging.info(f"✅ Успех: Найдено {len(result)} товаров.")
            return True
            
        return True

    def save_to_csv(self, filename="my_yearly_shopping.csv"):
        if not self.all_items:
            logging.warning("Покупки не найдены, файл не создан.")
            return
        keys = self.all_items[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8-sig') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(self.all_items)
        logging.info(f"💾 ВСЁ ГОТОВО! Сохранено {len(self.all_items)} покупок в файл: {filename}")

    def run_yearly_sync(self, months_back=12):
        cookie_str = self.session_manager.load_cookies()
        if not cookie_str:
            self.update_auth(force_login=True)
        else:
            self.headers["Cookie"] = cookie_str
            self.session.headers.update(self.headers)

        today = datetime.now()
        for i in range(months_back):
            start_date = (today - timedelta(days=30*(i+1))).strftime("%Y-%m-%d")
            end_date = (today - timedelta(days=30*i)).strftime("%Y-%m-%d")
            
            success = False
            retries = 0
            while not success and retries < 3:
                success = self.fetch_month(start_date, end_date)
                if not success:
                    retries += 1
                    time.sleep(2)
        
        self.save_to_csv()

if __name__ == "__main__":
    print("Привет! Добро пожаловать в мой канал НЕЙРОПРОДЕЛКИ t.me/neyroprodelki! Меня зовут Тигран и я рад, что созданная мной программа, может быть полезна не только мне но и тебе. Приятного использования!")
    parser = X5AutoParser()
    # Собираем данные за последние 12 месяцев (можете поменять цифру)
    parser.run_yearly_sync(months_back=12)
