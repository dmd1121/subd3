import random
import string
import pandas as pd
import datetime
from clickhouse_driver import Client

# Подключение к ClickHouse
client = Client(host='localhost', port=9000)

# Функция для генерации случайной строки
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

# Функция для генерации случайной даты
def random_date():
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2022, 1, 1)
    return random.choice(pd.date_range(start=start_date, end=end_date)).date()

# Генерация и вставка случайных данных для таблицы meds
meds_data = []
for _ in range(1000):  # Генерация 1000 записей
    med_id = random.randint(1, 1000)
    med_name = random_string(10)
    manufacturer = random_string(10)
    price = round(random.uniform(10, 1000), 2)
    prescription = random.choice([1, 0])  # ClickHouse не поддерживает тип Bool, используем 1 или 0
    meds_data.append((med_id, med_name, manufacturer, price, prescription))

client.execute("INSERT INTO meds VALUES", meds_data)
print("Данные для таблицы meds успешно сгенерированы и вставлены.")

# Генерация и вставка случайных данных для таблицы customers
customers_data = []
for _ in range(1000):  # Генерация 1000 записей
    cust_id = random.randint(1, 1000)
    cust_name = random_string(10)
    email = f"{cust_name}@example.com"
    password = random_string(8)
    customers_data.append((cust_id, cust_name, email, password))

client.execute("INSERT INTO customers VALUES", customers_data)
print("Данные для таблицы customers успешно сгенерированы и вставлены.")

# Генерация и вставка случайных данных для таблицы orders
orders_data = []
for _ in range(1000):  # Генерация 1000 записей
    med_id = random.randint(1, 1000)
    cust_id = random.randint(1, 1000)
    date = random_date()
    orders_data.append((med_id, cust_id, date))

client.execute("INSERT INTO orders VALUES", orders_data)
print("Данные для таблицы orders успешно сгенерированы и вставлены.")
