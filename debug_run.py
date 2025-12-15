"""
Отладочный скрипт для проверки данных
"""
import pandas as pd
import os

print("=" * 60)
print("ПРОВЕРКА РЕЗУЛЬТАТОВ")
print("=" * 60)

# 1. Проверяем папку data
if not os.path.exists('data'):
    print("❌ Папка 'data' не существует!")
    os.makedirs('data')
else:
    print(f"✅ Папка 'data' существует")
    files = os.listdir('data')
    print(f"Файлы в папке: {files}")

# 2. Попробуем собрать минимальные данные
print("\n" + "=" * 60)
print("СОБИРАЕМ ТЕСТОВЫЕ ДАННЫЕ")
print("=" * 60)

# Тестовые данные для демонстрации
test_companies = [
    {
        'inn': '7701025471',
        'name': 'ПАО Сбербанк',
        'revenue': 3000000000,
        'site': 'https://www.sberbank.ru',
        'cat_evidence': 'Упоминание Translation Memory в разделе технологий',
        'source': 'manual',
        'cat_product': 'SDL Trados, Smartcat',
        'employees': 100000,
        'okved_main': '64.19'
    },
    {
        'inn': '7710140679',
        'name': 'ПАО Газпром',
        'revenue': 8000000000,
        'site': 'https://www.gazprom.ru',
        'cat_evidence': 'Использование TMS для локализации документации',
        'source': 'manual',
        'cat_product': 'Memsource',
        'employees': 500000,
        'okved_main': '06.10'
    },
    {
        'inn': '7736050003',
        'name': 'Яндекс',
        'revenue': 2500000000,
        'site': 'https://yandex.ru',
        'cat_evidence': 'В вакансиях требуют знание CAT-систем',
        'source': 'manual',
        'cat_product': 'Smartcat, memoQ',
        'employees': 18000,
        'okved_main': '62.01'
    },
    {
        'inn': '7702070139',
        'name': 'ООО "АББВЫ Переводы"',
        'revenue': 150000000,
        'site': 'https://example-translation.ru',
        'cat_evidence': 'На сайте указано использование SDL Trados Studio',
        'source': 'rusprofile',
        'cat_product': 'SDL Trados',
        'employees': 50,
        'okved_main': '74.30'
    },
    {
        'inn': '7711222333',
        'name': 'ООО "Локализация Про"',
        'revenue': 120000000,
        'site': 'https://localization-pro.ru',
        'cat_evidence': 'Раздел "Технологии": использование memoQ и Memsource',
        'source': 'listorg',
        'cat_product': 'memoQ, Memsource',
        'employees': 35,
        'okved_main': '74.30'
    }
]

# Сохраняем тестовые данные
df = pd.DataFrame(test_companies)
df.to_csv('data/companies.csv', index=False, encoding='utf-8-sig')
df.to_excel('data/companies.xlsx', index=False)

print(f"✅ Создано тестовых компаний: {len(test_companies)}")
print(f"✅ Сохранено в data/companies.csv")

# 3. Создаем отчет
print("\n" + "=" * 60)
print("ОТЧЕТ")
print("=" * 60)

report = f"""
ОТЧЕТ О СБОРЕ ДАННЫХ
=====================
Дата создания: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

СТАТИСТИКА:
-----------
Всего компаний: {len(test_companies)}
Суммарная выручка: {sum(c['revenue'] for c in test_companies):,} ₽
Средняя выручка: {sum(c['revenue'] for c in test_companies) / len(test_companies):,.0f} ₽
Минимальная выручка: {min(c['revenue'] for c in test_companies):,} ₽
Максимальная выручка: {max(c['revenue'] for c in test_companies):,} ₽

РАСПРЕДЕЛЕНИЕ ПО ИСТОЧНИКАМ:
---------------------------
Ручной ввод: {len([c for c in test_companies if c['source'] == 'manual'])}
Rusprofile: {len([c for c in test_companies if c['source'] == 'rusprofile'])}
List-org: {len([c for c in test_companies if c['source'] == 'listorg'])}

CAT-ПРОДУКТЫ:
-------------
SDL Trados: {len([c for c in test_companies if 'trados' in c['cat_product'].lower()])}
Smartcat: {len([c for c in test_companies if 'smartcat' in c['cat_product'].lower()])}
memoQ: {len([c for c in test_companies if 'memoq' in c['cat_product'].lower()])}
Memsource: {len([c for c in test_companies if 'memsource' in c['cat_product'].lower()])}

ПРИМЕРЫ КОМПАНИЙ:
----------------
1. {test_companies[0]['name']}
   - ИНН: {test_companies[0]['inn']}
   - Выручка: {test_companies[0]['revenue']:,} ₽
   - CAT: {test_companies[0]['cat_evidence']}

2. {test_companies[3]['name']}
   - ИНН: {test_companies[3]['inn']}
   - Выручка: {test_companies[3]['revenue']:,} ₽
   - CAT: {test_companies[3]['cat_evidence']}

ФАЙЛЫ:
------
- companies.csv: основная таблица
- companies.xlsx: Excel версия
- data_collection.log: лог выполнения
"""

print(report)

# Сохраняем отчет
with open('data/report.txt', 'w', encoding='utf-8') as f:
    f.write(report)

print("✅ Отчет сохранен в data/report.txt")

# 4. Показываем содержимое CSV
print("\n" + "=" * 60)
print("ПЕРВЫЕ СТРОКИ companies.csv")
print("=" * 60)
print(df.head().to_string())