# Создание таблицы в БД

import psycopg2 as pg
import configparser
import pathlib

def load_config(filename='config.ini', section='Database'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    # считываем секцию, секция по умолчанию Database
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Секция {0} не найдена в файле {1} '.format(section, filename))
    return config

work_dir = pathlib.Path(__file__).resolve().parent.parent
db_cfg = load_config(filename=work_dir / 'config.ini')

query = '''
    CREATE TABLE IF NOT EXISTS sales (
    shop_num varchar not null, -- Номер магазина
    cash_num varchar not null, -- Номер кассы
    doc_date timestamp not null, -- Дата чека
    doc_id varchar NOT NULL,      -- Численно-буквенный идентификатор чека
    item varchar NOT NULL,       -- Название товара
    category TEXT NOT NULL,   -- Категория товара
    amount INT NOT NULL,  -- Количество
    price NUMERIC NOT NULL,      -- Цена без скидки
    discount NUMERIC NOT NULL,    -- Сумма скидки
    PRIMARY KEY (shop_num, cash_num, doc_date, doc_id, item, category, amount)
);
'''

with pg.connect(
            host=db_cfg['host'],
            port=db_cfg['port'],
            database=db_cfg['database'],
            user=db_cfg['user'],
            password=db_cfg['password'],
            ) as dbc:
    cursor = dbc.cursor()
    cursor.execute(query)
    dbc.commit()