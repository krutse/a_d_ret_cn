#sales_data_loader.py

import pathlib, csv, logging
import psycopg2 as pg
import configparser
from datetime import datetime, timedelta

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
        raise Exception(f'Секция {section} не найдена в файле {filename}')
    return config

def drop_logging():
    logger = logging.getLogger() # получение корневого объекта logger
    # Удаляем все предыдущие обработчики
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
        # Также удаляем обработчики у всех дочерних логгеров
        for handler in logger.handlers:
            if handler in logger.handlers:
                logger.removeHandler(handler)

def start_logging(work_path): 
    log_path = work_path / 'log' # подкаталог log 
    # создаём папку если её нет
    if not log_path.exists():
        log_path.mkdir()

    dt = datetime.now().date()
    log_file = f'log{datetime.strftime(dt, '%Y%m%d')}.log'
    date_10d = (dt - timedelta(days = 10)) # дата проверки
    # настройка логирования
    logging.basicConfig(level=logging.INFO, filename=log_path / log_file, filemode="a", encoding='utf-8', format='%(asctime)s: %(name)s - %(levelname)s: %(message)s')
    logging.info("===Start logger ====================================")

    # получаем список log файлов, проверяем дату создания и удаляем если старше ...
    filelist = log_path.glob('*.log')  
    for file in filelist:
        file_crtime = datetime.fromtimestamp(pathlib.Path(file).stat().st_birthtime).date()
        if file_crtime < date_10d:
            try:
                file.unlink()
                logging.info(f"Лог файл {file} : дата создания - {file_crtime} удален")
            except Exception as err:
                logging.error(f'Ошибка удаления лог файла {file}')


# основной скрипт
dt = datetime.now().date() - timedelta(days = 1) # предполагаем что загружаем данные за вчерашний день

try:
    work_dir = pathlib.Path(__file__).resolve().parent
except NameError:
    work_dir = pathlib.Path.cwd()

data_dir = pathlib.Path(work_dir / 'data')

db_cfg = load_config(filename=work_dir / 'config.ini')

drop_logging()
start_logging(work_dir)  

logging.info('Начало работы скрипта загрузки данных продаж.')

query_s = 'insert into sales values(%s, %s, %s, %s, %s, %s, %s, %s, %s)'

if data_dir.exists():
    try:
        with pg.connect(
                host=db_cfg['host'],
                port=db_cfg['port'],
                database=db_cfg['database'],
                user=db_cfg['user'],
                password=db_cfg['password'],
                ) as dbc:
            cursor = dbc.cursor()
            for file in data_dir.glob('[1-9]*_*[0-9].csv'):   
                # чтение файла и запись в БД
                fn = file.stem.split('_')  
                if len(fn) == 2:
                    logging.info(f'Обработка файла: {file.name}')
                    shop_num = fn[0]
                    cash_num = fn[1]
                    try:
                        with open(file, 'r') as f:
                            y = [shop_num, cash_num, dt.strftime('%Y-%m-%d') ]    
                            reader = csv.reader(f, delimiter=',')
                            header = next(reader)
                            # проверить заголовки столбцов
                            if header == ['doc_id', 'item', 'category', 'amount', 'price', 'discount']:
                                data = [tuple(y + row) for row in reader]     
                                data_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in data)   
                                query = 'insert into sales values' +  data_str 
                                # Запись в БД
                                fl = file  
                                by_line = False
                                try:
                                    cursor.execute(query)  
                                    dbc.commit()
                                except Exception as err:
                                    by_line = True
                                    fl = None  
                                    logging.error(f'Ошибка массовой загрузки файла {file.name}: {repr(err)}') 
                                    cursor.execute('ROLLBACK;')
                                else:
                                    logging.info(f'Массовая загрузка файла {file.name} в БД завершена')       
                                if by_line:
                                    fl = file
                                    logging.info(f'Начало построчной загрузки файла {file.name} в БД')
                                    for i, ln in enumerate(data):
                                        try:
                                            cursor.execute(query_s, ln)
                                            dbc.commit()    
                                        except Exception as err:
                                            fl = None
                                            logging.error(f'Ошибка загрузки строки {i} файла {file.name}: {repr(err)}')
                                            cursor.execute('ROLLBACK;')
                                    logging.info(f'Окончание построчной загрузки файла {file.name} в БД')
                            else:
                                logging.warning(f'Заголовок файла {file.name} не соответствует формату. Обработка пропущена')                
                    except Exception as err:
                        logging.error(f'Ошибка открытия файла {file.name}: {repr(err)}')                    
                # удаление файла
                if fl:
                    try:
                        fl.unlink()
                        logging.info(f'Файл {fl.name} удален')
                    except Exception as err:
                        logging.error(f'Ошибка удаления файла: {fl.name}, ошибка: {repr(err)}')    
    except Exception as err:
        logging.error(f'Ошибка подключения к БД: {repr(err)}')                
else:
    logging.info('Отсутствуют файлы для обработки')
logging.info('Завершение работы скрипта загрузки данных продаж.')       
drop_logging() 