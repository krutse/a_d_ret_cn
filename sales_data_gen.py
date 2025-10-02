# Fake Generator   #sales_data_gen.py
from faker import Faker
import csv, os, pathlib

try:
    work_dir = pathlib.Path(__file__).resolve().parent
except NameError:
    work_dir = pathlib.Path.cwd() 

data_dir = pathlib.Path(work_dir / 'data')

if not data_dir.exists():        
    data_dir.mkdir(parents=True, exist_ok=True)    

fake = Faker('ru_RU')

def generate_mm(category):
    mm_chem = ["Универсальный очиститель", "Дезинфицирующий спрей", "Моющий гель", "Средство моющее", "Концентрат очищающий"]
    mm_fabric = ["Подушка на стул", "Полотенце кухонное", "Комплект постельного белья", "Полотенце махровое", "Покрывало стеганое"]
    mm_suits = ["Футболка белая", "Куртка осенняя", "Пальто зимнее", "Пиджак", "Свитер"]
    mm_kitchenware =['Кастрюля', 'Сковорода', 'Кружка', 'Салатник', 'Миска']
    
    if category == 'Бытовая химия':
        return fake.random_element(mm_chem) + ' ' + fake.word(ext_word_list=['0.25 л.', '0.5 л.', '0.75 л.', '1 л.', '5 л.'])
    elif category == 'Текстиль':
        return fake.random_element(mm_fabric) + ' цв. ' + fake.word(ext_word_list=['синий', 'красный', 'зеленый', 'черный', 'белый'])
    elif category == 'Посуда':
        return fake.random_element(mm_kitchenware) + ' ' + fake.word(ext_word_list=['мал.', 'сред.', 'бол.'])
    elif category == 'Одежда':
        return fake.random_element(mm_suits) + ' ' + fake.word(ext_word_list=['муж.', 'жен.', 'дет.'])

def generate_random_sales(cash_num, counts = 10000):
    '''
    cash_num - номер кассы
    counts - количество чеков
    `doc_id` - численно-буквенный идентификатор чека
    `item` - название товара
    `category` - категория товара (бытовая химия, текстиль, посуда и т.д.)
    `amount` - кол-во товара в чеке
    `price` - цена одной позиции без учета скидки
    `discount` - сумма скидки на эту позицию (может быть 0)
    '''
    data =  []
    categories = ['Бытовая химия', 'Текстиль', 'Посуда', 'Одежда']
    for _ in range(counts): 
        lines = fake.random_int(1, 5)  # количество линий в чеке
        doc_id = fake.bothify(text='%###-') + cash_num  # номер чека
        for _ in range(lines):
            category = fake.random_element(categories)
            price = fake.pyfloat(min_value=10, max_value=10000, right_digits=2)
            sale = {
                'doc_id': doc_id,
                'item': generate_mm(category),
                'category': category,
                'amount':  fake.random_int(1, 20), 
                'price': price, 
                'discount': fake.pyfloat(min_value=0, max_value=price * 0.5, right_digits=2) 
                }
            data.append(sale)
    return data

def write_to_csv(data, filename):
    # проверка на существование файла
    if filename.exists():
        filename.unlink()
    fields = data[0].keys()
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields, delimiter=',')
        writer.writeheader()
        writer.writerows(data) 

def files_generator(shop_cnt=10):
    '''
    shop_num # Номер магазина
    cash_num # Номер кассы
    '''
    for i in range(1, shop_cnt+1):  # количество магазинов
        cash_cnt = fake.random_int(1, 10) # Количество касс в магазине
        for c in range(1, cash_cnt+1):
            cash_addr = fake.bothify(text='??', letters = 'ABCDE') # префик кассы
            chk_num = fake.random_int(100, 200) # количество чеков
            data = generate_random_sales(cash_addr + f'-{c}', chk_num) #генерация данных
            filename = data_dir / f'{i}_{c}.csv'
            write_to_csv(data, filename)

if __name__ == '__main__':
    files_generator()
