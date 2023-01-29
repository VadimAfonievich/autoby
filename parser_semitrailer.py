# -*- coding: UTF-8 -*-
# requirements: mysql-connector-python, requests, requests[socks], pillow

import re
import requests
import math
import concurrent.futures
import os
from io import BytesIO
from PIL import Image
import time
import datetime
import uuid
import mysql.connector as mariadb
import argparse
import sys


UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}

# ssh -fgN -L 3306:127.0.0.1:3306 root@176.57.184.131
DB = mariadb.connect(
        user="autoby_by",
        password="7cxSCC7xrAPFZD7A",
        host="localhost",
        port=3306,
        database="autoby_by"
    )

PROXIES = {}
HOST = "https://www.mobile.de/ru/"
TIMEOUT = 20  # таймаут запросов в секундах
PHOTO_DIR = "/www/wwwroot/autoby/frontend/web/uploads"
CROP_DIRS = ['original', '1300x0', '700x0', '600x0', '400x0', '300x0', '200x0', '100x0']


def create_parser():
    parser = argparse.ArgumentParser(
        prog='parser_semitrailer.py',
    )
    subparsers = parser.add_subparsers(dest='command',
                                       title='Возможные аргументы',
                                       description='%(prog)s [options]')
    parser.add_argument('-s', '--start', default=30000, type=int, help="Нижняя цена поиска")
    parser.add_argument('-e', '--end', default=9999999, type=int, help="Верхняя цена поиска")
    parser.add_argument('-p', '--proxy', default='', type=str, help="Прокси сервер socks5 ip:port")
    parser.add_argument('-t', '--type', default=2, type=int, help="1 - первичный сбор(по умолчанию) "
                                                                  "2 - проверка новых объявлений у донора"
                                                                  "3 - проверка объявлений у донора (удаление)")

    return parser


def timed(func):
    """
    records approximate durations of function calls
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        print('{name:<30} started'.format(name=func.__name__))
        result = func(*args, **kwargs)
        duration = "{name:<30} finished in {elapsed:.2f} seconds".format(
            name=func.__name__, elapsed=time.time() - start
        )
        print(duration)
        return result
    return wrapper


class ParsProperty:
    """
    Используется для приведения значений данных донора к необходимому формату имеющихся таблиц
    """
    def __init__(self, regex=None, model_name=None, replace_array=None, lower_case=None, to_digit=None):
        self.regex = regex
        self.model_name = model_name
        self.replace_array = replace_array
        self.lower_case = lower_case
        self.to_digit = to_digit
        

class Car:
    """
    Модель автомобиля. Парсинг и приведение полученных данных к структуре БД сайта.
    """
    exclude_keys = ["title"]
    def __init__(self, page_content):
        # Значения по умолчанию
        self.donor_id = None
        self.title = None
        self.description_de = None
        self.year = datetime.date.today().year
        self.gvw = None
        self.load_max = None
        self.length = None
        self.width = None
        self.height = None
        self.category = None
        self.volume = None
        self.price_original = None
        self.is_load_photo = 0
        self.status_key = 1
        self.currency = 1
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()

        property_patterns = {
            'id': None,
            'donor_id': ParsProperty(r'data-vehicle-id="(.+?)"'),
            'title': ParsProperty(r'<h1 class="h2 g-col-8">(.+?)</h1>', replace_array={r"&quot;": '"'}),
            'description_de': ParsProperty(r'<div class="g-row"><div class="description-text js-original-description g-col-12">(.+?)</div>',
                                           replace_array={r"</li>\s*<li>": "\n",
                                                          r"&quot;": '"',
                                                          r"&amp;": " ",
                                                          r"</\w+>|<\w+>": ""}),
            'year': ParsProperty(r'u-text-bold">\d\d/(\d\d\d\d)<'),
            'gvw': ParsProperty(r'GVW\)</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)..</\w+>', to_digit=True),
            'load_max': ParsProperty(r'Мосты</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)</\w+>', to_digit=True),
            'length': ParsProperty(r'Длина кузова</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)..</\w+>', to_digit=True),
            'width': ParsProperty(r'Ширина кузова</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)..</\w+>', to_digit=True),
            'height': ParsProperty(r'Высота кузова</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)..</\w+>', to_digit=True),
            'category': ParsProperty(r'Категория</\w+><\w+ class="g-col-6 u-text-bold">.+?</\w+>.+?Категория</\w+><\w+ class="g-col-6 u-text-bold">(.+?)</\w+>', to_digit=True),
            'volume': ParsProperty(r'Объем грузового пространства</\w+><\w+ class="g-col-6 u-text-bold">([0-9\s]+)..</\w+>', to_digit=True),
            'price_original': ParsProperty(r'<\w+ class="netto-price">([0-9\s]+)', to_digit=True),
            'is_load_photo': None,
            'status_key': None,
            'created_at': None,
            'updated_at': None,
        }

        for key, prop in property_patterns.items():
            if not prop:
                continue

            value = None
            m = re.search(prop.regex, page_content)
            if m:
                value = m.group(1)
                value = re.sub(r"%20", " ", value)

                if prop.replace_array and value:
                    for old, new in prop.replace_array.items():
                        value = re.sub(old, new, value)

                if prop.lower_case:
                    value = value.lower()

                if prop.model_name and value:
                    class_name, value_name = prop.model_name
                    model = class_name()
                    for k in self.__dict__.keys():
                        if hasattr(model, k) and k not in self.exclude_keys:
                            setattr(model, k, getattr(self, k))

                    prop_id = model.from_db(value, value_name)
                    value = prop_id

                if prop.to_digit:
                    strip_v = re.sub(r"\s", "", value)
                    if strip_v.isdigit():
                        value = int(strip_v)
            if value is not None:
                setattr(self, key, value)
        DB.commit()


class BaseModel:
    """
    Базовая модель
    """
    table_name = None

    def from_db(self, value, value_name):
        table_name = str(self.table_name)
        del self.table_name
        values = [value]
        names = []
        where = ''
        if type(value_name) is list:
            names = value_name[1:]
            value_name = value_name[0]
            where = f'{value_name}=%s and '
            for n in names:
                values.append(getattr(self, n))
                where += f'{n}=%s and '
        else:
            where = f'{value_name}=%s'

        cursor = DB.cursor()
        query = f"SELECT id FROM {table_name} WHERE {where.rstrip(' and ')} LIMIT 1"
        cursor.execute(query, values)
        row = cursor.fetchone()
        if row:
            (mid, ) = row
            cursor.close()
            return mid
        else:
            setattr(self, value_name, value)

            for k, v in self.__dict__.items():
                if str(v).startswith('%!%('):
                    atr = getattr(self, v.replace('%!%(', '').replace(')', ''))
                    atr = atr.replace('_', ' ').title()
                    setattr(self, k, atr)

            keys = self.__dict__.keys()
            values = self.__dict__.values()
            qa = ['%s' if v is not None else 'NULL' for v in values]
            query = f"INSERT INTO {table_name} ({', '.join([f'`{k}`' for k in keys])}) VALUES ({', '.join(qa)})"

            out = []
            for v in values:
                if v is not None:
                    out.append(v)

            cursor.execute(query, out)
            row_id = cursor.lastrowid
            cursor.close()
            return row_id


class PhotoModel(BaseModel):
    def __int__(self):
        self.table_name = "ab_trailer_photo"

        # self.id = None
        self.user_id = 1
        self.post_id = None
        self.uid = None
        self.image = None
        self.position = None
        self.is_main = None
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()
        self.is_webp = None


def create_folder(photo_id: int):
    """
    Создает логику формирования новых папок для фотографий машин
    """
    n = int(photo_id)
    c = int(n / 125000000)
    a = int((n - c * 125000000)/250000)
    b = int((n - a * 250000 - c * 125000000) / 500)

    return f"{c}/{a}/{b}"


@timed
def imgs_crawler(page_content, bull_id, post_id):
    cursor = DB.cursor()
    """
    Загружает фотографии автомобилей в многопотоке по id объявления
    """
    img_links = re.findall(r'<div class="gallery-bg js-gallery-img js-load-on-demand" data-src="(.+?)"', page_content)
    if len(img_links) == 0:
        return

    print(f'{bull_id} - {len(img_links)} Photos')
    futures_list = []
    is_load = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        for key, img_link in enumerate(img_links):
            futures = executor.submit(load_img, img_link, key, CROP_DIRS, TIMEOUT)
            futures_list.append(futures)

        for future in futures_list:
            try:
                result = future.result(timeout=60)

                img_name, thumbnails, is_main = result

                if not img_name:
                    continue

                img_uid = uuid.uuid4()
                query = f"INSERT INTO ab_trailer_photo (user_id, post_id, uid, image, position, is_main, " \
                        f"created_at, updated_at, is_webp) VALUES (1, {post_id}, '{img_uid}', '{img_name}', 0, {is_main}, " \
                        f"CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)"
                cursor.execute(query)
                photo_id = cursor.lastrowid
                cab = create_folder(photo_id)

                for d, thumb in thumbnails.items():
                    full_dir = f'{PHOTO_DIR}/{d}/post_photo/image/{cab}'
                    os.makedirs(f'{full_dir}', exist_ok=True)
                    thumb.save(f'{full_dir}/{img_name}')

                is_load = True
            except Exception:
                # print(e)
                continue

        if is_load:
            query = f"UPDATE ab_trailer_post SET is_load_photo=1 WHERE id=%s;"
            cursor.execute(query, (post_id, ))

        DB.commit()
    cursor.close()


def load_img(img_link, key_id, crop_dirs, timeout=10):
    """
    Скачивает фото по ссылке
    """
    try:
        r = requests.get(img_link, headers=UA, timeout=timeout, proxies=PROXIES)
        thumbnails = {}
        if r.status_code == 200:

            img = Image.open(BytesIO(r.content))
            orig = crop_dirs[0]
            thumbnails[orig] = img
            for d in crop_dirs[1:]:

                size = int(d.split('x')[0])
                img.thumbnail(size=(size, size))
                thumbnails[d] = img.copy()

            img_name = img_link.split("/")[-1]
            img_name = img_name.replace("?rule=mo-1024", "")
            return img_name, thumbnails, 1 if key_id == 0 else 0
    except Exception:
        pass

    return None, None, None


def crawl_search(start_price: int, end_price=9999999, check_new: bool = False):
    """
    По заданным параметрам (цена) выгружает данные из поиска
    """
    add_count = 0

    if check_new:
        # Поиск с сортировкой от новых к старым
        link = f"{HOST}%D0%BA%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F/%D0%BF%D0%BE%D0%BB%D1%83%D0%BF%D1%80%D0%B8%D1%86%D0%B5%D0%BF/" \
               f"vhc:semitrailer,pgn:1,pgs:50,srt:date,sro:desc,frn:2022,prn:{start_price},dmg:false,vat:1"
    else:
        # Поиск с сортировкой по цене от дешевых к дорогим
        link = f"{HOST}%D0%BA%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F/%D0%BF%D0%BE%D0%BB%D1%83%D0%BF%D1%80%D0%B8%D1%86%D0%B5%D0%BF/" \
               f"vhc:semitrailer,pgn:1,pgs:50,srt:date,sro:asc,frn:2022,prn:{start_price},dmg:false,vat:1"

    r = None
    page_count = 0
    try:
        r = requests.get(link, headers=UA, proxies=PROXIES)
        if r.status_code == 403:
            print(f"\n Нас забанили! \n")
            exit(0)
        results = re.search(r'data-result-count="(.+?)"', r.text)
        pgc = re.sub(r"\D", "", results.group(1))
        page_count = math.ceil(int(pgc) / 50)
    except Exception:
        pass

    if page_count == 0:
        print('Закончили!')
        exit(0)

    page_count = page_count if page_count < 40 else 40

    for p in range(1, page_count+1):
        already_exists = 0
        print("===============================================================")
        print(f"Обрабатываю страницу № {p}")
        err_cnt = 0
        cars_blocks = []
        try:
            r = requests.get(link.replace(f"pgn:1", f"pgn:{p}"), timeout=TIMEOUT, headers=UA, proxies=PROXIES)
            if r.status_code == 403:
                print(f"\n Нас забанили! \n")
                exit(0)
            cars_blocks = re.findall(r'<article class="list-entry g-row(.+?)</article>', r.text)
        except Exception:
            pass

        print(f"Кол-во машин на странице {len(cars_blocks)}")

        for b in cars_blocks:
            bull_id = re.search(r'data-vehicle-id="(\d+)"', b).group(1)

            print(f"Обрабатываю объявление № {bull_id}")

            try:
                print()
                add = crawl_bull(bull_id)
                if check_new and not add:
                    already_exists += 1
                    if already_exists >= 20:  # Если 20 объявлений подряд были в базе останавливаем сбор
                        print()
                        print(f"{datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}: Добавлено {add_count} объявлений")
                        exit(0)
                elif add:
                    already_exists = 0
                    add_count += 1


            except Exception as ex:
                print(f"ERROR: _____________________________________ {ex}")
                print("Не удалось обработать объявление ^^^ ")
                err_cnt += 1
                if err_cnt >= 4:
                    print("Превышено кол-во ошибок!")
                    print(f"Страница:{p} , Диапазон цены: {start_price} - {end_price}")
                    exit(0)
            print()

    matches = re.findall(r'seller-currency u-text-bold">([\d\s]+)', r.text)
    last_prc = int(re.sub(r'\D', '', matches[-1])) if matches else 0
    return last_prc


@timed
def crawl_bull(bull_id):
    """
    Выгружает информацию из страницы объявления донора
    """
    cursor = DB.cursor()
    cursor.execute(f"SELECT id FROM ab_trailer_post WHERE donor_id=%s LIMIT 1", (bull_id,))
    row = cursor.fetchone()
    if row:
        (value,) = row
        print(f"Есть в базе id = {value}")
        cursor.close()
        return False

    link_pattern = f"{HOST}vip/0/pg:vipsemitrailer/%s.html"

    car_link = link_pattern % bull_id
    r = requests.get(car_link, timeout=TIMEOUT, headers=UA, proxies=PROXIES)
    if r.status_code == 403:
        print(f"\n Нас забанили! \n")
        exit(0)

    if r.status_code == 200:
        car = Car(page_content=r.text)
        keys = car.__dict__.keys()
        values = car.__dict__.values()

        qa = ['%s' if v is not None else 'NULL' for v in values]
        query = f"INSERT INTO ab_trailer_post ({', '.join([f'`{k}`' for k in keys])}) VALUES ({', '.join(qa)})"
        out = []
        for v in values:
            if v is not None:
                out.append(v)

        cursor.execute(query, out)
        post_id = cursor.lastrowid
        if car.price_original:
            query = f"INSERT INTO ab_trailer_price (price, post_id, currency) VALUES (%s, %s, %s)"
            cursor.execute(query, (car.price_original, post_id, car.currency))

        DB.commit()
        cursor.close()
        imgs_crawler(r.text, bull_id, post_id)
        return True

    return False


#@timed
def check_bull(bull_id):
    """
    Проверяет актуальность объявления у донора
    """
    link_pattern = f"{HOST}ajax/vehiclePreview/vhc:semitrailer/%s"  # меньше данных подходит для проверки актуальности         # TODO: разобарться со ссылкой во всех 3 парсерах
    car_link = link_pattern % bull_id
    r = requests.get(car_link, timeout=TIMEOUT, headers=UA, proxies=PROXIES)
    if r.status_code == 403:
        print(f"\n Нас забанили! \n")
        exit(0)

    if r.status_code == 404:
        return False
    if r.status_code == 200:
        return True

    return True     # Если неизвестный код, то не затираем объявление


if __name__ == '__main__':
    # import http.client as http_client
    # http_client.HTTPConnection.debuglevel = 1

    namespace = create_parser().parse_args(sys.argv[1:])
    print(namespace)
    if namespace.proxy:
        PROXIES = {'http': f"socks5://{namespace.proxy}", 'https': f"socks5://{namespace.proxy}"}

    try:
        r = requests.get('https://ifconfig.co', proxies=PROXIES)
        m = re.search(r'<td>(\d+\.\d+\.\d+\.\d+)</td>', r.text)
        print(f'IP: {m.group(1)}')
        print()
    except Exception as e:
        print("Умер прокси!")
        exit(1)

    if namespace.type == 1:     # первичный сбор: parser_semitrailer.py -t 1 -s 50000 -e 50000 -p 127.0.0.1:9988
        start_price = namespace.start
        last_price = namespace.end
        while start_price < namespace.end:
            print()
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print(f"Обрабатываю цены {start_price} - {last_price}")
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print()

            last_price = crawl_search(start_price, namespace.end)
            if last_price == 0 or last_price == start_price:
                print('Не смог найти последнюю цену на странице')
                exit(0)
            start_price = last_price

    if namespace.type == 2:     # добавление новых объявлений: parser_semitrailer.py -t 2 -p 127.0.0.1:9988
        start_price = namespace.start
        while start_price < namespace.end:
            print()
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print(f"Запускаю сбор новых объявлений")
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print()

            last_price = crawl_search(start_price, check_new=True)
            if last_price == 0 or last_price == start_price:
                print('Не смог найти последнюю цену на странице')
                exit(0)
            start_price = last_price

    if namespace.type == 3:  # проверка объявлений у донора на удаление: parser_semitrailer.py -t 3 -p user:pass@@host:port
        del_count = 0
        cursor = DB.cursor()
        try:
            rows = [1]
            post_id = 1
            while len(rows) > 0:
                err_cnt = 0
                to_del = []
                cursor.execute('SELECT id, donor_id FROM ab_trailer_post WHERE id > %s ORDER BY id ASC LIMIT 300', (post_id, ))
                rows = cursor.fetchall()
                for row in rows:
                    (post_id, bull_id) = row
                    # print(f"Проверяю post_id = {post_id}    donor_id = {bull_id}")
                    try:
                        bull_exist = check_bull(bull_id)

                        if not bull_exist:
                            print(f"К удалению post_id = {post_id}    donor_id = {bull_id}")
                            to_del.append((post_id, ))
                    except Exception:
                        err_cnt += 1
                        if err_cnt >= 20:
                            print("20 крашей при актуализациии объявлений. Остановка")
                            exit(0)

                if len(to_del) >= 250:
                    print("Слишком много объявлений к удалению!!! Кажется что-то пошло не так!?")
                    exit(0)

                if len(to_del) > 0:
                    for p in to_del:  # удаление фото с сервера
                        cursor.execute('SELECT id, image FROM ab_trailer_photo WHERE post_id = %s', p)
                        i_rows = cursor.fetchall()
                        if i_rows:
                            for photo_id, image in i_rows:
                                cab = create_folder(photo_id)
                                for d in CROP_DIRS:
                                    full_img_path = f'{PHOTO_DIR}/{d}/post_photo/image/{cab}/{image}'
                                    if os.path.isfile(full_img_path):
                                        os.remove(full_img_path)
                                        # print('del ' + full_img_path)

                    cursor.executemany('DELETE FROM ab_trailer_post WHERE id = %s', to_del)
                    cursor.executemany('DELETE FROM ab_trailer_price WHERE post_id = %s', to_del)
                    cursor.executemany('DELETE FROM ab_trailer_photo WHERE post_id = %s', to_del)
                    DB.commit()
                    del_count += len(to_del)

        except Exception as ex:
            print(f'ERROR: {ex}')

        print(f"{datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}: Удалено {del_count} объявлений")
        cursor.close()

DB.close()

