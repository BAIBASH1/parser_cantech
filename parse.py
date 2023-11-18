import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
import csv
import json
import os.path
import pandas
from tqdm import tqdm
import concurrent.futures
import time
import asyncio
import aiohttp
import lxml
import aiofiles


CONNECTIONS = 25



from requests import ConnectTimeout


def parse_url_goods():
    urls = []
    base_url = 'https://www.santech.ru/catalog'
    response = requests.get(base_url)
    response.raise_for_status()
    main_page_soup = BeautifulSoup(response.text, 'lxml')
    blocks = main_page_soup.find('ul', class_="ss-catalog-menu__menu ss-scrollbar-hide").findAll('li')[1:]
    for block in blocks:
        catalog_urls = block.findAll('div', class_='ss-catalog-menu__submenu-item')
        for url in catalog_urls:
            urls.append(url.find('a').get('href'))

    for url in urls:
        goods_urls = []
        n = 1
        while True:
            params = {
                'page': n
            }
            catalog_url = urljoin(base_url, url)
            response = requests.get(catalog_url, params=params)
            soup = BeautifulSoup(response.text, 'lxml')
            goods_urls_page = [block.find('a').get('href') for block in soup.findAll('div', class_='ss-catalog-product__title')]
            if goods_urls_page:
                goods_urls += [block.find('a').get('href') for block in soup.findAll('div', class_='ss-catalog-product__title')]
                n += 1
                print(n)
                print(catalog_url)
            else:
                break
        with open('finally_goods_urls.txt', 'a') as file:
            for url_product in goods_urls:
                file.write(f'{url_product}\n')


def get_characteristics(soup):
    characteristics = {}
    blocks_html = soup.find('div', class_="ss-mt-20 ss-mt-xl-0").find('div', class_="ss-product-property")
    while True:
        delete_p = soup.find('div', class_="ss-mt-20 ss-mt-xl-0").find('div', class_="ss-product-property").find('div',
                                                                                                                 class_="tip a-inline-block")
        if delete_p:
            deleted_p = delete_p.replace_with("")
        else:
            break
    characteristic_blocks = list(blocks_html.stripped_strings)
    for n, characteristic_block in enumerate(characteristic_blocks[:-1]):
        if n % 2 == 0:
            characteristics[characteristic_block] = characteristic_blocks[n + 1]
    return characteristics


def get_url_other_variables(url):
    base_url = 'https://www.santech.ru/catalog'
    full_url = urljoin(base_url, url)
    response = requests.get(full_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    other_variables = [block.a.get('href') for block in
                       soup.findAll('tr', class_='ss-product-other-variants__no-border-bottom')]
    return other_variables


def get_name(soup):
    name = soup.find('h1', class_='ss-category-title').text.strip()
    return name


def get_price(soup):
    price_block = list(soup.find('div', class_="ss-product-info__price").stripped_strings)
    for_what = price_block[0].strip()
    price = price_block[1].strip()
    return for_what, price


def save_docs(soup):
    docs_block = soup.find('div', id='product-documents')
    docs = docs_block.findAll('div', class_='ss-cert-list__item')
    docs_urls = [url.a.get('href') for url in docs]
    docs_names =[]
    for url in docs_urls:
        hash = urlparse(url).query
        docs_names.append(hash)
        with open(f'docs/{hash}.pdf', 'wb') as file:
            base_url = 'https://www.santech.ru'
            full_url = urljoin(base_url, url)
            response = requests.get(full_url)
            file.write(response.content)
    return docs_names


async def save_photos(soup):
    photo_block = soup.find('div', id="product_slider_full")
    if photo_block.find('div', class_='ss-slider__item--no-drag'):
        photos = photo_block.findAll('div', class_='ss-slider__item')[1:]
    else:
        photos = photo_block.findAll('div', class_='ss-slider__item')
    photos_urls = [block.img.get('src') for block in photos]
    photos_names = [urlparse(url).query for url in photos_urls]
    for url in photos_urls:
        hash = urlparse(url).query
        if not os.path.exists(f'finally_media/{hash}.jpg'):
            base_url = 'https://www.santech.ru'
            full_url = urljoin(base_url, url)
            retries = 0
            async with aiohttp.ClientSession() as session:
                while retries < 5:
                    try:
                        async with session.get(url=full_url) as response:
                            async with aiofiles.open(f'finally_media/{hash}.jpg', 'wb') as file:
                                await file.write(await response.content.read())
                            return photos_names
                    except aiohttp.client_exceptions.ServerDisconnectedError:
                        retries += 1
                        await asyncio.sleep(retries)

def get_availability(soup):
    block = soup.find('div', class_='ss-product-info__box')
    try:
        availability = block.find('div', class_='territory-choose__list-count').text.strip()
    except AttributeError:
        return 'Под заказ'
    print(availability)
    if availability != 'под заказ':
        availability = 'В наличие'
    else:
        availability = 'Под заказ'
    return availability



def write_json(product_inf):
    try:
        with open('finally_result.json', 'r', encoding='utf8') as file:
            parsed_before_inf = json.load(file)
    except Exception:
        parsed_before_inf = []
    all_inf = parsed_before_inf + product_inf
    with open('finally_result.json', 'w', encoding='utf8') as file:
        json.dump(all_inf, file, ensure_ascii=False)



async def parse_product_page(url):
    try:
        base_url = 'https://www.santech.ru/catalog'
        full_url = urljoin(base_url, url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=full_url) as response:
                soup = BeautifulSoup(await response.text(), 'lxml')
                photos_names = await save_photos(soup)
                characteristics = get_characteristics(soup)
                name = get_name(soup)
                for_what, price = get_price(soup)
                print(full_url)
                # docs = save_docs(soup)
                availability = get_availability(soup)
                product_inf = {
                    **characteristics,
                    'ссылка': full_url,
                    'название': name,
                    'цена': price,
                    'за что': for_what,
                    'документы': 'не стал скачивать',
                    'фотографии': photos_names,
                    'наличие': availability
                }
                return product_inf
    except asyncio.TimeoutError:
        print("Таймаут при выполнении запроса на товар:", url)
        return None


def count():
    with open('result.json', 'r', encoding='utf8') as file:
        parsed_inf = json.load(file)
    print(len(parsed_inf))



def split_list(all_product_urls, length):
    """делит все ссылки на куски по length штук"""
    splitted_urls = []
    first_index = 0
    for i in list(range(len(all_product_urls)))[::length]:
        if i:
            splitted_urls.append(all_product_urls[first_index:i])
        first_index = i
    splitted_urls.append(all_product_urls[first_index:])
    return splitted_urls



def connect_results():
    with open('result.json', 'r', encoding='utf8') as file:
        parsed_before_inf_1 = json.load(file)
    with open('result_5001to.json', 'r', encoding='utf8') as file:
        parsed_before_inf_2 = json.load(file)
    with open('result_8001to.json', 'r', encoding='utf8') as file:
        parsed_before_inf_3 = json.load(file)
    with open('result_11001to.json', 'r', encoding='utf8') as file:
        parsed_before_inf_4 = json.load(file)

    all_inf = parsed_before_inf_1 + parsed_before_inf_2 + parsed_before_inf_3 + parsed_before_inf_4
    print(len(all_inf))
    with open('all_result.json', 'w', encoding='utf8') as file:
        json.dump(all_inf, file, ensure_ascii=False)


def print_to_excell(new_all_inf):
    inf_list = []
    inf_list_photo = []
    for index, product in enumerate(new_all_inf):
        for n, photo in enumerate(product['фотографии']):
            if n == 0:
                inf_list.append([index + 7048, product['наличие'], f"media\\{photo}.jpg", product['документы'], product['за что'], (product['цена'][:-2].replace(' ', '').replace(',', '')), product['название'], product['ссылка'], product['описание'], product['каталог'], product['код']])
            else:
                inf_list_photo.append([index + 7048, '', f"media\\{photo}.jpg", '', '', '', '', '', '', '', ''])

    df1 = pandas.DataFrame(inf_list, columns=['индекс', 'наличие', 'фотографии', 'документы', 'за что', 'цена', 'название', 'ссылка', 'описание', 'каталог', 'код'])
    df2 = pandas.DataFrame(inf_list_photo, columns=['индекс', 'наличие', 'фотографии', 'документы', 'за что', 'цена', 'название', 'ссылка', 'описание', 'каталог', 'код'])
    with pandas.ExcelWriter('products.xlsx') as writer:
        df1.to_excel(writer, sheet_name='Продукты')
        df2.to_excel(writer, sheet_name='фотографии')


def get_catalogs(url):
    while True:
        base_url = 'https://www.santech.ru/catalog'
        full_url = urljoin(base_url, url)
        try:
            response = requests.get(full_url)
            soup = BeautifulSoup(response.text, 'lxml')
            catalog_items = soup.find('nav', class_='ss-breadcrumbs').findAll('li', itemscope="itemscope")[2:]
            catalog = ';'.join(item.a.text.strip() for item in catalog_items)
            url_catalog = {
                'ссылка': full_url,
                'каталог': catalog
            }
            return url_catalog
        except Exception as exc:
            print(exc)
            print(full_url)
            return {
                'ссылка': full_url,
                'каталог': ''
            }



def write_descriptions(all_inf):
    new_all_inf = []
    for product in all_inf:
        # del product['наличие'], product['фотографии'], product['документы'], product['за что'], product['цена'], product['название'], product['ссылка']
        text = ''
        # print(type(product))
        # return
        characteristics = []
        for name, value in product.items():
            if name not in ['наличие', 'фотографии', 'документы', 'за что', 'цена', 'название', 'ссылка', 'каталог', 'код']:
                text += f'<p>{name}: <b>{value}</b></p>'
                characteristics.append(name)
        for characteristic in characteristics:
            del product[characteristic]
        product['описание'] = text
        new_all_inf.append(product)
    return new_all_inf


def url_catalogs():
    with open('finally_goods_urls.txt', 'r') as file:
        urls = file.readlines()

    catalogs_url = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = (executor.submit(get_catalogs, url) for url in urls)
        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urls)):
            try:
                url_catalog = future.result()
            except Exception as exc:
                print(exc)
                print('ошибка при обработке')
                url_catalog = ''
            finally:
                if url_catalog:
                    catalogs_url.append(url_catalog)

    with open('finally_url_catalogs.json', 'w', encoding='utf8') as file:
        json.dump(catalogs_url, file, ensure_ascii=False)


async def main():
    # parse_url_goods()
    with open('finally_goods_urls.txt', 'r') as file:
        urls = file.readlines()

    splitted_urls = split_list(urls, 10)
    for num, urls in tqdm(enumerate(splitted_urls), total=len(splitted_urls)):
        tasks = []
        for n, url in enumerate(urls):
            while True:
                g = 0
                try:
                    if num >= 0:
                        try:
                            other_variables_urls = get_url_other_variables(url)
                        except AttributeError:
                            other_variables_urls = []
                        if other_variables_urls:
                            for other_variables_url in other_variables_urls:
                                task_parse_product_inf = asyncio.create_task(parse_product_page(other_variables_url))
                                tasks.append(task_parse_product_inf)
                        else:
                            task_parse_product_inf = asyncio.create_task(parse_product_page(url))
                            tasks.append(task_parse_product_inf)
                    break
                except ConnectTimeout as ex:
                    time.sleep(10)
                    print(ex)
                    print(ex)
                    g += 1
                    if g == 10:
                        break
        await asyncio.gather(*tasks)

        product_inf_list = []
        for product_task in tasks:
            if product_task.result():
                product_inf_list.append(product_task.result())
        write_json(product_inf_list)


if __name__ == "__main__":
    asyncio.run(main())


    # url_catalogs()
    # parse_url_goods()

    # count()
    # parse_product_page('https://www.santech.ru/catalog/259/276/i15334/v13/')
    # connect_results()
    # with open('url_catalogs.json', 'r', encoding='utf8') as file:
    #     url_catalogs = json.load(file)
    # with open('all_result.json', 'r', encoding='utf8') as file:
    #     all_results = json.load(file)
    #
    # new_all_inf = []
    # for n, result in enumerate(all_results):
    #     for url_catalog in url_catalogs:
    #         url = url_catalog['ссылка']
    #         catalog = url_catalog['каталог']
    #         if url in result['ссылка']:
    #             new_all_inf.append({
    #                 **result,
    #                 'каталог': catalog
    #             })
    #             break
    #     if n % 500 == 0:
    #         print(n)
    #
    # print(new_all_inf[:4])
    # print(len(new_all_inf))
    # print(len(all_results))
    # print(len(url_catalogs))
    #
    # with open('all_inf_with_catalog.json', 'r', encoding='utf8') as file:
    #     all_inf = json.load(file)
    #
    # # write_descriptions(all_inf)
    # alll_inf = []
    # for n, product in enumerate(all_inf):
    #     try:
    #         product['код'] = product['Номенклатурный номер']
    #     except KeyError as ex:
    #         product['код'] = f'111-1{n}'
    #     finally:
    #         alll_inf.append(product)
    #
    #
    # all_inf_with_disc = write_descriptions(alll_inf)
    #
    # with open('all_inf_with_descr2.json', 'w', encoding='utf8') as file:
    #     json.dump(all_inf_with_disc, file, ensure_ascii=False)
    #
    #
    # with open('all_inf_with_descr2.json', 'r', encoding='utf8') as file:
    #     new_all_inf = json.load(file)
    #
    #
    # print(len(new_all_inf))
    # print_to_excell(new_all_inf)




