import log
import csv
import os
import status_checker
from bs4 import BeautifulSoup
import re
from playwright.sync_api import sync_playwright
from settings import db_connect, connection_url
from constants import kw_parking
import psycopg2

class Scraper_browser_bd:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Scraper_browser_bd')
        self.__status_checker = status_checker.Status_checker()

    def main(self):
        self.__logger.info('start scraper browser zenrows')
        self.__logger.info('getting domains')
        list_domains = self.get_all_domain_attributes()

        # open browser
        # URL de conexión de ZenRows
        list_to_save = []
        for idx, elem in enumerate(list_domains, start=1):
            self.__logger.info(f'[{idx}/{len(list_domains)}] scan site: {elem["domain"]}')
            proxy_service = 'BrightData'
            with sync_playwright() as pw:
                try:
                    AUTH = 'brd-customer-hl_f416ecd9-zone-bd_scraping_browser_1:tqa4jbuibqcp'
                    SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'

                    print('Connecting to Scraping Browser...')
                    browser = pw.chromium.connect_over_cdp(SBR_WS_CDP)

                    print('Connected! Navigating...')
                    page = browser.new_page()

                    list_ad_chains_url, list_current_url = self.capture_traffic(page, elem["domain"])
                    path_screenshot = f'screenshots/{proxy_service}/{elem["domain"]}.png'
                    page.screenshot(path=path_screenshot)
                    status_dict = self.__status_checker.status_checker(page, elem["domain"], list_ad_chains_url)
                    current_url = page.url


                    dict_to_save = {
                        'domain': elem,
                        'offline_type': status_dict['offline_type'],
                        'online_status': status_dict['online_status'],
                        'redirect_url': status_dict['redirect_url'],
                        'status_msg': f"zenrows-{status_dict['status_msg']}"
                    }
                    self.save_domain_status(dict_to_save, elem['domain_id'],current_url, proxy_service)
                    self.save_csv_name(dict_to_save, 'test.csv')
                    # Cerrar el navegador
                    browser.close()
                except Exception as e:
                    self.__logger.error(f'error on main: {e}')


    def get_all_domain_attributes(self):
        # Try to connect to the DB
        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise
        else:
            # sql_string = "select domain_id , domain from domain_attributes where domain_attributes.domain_id > 10 and domain_attributes.domain_id <30 "
            sql_string = """select domain_id , domain from domain_attributes da where da."domain" = 'batotoo.com'"""
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_all_domain_attributes = []
                    for elem in respuesta:
                        domain_data = {
                            'domain_id': elem[0],
                            'domain': elem[1],

                        }
                        list_all_domain_attributes.append(domain_data)
                else:
                    list_all_domain_attributes = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_domain_attributes

    def update_domain_attributes(self, values_dict, domain_id, url):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the  collection job information
        """

        # Try to connect to the DB
        try:

            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise

        else:

            sql_string = "UPDATE public.domain_attributes SET domain_classification_id=%s, online_status=%s, " \
                         "offline_type=%s, site_url=%s, status_msg=%s WHERE domain_id =%s;"

            data = (values_dict['domain_classification_id'],
                    values_dict['online_status'],
                    values_dict['offline_type'],
                    url,
                    values_dict['status_msg'],
                    domain_id
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
                self.__logger.info(
                    f"::domain_attributes:: Domain_attributes updated successfully - domain_id {domain_id} - online_status {values_dict['online_status']}")

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Update Domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

    def save_domain_status(self, input_dict, domain_id, url, proxy_service):
        try:
            online_status = input_dict['online_status']
            domain_classification_id = None
            offline_type = input_dict['offline_type']
            # redirect_url= input_dict['status_dict']['redirect_url']
            # self.__logger.info(f'-- site are not online - update domain table - {domain_id}')
            if online_status == 'Offline | Ad Sniffer':
                domain_classification_id = 4
            elif online_status == 'Blocked':
                domain_classification_id = 2
                offline_type = f'{proxy_service} - Blocked'
            elif online_status == 'Online':
                domain_classification_id = 2
                # status_dict['offline_type'] = proxy_service
            status_dict = {
                'domain_classification_id': domain_classification_id,
                'online_status': online_status,
                'offline_type': offline_type,
                'status_msg': input_dict['status_msg']
            }

            bd_domain_status_dict = self.get_domain_status_by_id(domain_id)
            if online_status == 'Online' and bd_domain_status_dict['offline_type'] != 'ScrapingBrowser':
                self.update_domain_attributes(status_dict, domain_id, url)
            else:
                if bd_domain_status_dict['online_status'] != 'Offline | Analyst':
                    self.update_domain_attributes(status_dict, domain_id, url)
                else:
                    self.__logger.info(f'-- The site is already classified for the analyst. - domain_id {domain_id}')
        except Exception as e:
            self.__logger.error(f'-- Error update domain table - {domain_id} -  error: {e}')

    def get_domain_status_by_id(self, domain_id):
        # Try to connect to the DB
        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise
        else:
            sql_string = "select online_status, offline_type,status_msg from domain_attributes where domain_id = %s"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (domain_id,))
                respuesta = cursor.fetchone()
                conn.commit()
                if respuesta:
                    dict_status = {
                        'online_status': respuesta[0],
                        'offline_type': respuesta[1],
                        'status_msg': respuesta[2]
                    }

                else:
                    dict_status = None

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_online_status_by_id - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return dict_status

    def capture_traffic(self, page, site):
        try:
            # Crea una lista vacía para almacenar los datos de las solicitudes HTTP
            list_ad_chains_url = []
            list_current_url = []
            self.__logger.info(f" --- start capture traffic playwright chromium ---")

            def handle_response(response):
                try:
                    # Extraer los datos de la respuesta y agregarlos a la lista "responses"
                    status = response.status
                    url = response.url
                    headers = response.headers
                    current_url_load = page.url
                    list_current_url.append(current_url_load)

                    # extrae todas urls de las responses y las agrega a la lista
                    data_url = {
                        "status": status,
                        "url": url,
                        "post_clic": False
                    }
                    list_ad_chains_url.append(data_url)
                except Exception as e:
                    self.__logger.error(f'error handle response {e}')

            # Subscribe al evento "response"
            page.on("response", handle_response)

            # Navega a una página web para capturar el tráfico de red
            site_to_load = f'http://{site}'
            # page.goto(site_to_load, wait_until="networkidle")
            load_site = False
            tries = 1
            while tries < 3:
                try:
                    page.goto(site_to_load, wait_until='load', timeout=50000)
                    page.wait_for_selector("body", timeout=15000)
                    # page.wait_for_load_state(timeout=50000)
                    load_site = True
                    break
                except Exception as e:
                    self.__logger.error(f'error load page {e}')
                    tries += 1
        except Exception as e:
            self.__logger.error(f'capture_traffic - {e}')
        return list_ad_chains_url, list_current_url

    def read_csv(self, input_csv):
        try:
            webs_list = []

            with open(input_csv, encoding='utf-8', newline='') as csvfile:
                data = csv.reader(csvfile, delimiter=';')
                try:
                    for row in data:
                        # webs_list.append(row[0])
                        webs_list.append(row[0])
                except:
                    pass
                webs_list.pop(0)
                return webs_list

        except Exception as e:
            self.__logger.error(f" - Error reading csv - {e}")

    def save_csv_name(self, dict, name_csv):
        # open file
        name_to_save = f'{name_csv}.csv'
        try:
            with open(name_to_save, mode='a', encoding='utf-8') as csv_file:
                headers2 = list(dict.keys())
                writer = csv.DictWriter(csv_file, fieldnames=headers2, delimiter=';', lineterminator='\n')
                # create headers
                if os.stat(name_to_save).st_size == 0:
                    writer.writeheader()

                # save data
                writer.writerow(dict)
        except Exception as e:
            pass




