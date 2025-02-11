import log
import csv
import os
from bs4 import BeautifulSoup
import re
from playwright.sync_api import sync_playwright
from settings import db_connect
import psycopg2

class status_checker_zenrows:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Scraper_browser_zenrows')

    def main(self):
        self.__logger.info('start scraper browser zenrows')
        self.__logger.info('getting domains')
        list_domains = self.get_all_domain_attributes()

        # open browser
        # URL de conexión de ZenRows
        list_to_save = []
        connection_url = 'wss://browser.zenrows.com?apikey=1382ea86bd5c48f04fd7889868aa1930e11a3bc5'
        for elem in list_domains:
            self.__logger.info(f'scan site: {elem["domain"]}')
            with sync_playwright() as p:
                try:
                    # Conectar al navegador a través de CDP
                    browser = p.chromium.connect_over_cdp(connection_url)
                    # Crear una nueva página
                    page = browser.new_page()

                    list_ad_chains_url, list_current_url = self.capture_traffic(page, elem)

                    status_dict = self.status_checker(page, elem, list_ad_chains_url)

                    path_screenshot = f'screenshots/{elem}.png'
                    page.screenshot(path=path_screenshot)
                    dict_to_save = {
                        'domain': elem,
                        'offline_type': status_dict['offline_type'],
                        'online_status': status_dict['online_status'],
                        'redirect_url': status_dict['redirect_url'],
                        'status_msg': status_dict['status_msg']}
                    self.save_csv_name(dict_to_save, 'test.csv')
                    # Cerrar el navegador
                    browser.close()
                except Exception as e:
                    self.__logger.error(f'')


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
            sql_string = "select domain_id , domain from domain_attributes limit 10 "

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

    def status_checker(self, page, site, list_ad_chains_url):
        try:
            current_url = page.url
            current_domain = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]
            html = page.content()
        except:
            current_url = list_ad_chains_url[0]['url']
            current_domain = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]
            html = page.content()

        status_dict = {}
        offline_type = ''
        status_msg = ''
        # check redirect
        redirect_url = None
        if current_domain != site:
            redirect_url = current_url
        try:
            # check bright data block
            if len(list_ad_chains_url) == 1:
                ad_chain_url_status_code = list_ad_chains_url[0]['status']
                if 400 < ad_chain_url_status_code < 500:
                    online_status = 'Offline'
                    offline_type = 'Error[400-500]'
                    status_dict = {
                        'online_status': online_status,
                        'offline_type': offline_type,
                        'redirect_url': redirect_url,
                        'status_msg': ''
                    }

                else:
                    status_dict = self.check_html(html, ad_chain_url_status_code)

            else:
                # check online
                if list_ad_chains_url[0]['status'] == 200:
                    online_status = 'Online'
                    status_dict = {
                        'online_status': online_status,
                        'offline_type': offline_type,
                        'redirect_url': redirect_url,
                        'status_msg': ''
                    }
                    if 'Domain Seized' in html:
                        online_status = 'Blocked'
                        offline_type = 'Domain Seized'
                        status_dict = {
                            'online_status': online_status,
                            'offline_type': offline_type,
                            'redirect_url': redirect_url,
                            'status_msg': 'Domain Seized'
                        }

                # check redirect
                elif 299 < list_ad_chains_url[0]['status'] < 400:
                    found_flag = False
                    for ad_chain_url in list_ad_chains_url[1:10]:
                        if ad_chain_url['status'] == 200:
                            found_flag = True
                            first_domain = re.findall(r'https?:\/\/([^\/]+)', list_ad_chains_url[0]['url'])[0]
                            second_domain = re.findall(r'https?:\/\/([^\/]+)', ad_chain_url['url'])[0]
                            if first_domain in second_domain:
                                online_status = 'Online'
                                offline_type = 'Redirect- same domain'

                                status_dict = {
                                    'online_status': online_status,
                                    'offline_type': offline_type,
                                    'redirect_url': redirect_url,
                                    'status_msg': 'Redirect same domain'
                                }
                                if 'Domain Seized' in html:
                                    online_status = 'Blocked'
                                    offline_type = 'Domain Seized'
                                    status_dict = {
                                        'online_status': online_status,
                                        'offline_type': offline_type,
                                        'redirect_url': redirect_url,
                                        'status_msg': 'Domain Seized'
                                    }

                                break
                            else:
                                online_status = 'Online'
                                offline_type = 'Redirect'
                                status_dict = {
                                    'online_status': online_status,
                                    'offline_type': offline_type,
                                    'redirect_url': redirect_url,
                                    'status_msg': ''
                                }
                        # check redirect off line
                        if found_flag == False:
                            ad_chain_url_status_code = list_ad_chains_url[-1]['status']
                            status_dict = self.check_html(html, ad_chain_url_status_code)
                            status_dict['redirect_url'] = redirect_url
                        if status_dict['online_status'] != 'Online':
                            break


                else:
                    # offline
                    ad_chain_url_status_code = list_ad_chains_url[0]['status']
                    status_dict = self.check_html(html, ad_chain_url_status_code)
        except:
            pass
        return status_dict

    def check_html(self, html, ad_chain_url_status_code):
        html = html.lower()
        soup = BeautifulSoup(html, 'html.parser')
        visible_text = soup.get_text()
        html = visible_text.lower()
        status_dict = {}
        try:
            status_msg = ''
            if 'blocked' in html and 'bright data usage policy' in html:
                online_status = 'Blocked'
                offline_type = f"Error[BrightData-{ad_chain_url_status_code}]"
                status_msg = 'bright data usage policy'
            elif 'webpage not available' in html or '404 not found' in html or 'this page isn’t working' in html:
                online_status = 'Offline | Ad Sniffer'
                offline_type = f"Error[{ad_chain_url_status_code}]"
            elif 'cloudflare' in html and 'ray id' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Cloudflare-{ad_chain_url_status_code}]"
                status_msg = 'cloudflare'
            elif 'verifying you are human. this may take a few seconds' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Cloudflare-{ad_chain_url_status_code}]"
                status_msg = 'cloudflare'
            # elif 'captcha' in html :
            #     online_status = 'Blocked'
            #     offline_type = f"Error[captcha-{ad_chain_url_status_code}]"
            #     status_msg = 'captcha'
            elif 'proxy authentication required' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Proxy Authentication Required-{ad_chain_url_status_code}]"
                status_msg = 'proxy authentication required'
            elif 'domain seized' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Domain Seized-{ad_chain_url_status_code}]"
                status_msg = 'Domain Seized'
            elif 'cannot establish connection to requested target' in html or 'could not resolve host https in html' in html:
                online_status = 'Offline | Ad Sniffer'
                offline_type = f"Error[not resolve host-{ad_chain_url_status_code}]"
            elif 'bad request' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Bad Request-{ad_chain_url_status_code}]"
            elif 'auth failed (code: ip_forbidden)' in html:
                online_status = 'Blocked'
                offline_type = f"Error[auth failed-{ad_chain_url_status_code}]"
            elif 'sorry, you have been blocked' in html:
                online_status = 'Blocked'
                offline_type = f"blocked-{ad_chain_url_status_code}]"


            else:
                if ad_chain_url_status_code == 200:
                    online_status = 'Online'
                    offline_type = 'None'
                else:
                    online_status = 'Offline | Ad Sniffer'
                    offline_type = f"Error[{ad_chain_url_status_code}]"

            status_dict = {
                'online_status': online_status,
                'offline_type': offline_type,
                'status_msg': status_msg,
            }
        except:
            self.__logger.error(f'Error check_html - {ad_chain_url_status_code}')

        return status_dict

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
