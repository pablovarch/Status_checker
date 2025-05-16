import log
import csv
import os
import status_checker
from settings import db_connect, user_data_dir, proxy_dict
import psycopg2
from playwright.sync_api import Playwright, sync_playwright

class Disc_domain_browser_check:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Disc_domain_browser_check')
        self.__status_checker = status_checker.Status_checker()

    def main(self):
        self.__logger.info('start Disc_domain_browser_check')
        self.__logger.info('getting domains')
        list_domains = self.get_all_domain_discovery()

        # open browser
        for idx, elem in enumerate(list_domains, start=1):
            disc_domain_id = elem['disc_domain_id']
            disc_domain = elem['disc_domain']
            self.__logger.info(f'[{idx}/{len(list_domains)}] scan site: {disc_domain}')
            try:
                with sync_playwright() as p:

                    pw: Playwright = p
                    browser = pw.chromium.launch(channel='msedge', headless=False)

                    context = p.chromium.launch_persistent_context(

                        user_data_dir= user_data_dir,
                        headless=False,
                        # user_agent=user_agent,
                        channel='msedge',
                        # permissions=['notifications'],
                        args=[
                            # f"--disable-extensions-except={constants.path_to_extension}",
                            # f"--load-extension={constants.path_to_extension}",
                            # f'--profile-directory={constants.user_profile}'

                            "--start-fullscreen"
                        ],

                        proxy = proxy_dict,
                        # locale=f'{iso_name}',

                    )

                    page = context.new_page()

                    list_ad_chains_url, list_current_url = self.capture_traffic(page, disc_domain)
                    status_dict = self.__status_checker.status_checker(page, disc_domain, list_ad_chains_url)


                    dict_to_save = {
                        'disc_domain_id': disc_domain_id,
                        'online_status': status_dict['online_status'],
                        'status_details': 'Browser-check'
                    }
                    self.update_domain_discovery(dict_to_save)

                    # Cerrar el navegador
                    browser.close()
            except Exception as e:
                self.__logger.error(f'error on main: {e}')

    def get_all_domain_discovery(self):
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
            sql_string = """select disc_domain_id , disc_domain from domain_discovery where online_status = 'Online' and  status_details != 'Browser-check' limit 5000"""
            # sql_string = """select disc_domain_id , disc_domain from domain_discovery where disc_domain ='boyadekorasyonustasi.xyz'"""
            list_all_domain_discovery = []
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:

                    for elem in respuesta:
                        domain_data = {
                            'disc_domain_id': elem[0],
                            'disc_domain': elem[1],

                        }
                        list_all_domain_discovery.append(domain_data)
                else:
                    list_all_domain_discovery = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_domain_discovery - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_domain_discovery

    def update_domain_discovery(self, values_dict):
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

            sql_string = "UPDATE public.domain_discovery SET online_status=%s ,status_details=%s WHERE disc_domain_id=%s;"

            data = (values_dict['online_status'],
                    values_dict['status_details'],
                    values_dict['disc_domain_id'],
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
                self.__logger.info(
                    f"::domain_attributes:: Domain_discovery updated successfully - domain_id {values_dict['disc_domain_id']} - online_status {values_dict['online_status']}")

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Update Domain_discovery - {}'.format(e))

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
                    page.goto(site_to_load, wait_until='domcontentloaded', timeout=50000)
                    page.wait_for_selector("body", timeout=15000)
                    # page.wait_for_load_state(timeout=50000)
                    load_site = True
                    break
                except Exception as e:
                    self.__logger.error(f'error load page {e}')
                    tries += 1
            if not load_site:
                self.__logger.error("Carga fallida, forzando detención.")
                page.goto("about:blank", timeout=5000)

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




