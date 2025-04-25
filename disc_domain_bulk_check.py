import log
import psycopg2
import re
import json
import requests
import subprocess

from settings import db_connect
from concurrent.futures import ThreadPoolExecutor, as_completed

class Disc_domain_bulk_checker:

    def __init__(self):
        self.__logger = log.Log().get_logger(name='disc_domain_bulk_checker')

    def main(self):

        self.__logger.info('init _disc_domain bulk_checker')
        self.__logger.info('getting domains')
        list_domains = self.get_all_domain_discovery()
        self.check_all_domains(list_domains)

    def process_domain(self, idx, elem, total):
        domain = elem["disc_domain"]
        disc_domain_id = elem["disc_domain_id"]
        self.__logger.info(f'[{idx}/{total}] scan site: {domain} - domain_id: {disc_domain_id}')

        try:
            url = f'https://{domain}'

            try:
                is_online = self.is_domain_online(url)
            except Exception as e:
                self.__logger.error(f"[{domain}] Error in is_domain_online: {e}")
                is_online = False

            try:
                ping_result = self.ping(domain)
            except Exception as e:
                self.__logger.error(f"[{domain}] Error in ping: {e}")
                ping_result = False

            try:
                traceroute_result = self.traceroute(domain)
            except Exception as e:
                self.__logger.error(f"[{domain}] Error in traceroute: {e}")
                traceroute_result = False

            result = f"request:{is_online} - ping: {ping_result} - tracer: {traceroute_result}"
            self.__logger.info(f"domain_id {disc_domain_id} ---- {result}")

            final_status = 'Offline' if not is_online and not ping_result and not traceroute_result else 'Online'

            dict_to_save = {
                'disc_domain_id': disc_domain_id,
                'online_status': final_status,
                'status_details': 'Bulk-check'
            }

            try:
                self.update_domain_discovery(dict_to_save)
            except Exception as e:
                self.__logger.error(f"[{domain}] Error updating domain discovery: {e}")

        except Exception as e:
            self.__logger.error(f"[{domain}] Unexpected error in process_domain: {e}")

    def check_all_domains(self, list_domains):
        total = len(list_domains)
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(self.process_domain, idx, elem, total)
                for idx, elem in enumerate(list_domains, start=1)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.__logger.error(f"Unhandled exception in thread: {e}")


    def is_domain_online(self, url):
        try:

            proxy_user = "customer-production4"
            proxy_pass = 'Production12'
            proxy_host = "pr.oxylabs.io"
            proxy_port = "7777"

            proxies = {
                "http": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}",
                "https": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}",
            }
            response = requests.get(url, proxies=proxies, timeout=5)
            return response.status_code < 400  # 2xx y 3xx son respuestas "buenas"
        except requests.RequestException:
            return False

    def ping(self, domain):
        try:
            result = subprocess.run(
                ["ping", "-n", "4", domain], capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False
        except Exception as e:
            print(e)
            return False

    def traceroute(self, domain):
        try:
            result = subprocess.run(
                ["tracert", domain], capture_output=True, text=True, timeout=20
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False
        except Exception as e:
            print(e)
            return False

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
            # sql_string = "select domain_id , domain from domain_attributes where domain_attributes.domain_id > 10 and domain_attributes.domain_id <30 "
            sql_string = """select * from domain_discovery dd  where online_status is null order by dd.disc_domain_id limit 5000"""
            # sql_string = """select domain_id , domain from domain_attributes da where domain ='dnoid.to'"""
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

    def get_subdomains_oxy_api(self, domain_source, country):
        try:
            list_dom = []

            url = "https://realtime.oxylabs.io/v1/queries"
            query = f' site:{domain_source}'
            payload = json.dumps({
                "source": "google_search",
                "domain": "com",
                "query": query,
                "geo_location": country,
                "parse": True
            })
            headers = {
                'Content-Type': 'application/json',
                # 'Authorization': 'Basic ZGF0YV9zY2llbmNlOm5xRHJ4UUZMeHNxNUpOOHpTekdwMg=='
                'Authorization': 'Basic Y2hfYWRfc25pZmZlcl9pc2ZQWjpNM00rRVhzazlNTm8zcExQZmFM'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            json_response = json.loads(response.text)
            json_result = json_response['results'][0]['content']['results']['organic']

            for elem in json_result:
                try:
                    domain = re.findall(r'https?:\/\/([^\/]+)', elem['url'])[0]
                    if domain_source in domain:
                        dict_subdomain = {
                            'url': elem['url'],
                            'domain': domain
                        }
                        list_dom.append(dict_subdomain)
                except:
                    print(f'error compare domains {elem}')

            # delete duplicates list_dom
            list_dom = self.delete_duplicates_subdomains(list_dom)

        except Exception as e:
            self.__logger.error(f" ::Get subdomains Error:: {e}")
        return list_dom

    def get_subdomains(self, domain_id, country):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the subdomain information
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

            sql_string = """SELECT full_url FROM public.subdomains WHERE domain_id = %s and country = %s and online_status='Online';"""
            domain_id_ = str(domain_id)
            data = (domain_id_, country)

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                subdomains = cursor.fetchall()
                conn.commit()
                if subdomains:
                    subdomains = [item[0] for item in subdomains]
                else:
                    subdomains = []
            except Exception as e:
                self.__logger.error('::subdomain:: Error found trying to Save Data subdomain - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return subdomains

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