from asyncio import timeout

import log
import constants
import psycopg2
import re
import json
import requests
import subprocess
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
from settings import db_connect

class Bulk_checker:

    def __init__(self):
        self.__logger = log.Log().get_logger(name='Bulk_checker')

    def main(self):

        self.__logger.info('init bulk_checker')
        self.__logger.info('getting domains')
        list_domains = self.get_all_domain_attributes()
        # check with request
        for idx, elem in enumerate(list_domains, start=1):
            domain = elem["domain"]
            domain_id = elem["domain_id"]
            self.__logger.info(f'[{idx}/{len(list_domains)}] scan site: {domain} -  domain_id: {domain_id}')
            url=f'https://{domain}'
            is_online = self.is_domain_online(url)
            ping_result = self.ping(domain)
            traceroute_result = self.traceroute(domain)
            result = f"request:{is_online} - ping: {ping_result} - tracer: {traceroute_result}"
            self.__logger.info(f"domain_id {domain_id} ---- {result}")
            if not is_online and not ping_result and not traceroute_result:
                final_status = 'Offline'
            else:
                final_status = 'Online'
            dict_to_save = {
                'domain_id': domain_id,
                'online_status': final_status,
                'offline_type': 'Bulk-check',
                'status_msg': result,
                'updated_by': 'Bulk-check'
            }
            self.update_domain_attributes(dict_to_save)


    def is_domain_online(self, url):
        try:
            response = requests.get(url, timeout=5)
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
            sql_string = """select domain_id , domain from domain_attributes da where domain_id < 20 """
            # sql_string = """select domain_id , domain from domain_attributes da where domain ='dnoid.to'"""
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

    def update_domain_attributes(self, values_dict):
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

            sql_string = "UPDATE public.domain_attributes SET  online_status=%s, " \
                         "offline_type=%s, status_msg=%s, updated_by=%s WHERE domain_id =%s;"

            data = (values_dict['online_status'],
                    values_dict['offline_type'],
                    values_dict['status_msg'],
                    values_dict['updated_by'],
                    values_dict['domain_id'],
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
                self.__logger.info(
                    f"::domain_attributes:: Domain_attributes updated successfully - domain_id {values_dict['domain_id']} - online_status {values_dict['online_status']}")

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Update Domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()