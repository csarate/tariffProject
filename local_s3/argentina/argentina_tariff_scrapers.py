import time
import utils.util as util
import datetime
import requests
from bs4 import BeautifulSoup
import requests_html
import urllib.parse
import pandas as pd


def scraper_epe_santa_fe(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.epe.santafe.gov.ar/index.php?id=cuadrotarifariobim'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "EPE SANTA FE" , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_epe_cordoba(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.epec.com.ar/informacion-comercial/especiales/showOneTwo'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                    print("Novo arquivo")
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "EPE CORDOBA" , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_edea(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'http://www.edeaweb.com.ar/informacion/cuadro-tarifario'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                    print("Novo arquivo")
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDEA" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_edea_mediana_grande_demanda(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'http://www.edeaweb.com.ar/informacion/medianas-y-grandes-demandas/cuadro-tarifario'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                    print("Novo arquivo")
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDEA" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_edemsa(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.edemsa.com/cuadro-tarifario/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDEMSA" ,
                                 "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_secheep(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.secheep.gob.ar/?page_id=5601'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "secheep".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_energiasanjuan(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.energiasanjuan.com.ar/index.php?ver=cuadro_tarifario'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "ENERGIA SAN JUAN" , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_enersa(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.enersa.com.ar/informacion-comercial/#SituacionTarifaria'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "ENERSA" ,
                                 "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_ecsapem(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'http://www.ecsapem.com.ar/cuadros-tarifarios'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "ECSAPEM" ,
                                 "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True

def scraper_edeste(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'http://www.edeste.com.ar/services/cuadrotarifario/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDESTE" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True

def scraper_edensa(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.edensa.com.ar/usuarios/reglamentos-de-suministro/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDENSA" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_edes(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.infoedes.com/usuarios/reglamentos-de-suministro/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "EDES" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_refsa(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.recursosyenergia.com.ar/tarifas'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "REFSA" , "" ,
                                 "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_oceba(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://oceba.gba.gov.ar/nueva_web/s.php?i=17'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" , "OCEBA" ,
                                 "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_cooperativadelujan(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.cooperativadelujan.com.ar/contenido?st=cuadro_tarifario_vigente'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "cooperativa de lujan".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_coopelectric(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        base_link = 'http://www.coopelectric.com.ar/eelectrica/?m=tarifas'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , headers=headers , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "coopelectric".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_coopelsalto(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.coopelsalto.com.ar/tarifas/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "coopelsalto".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_ceys(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://ceys.com.ar/servicios/electricidad/cuadro-tarifario-vigente'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "CEYS".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_novedadescec(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://novedadescec.com.ar/reglamentacion-del-sector-electrico/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "novedadescec".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_usinatandil(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.usinatandil.com.ar/cuadros-tarifarios/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and ".pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "usina tandil".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_coopelec(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'http://www.coopelec.com.ar/cuadro-tarifario/'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and "pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "coopelec".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_ceb(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.ceb.coop/area-energia/cuadros-tarifarios.html'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and "pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "CEB".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_cegc(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://www.cegc.com.ar/?page_id=368'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower()

                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and "pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "CEGC".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_scpl(time_to_search , output_path):
    beginning_time = time.time()
    retries = 0
    while time.time() - beginning_time < time_to_search:
        base_link = 'https://scpl.coop/index.php/site/ver-publicacion?id=1096'
        util.log("Connecting to " + base_link)
        try:
            if retries > 3:
                return False
            response = requests.get(base_link , verify=False)
            response.raise_for_status()
        except Exception as e:
            retries += 1
            util.log("Couldn't access website: " + base_link + f". Error description: {e}")
            time.sleep(1)
        else:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text , 'html5lib')
            reference_form_link_list = soup.find_all('a')
            data = util.read_files_s3(output_path)
            past_links = []
            try:
                for i in range(len(data)):
                    past_links.append(data[i][2])
            except:
                pass
            for link in reference_form_link_list:
                try:
                    link_reference = link['href']
                    extension = link['href'].split("/")[-1].split(".")[-1]
                    year = link['href'].split("/")[-2]
                    link_title = util.slugify(
                        link['href'].split("/")[-1].split(".")[0].replace("%20" , "-")) + "_" + str(
                        year) + "." + extension.lower() + "-" + str(datetime.date.today())
                    print(link_title)
                except:
                    continue
                if link_reference != None and link_reference not in \
                        past_links and "pdf" in link_reference.lower():
                    print("Novo arquivo")
                    data.append([base_link , link_title , link_reference ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "scpl".upper() , "" , "" , ""])
            util.dump_files_s3(data , output_path)
            util.log("All links gathered from " + base_link)
            return True


def scraper_enre(time_to_search , output_path):
    status = False
    counter = 1
    row_number = 1
    to_be_visited = []
    beginning_time = time.time()
    while time.time() - beginning_time < time_to_search:
        try:
            past_links = []
            data = util.read_files_s3(output_path)
            if data== None:
                data=[]
            for i in range(len(data)):
              past_links.append(data[i][2])
        except:
            pass
        while status == False:
            try:
                base_link="https://www.enre.gov.ar/web/TARIFASD.nsf/todoscuadros?OpenView&Start=" + str(row_number)
                response = requests.get(base_link, verify=False)
            except Exception as e:
                util.log("Couldn't access website: " + base_link + f". Error description: {e}")
                pass
            row_number = counter * 29
            print(row_number)
            if row_number>1000:
                return False
            soup = BeautifulSoup(response.text , "lxml")
            soup.encode("UTF-8")
            if "No documents found" in response.text:
                status = True
            links = soup.find_all("a" , href=True)
            for link in links:
                if "Cuadro Tarifario" in link.text:
                    to_be_visited.append([link.text , urllib.parse.urljoin("https://www.enre.gov.ar/" , link["href"])])
            counter += 1
        for link in to_be_visited:
            try:
                if link in past_links:
                    continue
                response = requests.get(link[1], verify=False)
                df = pd.read_html(response.text)
                print(df)
            except Exception as e:
                util.log("Couldn't access website: " + link + f". Error description: {e}")
                time.sleep(1)
            contador = 0
            for i in range(len(df)):
                try:
                    contador += 1
                    name = link[0].replace("/" , "-").strip() + "_" + str(contador)
                    temppath = "temp/" + name+".csv"
                    df[i].to_csv(temppath)
                    with open(temppath, "rb") as f:
                        util.save_directly_s3(f , filename=name+".csv" , outputpath="downloaded_files/Argentina/Enre")
                    f.close()
                    data.append([base_link , link , link ,
                                 datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") , 0 , "Argentina" ,
                                 "ENRE".upper() , "" , "" , ""])
                    util.dump_files_s3(data , output_path)
                except Exception as e:
                    print(e)
                    continue
scraper_enre(time_to_search=600 , output_path="to_be_downloaded/argentina_ceal_links.json")
