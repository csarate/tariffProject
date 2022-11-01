from scrapers.argentina import scraper_argentina, scraper_argentina_oceba, consolidate_argentina
from scrapers.brasil import scraper_brasil
from scrapers.peru import scraper_peru
from scrapers.el_salvador import scraper_el_salvador, consolidate_el_salvador
from scrapers.mexico import scraper_mexico_vigentes, scraper_mexico_historico
from scrapers.nicaragua import scraper_nicaragua, consolidate_nicaragua
from scrapers.panama import scraper_panama, consolidate_panama
from scrapers.utils_test import util


def ejecutar_scraping(event: dict, pais):
    bln_continuar_ejecucion: bool = False
    date_lower_limit = '201112'

    # Set event in util library with execution parameters
    util.set_event(event)
    
    if "argentina" in pais:
        msg, bln_continuar_ejecucion = argentina_enre(event, bln_continuar_ejecucion, date_lower_limit, pais)
        return msg, bln_continuar_ejecucion
    if "el_salvador" in pais:
        msg, bln_continuar_ejecucion = el_salvador_siget(event, bln_continuar_ejecucion, pais)
        return msg, bln_continuar_ejecucion
    if "mexico" in pais:
        if "mexico_vigentes" in pais:
            pais = "mexico"
            msg, bln_continuar_ejecucion = mexico_fce_vigente(event, bln_continuar_ejecucion, pais)
            return msg, bln_continuar_ejecucion
        if "mexico_historico" in pais:
            pais = "mexico"
            msg, bln_continuar_ejecucion = mexico_fce_historico(event, bln_continuar_ejecucion, pais)
            return msg, bln_continuar_ejecucion
    if "nicaragua" in pais:
        msg, bln_continuar_ejecucion = nicaragua_ine(event, bln_continuar_ejecucion, pais)
        return msg, bln_continuar_ejecucion
    if "panama" in pais:
        msg, bln_continuar_ejecucion = panama_asep(event, bln_continuar_ejecucion, pais)
        return msg, bln_continuar_ejecucion


def argentina_enre(event: dict, bln_continuar_ejecucion, date_lower_limit, pais):
    # Inicializar los logs
    local = 'local_'+pais
    util.start_log(local)

    try:
        # runs the scraper of the company's website and save csv files in the relative path
        response_argentina = scraper_argentina.argentina_tariffs(200, 'data.txt', date_lower_limit, event)

        # Falta el control de que pase si no termina!.
        if response_argentina:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### Argentina Scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file

            response_argentina_tariff = consolidate_argentina.consolidate_tariff(50, 'data_consolidate.txt', event)

            if response_argentina_tariff:  # if we got the response = True it means the scraper was succesful finished
                msg = '#### Argentina Consolidation finished succesfully ####'
                util.log(local, msg)  # register in the log file
            else:
                msg = '#### Argentina Consolidation NOT finished ####'
                util.log(local, msg)  # register in the log file
                bln_continuar_ejecucion = True

        else:
            msg = '#### Argentina Scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e

    finally:
        # Salvo el log
        util.save_log(local)

    return msg, bln_continuar_ejecucion


def el_salvador_siget(event: dict, bln_continuar_ejecucion, pais):
    # Inicializar los logs
    local = 'local_' + pais
    util.start_log(local)
    try:
        response_el_salvador = scraper_el_salvador.el_salvador_tariff(200, 'data.txt', event, local)
    
        if response_el_salvador:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### ' + pais + ' scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file
            print(msg)

            response_el_salvador_tariff = \
                consolidate_el_salvador.consolidate_tariff(50, 'data_consolidate.txt', event, local)

            if response_el_salvador_tariff:  # if we got the response = True it means the scraper was succesful finished
                msg = '#### ' + pais + ' consolidation finished succesfully ####'
                util.log(local, msg)  # register in the log file
            else:
                msg = '#### ' + pais + ' consolidation NOT finished ####'
                util.log(local, msg)  # register in the log file
                bln_continuar_ejecucion = True
        else:
            msg = '#### ' + pais + ' scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e

    finally:
        # Salvo el log
        util.save_log(local)

    return msg, bln_continuar_ejecucion


def mexico_fce_vigente(event: dict, bln_continuar_ejecucion, pais):
    # Inicializar los logs
    local = 'local_' + pais
    util.start_log(local)
    try:
        response_mexico = scraper_mexico_vigentes.scraper_cfe_current_tariffs(200, 'data.txt', event, local)
    
        if response_mexico:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### ' + pais + ' scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file
            print(msg)
        
            # response_mexico_tariff = \
            #     consolidate_mexico.consolidate_tariff(50, 'data_consolidate.txt', event, local)
            #
            # if response_mexico_tariff:  # if we got the response = True it means the scraper was succesful finished
            #     msg = '#### ' + pais + ' consolidation finished succesfully ####'
            #     util.log(local, msg)  # register in the log file
            # else:
            #     msg = '#### ' + pais + ' consolidation NOT finished ####'
            #     util.log(local, msg)  # register in the log file
            #     bln_continuar_ejecucion = True
        else:
            msg = '#### ' + pais + ' scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e

    finally:
        # Salvo el log
        util.save_log(local)

    return msg, bln_continuar_ejecucion


def mexico_fce_historico(event: dict, bln_continuar_ejecucion, pais):
    # Inicializar los logs
    local = 'local_' + pais
    util.start_log(local)
    try:
        response_mexico = scraper_mexico_historico.scraper_cfe_historical_tariffs(200, 'data.txt', event, local)
    
        if response_mexico:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### ' + pais + ' scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file
            print(msg)
        
            # response_mexico_tariff = \
            #     consolidate_mexico.consolidate_tariff(50, 'data_consolidate.txt', event, local)
            #
            # if response_mexico_tariff:  # if we got the response = True it means the scraper was succesful finished
            #     msg = '#### ' + pais + ' consolidation finished succesfully ####'
            #     util.log(local, msg)  # register in the log file
            # else:
            #     msg = '#### ' + pais + ' consolidation NOT finished ####'
            #     util.log(local, msg)  # register in the log file
            #     bln_continuar_ejecucion = True
        else:
            msg = '#### ' + pais + ' scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e

    finally:
        # Salvo el log
        util.save_log(local)

    return msg, bln_continuar_ejecucion


def nicaragua_ine(event: dict, bln_continuar_ejecucion, pais):
    # Inicializar los logs
    local = 'local_' + pais
    util.start_log(local)
    try:
        response_nicaragua = scraper_nicaragua.nicaragua_ine_tariff(200, 'data.txt', event, local)
        
        if response_nicaragua:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### ' + pais + ' scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file
            print(msg)
            
            # response_nicaragua_tariff = \
            #     consolidate_nicaragua.consolidate_tariff(50, 'data_consolidate.txt', event, local)
            #
            # if response_nicaragua_tariff:  # if we got the response = True it means the scraper was succesful finished
            #     msg = '#### ' + pais + ' consolidation finished succesfully ####'
            #     util.log(local, msg)  # register in the log file
            # else:
            #     msg = '#### ' + pais + ' consolidation NOT finished ####'
            #     util.log(local, msg)  # register in the log file
            #     bln_continuar_ejecucion = True
        else:
            msg = '#### ' + pais + ' scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e
    
    finally:
        # Salvo el log
        util.save_log(local)
    
    return msg, bln_continuar_ejecucion


def panama_asep(event: dict, bln_continuar_ejecucion, pais):
    # Inicializar los logs
    local = 'local_' + pais
    util.start_log(local)
    try:
        response_panama = scraper_panama.panama_asep_tariff(200, 'data.txt', event, local)
    
        if response_panama:  # if we got the response = True it means the scraper was succesful finished
            msg = '#### ' + pais + ' scraping finished succesfully ####'
            util.log(local, msg)  # register in the log file
            print(msg)
        
            # response_panama_tariff = \
            #     consolidate_panama.consolidate_tariff(50, 'data_consolidate.txt', event, local)
            #
            # if response_panama_tariff:  # if we got the response = True it means the scraper was succesful finished
            #     msg = '#### ' + pais + ' consolidation finished succesfully ####'
            #     util.log(local, msg)  # register in the log file
            # else:
            #     msg = '#### ' + pais + ' consolidation NOT finished ####'
            #     util.log(local, msg)  # register in the log file
            #     bln_continuar_ejecucion = True
        else:
            msg = '#### ' + pais + ' scraping NOT finished ####'
            util.log(local, msg)  # register in the log file
            bln_continuar_ejecucion = False
    except Exception as e:
        raise e

    finally:
        # Salvo el log
        util.save_log(local)

    return msg, bln_continuar_ejecucion

    # scraper_brasil.brasil_Tariff(120, 'data.txt')
    # scraper_brasil.brasil_SAMP(120, 'data.txt')
    # scraper_brasil.brasil_PCAT_SPARTA(120, 'data.txt')
    # scraper_brasil.brasil_SAMP(120, 'data.txt')

    # segundos, archivo donde guardar los enlaces ya procesados
    # fechas para hacer un corte desde -> hasta
    # scraper_peru.scraper_osinergmin(120, 'data.txt', '01-08-2022', '01-10-2022')
