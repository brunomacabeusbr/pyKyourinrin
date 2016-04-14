from . import Crawler
import re
import requests
from selenium import webdriver


class CrawlerQSelecao(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                        'entity_person_id INTEGER,'
                        'name_public_tender TEXT,'
                        'course TEXT'
                        ');' % (self.name() + '_public_tender'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'public_tender'},
        )

    @staticmethod
    def name():
        return 'qselecao'

    @staticmethod
    def dependencies():
        return None

    @staticmethod
    def crop():
        return 'name', 'identity', 'birthday_day', 'birthday_month', 'birthday_year',

    @staticmethod
    def entity_required():
        return 'entity_person',

    @staticmethod
    def __get_cod_concursos():
        phantom = webdriver.PhantomJS()

        phantom.get('http://qselecao.ifce.edu.br/lista_concursos.aspx')
        links_elements = phantom.execute_script("return $('#ctl00_ContentPlaceHolderPrincipal_grvLocaisProva a, #ctl00_ContentPlaceHolderPrincipal_grvResultados a, #ctl00_ContentPlaceHolderPrincipal_grvEncerrados a')")
        regex_get_cod_concurso = re.compile(r'cod_concurso=(\d+)')
        return [int(regex_get_cod_concurso.search(i.get_attribute('href')).group(1)) for i in links_elements]

    @classmethod
    def trigger(cls, table_row):
        import time
        import json

        if table_row.value() is None:
            cls.harvest()

            list_cod_concurso = cls.__get_cod_concursos()
            table_row.update(json.dumps(list_cod_concurso))
            time.sleep(3600 * 24 * 30)

        while True:
            list_cod_concurso_now = cls.__get_cod_concursos()
            list_cod_concurso_previous = json.loads(table_row.value())

            diff = list(set(list_cod_concurso_now) - set(list_cod_concurso_previous))

            for i in diff:
                cls.harvest(specifc_concurso=i)

            table_row.update(json.dumps(list_cod_concurso_now))

            time.sleep(3600 * 24 * 30)

    # salva no banco todos dados de todos os candidatos de todos os concursos ou do concurso especificado
    @classmethod
    def harvest(cls, specifc_concurso=None):
        # retorna todos os id de candidatos de concursos
        def crawler_all_qselecao_concursos():
            list_cod_concurso = cls.__get_cod_concursos()

            list_to_return = []
            for i in list_cod_concurso:
                list_to_return.extend(crawler_specific_qselecao_concursos(i))
            return list_to_return

        # retorna o numero de cada candidato do concurso especificado
        def crawler_specific_qselecao_concursos(id):
            phantom = webdriver.PhantomJS()

            phantom.get('http://qselecao.ifce.edu.br/listagem.aspx?idconcurso=' + str(id) + '&etapa=1')

            def pages_number_next():
                element_next_page = phantom.execute_script(
                    "var pages_number = $('#ctl00_ContentPlaceHolderPrincipal_grvConsulta tbody:last td');"
                    "var page_number_current = $('#ctl00_ContentPlaceHolderPrincipal_grvConsulta tbody:last span').parent();"
                    "for (var i = 0; i < pages_number.length; i++) {"
                    "  if (pages_number.eq(i).is(page_number_current)) {"
                    "    break"
                    "  }"
                    "}"
                    "var page_number_next = pages_number.eq(i + 1).find('a');"
                    "if (page_number_next.length) {"
                    "  return page_number_next;"
                    "} else {"
                    "  return false;"
                    "}")

                if element_next_page is False:
                    return False

                element_next_page[0].click()
                return True

            def pages_letters_get_element():
                return phantom.execute_script("return $('[style=\"font-size: 8pt\"]').eq(1).find('a')")

            def pages_letters_go_to(number):
                pages_letters_get_element()[number].click()

            list_to_return = []
            regex_get_id_candidato_concurso = re.compile(r'idcandidatoconcurso=(\d+)')
            for i in range(26):
                while True:
                    person = phantom.execute_script("return $('#ctl00_ContentPlaceHolderPrincipal_grvConsulta tbody tr a[target]')")
                    for i3 in person:
                        list_to_return.append(int(regex_get_id_candidato_concurso.search(i3.get_attribute('href')).group(1)))

                    if not pages_number_next():
                        break

                if 26 > i + 1:
                    pages_letters_go_to(i + 1)
                else:
                    break

            return list_to_return

        # salva no banco informações do cartoes de identificacao especificado
        def crawler_specific_qselecao_cartao_identificacao(id):
            r = requests.get('http://qselecao.ifce.edu.br/cartao_identificacao_dinamico.aspx?idcandidatoconcurso=' + str(id) + '&etapa=1')

            if r.text[:43] == 'Não foi possível obter os dados desta etapa':
                return

            span_public_tender =  '<span style="display:inline-block;font-family:Arial;font-size:8pt;font-weight:bold;height:4px;width:180px;position:absolute;left:0px;top:2px;width:680px;Height:15px;text-align:center;">'
            span_course = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:155px;position:absolute;left:113px;top:123px;width:586px;Height:15px;text-align:left;">'
            span_name = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:155px;position:absolute;left:117px;top:43px;width:586px;Height:15px;text-align:left;">'
            span_identity = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:30px;position:absolute;left:113px;top:57px;width:113px;Height:15px;text-align:left;">'
            span_birthday = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:30px;position:absolute;left:330px;top:57px;width:113px;Height:15px;text-align:left;">'

            regex_public_tender = re.compile(span_public_tender + '(.*?)</span>')
            regex_course = re.compile(span_course + '(.*?)</span>')
            regex_name = re.compile(span_name + '(.*?)</span>')
            regex_identity = re.compile(span_identity + '(.*?)</span>')
            regex_birthday = re.compile(span_birthday + '(\d+)/(\d+)/(\d+)</span>')

            publicTender =  regex_public_tender.search(r.text).group(1)
            course = regex_course.search(r.text).group(1)
            people_name = regex_name.search(r.text).group(1)
            people_birthday_day, people_birthday_month, people_birthday_year = regex_birthday.search(r.text).groups()
            people_identity = re.sub('[^\d]+', '', regex_identity.search(r.text).group(1))

            infos = {'birthday_day': people_birthday_day, 'birthday_month': people_birthday_month, 'birthday_year': people_birthday_year}
            if people_identity.isdecimal():
                # há casos a serem lidados como em http://qselecao.ifce.edu.br/cartao_identificacao_dinamico.aspx?idcandidatoconcurso=328680&etapa=1
                infos['identity'] = people_identity

            entity_id = cls.db.update_entity_row(infos, entity_filter={'name': people_name}, entity_name='entity_person')

            try:
                cls.update_my_table({}, entity_id=entity_id, entity_name='entity_person')
            except:
                pass
            cls.update_my_table({'name_public_tender': publicTender, 'course': course}, table='public_tender', entity_id=entity_id, entity_name='entity_person')
            cls.update_crawler_status(True, entity_id=entity_id, entity_name='entity_person')

        if specifc_concurso is None:
            target = crawler_all_qselecao_concursos()
        else:
            target = crawler_specific_qselecao_concursos(specifc_concurso)

        total = len(target)
        x = 0
        for i in target:
            print(x, '/', total)
            x += 1
            crawler_specific_qselecao_cartao_identificacao(i)
