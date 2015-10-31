from . import Crawler
import re
import requests
from selenium import webdriver


class CrawlerQSelecao(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'peopleid INTEGER,'
                            'name_public_tender TEXT,'
                            'course TEXT,'
                            'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'qselecao'

    @staticmethod
    def dependencies():
        return ''

    @staticmethod
    def crop():
        return 'name', 'identity', 'birthday_day', 'birthday_month', 'birthday_year',

    # salva no banco todos dados de todos os candidatos de todos os concursos ou do concurso especificado
    # todo: salvar no banco os cocnursos já lidos, já isso precisará ser criada uma tabela especial, a tabela do anythingCollector com as configs dele
    @classmethod
    def harvest(cls, specifc_concurso=None):
        # retorna todos os id de candidatos de concursos
        def crawler_all_qselecao_concursos():
            phantom = webdriver.PhantomJS()
            phantom.get('http://qselecao.ifce.edu.br/lista_concursos.aspx')
            links_elements = phantom.execute_script("return $('#ctl00_ContentPlaceHolderPrincipal_grvLocaisProva a, #ctl00_ContentPlaceHolderPrincipal_grvResultados a, #ctl00_ContentPlaceHolderPrincipal_grvEncerrados a')")
            regex_get_cod_concurso = re.compile(r'cod_concurso=(\d+)')
            list_cod_concurso = [int(regex_get_cod_concurso.search(i.get_attribute('href')).group(1)) for i in links_elements]

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
                    peoples = phantom.execute_script("return $('#ctl00_ContentPlaceHolderPrincipal_grvConsulta tbody tr').find('a')")
                    for i3 in peoples:
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

            spanPublicTender =  '<span style="display:inline-block;font-family:Arial;font-size:8pt;font-weight:bold;height:4px;width:180px;position:absolute;left:0px;top:2px;width:680px;Height:15px;text-align:center;">'
            spanCourse = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:155px;position:absolute;left:113px;top:123px;width:586px;Height:15px;text-align:left;">'
            spanName = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:155px;position:absolute;left:117px;top:43px;width:586px;Height:15px;text-align:left;">'
            spanIdentity = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:30px;position:absolute;left:113px;top:57px;width:113px;Height:15px;text-align:left;">'
            spanBirthday = '<span style="display:inline-block;font-family:Arial;font-size:8pt;height:4px;width:30px;position:absolute;left:330px;top:57px;width:113px;Height:15px;text-align:left;">'

            regexPublicTender = re.compile(spanPublicTender + '(.*?)</span>')
            regexCourse = re.compile(spanCourse + '(.*?)</span>')
            regexName = re.compile(spanName + '(.*?)</span>')
            regexIdentity = re.compile(spanIdentity + '(.*?)</span>')
            regexBirthday = re.compile(spanBirthday + '(\d+)/(\d+)/(\d+)</span>')

            publicTender =  regexPublicTender.search(r.text).group(1)
            course = regexCourse.search(r.text).group(1)
            peopleName = regexName.search(r.text).group(1)
            peopleBirthdayDay, peopleBirthdayMonth, peopleBirthdayYear = regexBirthday.search(r.text).groups()
            peopleIdentity = re.sub('[^\d]+', '', regexIdentity.search(r.text).group(1))

            cls.db.update_people({'name': peopleName},
                                  {'birthday_day': peopleBirthdayDay, 'birthday_month': peopleBirthdayMonth, 'birthday_year': peopleBirthdayYear})
            if peopleIdentity.isdecimal():
                # há casos a serem lidados como em http://qselecao.ifce.edu.br/cartao_identificacao_dinamico.aspx?idcandidatoconcurso=328680&etapa=1
                cls.db.update_people({'name': peopleName}, {'identity': peopleIdentity})

            talbeid = cls.db.get_tableid_of_people({'name': peopleName})
            cls.update_my_table(talbeid, {'name_public_tender': publicTender, 'course': course})
            cls.update_crawler(peopleName, 1)

        if specifc_concurso is None:
            target = crawler_all_qselecao_concursos()
        else:
            target = crawler_specific_qselecao_concursos(specifc_concurso)

        for i in target:
            crawler_specific_qselecao_cartao_identificacao(i)
