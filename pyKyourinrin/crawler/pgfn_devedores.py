from . import Crawler
from selenium import webdriver
import urllib.request
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class CrawlerPgfnDevedores(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_person_id INTEGER,'
                            'primitive_firm_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_person_id INTEGER,'
                            'primitive_firm_id INTEGER,'
                            'inscription_number TEXT,'
                            'value FLOAT,'
                            'type TEXT'
                        ');' % (self.name() + '_debt'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'debt'},
        )

    @staticmethod
    def macro_at_data():
        def pgfn_debt_total(read):
            return sum([i['value'] for i in read['pgfn_devedores_debt']])

        return (
            {'column_name': 'pgfn_debt_total', 'how': pgfn_debt_total},
        )

    @staticmethod
    def name():
        return 'pgfn_devedores'

    @staticmethod
    def dependencies():
        return ('name',), ('cpf',), ('cnpj',), ('razao_social',),

    @staticmethod
    def crop():
        return ('name', 'primitive_person'), ('cpf', 'primitive_person'), ('cnpj', 'primitive_firm'), ('razao_social', 'primitive_firm'), 'pgfn_debt_total'

    @staticmethod
    def primitive_required():
        return 'primitive_person', 'primitive_firm'

    @classmethod
    def harvest(cls, primitive_person=None, primitive_firm=None, dependencies=None):
        phantom = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any'])
        phantom.get('https://www2.pgfn.fazenda.gov.br/ecac/contribuinte/devedores/listaDevedores.jsf')
        captchar_id = phantom.find_element_by_id('txtToken_captcha_serpro_gov_br').get_attribute('value')

        # todo: automatizar resolução do captchar de audio
        #urllib.request.urlretrieve('https://www2.pgfn.fazenda.gov.br/captchaserpro/captcha/2.0.0/som/som.wav?' + captchar_id,
        #                           filename='captchar3.wav')
        phantom.save_screenshot('foo.png')
        captcha_text = input('Veja a imagem "foo.png" e digite o captchar:')

        phantom.find_element_by_id('listaDevedoresForm:captcha').send_keys(captcha_text)
        if 'name' in dependencies:
            phantom.find_element_by_id('listaDevedoresForm:nomeInput').send_keys(dependencies['name'])
            phantom.find_element_by_id('listaDevedoresForm:tipoConsultaRadio:2').click()
        elif 'razao_social' in dependencies:
            phantom.find_element_by_id('listaDevedoresForm:nomeInput').send_keys(dependencies['razao_social'])
            phantom.find_element_by_id('listaDevedoresForm:tipoConsultaRadio:2').click()
        elif 'cpf' in dependencies:
            phantom.find_element_by_id('listaDevedoresForm:identificacaoInput').send_keys(dependencies['cpf'])
            phantom.find_element_by_id('listaDevedoresForm:tipoConsultaRadio:0').click()
        else:
            phantom.find_element_by_id('listaDevedoresForm:identificacaoInput').send_keys(dependencies['cnpj'])
            phantom.find_element_by_id('listaDevedoresForm:tipoConsultaRadio:0').click()
        phantom.find_element_by_id('listaDevedoresForm:consultarButton').click()

        count_row = len(phantom.find_elements_by_class_name('rich-table-row'))
        if count_row == 0:
            # Nada foi retornado
            cls.update_crawler_status(False)
            return
        elif count_row > 1:
            # Mais que uma coisa foi retornada. Isso pode acontecer, por exemplo, se quisermos exatamente a pessoa
            # "José da Silva" e tiver vários que também tenham "José da Silva" no nome.
            # Nesse caso, precisamos filtrar para encontrar apenas o "José da Silva" que desejamos.
            # todo
            raise ValueError('A busca me retornou mais que um único resultado, e ainda não sei filtrar isso!')

        phantom.find_element_by_css_selector('.rich-table-row a').click()
        try:
            WebDriverWait(phantom, 10).until(
                EC.presence_of_element_located((By.ID, 'debitosTable:0:j_id39')) # todo: verificar se essa condição da certo sempre
            )
        finally:
            # todo: e se não carregar?
            pass

        cls.update_my_table({})

        if primitive_person is not None:
            cls.db.update_primitive_row(
                {'cpf': phantom.find_element_by_id('listaDevedoresForm:devedoresTable:0:j_id80').text,
                'name': phantom.find_element_by_id('listaDevedoresForm:devedoresTable:0:j_id83').text}
            )
        else:
            cls.db.update_primitive_row(
                {'cnpj': phantom.find_element_by_id('listaDevedoresForm:devedoresTable:0:j_id80').text,
                'razao_social': phantom.find_element_by_id('listaDevedoresForm:devedoresTable:0:j_id83').text}
            )

        for i in phantom.find_elements_by_css_selector('#debitosTable tr')[1:]:
            columns = i.find_elements_by_tag_name('td')
            cls.update_my_table({'inscription_number': columns[0].text, 'value': columns[1].text.replace('.', '').replace(',', '.'), 'type': columns[2].text}, table='debt')

        cls.update_crawler_status(True)
