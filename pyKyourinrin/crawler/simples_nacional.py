from . import Crawler
from selenium import webdriver


class CrawlerSimplesNacional(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_firm_id INTEGER,'
                            'date_start_simples_nacional TEXT,'
                            'date_start_simei TEXT'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_firm_id INTEGER,'
                            'date_initial TEXT,'
                            'date_end TEXT,'
                            'message TEXT'
                        ');' % (self.name() + '_previous_periods_simples_nacional'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'previous_periods_simples_nacional'},
        )

    @staticmethod
    def column_export():
        def history_simples_nacional(read):
            if len(read['previous_periods_simples_nacional']) > 0:
                return read['previous_periods_simples_nacional']
            else:
                return None

        return (
            {'column_name': 'history_simples_nacional', 'how': history_simples_nacional},
        )

    @staticmethod
    def name():
        return 'simples_nacional'

    @staticmethod
    def dependencies():
        return 'cnpj',

    @staticmethod
    def crop():
        return 'razao_social', 'date_start_simples_nacional', 'date_start_simei', 'history_simples_nacional'

    @staticmethod
    def primitive_required():
        return 'primitive_firm',

    @classmethod
    def harvest(cls, primitive_firm=None, dependencies=None):
        phantom = webdriver.PhantomJS()

        ###
        # Preencher formulário
        # todo: automatizar resolução do captchar de audio
        while True:
            phantom.get('http://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATBHE/ConsultaOptantes.app/ConsultarOpcao.aspx')
            phantom.save_screenshot('foo.png')
            captcha_text = input('Veja a imagem "foo.png" e digite o captchar:')

            phantom.find_element_by_class_name('caixaTexto').send_keys(dependencies['cnpj'])
            phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_txtTexto_captcha_serpro_gov_br').send_keys(captcha_text)
            phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_btnConfirmar').click()

            if len(phantom.find_elements_by_id('ctl00_ContentPlaceHolderConteudo_lblErroCaptcha')) == 0:
                break

        if len(phantom.find_elements_by_id('ctl00_ContentPlaceHolderConteudo_lblErroCaptcha')) > 0:
            # O CNPJ fornecido é inválido
            cls.update_crawler_status(False)
            return

        ###
        # Colher informações
        text_simples_nacional = phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_lblSituacaoSimples').text
        text_simei = phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_lblSituacaoMei').text

        if text_simples_nacional[:3] == 'NÃO':
            save_simples_nacional = -1
        else:
            save_simples_nacional = text_simples_nacional[-10:]

        if text_simei[:3] == 'NÃO':
            save_simei = -1
        else:
            save_simei = text_simei[-10:]

        cls.db.update_primitive_row({'razao_social': phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_lblNomeEmpresa').text})
        cls.update_my_table({'date_start_simples_nacional': save_simples_nacional, 'date_start_simei': save_simei})

        table_history_simples_nacional = phantom.find_elements_by_id('ctl00_ContentPlaceHolderConteudo_GridViewOpcoesAnteriores')
        if len(table_history_simples_nacional) > 0:
            for i in table_history_simples_nacional[0].find_elements_by_tag_name('tr')[1:]:
                date_initial, date_end, message = [i2.text for i2 in i.find_elements_by_tag_name('td')]
                cls.update_my_table({'date_initial': date_initial, 'date_end': date_end, 'message': message},
                                    table='previous_periods_simples_nacional')

        if phantom.find_element_by_id('ctl00_ContentPlaceHolderConteudo_lblSIMEIPeriodosAnteriores').text[-11:] != 'Não Existem':
            # todo: não implementei ainda a parte de colher em "Opções pelo SIMEI em Períodos Anteriores" por não ter
            # encontrado uma empresa de exemplo
            print('Nessa empresa de id {} há informação coletável em "Opções pelo SIMEI em Períodos Anteriores" ainda não implementada no crawler!'.format(primitive_firm))

        cls.update_crawler_status(True)
