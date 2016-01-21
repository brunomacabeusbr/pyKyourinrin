from . import Crawler
from selenium import webdriver


class CrawlerAbrTelecom(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_cellphone_id INTEGER'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'abr_telecom'

    @staticmethod
    def dependencies():
        return 'phone_number',

    @staticmethod
    def crop():
        return 'operator',

    @staticmethod
    def primitive_required():
        return 'primitive_cellphone',

    @classmethod
    def harvest(cls, primitive_cellphone=None, dependencies=None):
        phantom = webdriver.PhantomJS()

        def get_error():
            message_erro = phantom.find_elements_by_class_name('erros')
            if len(message_erro) > 0:
                return message_erro[0].get_attribute('value')
            else:
                return None

        ###
        # Preencher formulário
        # todo: automatizar resolução do captchar
        while True:
            phantom.get('http://consultanumero.abr.net.br/consultanumero/consulta/consultaSituacaoAtualCtg')
            phantom.save_screenshot('foo.png')
            captcha_text = input('Veja a imagem "foo.png" e digite o captchar:')

            phantom.find_element_by_name('telefone').send_keys(dependencies['phone_number'])
            phantom.find_element_by_name('jCaptchaValue').send_keys(captcha_text)
            phantom.find_element_by_id('consultaSituacao').click()
            if get_error() != 'Digite os caracteres corretamente!':
                break

        ###
        # Colher informações
        if get_error() == 'Não existem dados retornados para a consulta!':
            cls.update_crawler(primitive_cellphone, 'primitive_cellphone', -1)
            return

        cls.db.update_primitive_row({'id': primitive_cellphone}, 'primitive_cellphone',
                                    {'operator': phantom.find_element_by_class_name('gridselecionado').find_elements_by_tag_name('td')[2].text})
        cls.update_crawler(primitive_cellphone, 'primitive_cellphone', 1)
