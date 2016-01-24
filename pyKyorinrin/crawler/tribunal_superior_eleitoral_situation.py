from crawler import Crawler
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
from tools.misc_phantom import element_image_download


class CrawlerTribunalSuperiorEleitoralSituation(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_peoples_id INTEGER,'
                            'voter_registration TEXT,'
                            'voter_situation_inscription TEXT'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'tribunal_superior_eleitoral_situation'

    @staticmethod
    def dependencies():
        return 'name', 'birthday_day', 'birthday_month', 'birthday_year',

    @staticmethod
    def crop():
        return 'voter_registration', 'voter_situation_inscription'

    @staticmethod
    def primitive_required():
        return 'primitive_peoples',

    @classmethod
    def harvest(cls, primitive_peoples=None, dependencies=None):
        # todo: falta considerar caso a pessoa não tenha título de eleitor
        phantom = webdriver.PhantomJS()

        def write_form():
            captcha_text = ''
            while True:
                phantom.delete_all_cookies() # deletar os cookies para prevenir erros de não mudar a imagem do captchar
                phantom.get('http://apps.tse.jus.br/saae/consultaNomeDataNascimento.do')

                element_image_download(phantom, phantom.find_element_by_tag_name('img'), padding_y=5, padding_height=-10, file_name='captcha')

                from tools.captchar import tse_read_captcha
                captcha_text = tse_read_captcha('captcha.jpg')
                import os
                os.remove('captcha.jpg')

                # captchar do TSE sempre tem 5 letras, então se tiver algo diferente de 5, a leitura está incompleta
                # logo, devemos tentar ler outro captchar
                if len(captcha_text) == 5:
                    break

            form_consultation = phantom.find_element_by_name('nomeEleitor')
            form_consultation.send_keys(dependencies['name'], Keys.TAB,
                                        '{:02}'.format(dependencies['birthday_day']), '{:02}'.format(dependencies['birthday_month']), '{:02}'.format(dependencies['birthday_year']))
            phantom.find_element_by_name('codigoCaptcha').send_keys(captcha_text)

        phantom.get('http://apps.tse.jus.br/saae/consultaNomeDataNascimento.do')
        while phantom.title == 'Tribunal Superior Eleitoral - Consulta por Nome e Data Nascimento -':
            write_form()
            phantom.delete_cookie('BIGipServerPool_CertidaoQuitacao') # por alguma razão, precisa apagar esses cookies para efetuar a consulta
            phantom.find_element_by_name('Consultar').click()

        x = phantom.page_source
        regexp = re.compile(r'<\/label>\s(.*)')
        l = re.findall(regexp, x)

        cls.update_my_table({'voter_registration': l[0], 'voter_situation_inscription': l[3].title()})
        cls.update_crawler(1)
