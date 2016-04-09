from crawler import Crawler
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from tools.misc_phantom import element_image_download


class CrawlerTribunalSuperiorEleitoralDomicilioEleitoral(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'voter_registration TEXT,'
                            'domicilio_eleitoral_zona TEXT,'
                            'domicilio_eleitoral_secao TEXT,'
                            'domicilio_eleitoral_local TEXT,'
                            'domicilio_eleitoral_edereco TEXT,'
                            'domicilio_eleitoral_city TEXT,'
                            'domicilio_eleitoral_state TEXT'
                        ');' % self.name())

    @staticmethod
    def macro_at_data():
        def domicilio_eleitoral(read):
            return 'Estado: {}; Cidade: {}; Endereço: {}; Local: {}; Zona: {}; Seção: {}'.format(
                read['domicilio_eleitoral_state'],
                read['domicilio_eleitoral_city'],
                read['domicilio_eleitoral_edereco'],
                read['domicilio_eleitoral_local'],
                read['domicilio_eleitoral_secao'],
                read['domicilio_eleitoral_zona']
            )

        return (
            {'column_name': 'domicilio_eleitoral', 'how': domicilio_eleitoral},
        )

    @staticmethod
    def name():
        return 'tribunal_superior_eleitoral_domicilio_eleitoral'

    @staticmethod
    def dependencies():
        return ('name', 'birthday_day', 'birthday_month', 'birthday_year', 'name_monther'),\
               ('voter_registration', 'birthday_day', 'birthday_month', 'birthday_year', 'name_monther')

    @staticmethod
    def crop():
        return 'voter_registration', 'name', 'domicilio_eleitoral'

    @staticmethod
    def entity_required():
        return 'entity_person',

    @classmethod
    def harvest(cls, entity_person=None, dependencies=None):
        # todo: falta considerar caso a pessoa não tenha título de eleitor
        if 'name' in dependencies:
            url = 'http://apps.tse.jus.br/saae/consultaLocalVotacaoNome.do'
            first_input_value = dependencies['name']
        else:
            url = 'http://apps.tse.jus.br/saae/consultaLocalVotacaoInscricao.do'
            first_input_value = dependencies['voter_registration']

        phantom = webdriver.PhantomJS()

        def write_form():
            captcha_text = ''
            while True:
                phantom.delete_all_cookies() # deletar os cookies para prevenir erros de não mudar a imagem do captchar
                phantom.get(url)

                element_image_download(phantom, phantom.find_element_by_tag_name('img'), padding_y=5, padding_height=-10, file_name='captcha')

                from tools.captchar import tse_read_captcha
                captcha_text = tse_read_captcha('captcha.jpg')
                import os
                os.remove('captcha.jpg')

                # captchar do TSE sempre tem 5 letras, então se tiver algo diferente de 5, a leitura está incompleta
                # logo, devemos tentar ler outro captchar
                if len(captcha_text) == 5:
                    break

            form_consultation = phantom.find_element_by_css_selector('input:not([type="hidden"])')
            # todo: falta por o "não consta" no nome da mãe
            form_consultation.send_keys(first_input_value, Keys.TAB,
                                        '{:02}'.format(dependencies['birthday_day']), '{:02}'.format(dependencies['birthday_month']), '{:02}'.format(dependencies['birthday_year']), Keys.TAB,
                                        dependencies['name_monther'])
            phantom.find_element_by_name('codigoCaptcha').send_keys(captcha_text)

        phantom.get(url)
        while True:
            write_form()
            phantom.delete_cookie('BIGipServerPool_CertidaoQuitacao') # por alguma razão, precisa apagar esses cookies para efetuar a consulta
            phantom.find_element_by_name('Consultar').click()
            l = phantom.find_elements_by_tag_name('td')
            if len(l) > 2:
                break

        l = phantom.find_elements_by_tag_name('td')

        s = l[15].text.split(' - ')

        # todo: por para salvar o nome no person
        # todo: tá salvando o estado com espaço no começo
        cls.update_my_table({'voter_registration': l[2].text, 'domicilio_eleitoral_zona': l[7].text,
                             'domicilio_eleitoral_secao': l[9].text, 'domicilio_eleitoral_local': l[11].text,
                             'domicilio_eleitoral_edereco': l[13].text, 'domicilio_eleitoral_city': s[0],
                             'domicilio_eleitoral_state': s[1]})
        cls.update_crawler_status(True)
