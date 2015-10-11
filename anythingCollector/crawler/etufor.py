from . import Crawler
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.keys import Keys


class CrawlerEtufor(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'peopleid INTEGER,'
                            'cia INTEGER,'
                            'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'etufor'

    @staticmethod
    def dependencies():
        return 'name', 'birthday_day', 'birthday_month', 'birthday_year',

    @staticmethod
    def crop():
        return 'name_social', 'cia', 'name_monther',

    @classmethod
    def harvest(cls, id=None, dependencies=None):
        # todo: recolher mais informações ainda e melhorar o código
        phantom = webdriver.PhantomJS()

        phantom.get('http://www.fortaleza.ce.gov.br/etuforComponents/CarteiradeEstudantes/consultaSolicitacao.php')
        elem = phantom.find_element_by_name('Nome')
        elem.send_keys(dependencies['name'] + Keys.TAB + '{:02}'.format(dependencies['birthday_day']) + '{:02}'.format(dependencies['birthday_month']) + '{:02}'.format(dependencies['birthday_year']))

        phantom.find_element_by_name('btnpesq').click()

        phantom.implicitly_wait(1) # usar wait mais bacana: http://selenium-python.readthedocs.org/en/latest/waits.html

        if len(phantom.find_elements_by_css_selector('font')) == 7:
            # pessoa nao tem carteira da etufor
            cls.update_crawler(id, -1)
            return

        etuforCia = phantom.find_elements_by_css_selector('font')[6].get_attribute('innerHTML') # melhorar essa selecao
        nameSocial = phantom.find_elements_by_css_selector('font')[9].get_attribute('innerHTML').strip()

        cls.update_my_table(id, {'cia': etuforCia})

        try:
            phantom.find_element_by_tag_name('a').click()

            phantom.implicitly_wait(2)

            if len(phantom.find_elements_by_css_selector('font')) <= 15:
                cls.db.execute("UPDATE peoples SET name_social='%s' WHERE id=%i" %
                        (nameSocial, id))
                cls.update_crawler(id, 1)
                return

            montherName = phantom.find_elements_by_css_selector('font')[16].get_attribute('innerHTML').strip()

            cls.db.update_people(dependencies['name'], {'name_social': nameSocial, 'name_monther': montherName})
        except exceptions.NoSuchElementException as e:
            cls.db.update_people(dependencies['name'], {'name_social': nameSocial})

        cls.update_crawler(id, 1)