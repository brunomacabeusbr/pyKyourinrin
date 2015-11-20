from . import Crawler
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.keys import Keys
import datetime
import re


class CrawlerEtufor(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                        'peopleid INTEGER,'
                        'cia INTEGER,'
                        'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                        'peopleid INTEGER,'
                        'timestamp TEXT,'
                        'school TEXT,'
                        'type TEXT,'
                        'course TEXT,'
                        'turn TEXT,'
                        'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % (self.name() + '_records_school'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'records_school'},
        )

    @staticmethod
    def secondary_tables_export():
        def last_school_name(readed):
            if len(readed['records_school']) > 0:
                return readed['records_school'][-1]['school']
            else:
                return None

        return (
            {'column_name': 'last_school_name', 'how': last_school_name},
        )

    @staticmethod
    def name():
        return 'etufor'

    @staticmethod
    def dependencies():
        return ('name', 'birthday_day', 'birthday_month', 'birthday_year'), ('cia',),

    @staticmethod
    def crop():
        return 'name_social', 'cia', 'name', 'name_monther', 'birthday_day', 'birthday_month', 'birthday_year', 'last_school_name'

    @classmethod
    def harvest(cls, id=None, dependencies=None):
        phantom = webdriver.PhantomJS()

        phantom.get('http://www.etufor.ce.gov.br/index_novo.asp?pagina=sit_carteira2007.asp')

        if 'cia' in dependencies:
            form_consultation = phantom.find_element_by_name('CIA')
            form_consultation.send_keys(dependencies['cia'])
        else:
            form_consultation = phantom.find_element_by_name('Nome')
            form_consultation.send_keys(dependencies['name'] + Keys.TAB + '{:02}'.format(dependencies['birthday_day']) + '{:02}'.format(dependencies['birthday_month']) + '{:02}'.format(dependencies['birthday_year']))
        phantom.find_element_by_name('btnpesq').click()
        phantom.switch_to.window(phantom.window_handles[1])

        def count_total_box_table():
            return len(phantom.find_elements_by_css_selector('font'))

        def get_text_in_table(index):
            value = phantom.find_elements_by_css_selector('font')[index].get_attribute('innerHTML').strip()
            if value == '&nbsp;':
                value = ''
            return value

        if count_total_box_table() == 7:
            # pessoa nao tem carteira da etufor
            cls.update_crawler(id, -1)
            return

        cls.update_crawler(id, 1)

        regexp_date = re.compile(r'(\d+)\/(\d+)\/(\d+)')
        birthday_day, birthday_month, birthday_year = regexp_date.search(get_text_in_table(10)).groups()
        cls.db.update_people({'id': id},
                             {'name_social': get_text_in_table(9), 'birthday_day': birthday_day, 'birthday_month': birthday_month, 'birthday_year': birthday_year})
        cls.update_my_table(id, {'cia': get_text_in_table(6)})

        try:
            phantom.find_element_by_tag_name('a').click()
        except exceptions.NoSuchElementException:
            return

        if count_total_box_table() <= 15:
            # não há mais dados a serem colhidos
            return

        cls.db.update_people({'id': id}, {'name_monther': get_text_in_table(16)})

        regexp_timestamp = re.compile(r'(\d+)\/(\d+)\/(\d+)\s(\d+):(\d+):(\d+)')
        pos = 37
        while True:
            if get_text_in_table(pos) == '_______ HISTÓRICO DE ETAPAS DO PROCESSO \n\t\t\t\t\t\t\tATUAL&nbsp;_______':
                break
            timestamp, school, school_type, course, turn =\
                get_text_in_table(pos), get_text_in_table(pos + 2), get_text_in_table(pos + 3), get_text_in_table(pos + 8), get_text_in_table(pos + 4)

            timestamp_day, timestamp_month, timestamp_year, timestamp_hour, timestamp_minute, timestamp_second =\
                regexp_timestamp.search(timestamp).groups()
            timestamp_iso = str(datetime.datetime(int(timestamp_year), int(timestamp_month), int(timestamp_day), int(timestamp_hour), int(timestamp_minute), int(timestamp_second)))

            cls.update_my_table(id, {'timestamp': timestamp_iso, 'school': school, 'type': school_type, 'course': course, 'turn': turn}, table='records_school')
            pos += 10

        cls.update_crawler(id, 1)