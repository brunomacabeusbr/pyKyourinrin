from . import Crawler
import requests
import os
import re
from PyPDF2 import PdfFileReader


class CrawlerSspds(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'peopleid INTEGER,'
                            'registrocriminal TEXT,'
                            'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'sspds'

    @staticmethod
    def dependencies():
        return 'identity', 'name', 'birthday_day', 'birthday_month', 'birthday_year', 'name_monther'

    @staticmethod
    def crop():
        return 'cpf', 'registrocriminal',

    @classmethod
    def harvest(cls, id=None, dependencies=None):
        r2 = requests.post('http://www.sspds.ce.gov.br/AtestadoAntecedentes/AtestadoPesquisa.do?action=pesquisar',
                      {'numRg': str(dependencies['identity']), 'nome': dependencies['name'], 'dataNasc': '{:02}'.format(dependencies['birthday_day']) + '/' + '{:02}'.format(dependencies['birthday_month']) + '/' + '{:02}'.format(dependencies['birthday_year']), 'mae': dependencies['name_monther']})

        if r2.headers['content-type'] == 'application/pdf':
            with open('myfile.pdf', 'wb') as f:
                for chunk in r2.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            f = open('myfile.pdf', 'rb')
            reader = PdfFileReader(f)
            contents = reader.getPage(0).extractText()
            f.close()
            os.remove('myfile.pdf')

            regexp_antecedentes = re.compile('(CPF|RG)[\sNº]*([\d-]+)\s*\.(.*?)Fortaleza.*$')
            document, people_cpf, people_antecedentes = regexp_antecedentes.search(contents).groups()

            if document == 'CPF':
                cls.db.update_people({'name': dependencies['name']}, {'cpf': people_cpf})

            cls.update_my_table(id, {'registrocriminal': people_antecedentes})
            cls.update_crawler(id, 1)
        else:
            cls.update_crawler(id, -1)