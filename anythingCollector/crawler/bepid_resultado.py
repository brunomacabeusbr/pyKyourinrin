from . import Crawler
import requests
import os
import re
from PyPDF2 import PdfFileReader


class CrawlerBepidResultado(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'peopleid INTEGER,'
                            'bepid_position INTEGER,'
                            'bepid_score INTEGER,'
                            'bepid_ranked INTEGER,'
                            'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'bepid_resultado'

    @staticmethod
    def dependencies():
        return '',

    @staticmethod
    def crop():
        return 'name', 'birthday_day', 'birthday_month', 'birthday_year',

    @classmethod
    def harvest(cls, id=None, dependencies=None):
        # Aviso: Antes de colher aqui, deve-se user ManagerDatabase().crawler_qselecao.harvest(specifc_concurso=2890)

        r = requests.get('http://www.bepid.ifce.edu.br/resultado_prova_selecao.pdf')
        with open('myfile.pdf', 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        f = open('myfile.pdf', 'rb')
        reader = PdfFileReader(f)
        content = ''
        for i in reader.pages:
            content += i.extractText()
        f.close()
        os.remove('myfile.pdf')
        content = ' '.join(content.replace('\xa0', ' ').strip().split())

        regexp = re.compile(r'(\d+)(\D+) (\d{2})\/(\d{2})\/(\d{4})(\d+,\d)(\w+)')

        for i in regexp.findall(content):
            tableid = cls.db.get_tableid_of_people({'name': i[1], 'birthday_day': i[2] , 'birthday_month': i[3], 'birthday_year': i[4]})

            cls.update_my_table(tableid, {'position': i[0], 'score': float(i[5].replace(',', '.')), 'ranked': (0, 1)[i[6] == 'Classificado']})
            cls.update_crawler(tableid, 1)
