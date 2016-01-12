from . import Crawler
from hashlib import sha1
from hmac import new as hmac
import requests
import json


class CrawlerFazendaReceita(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_peoples_id INTEGER,'
                            'death_year INTEGER'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'fazenda_receita'

    @staticmethod
    def dependencies():
        return 'cpf', 'birthday_day', 'birthday_month', 'birthday_year',

    @staticmethod
    def crop():
        return 'name', 'death_year',

    @staticmethod
    def primitive_required():
        return 'primitive_peoples',

    @classmethod
    def harvest(cls, primitive_peoples=None, dependencies=None):
        day_month_year = '{:02}{:02}{:04}'.format(dependencies['birthday_day'], dependencies['birthday_month'], dependencies['birthday_year'])
        my_hash = hmac(b'Sup3RbP4ssCr1t0grPhABr4sil', bytes(dependencies['cpf'] + day_month_year, 'utf8'), sha1).hexdigest()

        r = requests.post('https://movel01.receita.fazenda.gov.br/servicos-rfb/v2/IRPF/cpf',
                           {'cpf': dependencies['cpf'], 'dataNascimento': day_month_year},
                           headers={'token': my_hash, 'plataforma': 'iPhone OS', 'dispositivo': 'iPhone', 'aplicativo': 'Pessoa Física', 'versao': '8.3', 'versao_app': '4.1'},
                           verify=False)
        json_return = json.loads(r.text)

        if json_return['mensagemRetorno'] != 'OK':
            # se não dê certo, então o cpf não casa com a data de nascimento; um deles, ou ambos, estão incorretos
            cls.update_crawler(primitive_peoples, 'primitive_peoples', -1)
            return

        cls.db.update_primitive_row({'id': primitive_peoples}, 'primitive_peoples', {'name': json_return['nome'].title()})
        cls.update_my_table(primitive_peoples, 'primitive_peoples', {'death_year': json_return['anoObito']})
        cls.update_crawler(primitive_peoples, 'primitive_peoples', 1)

        # todo: falta ainda checar a mensagem de óbito, para salvar no banco
