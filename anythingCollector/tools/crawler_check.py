import inspect
import re
import sys
from abc import ABCMeta, abstractmethod

###
# Classes fakes para serem usadas na checagem de erros
# Elas substituirão as padrões do anythingCollector no crawler que será analisado
class DataBaseFak:
    def __init__(self):
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)

    def get_sql_list(self):
        return self.sql

    def clear_sql_list(self):
        self.sql = []

class CrawlerFak:
    __metaclass__ = ABCMeta
    db = DataBaseFak()

    @abstractmethod
    def create_my_table(self): pass

    @classmethod
    def update_my_table(cls, id, column_and_value, table=None): pass

    @classmethod
    def update_crawler(cls, id, result): pass

    @staticmethod
    @abstractmethod
    def name(): pass

    @staticmethod
    @abstractmethod
    def dependencies(): pass

    @staticmethod
    @abstractmethod
    def crop(): pass

    @classmethod
    @abstractmethod
    def harvest(self, id): pass

###
# Carregar crawler a ser testado
file_name = sys.argv[1]
crawler_name = 'Crawler' + file_name.title().replace('_', '')

exec('from crawler.' + file_name + ' import ' + crawler_name, locals(), globals())
exec("CrawlerTest = type('CrawlerTest', (CrawlerFak,), dict(" + crawler_name + ".__dict__))", locals(), globals())

ct = CrawlerTest()
ct_name = ct.name()

print('Checando o crawler %s...' % ct_name)

###
# Checar método create_my_table
print('* Checar método create_my_table *\n')
ct.create_my_table()

if len(ct.db.get_sql_list()) == 0:
    print('Erro: É preciso criar ao menos a tabela base do crawler!')
else:
    without_table_base = True
    table_name_outside_standard = False
    table_base_without_column_personid = False

    re_get_table_name = re.compile('CREATE TABLE IF NOT EXISTS (.*?)\(')
    re_check_table_name = re.compile(ct_name + '($|_.*$)')
    re_check_column_personid = re.compile('peopleid INTEGER,.*FOREIGN KEY\(peopleid\) REFERENCES peoples\(id\)')

    for i in ct.db.get_sql_list():
        table_name = re_get_table_name.search(i).groups()[0]

        if table_name == ct_name:
            without_table_base = False
            if re_check_column_personid.search(i) is None:
                table_base_without_column_personid = True
        elif re_check_table_name.match(table_name) is None:
            table_name_outside_standard = True
            print('Erro: A tabela %s não tem o nome conforme os padrões!' % table_name)

    if without_table_base:
        print('Erro: É preciso ter uma tabela base! A tabela base tem exatamente o mesmo nome do crawler')
    elif table_base_without_column_personid:
        print('Erro: A table base precisa obrigatorialmente ter uma coluna chamada peopleid que é chave estrangeira para people(id)')

    if table_name_outside_standard:
        print('\nOs nomes das tabelas dos crawlers devem seguir o seguinte padrão:\n'
              '(nome do crawler)(|_sub_nome)\n'
              'Por exemplo: são nomes válidos "%s" e "%s_sub_nome"' % (ct_name, ct_name))

    if (not table_name_outside_standard) and (not without_table_base) and (not table_base_without_column_personid):
        print('Nenhum erro foi encontrado')

ct.db.clear_sql_list()

###
# Checar método dependencies
print('\n* Checar método dependencies *\n')

dependencies_fail = False

if type(ct.dependencies()) != tuple:
    print('Erro: As depedências sempre precisam ser uma tupla')
    dependencies_fail = True
else:
    have_dependencies = len(ct.dependencies()) > 0

    multiple_dependence_routes = type(ct.dependencies()[0]) == tuple
    dependencies_outside_standard = False

    if multiple_dependence_routes:
        for i in ct.dependencies():
            if type(i) != tuple:
                dependencies_outside_standard = True
                break
            for i2 in i:
                if type(i2) != str:
                    dependencies_outside_standard = True
                    break

    else:
        for i in ct.dependencies():
            if type(i) != str:
                dependencies_outside_standard = True
                break

    if dependencies_outside_standard:
        print('Erro: Retorno de depedencies fora dos padrões!\n'
              'As depedências podem ter tanto uma única rota como múltiplas.\n'
              'No caso de ter uma única rota, deve ser apenas uma única tupla e, dentro dessa única tupla, ter as strings com os nome das depedências dela.\n'
              'Por exemplo: se um crawler retornar (\'name\', \'name_monther\', \'cpf\',) depende obrigatorialmente desses dados\n'
              'No caso de ter múltiplas rotas, deve ter uma tupla e, dessa tupla, cada rota ter a sua tupla contendo as strings das depedências de cada rota.\n'
              'Por exemplo: se um crawler retornar ((\'name\', \'name_monther\',), (\'cpf\',)) depende do nome da pessoa e nome da mãe ou então depende do cpf\n'
              'Um exemplo inválido seria retornar ((\'name\', \'name_monther\',), \'cpf\',)')
    else:
        print('Nenhum erro foi encontrado')

###
# Checar método crop
print('\n* Checar método crop *\n')

if type(ct.crop()) != tuple:
    print('Erro: A safra sempre precisa ser uma tupla')
else:
    crop_all_right = True
    for i in ct.crop():
        if type(i) != str:
            print('Erro: Os elementos da tupla da safra sempre precisa ser uma string')
            crop_all_right = False
            break

    if crop_all_right:
        print('Nenhum erro foi encontrado')

###
# Checar método harvest
print('\n* Checar método harvest *\n')

if dependencies_fail:
    print('Não foi possível fazer essa verificação, pois houve falha grave em depedencies')
else:
    harvest_all_right = True

    if ct.harvest.__class__.__name__ == 'GetDependencies':
        # Devido ao decorator implícito de recolher as depedências, o método harvest pode acabar tornando-se objeto da classe GetDependencies
        # Aqui revertermos isso, para facilitar a análise
        ct.harvest = ct.harvest.harvest

    harvest_args = inspect.getargspec(ct.harvest).args

    if have_dependencies and 'dependencies' not in harvest_args:
        print('Erro: Crawler tem depedências, mas falta o parâmetro "depedencies" no cabeçalho de harvest')
        harvest_all_right = False

    if have_dependencies and 'id' not in harvest_args:
        print('Erro: Crawler tem depedências, mas falta o parâmetro "id" no cabeçalho de harvest')
        harvest_all_right = False

    if not have_dependencies and 'id' in harvest_args:
        print('Erro: Crawler não tem depedências, mas há o parâmetro "id" no cabeçalho de harvest')
        harvest_all_right = False

    if harvest_all_right:
        print('Nenhum erro foi encontrado')
