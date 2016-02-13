import inspect
import re
import sys
from abc import ABCMeta, abstractmethod

###
# Classes fakes para serem usadas na checagem de erros
# Elas substituirão as padrões do pyKyourinrin no crawler que será analisado
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

    @staticmethod
    def read_my_secondary_tables():
        return ()

    @staticmethod
    def column_export():
        return ()

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
    def trigger(cls, table_row): pass

    @classmethod
    @abstractmethod
    def harvest(self, id): pass

###
# Carregar crawler a ser testado
if len(sys.argv) != 2:
    print('Você precisa passar o nome do crawler a ser checado')
    exit()

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

list_secondary_tables_name = []

if len(ct.db.get_sql_list()) == 0:
    print('Erro: É preciso criar ao menos a tabela principal do crawler!')
else:
    without_table_base = True
    table_name_outside_standard = False
    table_main_without_column_personid = False

    re_get_table_name = re.compile('CREATE TABLE IF NOT EXISTS (.*?)\(')
    re_check_table_name = re.compile(ct_name + '($|_.*$)')
    re_check_column_personid = re.compile('peopleid INTEGER,.*FOREIGN KEY\(peopleid\) REFERENCES peoples\(id\)')

    for i in ct.db.get_sql_list():
        table_name = re_get_table_name.search(i).groups()[0]
        if table_name != ct_name:
            list_secondary_tables_name.append(table_name.replace(ct_name + '_', ''))

        if table_name == ct_name:
            without_table_base = False
            if re_check_column_personid.search(i) is None:
                table_main_without_column_personid = True
        elif re_check_table_name.match(table_name) is None:
            table_name_outside_standard = True
            print('Erro: A tabela %s não tem o nome conforme os padrões!' % table_name)

    if without_table_base:
        print('Erro: É preciso ter a tabela principal! A tabela principal tem exatamente o mesmo nome do crawler')
    elif table_main_without_column_personid:
        print('Erro: A tabela principal precisa obrigatorialmente ter uma coluna chamada peopleid que é chave estrangeira para people(id)')

    if table_name_outside_standard:
        print('\nOs nomes das tabelas secundarárias dos crawlers devem seguir o seguinte padrão:\n'
              '(nome do crawler)(|_sub_nome)\n'
              'Por exemplo: são nomes válidos "%s" e "%s_sub_nome"' % (ct_name, ct_name))

    if (not table_name_outside_standard) and (not without_table_base) and (not table_main_without_column_personid):
        print('Nenhum erro foi encontrado')

###
# Checar método read_my_secondary_tables
print('\n* Checar método read_my_secondary_tables *\n')

read_my_secondary_tables = ct.read_my_secondary_tables()

try:
    if type(read_my_secondary_tables) != tuple:
        print('Erro: O retorno precisa ser uma tupla')
        raise Exception

    for i in read_my_secondary_tables:
        if type(i) != dict:
            print('Erro: Os elementos da tupla de retorno do read_my_secondary_tables precisam ser um dicionário')
            raise Exception

        if 'table' not in i:
            print('Erro: Os elementos da tupla precisam ter o table')
        elif i['table'] == ct_name:
            print('Erro: Não deve-se especificar como ler a tabela principal, pois isso já é implícito')
        elif i['table'] not in list_secondary_tables_name:
            print('Erro: Não encontrei a tabela %s' % i['table'])

        if 'reference_column' in i:
            if type(i['reference_column']) != tuple or len(i['reference_column']) != 2:
                print('Erro: reference_column precisa ser uma tupla com dois elementos, sendo o primeiro o nome da tabela e o segundo a coluna que servirá para unirem')

        for i2 in i.keys():
            if i2 != 'table' and i2 != 'reference_column':
                print('Erro: Não sei o que fazer com %s' % i2)

    if len(ct.db.get_sql_list()) > len(read_my_secondary_tables) + 1:
        print('Erro: No método read_my_secondary_tables, ele deve especificar como é a leitura de todas as tabelas secundárias')
    elif len(ct.db.get_sql_list()) < len(read_my_secondary_tables) + 1:
        print('Erro: No método read_my_secondary_tables, ele está com mais elementos do que o necessário')

    print('Nenhum erro foi encontrado')
finally:
    pass

ct.db.clear_sql_list()

###
# Checar método read_my_secondary_tables
print('\n* Checar método column_export *\n')

column_export = ct.column_export()

try:
    if type(column_export) != tuple:
        print('Erro: O retorno precisa ser uma tupla')
        raise Exception

    for i in column_export:
        if type(i) is not dict:
            print('Erro: Os elementos da tupla precisam ser um dicionário')
            raise Exception

        if 'column_name' not in i or 'how' not in i:
            print('Erro: É obrigatório haver "column_name" e "how"')

        for i2 in i.keys():
            if i2 != 'column_name' and i2 != 'how':
                print('Erro: Não sei o que fazer com %s' % i2)

    print('Nenhum erro foi encontrado')
finally:
    pass

###
# Checar método dependencies
print('\n* Checar método dependencies *\n')

dependencies_fail = False

if type(ct.dependencies()) != tuple:
    print('Erro: As depedências sempre precisam ser uma tupla')
    dependencies_fail = True
else:
    have_dependencies = ct.dependencies()[0] != ''

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

    # Remover o método harvest do partial harvest_and_commit
    ct.harvest = ct.harvest.args[0]

    # Devido ao decorator implícito de recolher as depedências, o método harvest pode acabar tornando-se objeto da classe GetDependencies
    # Aqui revertermos isso
    if ct.harvest.__class__.__name__ == 'GetDependencies':
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
