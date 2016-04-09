import sqlite3
from crawler import Crawler, start_triggers
import inspect


class ManagerDatabase:
    def __init__(self, trigger=True):
        import os
        path_spyck = os.path.dirname(__file__)
        self.con = sqlite3.connect(path_spyck + '/mydatabase.db', check_same_thread=False)

        self.c = self.con.cursor()

        ###
        # criar/atualizar banco de dados

        # table main_trigger: usada para armazanar dados de configuração e temporização dos triggers
        self.execute('CREATE TABLE IF NOT EXISTS main_trigger('
                        'crawler TEXT,'
                        'infos TEXT'
                     ');')

        # Deixar crawlers pronto para serem usados e atualizar a tabela main_trigger
        Crawler.db = self
        for cls in Crawler.__subclasses__():
            setattr(self, 'crawler_' + cls.name(), cls())
            if cls.trigger.__code__ != Crawler.trigger.__code__:
                if len(self.execute("SELECT * FROM main_trigger WHERE crawler=?", (cls.name(),)).fetchall()) == 0:
                    self.execute('INSERT INTO main_trigger (crawler) VALUES (?)', (cls.name(),))

        # criar tabelas das primitives com base nos xml
        import xml.etree.ElementTree as ET

        for current_xml in os.listdir(path_spyck + '/primitives/'):
            xml_root = ET.parse('primitives/' + current_xml).getroot()
            columns = [(current_xml.find('name').text, current_xml.find('type').text) for current_xml in xml_root.findall('column')]

            primitive_name = current_xml[:-4]
            self.execute(
                'CREATE TABLE IF NOT EXISTS {}('
                    'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                    '{}'
                ');'.format('primitive_' + primitive_name,
                            ','.join([i[0] + ' ' + i[1] for i in columns]))
            )

            self.execute(
                'CREATE TABLE IF NOT EXISTS {}('
                    'id INTEGER,'
                    'FOREIGN KEY(id) REFERENCES {}(id)'
                ');'.format('primitive_' + primitive_name + '_crawler',
                            'primitive_' + primitive_name)
            )

        # Atualizar tabela primitive_##name_crawler de acordo com os cralwers que requerem determinada primitive
        primitive_list = [
            i[0] for i in self.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if i[0][:9] == 'primitive' and i[0][-7:] != 'crawler'
        ]
        crawlers_names = [i.name() for i in Crawler.__subclasses__()]

        for i in primitive_list:
            for i2 in [i3[0] for i3 in self.execute("SELECT name FROM sqlite_master WHERE sql LIKE '%{}_id INTEGER%'".format(i)).fetchall()]:
                if i2 in crawlers_names:
                    try:
                        self.execute('ALTER TABLE {} ADD COLUMN {} INTEGER DEFAULT 0;'.format(i + '_crawler', i2))
                    except:
                         # coluna já existe
                        pass

        # table main_arbitrary: permitir setar valores arbitrários
        self.execute('CREATE TABLE IF NOT EXISTS main_arbitrary('
                        'primitive_id INTEGER,'
                        'primitive_name TEXT,'
                        'column_name TEXT,'
                        'column_value TEXT,'
                        'column_set_integer INTEGER DEFAULT 0'
                     ');')

        # Executar crawlers trigáveis, se assim foi configurado
        if trigger:
            start_triggers()

        # salvar as mudanças no banco
        self.commit()

    def execute(self, sql, parameters=()):
        return self.c.execute(sql, parameters)

    def commit(self):
        return self.con.commit()

    def lastrowid(self):
        return self.c.lastrowid

    def select_column_and_value(self, sql, parameters=(), discard=[]):
        execute = self.execute(sql, parameters)
        fetch = execute.fetchone()

        if fetch is None:
            return {k[0]: None for k in execute.description}

        return {k[0]: v for k, v in list(zip(execute.description, fetch)) if k[0] not in discard}

    # todo: talvez possa juntar esse método com o de cima
    def select_column_and_value_many(self, sql, parameters=(), discard=[]):
        execute = self.execute(sql, parameters)
        fetch = execute.fetchall()

        to_return = []

        for i in fetch:
            to_return.append({k[0]: v for k, v in list(zip(execute.description, i)) if k[0] not in discard})

        return to_return

    def count_primitive_rows_with_this_filters(self, primitive_filter, primitive_name):
        # todo: só filtra com base nos dados da tabela pricipal da primitive
        return len(self.execute("SELECT * FROM %s WHERE %s" %
                                (primitive_name,
                                 'AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in primitive_filter.items()))).fetchall())

    def new_primitive_row(self, primitive_infos, primitive_name): # todo: esse parâmetro primitive_name foge dos padrões, pois precisa ser o "primitive_##name"
        primitive_infos = {k: "'{}'".format(str(v).replace("'", "''")) for k, v in primitive_infos.items()}
        self.execute('INSERT INTO ' + primitive_name + ' (' + ','.join(primitive_infos.keys()) + ') VALUES (' + ','.join(primitive_infos.values()) + ')')
        self.execute('INSERT INTO ' + primitive_name + '_crawler (id) VALUES (?)', (self.lastrowid(),))

    def update_primitive_row(self, column_and_value, primitive_filter=None, primitive_name=None):
        if hasattr(Crawler, 'temp_current_primitive_name') and primitive_name is None:
            if primitive_filter is not None or primitive_name is not None:
                raise ValueError('Se não for usar o valor temporário, vindo da primitive_id passada no harvest,'
                                 'use os parâmetros "primitive_filter" e "primitive_name"')

            primitive_name = Crawler.temp_current_primitive_name
            where_statement = ' WHERE id=' + str(Crawler.temp_current_primitive_id)
        else:
            if primitive_filter is None or primitive_name is None:
                raise ValueError('É necessário fornecer o parâmetro "primitive_filter" e "primitive_name" para eu saber qual primitive row eu irei atualizar,'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de primitive')
            if primitive_name not in Crawler.temp_current_crawler.primitive_required():
                raise ValueError('A primitive que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(primitive_name))

            # Verificar se a primitive row já existe e, caso não exista, cria
            # todo: essa checagem é otimista, pois não considera o seguinte caso:
            #  suponha que o filtro seja pelo nome "João", e no meu banco eu tenha uma pessoa que não sei o nome e um que se chame 1 João,
            #  então dará certo, pois como só tem uma pessoa que cumpra o filtro, e assim editará essa única primitive row,
            #  porém, a pessoa em que eu não sei o nome pode se chamar João e acabar editando a errada
            count_people = self.count_primitive_rows_with_this_filters(primitive_filter, primitive_name)
            if count_people == 0:
                self.new_primitive_row(primitive_filter, primitive_name)
            elif count_people > 1:
                raise ValueError('Há mais que uma primitive row com os critérios fornecidos! Não sei qual eu devo atualizar')

            # Definir qual linha da primitive deve ser atualizada
            where_statement = ' WHERE %s ' % ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in primitive_filter.items())

        # Salvar no banco
        column_and_value = {i: j for i, j in column_and_value.items() if j is not None}

        if len(column_and_value) > 0:
            self.execute("UPDATE " + primitive_name +
                         " SET " + ','.join("{}='{}'".format(key, str(val).replace("'", "''")) for key, val in column_and_value.items()) +
                         where_statement)

        # Retornar primitive id que foi editado - isso é útil para crawler nascente
        return self.execute("SELECT id FROM %s %s" % (primitive_name, where_statement)).fetchone()[0]

    def crawler_list_status(self, primitive_id, primitive_name):
        return self.select_column_and_value(
            'SELECT * FROM primitive_' + primitive_name + '_crawler WHERE id=?', (primitive_id,), discard=['id']
        )

    def crawler_list_used(self, primitive_id, primitive_name):
        return {k: v for k, v in self.crawler_list_status(primitive_id, primitive_name).items() if v != 0}

    def crawler_list_success(self, primitive_id, primitive_name):
        return [k for k, v in self.crawler_list_status(primitive_id, primitive_name).items() if v == 1]

    def get_primitive_row_info(self, primitive_id, primitive_name, get_tables_secondary=True):
        crawler_list_success = self.crawler_list_success(primitive_id, primitive_name)
        crawler_list_success_cls = [i for i in Crawler.__subclasses__() if i.name() in crawler_list_success]
        crawler_list_success_cls = [i for i in crawler_list_success_cls if 'primitive_' + primitive_name in inspect.getargspec(i.harvest_debug).args]

        ###
        # Recolher infos da tabela da primitive e da tabela principal dos crawlers
        fieldnames = self.select_column_and_value(
            'SELECT * FROM primitive_{} '.format(primitive_name) +
            ' '.join([
                'INNER JOIN {} ON {}.primitive_{}_id == {}'.format(i.name(), i.name(), primitive_name, primitive_id)
                for i in crawler_list_success_cls
            ]) +
            ' WHERE primitive_{}.id == {}'.format(primitive_name, primitive_id),
            discard=['id', 'primitive_{}_id'.format(primitive_name)]
        )

        ###
        # Recolher infos das tabelas secundárias dos crawlers que obtiveram sucesso
        if get_tables_secondary:
            def add_referenced_value(origin, to_add):
                if current_rule['table'] not in origin:
                    origin[current_rule['table']] = []

                origin[current_rule['table']].append(to_add)

            def get_deep_fieldnames():
                # essa função irá listar os itens que servem de referência de acordo com o current_rule
                deep = fieldnames[cls.name() + '_' + current_rule['reference'][0]]

                for deeping in current_rule['reference'][1:]:
                    deep = [t[deeping] for t in deep if deeping in t]
                    deep = [tt for t in deep for tt in t]

                return deep

            for cls in crawler_list_success_cls:
                # Percorrer lista com as regras de leitura das tabelas secundárias
                for current_rule in cls.read_my_secondary_tables():
                    current_table_name = current_rule['table']
                    current_table_name_full = cls.name() + '_' + current_table_name

                    # recolher infos da tabela
                    infos = self.select_column_and_value_many(
                        'SELECT * FROM {} WHERE {}.primitive_{}_id == {}'.format(
                            current_table_name_full, current_table_name_full, primitive_name, primitive_id
                        )
                    )

                    if 'reference' not in current_rule:
                        # se a tabela não é referenciada, adicionar os seus dados à raiz de fieldnames

                        fieldnames[current_table_name_full] = infos
                    else:
                        # se a tabela for referenciada, precisamos adicionar seu valores em sua respectiva referência

                        [
                            add_referenced_value(a, b)

                            for a in get_deep_fieldnames()
                            for b in infos

                            if a['reference'] == b['reference_' + current_rule['reference'][-1]]
                        ]

            ###
            # Chamar método macro_at_data dos crawlers que obtiveram sucesso
            for cls in crawler_list_success_cls:
                for i in cls.macro_at_data():
                    fieldnames[i['column_name']] = i['how'](fieldnames)

        ###
        # Listar primitives que referenciam para min
        # todo: precisa ser feito isso ainda
        # para isso, talvez eu precise criar uma tabela para fazer esse trabalho
        # sempre que uma linha for se referir a uma primitive, precisará escrever nessa tabela
        # ela terá as colunas "id", "nome da tabela em que foi referenciada", "nome da coluna em que a primitive id foi referenciada"

        ###
        # Recolher infos da tabela main_arbitrary
        def get_value_typed(j):
            if j['column_set_integer'] and j['column_value'] is not None:
                return int(j['column_value'])
            else:
                return j['column_value']

        fieldnames.update(
            {
                i['column_name']: get_value_typed(i) for i in
                self.select_column_and_value_many('SELECT * FROM main_arbitrary WHERE primitive_id=? and primitive_name=?', (primitive_id, primitive_name))
            }
        )

        ###
        # Apagar valores, agora desnecessários, no fieldnames, tais como reference
        # todo

        return fieldnames

    # Retorna um dicionário com os dados requeridos em "dependencies", porém,
    # se algum dos dados requeridos em "dependencies" não for pertecente à primitiva, retornará apenas False
    def get_dependencies(self, primitive_id, primitive_name, *dependencies):
        infos = self.get_primitive_row_info(primitive_id, primitive_name)
        infos_keys = list(infos.keys())

        if len([i for i in dependencies if i not in infos_keys]) > 0:
            return False

        return {k: v for k, v in infos.items() if k in dependencies}

    def get_primitive_id_by_filter(self, primitive_filter, primitive_name):
        count_people = self.count_primitive_rows_with_this_filters(primitive_filter, primitive_name)
        if count_people == 0:
            return False
        elif count_people > 1:
            raise ValueError('Há mais que uma linha com os critérios fornecidos! Não sei de qual eu devo entregar o ID')

        return self.execute("SELECT * FROM " + primitive_name + " WHERE %s" %
                            ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in primitive_filter.items())).fetchone()[0]
