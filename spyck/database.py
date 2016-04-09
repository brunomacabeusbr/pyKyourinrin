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

        # criar tabelas das entities com base nos xml
        import xml.etree.ElementTree as ET

        for current_xml in os.listdir(path_spyck + '/entities/'):
            xml_root = ET.parse('entities/' + current_xml).getroot()
            columns = [(current_xml.find('name').text, current_xml.find('type').text) for current_xml in xml_root.findall('column')]

            entity_name = current_xml[:-4]
            self.execute(
                'CREATE TABLE IF NOT EXISTS {}('
                    'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                    '{}'
                ');'.format('entity_' + entity_name,
                            ','.join([i[0] + ' ' + i[1] for i in columns]))
            )

            self.execute(
                'CREATE TABLE IF NOT EXISTS {}('
                    'id INTEGER,'
                    'FOREIGN KEY(id) REFERENCES {}(id)'
                ');'.format('entity_' + entity_name + '_crawler',
                            'entity_' + entity_name)
            )

        # Atualizar tabela entity_##name_crawler de acordo com os cralwers que requerem determinada entity
        entity_list = [
            i[0] for i in self.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if i[0][:6] == 'entity' and i[0][-7:] != 'crawler'
        ]
        crawlers_names = [i.name() for i in Crawler.__subclasses__()]

        for i in entity_list:
            for i2 in [i3[0] for i3 in self.execute("SELECT name FROM sqlite_master WHERE sql LIKE '%{}_id INTEGER%'".format(i)).fetchall()]:
                if i2 in crawlers_names:
                    try:
                        self.execute('ALTER TABLE {} ADD COLUMN {} INTEGER DEFAULT 0;'.format(i + '_crawler', i2))
                    except:
                         # coluna já existe
                        pass

        # table main_arbitrary: permitir setar valores arbitrários
        self.execute('CREATE TABLE IF NOT EXISTS main_arbitrary('
                        'entity_id INTEGER,'
                        'entity_name TEXT,'
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

    def count_entity_rows_with_this_filters(self, entity_filter, entity_name):
        # todo: só filtra com base nos dados da tabela pricipal da entity
        return len(self.execute("SELECT * FROM %s WHERE %s" %
                                (entity_name,
                                 'AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in entity_filter.items()))).fetchall())

    def new_entity_row(self, entity_infos, entity_name): # todo: esse parâmetro entity_name foge dos padrões, pois precisa ser o "entity_##name"
        entity_infos = {k: "'{}'".format(str(v).replace("'", "''")) for k, v in entity_infos.items()}
        self.execute('INSERT INTO ' + entity_name + ' (' + ','.join(entity_infos.keys()) + ') VALUES (' + ','.join(entity_infos.values()) + ')')
        self.execute('INSERT INTO ' + entity_name + '_crawler (id) VALUES (?)', (self.lastrowid(),))

    def update_entity_row(self, column_and_value, entity_filter=None, entity_name=None):
        if hasattr(Crawler, 'temp_current_entity_name') and entity_name is None:
            if entity_filter is not None or entity_name is not None:
                raise ValueError('Se não for usar o valor temporário, vindo da entity_id passada no harvest,'
                                 'use os parâmetros "entity_filter" e "entity_name"')

            entity_name = Crawler.temp_current_entity_name
            where_statement = ' WHERE id=' + str(Crawler.temp_current_entity_id)
        else:
            if entity_filter is None or entity_name is None:
                raise ValueError('É necessário fornecer o parâmetro "entity_filter" e "entity_name" para eu saber qual entity row eu irei atualizar,'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de entity')
            if entity_name not in Crawler.temp_current_crawler.entity_required():
                raise ValueError('A entity que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(entity_name))

            # Verificar se a entity row já existe e, caso não exista, cria
            # todo: essa checagem é otimista, pois não considera o seguinte caso:
            #  suponha que o filtro seja pelo nome "João", e no meu banco eu tenha uma pessoa que não sei o nome e um que se chame 1 João,
            #  então dará certo, pois como só tem uma pessoa que cumpra o filtro, e assim editará essa única entity row,
            #  porém, a pessoa em que eu não sei o nome pode se chamar João e acabar editando a errada
            count_people = self.count_entity_rows_with_this_filters(entity_filter, entity_name)
            if count_people == 0:
                self.new_entity_row(entity_filter, entity_name)
            elif count_people > 1:
                raise ValueError('Há mais que uma entity row com os critérios fornecidos! Não sei qual eu devo atualizar')

            # Definir qual linha da entity deve ser atualizada
            where_statement = ' WHERE %s ' % ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in entity_filter.items())

        # Salvar no banco
        column_and_value = {i: j for i, j in column_and_value.items() if j is not None}

        if len(column_and_value) > 0:
            self.execute("UPDATE " + entity_name +
                         " SET " + ','.join("{}='{}'".format(key, str(val).replace("'", "''")) for key, val in column_and_value.items()) +
                         where_statement)

        # Retornar entity id que foi editado - isso é útil para crawler nascente
        return self.execute("SELECT id FROM %s %s" % (entity_name, where_statement)).fetchone()[0]

    def crawler_list_status(self, entity_id, entity_name):
        return self.select_column_and_value(
            'SELECT * FROM entity_' + entity_name + '_crawler WHERE id=?', (entity_id,), discard=['id']
        )

    def crawler_list_used(self, entity_id, entity_name):
        return {k: v for k, v in self.crawler_list_status(entity_id, entity_name).items() if v != 0}

    def crawler_list_success(self, entity_id, entity_name):
        return [k for k, v in self.crawler_list_status(entity_id, entity_name).items() if v == 1]

    def get_entity_row_info(self, entity_id, entity_name, get_tables_secondary=True):
        crawler_list_success = self.crawler_list_success(entity_id, entity_name)
        crawler_list_success_cls = [i for i in Crawler.__subclasses__() if i.name() in crawler_list_success]
        crawler_list_success_cls = [i for i in crawler_list_success_cls if 'entity_' + entity_name in inspect.getargspec(i.harvest_debug).args]

        ###
        # Recolher infos da tabela da entity e da tabela principal dos crawlers
        fieldnames = self.select_column_and_value(
            'SELECT * FROM entity_{} '.format(entity_name) +
            ' '.join([
                'INNER JOIN {} ON {}.entity_{}_id == {}'.format(i.name(), i.name(), entity_name, entity_id)
                for i in crawler_list_success_cls
            ]) +
            ' WHERE entity_{}.id == {}'.format(entity_name, entity_id),
            discard=['id', 'entity_{}_id'.format(entity_name)]
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
                        'SELECT * FROM {} WHERE {}.entity_{}_id == {}'.format(
                            current_table_name_full, current_table_name_full, entity_name, entity_id
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
        # Listar entities que referenciam para min
        # todo: precisa ser feito isso ainda
        # para isso, talvez eu precise criar uma tabela para fazer esse trabalho
        # sempre que uma linha for se referir a uma entity, precisará escrever nessa tabela
        # ela terá as colunas "id", "nome da tabela em que foi referenciada", "nome da coluna em que a entity id foi referenciada"

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
                self.select_column_and_value_many('SELECT * FROM main_arbitrary WHERE entity_id=? and entity_name=?', (entity_id, entity_name))
            }
        )

        ###
        # Apagar valores, agora desnecessários, no fieldnames, tais como reference
        # todo

        return fieldnames

    # Retorna um dicionário com os dados requeridos em "dependencies", porém,
    # se algum dos dados requeridos em "dependencies" não for pertecente à primitiva, retornará apenas False
    def get_dependencies(self, entity_id, entity_name, *dependencies):
        infos = self.get_entity_row_info(entity_id, entity_name)
        infos_keys = list(infos.keys())

        if len([i for i in dependencies if i not in infos_keys]) > 0:
            return False

        return {k: v for k, v in infos.items() if k in dependencies}

    def get_entity_id_by_filter(self, entity_filter, entity_name):
        count_people = self.count_entity_rows_with_this_filters(entity_filter, entity_name)
        if count_people == 0:
            return False
        elif count_people > 1:
            raise ValueError('Há mais que uma linha com os critérios fornecidos! Não sei de qual eu devo entregar o ID')

        return self.execute("SELECT * FROM " + entity_name + " WHERE %s" %
                            ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in entity_filter.items())).fetchone()[0]
