import sqlite3
from crawler import Crawler, start_triggers
import copy


class ManagerDatabase:
    def __init__(self, trigger=True):
        import os
        path_pykyourinrin = os.path.dirname(__file__)
        self.con = sqlite3.connect(path_pykyourinrin + '/mydatabase.db', check_same_thread=False)

        self.c = self.con.cursor()

        ###
        # criar/atualizar banco de dados

        # criar tabelas das primitives com base nos xml
        import xml.etree.ElementTree as ET

        for current_xml in os.listdir(path_pykyourinrin + '/primitives/'):
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
        tables_primitive_list = [
            i[0] for i in self.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if i[0][:9] == 'primitive' and i[0][-7:] != 'crawler'
        ]

        for cls in Crawler.__subclasses__():
            primitive_required = cls.primitive_required()
            for i in primitive_required:
                if i not in tables_primitive_list:
                    raise ValueError('O crawler "{}" requer a primitiva desconhecida "{}"'.format(cls.name(), i))

                try:
                    self.execute('ALTER TABLE {} ADD COLUMN {} INTEGER DEFAULT 0;'.format(i + '_crawler', cls.name()))
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

        # table main_linker_primitives: relacionar dois elementos de primitives diferentes
        # todo: preciso limitar para um determinado par de "first_id" e "first_name" não possa repetir dois "second_name"
        self.execute('CREATE TABLE IF NOT EXISTS main_linker_primitives('
                        'first_id INTEGER,'
                        'first_name TEXT,'
                        'second_id INTEGER,'
                        'second_name TEXT'
                     ');')

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
        primitive_infos = {k: '"' + str(v) + '"' for k, v in primitive_infos.items()}
        self.execute('INSERT INTO ' + primitive_name + ' (' + ','.join(primitive_infos.keys()) + ') VALUES (' + ','.join(primitive_infos.values()) + ')')
        self.execute('INSERT INTO ' + primitive_name + '_crawler (id) VALUES (?)', (self.lastrowid(),))

    def update_primitive_row(self, column_and_value, primitive_filter=None, primitive_name=None):
        if hasattr(Crawler, 'temp_current_primitive_name'):
            if primitive_filter is not None or primitive_name is not None:
                raise ValueError('Não forneça o parâmetro "primitive_filter" nem "primitive_name",'
                                 'pois esse crawler recebeu como parâmetro um id de primitive')

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

    def get_primitive_row_info_all(self, primitive_id, primitive_name):
        # Recolher infos da tabela principal dele
        primitive_table = self.select_column_and_value(
            "SELECT * FROM primitive_" + primitive_name + " WHERE id=?", (primitive_id,), discard=['id']
        )
        fieldnames = copy.copy(primitive_table)

        # Recolher infos da tabela dos crawlers
        crawler_required = list(self.crawler_list_status(primitive_id, primitive_name).keys())
        crawler_list_success = self.crawler_list_success(primitive_id, primitive_name)
        for cls in Crawler.__subclasses__():
            # Verificar se essa primitiva usa esse crawler, para, se não usar, ignora-lo
            if cls.name() not in crawler_required:
                continue

            # Tabela principal
            dict_infos = self.select_column_and_value(
                "SELECT * FROM %s WHERE %s=?" % (cls.name(), 'primitive_' + primitive_name + '_id'), (primitive_id,),
                discard=['primitive_' + primitive_name + '_id']
            )
            dict_infos = {k: v for k, v in dict_infos.items() if not(k in fieldnames and v is None)}
            fieldnames.update(dict_infos)

            # Tabelas secundárias
            if cls.name() in crawler_list_success:
                # Adicionar valores das tabelas secundárias à variável "dict_infos"
                for current in cls.read_my_secondary_tables():
                    table = current['table']

                    if 'reference' not in current.keys():
                        # ler tabela não referenciada
                        rows = self.select_column_and_value_many(
                            "SELECT * FROM %s WHERE %s=?" % (cls.name() + '_' + table, 'primitive_' + primitive_name + '_id'), (primitive_id,),
                            discard=['primitive_' + primitive_name + '_id']
                        )

                        # no caso de colunas para referencia, precisamos que seja um dicionário,
                        # para posteriormente adicionar os valores das tabelas referenciadas
                        for current_row in rows:
                            if 'reference' in current_row.keys():
                                current_row['reference'] = {'reference_number': current_row['reference']}

                        # atalizar dict_infos
                        dict_infos.update({table: rows})
                    else:
                        # ler a tabela referenciada; substitui o id da coluna de referência para o seu respectivo conteúdo no banco
                        reference_table = current['reference']
                        if type(reference_table) == tuple:
                            inter = eval( # código para criar array com as colunas a serem acessadas e modificadas com o conteúdo da referência
                                '[' +\
                                    "{}['reference'][reference_table[-1]] ".format(chr(ord('i') + len(reference_table) - 2)) +\
                                    'for i in dict_infos[reference_table[0]] ' +\
                                    ' '.join(
                                        [
                                            "for {} in {}['reference'][reference_table[{}]]".format(
                                                chr(ord('i') + i), chr(ord('i') + i - 1), i
                                            )
                                            for i in range(1, len(reference_table) - 1)
                                        ]
                                    ) +\
                                ']',
                                {'reference_table': reference_table, 'dict_infos': dict_infos}
                            )
                            inter = [j for i in inter for j in i]
                            reference_table = reference_table[-1]
                        else:
                            inter = dict_infos[reference_table]

                        for i in inter:
                            referenceid = i['reference']['reference_number']
                            rows = self.select_column_and_value_many(
                                "SELECT * FROM %s WHERE %s=?" % (cls.name() + '_' + table, 'reference_' + reference_table), (referenceid,),
                                discard=['primitive_' + primitive_name + '_id', 'reference_' + reference_table]
                            )

                            # pode ser que uma tabela referenciada referencie outra tabela
                            # então precisamos tomar o mesmo cuidado que foi feito com as tabelas não referenciada
                            for current_row in rows:
                                if 'reference' in current_row.keys():
                                    current_row['reference'] = {'reference_number': current_row['reference']}

                            i['reference'][table] = rows

                # Em "dict_infos", apagar key temporária "reference_number" (ela é necessária para ligar tabela referenciada e nada mais além após isso)
                for k, v in dict_infos.items():
                    if type(v) is not list:
                        continue

                    for i in v:
                        if 'reference' in i:
                            del i['reference']['reference_number']

                # Adicionar colunas exportadas
                for current in cls.column_export():
                    fieldnames[current['column_name']] = current['how'](dict(dict_infos, **primitive_table))
            else:
                for current in cls.column_export():
                    fieldnames[current['column_name']] = None

        # Recolher infos da tabela main_arbitrary
        def get_value_typed(j):
            if j['column_set_integer'] and j['column_value'] is not None:
                return int(j['column_value'])
            else:
                return j['column_value']

        fieldnames.update(
            {
                i['column_name']: get_value_typed(i) for i in
                self.select_column_and_value_many("SELECT * FROM main_arbitrary WHERE primitive_id=? and primitive_name=?", (primitive_id, primitive_name), discard=['peopleid'])
            }
        )

        # Recolher infos da tabela main_linker
        for i in self.select_column_and_value_many("SELECT second_id, second_name FROM main_linker_primitives WHERE first_id=? and first_name=?", (primitive_id, primitive_name)):
            fieldnames[i['second_name']] = self.get_primitive_row_info_all(i['second_id'], i['second_name'])

        #
        return fieldnames

    # Retorna um dicionário com os dados requeridos em "dependencies", porém,
    # se algum dos dados requeridos em "dependencies" não for pertecente à primitiva, retornará apenas False
    def get_dependencies(self, primitive_id, primitive_name, *dependencies):
        infos = self.get_primitive_row_info_all(primitive_id, primitive_name)
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

    def get_primitive_row_with_criteria(self, primitive_name, want=[], crawler_need=None, crawler_exclude=None, restriction=None):
        to_return = []

        for current_primitive_row in self.execute('SELECT id FROM primitive_' + primitive_name).fetchall():
            current_id = current_primitive_row[0]

            if crawler_need is not None:
                list_crawler = self.crawler_list_status(current_id, primitive_name)
                pass_this_people = False
                for i in crawler_need:
                    if list_crawler[i] != 1:
                        pass_this_people = True
                        break
                if pass_this_people:
                    continue

            if crawler_exclude is not None:
                list_crawler = self.crawler_list_status(current_id, primitive_name)
                pass_this_people = False
                for i in crawler_exclude:
                    if list_crawler[i] == 1:
                        pass_this_people = True
                        break
                if pass_this_people:
                    continue

            # todo: resolver caso esteja comparando um valor numérico com uma coluna cujo valor é None
            if restriction is not None:
                dep = self.get_dependencies(current_id, primitive_name, *restriction.keys())
                pass_this_people = False
                for k, v in dep.items():
                    if str(v).replace('.', '', 1).isdigit():
                        if not eval(str(v) + ' ' + restriction[k]):
                            pass_this_people = True
                            break
                    elif str(v) == 'None':
                        if not eval(str(v) + ' ' + restriction[k]):
                            pass_this_people = True
                            break
                    else:
                        if not eval('"' + str(v) + '" ' + restriction[k]):
                            pass_this_people = True
                            break
                if pass_this_people:
                    continue

            my_dict = {'id': current_id}
            if len(want):
                my_dict.update({k: v for k, v in self.get_primitive_row_info_all(current_id, primitive_name).items() if k in want})

            to_return.append(my_dict)

        return to_return
