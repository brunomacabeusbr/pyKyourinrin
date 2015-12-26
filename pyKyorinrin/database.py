import sqlite3
from crawler import Crawler, start_triggers


class ManagerDatabase:
    def __init__(self, trigger=True):
        import os
        self.con = sqlite3.connect(os.path.dirname(__file__) + "/mydatabase.db", check_same_thread=False)

        self.c = self.con.cursor()

        # table peoples: informações básicas
        self.execute('CREATE TABLE IF NOT EXISTS peoples('
                        'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'name TEXT,'
                        'name_social TEXT,'
                        'birthday_day INTEGER,'
                        'birthday_month INTEGER,'
                        'birthday_year INTEGER,'
                        'identity TEXT,'
                        'cpf TEXT,'
                        'name_monther TEXT'
                     ');')

        # table crawler: se já colheu determinada fonte; 0 ainda não tentou colher, 1 colheu e deu certo, -1 falha ao tentar colher
        self.execute('CREATE TABLE IF NOT EXISTS crawler('
                        'peopleid INTEGER,'
                        'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                     ');')

        # table arbitrary: permitir setar valores não obtidos através dos crawlers à peoples
        self.execute('CREATE TABLE IF NOT EXISTS arbitrary('
                        'peopleid INTEGER,'
                        'column_name TEXT,'
                        'column_value TEXT,'
                        'column_set_integer INTEGER DEFAULT 0,'
                        'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                     ');')

        # Iniciar crawlers
        self.execute('CREATE TABLE IF NOT EXISTS trigger('
                        'crawler TEXT,'
                        'infos TEXT'
                     ');')

        Crawler.db = self
        for cls in Crawler.__subclasses__():
            setattr(self, 'crawler_' + cls.name(), cls())
            if cls.trigger.__code__ != Crawler.trigger.__code__:
                if len(self.execute("SELECT * FROM trigger WHERE crawler=?", (cls.name(),)).fetchall()) == 0:
                    self.execute('INSERT INTO trigger (crawler) VALUES (?)', (cls.name(),))

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

    def count_people_with_this_filters(self, filter):
        # todo: só filtra com base nos dados da tabela peoples
        return len(self.execute("SELECT * FROM peoples WHERE %s" %
                                ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in filter.items())).fetchall())

    def people_exists(self, filter):
        return self.count_people_with_this_filters(filter) > 0

    def new_people(self, filter):
        filter = {k: '"' + str(v) + '"' for k, v in filter.items()}
        self.execute('INSERT INTO peoples (' + ','.join(filter.keys()) + ') VALUES (' + ','.join(filter.values()) + ')')
        self.execute('INSERT INTO crawler (peopleid) VALUES (?)', (self.lastrowid(),))

    def update_people(self, filter, column_and_value=None):
        count_people = self.count_people_with_this_filters(filter)
        if count_people == 0:
            self.new_people(filter)
        elif count_people > 1:
            raise ValueError('Há mais que uma pessoa com os critérios fornecidos! Não sei qual eu devo atualizar')

        if column_and_value is not None:
            column_and_value = {i: j for i, j in column_and_value.items() if j is not None}

            self.execute("UPDATE peoples SET " + ','.join('{}="{}"'.format(key, val) for key, val in column_and_value.items()) + ' WHERE %s ' %
                         ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in filter.items()))

    def crawler_list_status(self, id):
        return self.select_column_and_value('SELECT * FROM crawler WHERE peopleid=?', (id,), discard=['peopleid'])

    def crawler_list_used(self, id):
        return {k: v for k, v in self.crawler_list_status(id).items() if v != 0}

    def crawler_list_success(self, id):
        return [k for k, v in self.crawler_list_status(id).items() if v == 1]

    def get_people_info_all(self, id):
        # Recolher dados da tabela peoples
        fieldnames = self.select_column_and_value("SELECT * FROM peoples WHERE id=?", (id,), discard=['id', 'peopleid'])

        # Recolher dados da tabela dos crawlers
        crawler_list_success = self.crawler_list_success(id)
        for cls in Crawler.__subclasses__():
            # Tabela principal
            dict_infos = self.select_column_and_value("SELECT * FROM %s WHERE peopleid=?" % cls.name(), (id,), discard=['peopleid'])
            dict_infos = {k: v for k, v in dict_infos.items() if not(k in fieldnames and v is None)}
            fieldnames.update(dict_infos)

            # Tabelas secundárias
            if cls.name() in crawler_list_success:
                for current in cls.read_my_secondary_tables():
                    table = current['table']

                    if 'reference_column' not in current.keys():
                        x = self.select_column_and_value_many("SELECT * FROM %s WHERE peopleid=?" % (cls.name() + '_' + table), (id,), discard=['peopleid'])

                        dict_infos.update({table: x})
                    else:
                        reference_table = current['reference_column'][0]
                        reference_column = current['reference_column'][1]

                        for i in dict_infos[cls.name() + '_' + reference_table]:
                            referenceid = i[reference_column]
                            x = self.select_column_and_value_many("SELECT * FROM %s WHERE %s=?" % (cls.name() + '_' + table, reference_column), (referenceid,), discard=['peopleid'])

                            i[reference_column] = x

                for current in cls.column_export():
                    fieldnames[current['column_name']] = current['how'](dict_infos)
            else:
                for current in cls.column_export():
                    fieldnames[current['column_name']] = None

        # Recolher dados da tabela arbitrary
        def get_value_typed(j):
            if j['column_set_integer'] and j['column_value'] is not None:
                return int(j['column_value'])
            else:
                return j['column_value']

        fieldnames.update(
            {
                i['column_name']: get_value_typed(i) for i in
                self.select_column_and_value_many("SELECT * FROM arbitrary WHERE peopleid=?", (id,), discard=['peopleid'])
            }
        )

        #
        return fieldnames

    def get_dependencies(self, id, *dependencies):
        return {k: v for k, v in self.get_people_info_all(id).items() if k in dependencies}

    def get_tableid_of_people(self, filter):
        count_people = self.count_people_with_this_filters(filter)
        if count_people == 0:
            return False
        elif count_people > 1:
            raise ValueError('Há mais que uma pessoa com os critérios fornecidos! Não sei qual eu devo entregar o ID')

        return self.execute("SELECT * FROM peoples WHERE %s" %
                            ' AND '.join("{}='{}'".format(k, str(v).replace("'", "''")) for k, v in filter.items())).fetchone()[0]

    def get_peoples_with_criteria(self, want=[], crawler_need=None, crawler_exclude=None, restriction=None):
        to_return = []

        for current_people in self.execute('SELECT id FROM peoples').fetchall():
            current_id = current_people[0]

            if crawler_need is not None:
                list_crawler = self.crawler_list_status(current_id)
                pass_this_people = False
                for i in crawler_need:
                    if list_crawler[i] != 1:
                        pass_this_people = True
                        break
                if pass_this_people:
                    continue

            if crawler_exclude is not None:
                list_crawler = self.crawler_list_status(current_id)
                pass_this_people = False
                for i in crawler_exclude:
                    if list_crawler[i] == 1:
                        pass_this_people = True
                        break
                if pass_this_people:
                    continue

            if restriction is not None:
                dep = self.get_dependencies(current_id, *restriction.keys())
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
                my_dict.update({k: v for k, v in self.get_people_info_all(current_id).items() if k in want})

            to_return.append(my_dict)

        return to_return
