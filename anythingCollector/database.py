import sqlite3
from crawler import Crawler


class ManagerDatabase:
    def __init__(self):
        import os
        self.con = sqlite3.connect(os.path.dirname(__file__) + "/mydatabase.db")

        self.c = self.con.cursor()

        # table peoples: informações básicas
        self.execute('CREATE TABLE IF NOT EXISTS peoples('
                        'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'name TEXT,'
                        'name_social TEXT,'
                        'birthday_day INTEGER,'
                        'birthday_month INTEGER,'
                        'birthday_year INTEGER,'
                        'identity INTEGER,'
                        'cpf INTEGER,'
                        'name_monther TEXT'
                    ');')

        # table crawler: se já colheu determinada fonte; 0 ainda não tentou colher, 1 colheu e deu certo, -1 falha ao tentar colher
        self.execute('CREATE TABLE IF NOT EXISTS crawler('
                        'peopleid INTEGER,'
                        'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                    ');')

        Crawler.db = self
        for cls in Crawler.__subclasses__():
            setattr(self, 'crawler_' + cls.name(), cls())
        # o loop acima iniciará todas as subclasses diretas de Crawler e inicializará, como por exemplo:
        # self.crawler_etufor = CrawlerEtufor()
        # self.crawler_qselecao = CrawlerQSelecao()

        # salvar as mudanças no banco
        self.commit()

    def execute(self, sql, parameters=()):
        return self.c.execute(sql, parameters)

    def commit(self):
        return self.con.commit()

    def lastrowid(self):
        return self.c.lastrowid

    def select_column_and_value(self, sql, parameters=()):
        execute = self.execute(sql, parameters)
        fetch = execute.fetchone()

        if fetch is None:
            return {k[0]: None for k in execute.description}

        return {k[0]: v for k, v in list(zip(execute.description, fetch))}

    def count_people_with_this_filters(self, filter):
        # todo: só filtra com base nos dados da tabela peoples
        return len(self.execute("SELECT * FROM peoples WHERE %s" %
                                ' AND '.join("{}='{}'".format(k, v) for k, v in filter.items())).fetchall())

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
                         ' AND '.join("{}='{}'".format(k, v) for k, v in filter.items()))

    def crawler_list_status(self, id):
        fieldnames = self.select_column_and_value('SELECT * FROM crawler WHERE peopleid=?', (id,))
        del fieldnames['peopleid']
        return fieldnames

    def crawler_list_used(self, id):
        return {k: v for k, v in self.crawler_list_status(id).items() if v != 0}

    def get_people_info_all(self, id):
        fieldnames = self.select_column_and_value("SELECT * FROM peoples WHERE id=?", (id,))

        for cls in Crawler.__subclasses__():
            # todo: desse jeito pegará apenas a tabela principal do crawler. tomar cuidado para saber se isso causará problemas ou não
            fieldnames.update(self.select_column_and_value("SELECT * FROM %s WHERE peopleid=?" % cls.name(), (id,)))

        del fieldnames['id']
        del fieldnames['peopleid']

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
                                ' AND '.join("{}='{}'".format(k, v) for k, v in filter.items())).fetchone()[0]
