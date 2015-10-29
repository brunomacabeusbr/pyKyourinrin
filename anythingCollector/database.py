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

    def new_people_if_not_exist(self, name): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        if len(self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchall()) == 0:
            self.execute('INSERT INTO peoples (name) VALUES (?)', (name,))
            id = self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchone()[0]
            self.execute('INSERT INTO crawler (peopleid) VALUES (?)', (id,))

    def update_people(self, name, column_and_value=None): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        self.new_people_if_not_exist(name)
        if not column_and_value is None:
            column_and_value = {i: j for i, j in column_and_value.items() if j is not None}
            self.execute("UPDATE peoples SET " + ','.join('{}="{}"'.format(key, val) for key, val in column_and_value.items()) + " WHERE name=?", (name,))

    def crawler_status(self, id):
        fieldnames = self.select_column_and_value('SELECT * FROM crawler WHERE peopleid=?', (id,))
        del fieldnames['peopleid']
        return fieldnames

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

    def get_tableid_of_people(self, name): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        return self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchone()[0]
