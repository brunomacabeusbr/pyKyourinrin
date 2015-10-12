import sqlite3
from crawler import Crawler


class DependenciesMissing(Exception):
    pass


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
        # self.crawler_etufor = CrawlerEtufor(self)
        # self.crawler_qselecao = CrawlerQSelecao(self)

        # salvar as mudanças no banco
        self.commit()

    def execute(self, sql, parameters=()):
        return self.c.execute(sql, parameters)

    def commit(self):
        return self.con.commit()

    def new_people_if_not_exist(self, name): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        if len(self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchall()) == 0:
            self.execute('INSERT INTO peoples (name) VALUES (?)', (name,))
            id = self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchone()[0]
            self.execute('INSERT INTO crawler (peopleid) VALUES (?)', (id,))

    def update_people(self, name, column_and_value): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        self.new_people_if_not_exist(name)
        self.execute("UPDATE peoples SET " + ','.join('{}="{}"'.format(key, val) for key, val in column_and_value.items()) + " WHERE name=?", (name,))

    def crawler_status(self, id):
        crawlers_to_select = []
        for cls in Crawler.__subclasses__():
            crawlers_to_select.append(cls.name())

        x = self.execute("SELECT " + ','.join(crawlers_to_select) + " FROM crawler WHERE peopleid=?", (id,)).fetchone()

        dict_to_return = {}
        i = 0
        for cls in Crawler.__subclasses__():
            dict_to_return[cls.name()] = x[i]
            i += 1

        return dict_to_return

    def get_people_info(self, id):
        x = self.execute("SELECT * FROM peoples WHERE id=?", (id,))
        y = x.fetchone()
        fieldnames = {}

        count = 0
        for i in x.description:
            fieldnames[i[0]] = y[count]
            count += 1

        del fieldnames['id']

        # todo: código terá que ser reformulado, para atender as mudanças na organização das tabelas, na qual há crawlers com tabelas próprias
        # por enquanto, fica com essa gambiarra aqui
        query = self.execute("SELECT cia FROM etufor WHERE peopleid=?", (id,)).fetchone()
        if query:
            fieldnames['cia'] = query[0]
        else:
            fieldnames['cia'] = None

        query = self.execute("SELECT registrocriminal FROM sspds WHERE peopleid=?", (id,)).fetchone()
        if query:
            fieldnames['registrocriminal'] = query[0]
        else:
            fieldnames['registrocriminal'] = None

        query = self.execute("SELECT anything FROM thisdoesnotexist WHERE peopleid=?", (id,)).fetchone()
        if query:
            fieldnames['anything'] = query[0]
        else:
            fieldnames['anything'] = None

        return fieldnames

    def get_dependences(self, id, *dependences):
        people_infos = self.get_people_info(id)

        to_return = {} # todo: talvez haja um meio melhor para fazer isso
        for i in dependences:
            to_return[i] = people_infos[i]

        return to_return

    def get_tableid_of_people(self, name): # todo: por para usar outros dados além do nome, algo como um parâmetro opcional dizendo a coluna alvo, ficadando column_target='name'
        return self.execute("SELECT * FROM peoples WHERE name=?", (name,)).fetchone()[0]