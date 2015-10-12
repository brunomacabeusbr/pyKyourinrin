from abc import ABCMeta, abstractmethod


# Crawler base class
class Crawler:
    __metaclass__ = ABCMeta
    db = None

    def __init__(self):
        self.create_my_table()
        try:
            self.db.execute('ALTER TABLE crawler ADD COLUMN %s INTEGER DEFAULT 0;' % self.name())
        except:
             # coluna já existe
            pass

    @abstractmethod
    def create_my_table(self): pass

    @classmethod
    def update_my_table(cls, id, column_and_value):
        Crawler.db.execute("INSERT INTO " + cls.name() + " (peopleid," + ','.join(column_and_value.keys()) + ") VALUES (?," + ('"' + '","'.join(list(map(str, column_and_value.values()))) + '"') + ")", (id,))

    # id deve ser, preferencialmente, o id da coluna da pessoa, ou então o nome dela
    @classmethod
    def update_crawler(cls, id, result):
        # result: 1 -> success ; -1 -> fail
        if not isinstance(id, int):
            id = Crawler.db.execute("SELECT * FROM peoples WHERE name=?", (id,)).fetchone()[0]
        Crawler.db.execute("UPDATE crawler SET %s = ? WHERE peopleid=?" % cls.name(), (result, id,))

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


# Carregar todos os crawlers da pasta
import os
import importlib

my_path = os.path.dirname(__file__)

for i in os.listdir(my_path):
    if not os.path.isfile(os.path.join(my_path, i)):
        continue

    py_name = os.path.splitext(i)[0]

    importlib.import_module('crawler.' + py_name)


# Carregar grafo de depedência a respeito dos cralwes
from graphdependencies import GraphDependenciesOfThisPeople

# Decorator para ser usado no, para pegar as depedências e colocar no dict dependencies da chamada do método
# todo: precisa implementar ainda um "ou exclussivo", para casos como o da etufor que pede data de nascimento *ou* cia
class GetDependencies:
    def __init__(self, f):
        self.depedencies = f.dependencies()
        self.harvest = f.harvest

    def __call__(self, *args, **kwargs):
        dependences_values = Crawler.db.get_dependences(kwargs['id'], *self.depedencies)

        missing = [i for i, j in dependences_values.items() if j is None]
        if len(missing) > 0:
            gdp = GraphDependenciesOfThisPeople(Crawler.db, kwargs['id'])
            gdp.harvest_dependence(missing[0])
            self.__call__(*args, **kwargs)
            return
            # optei por fazer assim pois o harvest_dependence vai alterar o banco de dados e, desse modo,
            # outras depedencias que possam existir podem já ter sido resolvidas em cascata
            # todo: esse código precisa ser melhor redigito

        self.harvest(*args, dependencies=dependences_values, **kwargs)
        Crawler.db.commit()


for i in Crawler.__subclasses__():
    if i.dependencies() != '':
        i.harvest = GetDependencies(i)
