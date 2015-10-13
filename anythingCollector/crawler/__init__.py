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
    def update_my_table(cls, id, column_and_value, table=None):
        if table is None:
            table = cls.name()
        else:
            table = cls.name() + '_' + table

        Crawler.db.execute("INSERT INTO " + table + " (peopleid," + ','.join(column_and_value.keys()) + ") VALUES (?," + ('"' + '","'.join(list(map(str, column_and_value.values()))) + '"') + ")", (id,))

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

# Decorator implícito, colocado nos métodos harvest dos crawlers que possuem depedências,
# para pega-las do banco de dados e colocar no dict 'dependencies' da chamada do método
# todo: precisa implementar ainda um "ou exclussivo", para casos como o da etufor que pede data de nascimento *ou* cia
class GetDependencies:
    def __init__(self, f):
        self.dependencies = f.dependencies()
        self.harvest = f.harvest

    def __call__(self, *args, **kwargs):
        people_id = kwargs['id']
        dict_dependencies = Crawler.db.get_dependencies(people_id, *self.dependencies)

        # Verificar se alguma dependência não está presente no banco
        # Se não estiver, então vai colhe-la e chamar novamente esse mesmo método
        for dependence_name, dependence_value in dict_dependencies.items():
            if dependence_value is None:
                GraphDependenciesOfThisPeople(Crawler.db, people_id).harvest_dependence(dependence_name)
                self.__call__(*args, **kwargs)
                return

        self.harvest(*args, dependencies=dict_dependencies, **kwargs)
        Crawler.db.commit()


for i in Crawler.__subclasses__():
    if i.dependencies() != '':
        i.harvest = GetDependencies(i)
