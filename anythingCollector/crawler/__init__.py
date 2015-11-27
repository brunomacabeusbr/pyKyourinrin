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

    @staticmethod
    def read_my_secondary_tables():
        return ()

    @staticmethod
    def secondary_tables_export():
        return ()

    @classmethod
    def update_my_table(cls, id, column_and_value, table=None):
        if table is None:
            table = cls.name()
            if Crawler.db.execute("SELECT COUNT(*) FROM " + table + " WHERE peopleid=?", (id,)).fetchone()[0] > 0:
                raise ValueError("Essa pessoa já está presente na tabela principal do crawler")
        else:
            table = cls.name() + '_' + table

        column_and_value = {i: j for i, j in column_and_value.items() if j is not None}
        if len(column_and_value) > 0:
            Crawler.db.execute("INSERT INTO " + table + " (peopleid," + ','.join(column_and_value.keys()) + ") VALUES (?," + ('"' + '","'.join(list(map(str, column_and_value.values()))) + '"') + ")", (id,))
        else:
            Crawler.db.execute("INSERT INTO " + table + " (peopleid) VALUES (?)", (id,))

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

    @classmethod
    def have_dependencies(cls):
        return cls.dependencies()[0] != ''

    @staticmethod
    @abstractmethod
    def crop(): pass

    @classmethod
    def trigger(cls, table_row): pass

    @classmethod
    @abstractmethod
    def harvest(cls, id): pass


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
class GetDependencies:
    def __init__(self, f):
        self.name = f.name()
        self.harvest = f.harvest
        self.dependencies = f.dependencies()
        self.multiple_dependence_routes = (type(self.dependencies[0]) == tuple)

    # todo: Em um raro caso pode ocasionar um loop infinito
    # Para esse caso acontecer, o crawler A precisa da info X, da qual não está presente no banco.
    # Então o método harvest_dependence será chamado para coletar a info X, da qual pode ser alcançada usando o cralwer B
    # Porém, uma depedência de B não presente no banco é a info Y, da qual pode ser coletada através do crawler A.
    # Esse raro caso resultada num loop infinito A -> B -> A -> B ...
    def __call__(self, *args, **kwargs):
        # Caso não seja usado um id do banco de dados, prosseguirá normalmente para a função harvest do crawler
        if 'id' not in kwargs:
            # Necessário para casos como do Portal da Transparencia, em que pode-se tanto buscar infos de uma pessoa
            # especificamente ou então coletar o site inteiro, bastando usar os parâmetros da função
            self.harvest(*args, **kwargs)
            Crawler.db.commit()
            return

        people_id = kwargs['id']

        gdp = GraphDependenciesOfThisPeople(Crawler.db, people_id)

        if self.multiple_dependence_routes:
            # Se houver várias rotas de depedência, seguirá o seguinte algorítimo:
            # 1 - Se uma das rotas já tiver todos os dados presentes no banco, irá usa-la
            # 2 - Se uma das rotas tem dados não alcançáveis, não a usará
            # 3 - Prioriza a rota com menos depedências
            dict_dependencies = None
            for i in self.dependencies:
                current_dict_dependencies = Crawler.db.get_dependencies(people_id, *i)

                # Já tem todos os dados presentes?
                if None not in current_dict_dependencies.values():
                    dict_dependencies = current_dict_dependencies
                    break

                # A rota tem todos os dados faltosos alcançáveis?
                use_it = True
                for k, v in current_dict_dependencies.items():
                    if v is not None:
                        continue

                    if gdp.is_dependence_reachable(k, exclude_crawler=self.name) is False:
                        use_it = False
                        break

                if use_it is False:
                    continue

                # Essa alternativa de rota é mais curta que a já selecionada?
                if dict_dependencies is not None and len(dict_dependencies) > len(current_dict_dependencies):
                    dict_dependencies = current_dict_dependencies

                if dict_dependencies is None:
                    dict_dependencies = current_dict_dependencies

            if dict_dependencies is None:
                return False
        else:
            dict_dependencies = Crawler.db.get_dependencies(people_id, *self.dependencies)

        # Verificar se alguma dependência não está presente no banco
        # Se não estiver, então vai colhe-la e chamar novamente esse mesmo método
        for dependence_name, dependence_value in dict_dependencies.items():
            if dependence_value is None:
                if gdp.harvest_dependence(dependence_name):
                    return self.__call__(*args, **kwargs)
                else:
                    return False

        self.harvest(*args, dependencies=dict_dependencies, **kwargs)


def harvest_and_commit(harvest_fun, *args, **kwargs):
    # Implicitamente, sempre será commitada as alterações ao banco de dados ao finalizar a colheita
    result = harvest_fun(*args, **kwargs)
    Crawler.db.commit()
    return result

import functools

for i in Crawler.__subclasses__():
    if i.have_dependencies():
        i.harvest = functools.partial(harvest_and_commit, GetDependencies(i))
    else:
        i.harvest = functools.partial(harvest_and_commit, i.harvest)

# Iniciar as threads dos triggers dos crawlers que tiverem
# Essa função será chamada ao final da iniciação do ManagerDatabase
def start_triggers():
    class TriggerTableRow:
        def __init__(self, crawler):
            self.crawler = crawler

        def value(self):
            return Crawler.db.execute("SELECT infos FROM trigger WHERE crawler=?", (self.crawler.name(),)).fetchone()[0]

        def update(self, value):
            Crawler.db.execute("UPDATE trigger SET infos=? WHERE crawler=?", (value, self.crawler.name(),))
            Crawler.db.commit()

    import threading

    for i in Crawler.__subclasses__():
        if i.trigger.__code__ != Crawler.trigger.__code__:
            t = threading.Thread(target=i.trigger, args=(TriggerTableRow(i),), name=i.name())
            t.start()
