from abc import ABCMeta, abstractmethod


# Crawler base class
class Crawler:
    __metaclass__ = ABCMeta
    db = None

    def __init__(self):
        self.create_my_table()

    @abstractmethod
    def create_my_table(self): pass

    @staticmethod
    def read_my_secondary_tables():
        return ()

    @staticmethod
    def macro_at_data():
        return ()

    @classmethod
    def update_my_table(cls, column_and_value, table=None, primitive_id=None, primitive_name=None):
        # Verificação a respeito das variáveis temporárias que armazenam a primitive_id e primitive_name
        if hasattr(cls, 'temp_current_primitive_name'):
            # Se elas estiverem presente, então, usará as variáveis temporárias;
            # por conta de crawler como o portal_transparencia, pode ser útil passa-la de forma redudante para esse método,
            # e precisamos apenas verificar se o valor redudante está correto, para evitar erros do crawler querer atualizar
            # uma primitiva row não passada ao crawler
            if (primitive_id is not None or primitive_name is not None) and\
                    (primitive_id != cls.temp_current_primitive_id or primitive_name != cls.temp_current_primitive_name):
                raise ValueError('Os valores passados estão diferentes do esperado')

            primitive_id = cls.temp_current_primitive_id
            primitive_name = cls.temp_current_primitive_name
        else:
            # Se as variáveis temporárias não estiverem presente, então o crawler não recebeu como parâmetro uma primitive row
            # e agora está precisando editar alguma primitive row
            # Então esse método precisa receber os parâmetros que identificam uma primitive row
            if primitive_id is None or primitive_name is None:
                raise ValueError('É necessário fornecer o parâmetro "primitive_id" e "primitive_name",'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de primitive')

        if primitive_name not in Crawler.temp_current_crawler.primitive_required():
            raise ValueError('A primitive que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(primitive_name))

        # Salvar no banco
        if table is None:
            table = cls.name()
            if Crawler.db.execute("SELECT COUNT(*) FROM " + table + " WHERE " + primitive_name + "_id=?", (primitive_id,)).fetchone()[0] > 0:
                raise ValueError("Já há registro disso na tabela principal do crawler")
        else:
            table = cls.name() + '_' + table

        column_and_value = {i: j for i, j in column_and_value.items() if j is not None}
        if len(column_and_value) > 0:
            Crawler.db.execute(
                "INSERT INTO " + table + " (" + primitive_name + "_id," + ','.join(column_and_value.keys()) + ") "
                "VALUES (?," + ("'" + "','".join([str(current_value).replace("'", "''") for current_value in column_and_value.values()]) + "'") + ")",
                (primitive_id,)
            )
        else:
            Crawler.db.execute(
                "INSERT INTO " + table + " (" + primitive_name + "_id) VALUES (?)",
                (primitive_id,)
            )

    @classmethod
    def update_crawler_status(cls, status, primitive_id=None, primitive_name=None):
        # Verificação a respeito das variáveis temporárias que armazenam a primitive_id e primitive_name
        # todo: código repetido com o método "update_my_table" (dica: lá tá comentanda essa bagunça daqui)
        if hasattr(cls, 'temp_current_primitive_name') and primitive_name is None:
            if (primitive_id is not None or primitive_name is not None) and\
                    (primitive_id != cls.temp_current_primitive_id or primitive_name != cls.temp_current_primitive_name):
                raise ValueError('Os valores passados estão diferentes do esperado')

            primitive_id = cls.temp_current_primitive_id
            primitive_name = cls.temp_current_primitive_name
        else:
            if primitive_id is None or primitive_name is None:
                raise ValueError('É necessário fornecer o parâmetro "primitive_id" e "primitive_name",'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de primitive')

        if primitive_name not in Crawler.temp_current_crawler.primitive_required():
            raise ValueError('A primitive que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(primitive_name))

        # Salvar no banco
        status = (-1, 1)[status]
        Crawler.db.execute(
            "UPDATE %s_crawler SET %s = ? WHERE id=?" % (primitive_name, Crawler.temp_current_crawler.name()),
            (status, primitive_id,)
        )

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

    @staticmethod
    @abstractmethod
    def primitive_required(): pass # deve-se listar aqui as primitives que são requeridos na chamada do harvest, usadas dentro do harvest salva ou nas dependencies

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
from graphdependencies import GraphDependenciesOfPrimitiveRow

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
        arg_primitive = [i for i in kwargs.keys() if i[:9] == 'primitive']

        # Caso não seja usado um id de primitive, logo não há dependências a serem puxadas,
        # então prosseguirá normalmente para a função harvest do crawler, se o crawler esperar por isso
        if len(arg_primitive) == 0:
            self.harvest(*args, **kwargs)
            Crawler.db.commit() # todo: isso aqui não é redudante?
            return

        # Checar erro na passagem da primitiva
        if len(arg_primitive) > 1:
            raise ValueError('Só é permitido passar um único id de primitive.\n'
                             'Se for necessário ambos para o crawler, crie um relacionamento na tabela "main_linker_primitives"')

        primitive_name = arg_primitive[0]
        import inspect
        harvest_args = inspect.getargspec(self.harvest).args

        if primitive_name not in harvest_args:
            raise ValueError('Primitiva não requerida na chamada desse crawler!')

        # Recolher dependências
        primitive_id = kwargs[primitive_name]

        gdp = GraphDependenciesOfPrimitiveRow(Crawler.db, primitive_id, primitive_name[10:])

        if self.multiple_dependence_routes:
            # Se houver várias rotas de depedência, seguirá o seguinte algorítimo:
            # 1 - Se uma das rotas já tiver todos os dados presentes no banco, irá usa-la
            # 2 - Se uma das rotas tem dados não alcançáveis, não a usará
            # 3 - Prioriza a rota com menos depedências
            dict_dependencies = None
            for i in self.dependencies:
                current_dict_dependencies = Crawler.db.get_dependencies(primitive_id, primitive_name[10:], *i)

                # Se o retorno de get_dependencies for false, então há dependências não pertecente à essa primitive,
                # logo, devemos ignorar essa rota de dependência
                if current_dict_dependencies is False:
                    continue

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
            dict_dependencies = Crawler.db.get_dependencies(primitive_id, primitive_name[10:], *self.dependencies)

        # Verificar se alguma dependência não está presente no banco
        # Se não estiver, então vai colhe-la e chamar novamente esse mesmo método
        for dependence_name, dependence_value in dict_dependencies.items():
            if dependence_value is None:
                if gdp.harvest_dependence(dependence_name):
                    return self.__call__(*args, **kwargs)
                else:
                    return False

        # "Passar" de forma implícita variáveis temporárias ao Crawler, úteis na hora de salvar as infos no banco de dados
        Crawler.temp_current_primitive_name = primitive_name
        Crawler.temp_current_primitive_id = primitive_id

        # Colher
        self.harvest(*args, dependencies=dict_dependencies, **kwargs)

        # Após a colheita, para evitar problemas, apagará as variáveis temporárias
        del Crawler.temp_current_primitive_name
        del Crawler.temp_current_primitive_id

def encapsulate_harvest(crawler_and_harvest, *args, **kwargs):
    # "Passar" de forma implícita variável temporária ao Crawler, útil na hora de salvar as infos no banco de dados
    Crawler.temp_current_crawler = crawler_and_harvest[0]

    # Chamar método harvest
    result = crawler_and_harvest[1](*args, **kwargs)

    # Após a colheita, para evitar problemas, apagará a variável temporária
    del Crawler.temp_current_crawler

    # Implicitamente, sempre será commitada as alterações ao banco de dados ao finalizar a colheita
    Crawler.db.commit()
    return result

import copy
import functools

for i in Crawler.__subclasses__():
    i.harvest_debug = copy.copy(i.harvest) # cópia direta do método harvest, útil em debug ou para pegar o cabeçalho do harvest

    if i.have_dependencies():
        i.harvest = GetDependencies(i)

    i.harvest = functools.partial(encapsulate_harvest, (i, i.harvest))

# Iniciar as threads dos triggers dos crawlers que tiverem
# Essa função será chamada ao final da iniciação do ManagerDatabase
def start_triggers():
    class TriggerTableRow:
        def __init__(self, crawler):
            self.crawler = crawler

        def value(self):
            return Crawler.db.execute("SELECT infos FROM main_trigger WHERE crawler=?", (self.crawler.name(),)).fetchone()[0]

        def update(self, value):
            Crawler.db.execute("UPDATE main_trigger SET infos=? WHERE crawler=?", (value, self.crawler.name(),))
            Crawler.db.commit()

    import threading

    for i in Crawler.__subclasses__():
        if i.trigger.__code__ != Crawler.trigger.__code__:
            t = threading.Thread(target=i.trigger, args=(TriggerTableRow(i),), name=i.name())
            t.start()
