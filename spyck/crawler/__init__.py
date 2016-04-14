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
    def update_my_table(cls, column_and_value, table=None, entity_id=None, entity_name=None):
        # Verificação a respeito das variáveis temporárias que armazenam a entity_id e entity_name
        if hasattr(cls, 'temp_current_entity_name'):
            # Se elas estiverem presente, então, usará as variáveis temporárias;
            # por conta de crawler como o portal_transparencia, pode ser útil passa-la de forma redudante para esse método,
            # e precisamos apenas verificar se o valor redudante está correto, para evitar erros do crawler querer atualizar
            # uma primitiva row não passada ao crawler
            if (entity_id is not None or entity_name is not None) and\
                    (entity_id != cls.temp_current_entity_id or entity_name != cls.temp_current_entity_name):
                raise ValueError('Os valores passados estão diferentes do esperado')

            entity_id = cls.temp_current_entity_id
            entity_name = cls.temp_current_entity_name
        else:
            # Se as variáveis temporárias não estiverem presente, então o crawler não recebeu como parâmetro uma entity row
            # e agora está precisando editar alguma entity row
            # Então esse método precisa receber os parâmetros que identificam uma entity row
            if entity_id is None or entity_name is None:
                raise ValueError('É necessário fornecer o parâmetro "entity_id" e "entity_name",'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de entity')

        if entity_name not in Crawler.temp_current_crawler.entity_required():
            raise ValueError('A entity que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(entity_name))

        # Salvar no banco
        if table is None:
            table = cls.name()
            if Crawler.db.execute("SELECT COUNT(*) FROM " + table + " WHERE " + entity_name + "_id=?", (entity_id,)).fetchone()[0] > 0:
                raise ValueError("Já há registro disso na tabela principal do crawler")
        else:
            table = cls.name() + '_' + table

        column_and_value = {i: j for i, j in column_and_value.items() if j is not None}
        if len(column_and_value) > 0:
            Crawler.db.execute(
                "INSERT INTO " + table + " (" + entity_name + "_id," + ','.join(column_and_value.keys()) + ") "
                "VALUES (?," + ("'" + "','".join([str(current_value).replace("'", "''") for current_value in column_and_value.values()]) + "'") + ")",
                (entity_id,)
            )
        else:
            Crawler.db.execute(
                "INSERT INTO " + table + " (" + entity_name + "_id) VALUES (?)",
                (entity_id,)
            )

    @classmethod
    def update_crawler_status(cls, status, entity_id=None, entity_name=None):
        # Verificação a respeito das variáveis temporárias que armazenam a entity_id e entity_name
        # todo: código repetido com o método "update_my_table" (dica: lá tá comentanda essa bagunça daqui)
        if hasattr(cls, 'temp_current_entity_name') and entity_name is None:
            if (entity_id is not None or entity_name is not None) and\
                    (entity_id != cls.temp_current_entity_id or entity_name != cls.temp_current_entity_name):
                raise ValueError('Os valores passados estão diferentes do esperado')

            entity_id = cls.temp_current_entity_id
            entity_name = cls.temp_current_entity_name
        else:
            if entity_id is None or entity_name is None:
                raise ValueError('É necessário fornecer o parâmetro "entity_id" e "entity_name",'
                                 'uma vez em que esse crawler não recebeu como parâmetro um id de entity')

        if entity_name not in Crawler.temp_current_crawler.entity_required():
            raise ValueError('A entity que você está tentando acessar, "{}", não está listada entre as requeridas pelo crawler'.format(entity_name))

        # Salvar no banco
        status = (-1, 1)[status]
        Crawler.db.execute(
            "UPDATE %s_crawler SET %s = ? WHERE id=?" % (entity_name, Crawler.temp_current_crawler.name()),
            (status, entity_id,)
        )

    @staticmethod
    @abstractmethod
    def name(): pass

    @staticmethod
    @abstractmethod
    def dependencies(): pass

    @staticmethod
    @abstractmethod
    def crop(): pass

    @staticmethod
    @abstractmethod
    def entity_required(): pass

    @classmethod
    def trigger(cls, table_row): pass

    @classmethod
    @abstractmethod
    def harvest(cls): pass


# Carregar todos os crawlers da pasta
import os
import importlib

my_path = os.path.dirname(__file__)

for i in os.listdir(my_path):
    if not os.path.isfile(os.path.join(my_path, i)):
        continue

    py_name = os.path.splitext(i)[0]

    importlib.import_module('crawler.' + py_name)


# Associar informações aos crawlers que o provém
# No dicionário 'dict_info_to_crawlers' a chave será o nome da info e o value uma lista com os crawlers em que pode-se consegui-la
from collections import defaultdict
dict_info_to_crawlers = defaultdict(list)

[
    dict_info_to_crawlers[current_crop].append(cls)

    for cls in Crawler.__subclasses__()
    if cls.dependencies() is not None

    for current_crop in cls.crop()
]

# Decorator implícito, colocado nos métodos harvest dos crawlers que possuem depedências,
# para pega-las do banco de dados e colocar no dict 'dependencies' da chamada do método
class GetDependencies:
    def __init__(self, f):
        self.f = f # todo: na verdade, basta salvar o self.f
        self.name = f.name()
        self.harvest = f.harvest
        self.dependencies = f.dependencies()
        self.multiple_dependence_routes = (type(self.dependencies[0]) == tuple)

    def __call__(self, *args, **kwargs):
        arg_entity = [i for i in kwargs.keys() if i[:6] == 'entity']

        # Caso não seja usado um id de entity, logo não há dependências a serem puxadas,
        # então prosseguirá normalmente para a função harvest do crawler, se o crawler esperar por isso
        if len(arg_entity) == 0:
            self.harvest(*args, **kwargs)
            return

        # Checar erro na passagem da primitiva
        if len(arg_entity) > 1:
            raise ValueError('Só é possível passar um único id de entity!\n')

        entity_name = arg_entity[0]
        import inspect
        harvest_args = inspect.getargspec(self.harvest).args

        if entity_name not in harvest_args:
            raise ValueError('Primitiva não requerida na chamada desse crawler!')

        # Recolher dependências
        entity_id = kwargs[entity_name]
        crawler_list_used = list(Crawler.db.crawler_list_used(entity_id, entity_name[7:]))

        if self.multiple_dependence_routes:
            # Se houver várias rotas de depedência, seguirá o seguinte algorítimo:
            # 1 - Se uma das rotas já tiver todos os dados presentes no banco, irá usa-la
            # 2 - Se uma das rotas tem dados não alcançáveis, não a usará
            # 3 - Prioriza a rota com menos depedências
            dict_dependencies = None
            for i in self.dependencies:
                current_dict_dependencies = Crawler.db.get_dependencies(entity_id, entity_name[7:], *i)

                # Se o retorno de get_dependencies for false, então há dependências não pertecente à essa entity,
                # logo, devemos ignorar essa rota de dependência
                if current_dict_dependencies is False:
                    continue

                # Já tem todos os dados presentes?
                if None not in current_dict_dependencies.values():
                    dict_dependencies = current_dict_dependencies
                    break

                # Alguns dos dados faltosos são alcançáveis apenas com crawlers que já tenham sido usado?
                use_it = True
                for k, v in current_dict_dependencies.items():
                    if v is not None:
                        continue

                    for i2 in dict_info_to_crawlers[k]:
                        if i2 not in crawler_list_used:
                            break
                        use_it = False

                    if use_it is False:
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
            dict_dependencies = Crawler.db.get_dependencies(entity_id, entity_name[7:], *self.dependencies)

        # Verificar se alguma dependência não está presente no banco
        # Se não estiver, então vai colhe-la e chamar novamente esse mesmo método
        for dependence_name, dependence_value in dict_dependencies.items():
            if dependence_value is None:
                if 'crawlers_tried' not in kwargs:
                    kwargs['crawlers_tried'] = []

                for i in dict_info_to_crawlers[dependence_name]:
                    # se o crawler já tiver sido usado, não devemos tentar usa-lo novamente
                    if i.name() in crawler_list_used or i in kwargs['crawlers_tried']:
                        continue

                    # verificar se esse crawler pode usar essa entity
                    harvest_args = inspect.getargspec(i.harvest_debug).args
                    if entity_name not in harvest_args:
                        continue

                    # adicionar à lita de crawlers já tentados, para evitar loop infinito
                    kwargs['crawlers_tried'].append(i)

                    # tentar colher o crawlers que provém a dependência faltante e então voltará a esse mesmo crawler de agora
                    i.harvest(*args, **kwargs)
                    Crawler.temp_current_crawler = self.f # todo: resolver esse código estranho! precisa redeclarar a variável temporária do crawler em que está executando
                    return self.__call__(*args, **kwargs)

                return False

        if 'crawlers_tried' in kwargs:
            del kwargs['crawlers_tried']

        # "Passar" de forma implícita variáveis temporárias ao Crawler, úteis na hora de salvar as infos no banco de dados
        Crawler.temp_current_entity_name = entity_name
        Crawler.temp_current_entity_id = entity_id

        # Colher
        self.harvest(*args, dependencies=dict_dependencies, **kwargs)

        # Após a colheita, para evitar problemas, apagará as variáveis temporárias
        del Crawler.temp_current_entity_name
        del Crawler.temp_current_entity_id

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

    if i.dependencies() is not None:
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
