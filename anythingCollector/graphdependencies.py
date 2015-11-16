import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
from crawler import Crawler
import math


class GraphDependencies:
    def __init__(self):
        self.graph = nx.DiGraph()

        list_crawler = Crawler.__subclasses__()

        ###
        # definir cores por crawler
        # cada crawler tem uma cor única. a cor é usada na hora de desenhar o edge
        import random

        colors_available = list(matplotlib.colors.cnames.keys())

        for i in list_crawler:
            color_choice = random.choice(colors_available)
            colors_available.remove(color_choice)
            i.my_color = color_choice

        ###
        # criar node
        # os nodes são todas as informações coletáveis
        for current_crawler in list_crawler:
            for current_crop in current_crawler.crop():
                if len(current_crop) > 0:
                    self.graph.add_node(current_crop)

        ###
        # crir edges da depedência para a info que colhe
        # os edges é um crawler ligando uma info à outra info, na ordem dependence -> crop
        for current_crawler in list_crawler:
            if current_crawler.have_dependencies() is False:
                continue

            crawler_infos = {'color': current_crawler.my_color, 'crawler': current_crawler, 'crawler_name': current_crawler.name()}

            dependencies = []
            if type(current_crawler.dependencies()[0]) == tuple:
                for i2 in current_crawler.dependencies():
                    for i3 in i2:
                        if i3 not in dependencies:
                            dependencies.append(i3)
            else:
                dependencies = current_crawler.dependencies()

            [self.graph.add_edge(current_dependence, current_crop, **crawler_infos)
             for current_dependence in dependencies for current_crop in current_crawler.crop()
             if len(current_crop) > 0 if current_dependence != current_crop]

        ###
        # definir posições dos nodes
        # isso só é necessário para desenhar o grafo
        self.pos = dict()

        def get_points(r, nodes):
            pos = dict()

            teta = 360 / len(nodes)
            teta = math.radians(teta)

            x = 0
            for i in nodes:
                pos[i] = (
                    (r * 1) * (math.cos(teta * x)),
                    (r * 1) * (math.sin(teta * x))
                )
                x += 1

            return pos

        # irá gerar a array com a ordem de depedencie
        # cada elemento será adicionado a uma nova array se depender apenas dos que já estão incluído nos leveis anteriores
        # por exemplo
        # [['a', 'b']] -> 'a' e 'b' dependem de ninguém
        # [['a', 'b'], ['c', 'd', 'e']] -> 'c', 'd' e 'e' depende de algum, ou todos, itens anteriores
        # [['a', 'b'], ['c', 'd', 'e'], ['f']] -> 'f' depende de algum, ou todos, elementos do segundo nível e/ou algum, ou todos, do primeiro nível

        level_dependence = []

        def level_dependence_expanded():
            return [i2 for i in level_dependence for i2 in i]

        def element_present(element):
            return element in level_dependence_expanded()

        list_dates_base = [i.crop() for i in list_crawler if i.have_dependencies() is False] # pegar todos as infos que podem ser alcançadas sem depedência
        list_dates_base = [x for xs in list_dates_base for x in xs] # remover das tuplas
        level_dependence.append(set(list_dates_base))

        total_infos = list(set([i2 for i in list_crawler for i2 in i.crop()]))

        level = 1
        while len(level_dependence_expanded()) != len(total_infos):
            level_dependence.append([])

            for current_crawler in list_crawler:
                for i2 in current_crawler.crop():
                    # verificar se ele mesmo já está presente
                    if element_present(i2):
                        continue

                    # verificar depedencias dele
                    my_dependencies = [i3 for i3 in current_crawler.dependencies()]
                    # verificar se há várias rotas de depedencia
                    multiple_dependence_routes = (type(my_dependencies[0]) == tuple)
                    if multiple_dependence_routes:
                        # se tiver multiplas rotas, vai verificar se alguma delas é alcançável com os dados já disponíveis
                        for ix in my_dependencies:
                            my_check_dep = [element_present(i3) for i3 in ix]
                            if False not in my_check_dep:
                                break
                    else:
                        my_check_dep = [element_present(i3) for i3 in my_dependencies]
                    if False not in my_check_dep:
                        level_dependence[level].append(i2)

            level += 1

        level = 1
        for i in level_dependence:
            self.pos.update(get_points(level, i))
            level += 1

        # todo: primeiro momento vai desenhar o grafo usando a alternativa 1, depoi vai desenhar usando a alternativa 2,
        # todo: desenhar legenda para tirar as labels de cima das arestas

    def draw(self):
        edges, colors = zip(*nx.get_edge_attributes(self.graph, 'color').items())

        nx.draw(self.graph, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                node_color='black', node_size=1000, alpha=0.5,
                edgelist=edges, edge_color=colors)
        nx.draw_networkx_edge_labels(self.graph, pos=self.pos, edge_labels=nx.get_edge_attributes(self.graph, 'crawler_name'), label_pos=0.85, font_size=8)
        # todo: em edge_labels, preciso separar por vírgula caso haja dois ou mais crawlers que levem para a mesma info
        plt.savefig("graph.png")
        plt.show()

gd = GraphDependencies()


class GraphDependenciesOfThisPeople:
    def __init__(self, db, id):
        self.id = id
        self.db = db
        self.gd = gd.graph.copy()
        self.pos = gd.pos

        ###
        # apagar edges de crawlers já usados
        crawler_list_used = self.db.crawler_list_used(self.id)
        for k in crawler_list_used.keys():
            [self.gd.remove_edge(*i) for i in self.gd.edges()
             if self.gd.get_edge_data(*i)['crawler'].name() == k]

        ###
        # marcar nodes com dados já obtidos
        people_status = self.db.get_people_info_all(self.id)
        for k, v in people_status.items():
            if k not in self.gd.node:
                continue

            if v is not None:
                self.gd.node[k]['node_color'] = 'blue'
            else:
                self.gd.node[k]['node_color'] = 'black'

    def draw(self):
        edges, colors = None, None
        edges_colors_items = nx.get_edge_attributes(self.gd, 'color').items()
        if len(edges_colors_items):
            edges, colors = zip(*edges_colors_items)

        nodes, node_color = zip(*nx.get_node_attributes(self.gd, 'node_color').items())

        nx.draw(self.gd, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                nodelist=nodes, node_color=node_color, node_size=1000,
                edgelist=edges, edge_color=colors)
        nx.draw_networkx_edge_labels(self.gd, pos=self.pos, edge_labels=nx.get_edge_attributes(self.gd, 'crawler_name'), label_pos=0.85, font_size=8)
        # todo: em edge_labels, preciso separar por vírgula caso haja dois ou mais crawlers que levem para a mesma info
        plt.savefig("graph_people.png")
        plt.show()

    def is_dependence_reachable(self, target, exclude_crawler=None):
        if exclude_crawler is None:
            return len(self.gd.in_edges(target)) != 0

        for i in self.gd.in_edges(target):
            if self.gd.get_edge_data(*i)['crawler'].name() == exclude_crawler:
                continue

            return True

        return False

    def harvest_dependence(self, target):
        # buscar os crawlers que levam ao node target
        crawlers_to_target = []
        for i in self.gd.in_edges(target):
            edge_crawler = self.gd.get_edge_data(*i)['crawler']
            if not edge_crawler in crawlers_to_target:
                crawlers_to_target.append(edge_crawler)

        # vamos usar os crawlers que levam ao node target
        # repare que esse código leva à recursividade, pois se uma das depedências desses crawlers não estiverem satisfeitas,
        # será chamado novamente esse mesmo código

        for i in crawlers_to_target:
            i.harvest(id=self.id)

            # Conseguiu colher o dado desejado? Se sim, para o loop
            if self.db.get_dependencies(self.id, target)[target] is not None:
                break

        # Retornar se teve sucesso em recolher a depedência desejada
        if self.db.get_dependencies(self.id, target)[target] is not None:
            return True
        else:
            return False

        # TODO: chamar os crawlers da lista crawlers_to_target com thread e, assim que o target for alcançado,
        # parar imediatamente todas as demais thread que estiverem rodando

        # só vai terminar essa função quando todas as threads forem finalizadas
