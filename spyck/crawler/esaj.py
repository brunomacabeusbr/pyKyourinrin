from . import Crawler
from selenium import webdriver


class CrawlerEsaj(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference INTEGER PRIMARY KEY AUTOINCREMENT,'
                            'processo_number TEXT,'
                            'processo_grau TEXT,'
                            'classe TEXT,'
                            'classe_area TEXT,'
                            'assunto TEXT,'
                            'entity_person_id_juiz INTEGER,'
                            'valor_acao FLOAT,'
                            'url TEXT,'
                            'FOREIGN KEY(entity_person_id_juiz) REFERENCES entity_person(id)'
                        ');' % (self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'reference INTEGER PRIMARY KEY AUTOINCREMENT,'
                            'principal INTEGER,'
                            'reu_preso INTEGER,'
                            'parte_type TEXT,'
                            'parte_name TEXT,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_partes', self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_partes INTEGER,'
                            'justiciario_type TEXT,'
                            'entity_person_id_justiciario_name INTEGER,'
                            'FOREIGN KEY(reference_partes) REFERENCES %s(reference_partes),'
                            'FOREIGN KEY(entity_person_id_justiciario_name) REFERENCES entity_person(id)'
                        ');' % (self.name() + '_partes_justiciario', self.name() + '_partes'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'data_day INTEGER,'
                            'data_month INTEGER,'
                            'data_year INTEGER,'
                            'descricao TEXT,'
                            'documento_url TEXT,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_movimentacoes', self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'data_day INTEGER,'
                            'data_month INTEGER,'
                            'data_year INTEGER,'
                            'descricao TEXT,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_peticoes', self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'data_day INTEGER,'
                            'data_month INTEGER,'
                            'data_year INTEGER,'
                            'classe TEXT,'
                            'href TEXT,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_incidentes', self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'data_day INTEGER,'
                            'data_month INTEGER,'
                            'data_year INTEGER,'
                            'audiencia TEXT,'
                            'situacao TEXT,'
                            'quantidade_pessoas INTEGER,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_audiencia', self.name() + '_processo'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'entity_person_id INTEGER,'
                            'entity_firm_id INTEGER,'
                            'reference_processo INTEGER,'
                            'documento TEXT,'
                            'numero TEXT,'
                            'distrito_policial TEXT,'
                            'city TEXT,'
                            'state TEXT,'
                            'FOREIGN KEY(reference_processo) REFERENCES %s(reference_processo)'
                        ');' % (self.name() + '_dados_delegacia', self.name() + '_processo'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'processo'},
            {'table': 'partes', 'reference': ('processo',)},
            {'table': 'partes_justiciario', 'reference': ('processo', 'partes')},
            {'table': 'movimentacoes', 'reference': ('processo',)},
            {'table': 'peticoes', 'reference': ('processo',)},
            {'table': 'incidentes', 'reference': ('processo',)},
            {'table': 'audiencia', 'reference': ('processo',)}
        )

    @staticmethod
    def macro_at_data():
        def aliados_juridicos(read):
            added = []
            to_return = []

            for i in read['esaj_processo']:
                for j in i['partes']:
                    if j['parte_name'] != read['name']:
                        continue

                    if 'partes_justiciario' not in j:
                        continue

                    for l in j['partes_justiciario']:
                        if l['entity_person_id_justiciario_name'] not in added:
                            added.append(l['entity_person_id_justiciario_name'])
                            to_return.append({
                                'entity_person_id_aliado_justiciario': l['entity_person_id_justiciario_name'],
                                'justiciario_type': l['justiciario_type'], 'justiciario_count': 1
                            })
                        else:
                            for i in to_return:
                                if i['entity_person_id_aliado_justiciario'] != l['entity_person_id_justiciario_name']:
                                    continue

                                i['justiciario_count'] = i['justiciario_count'] + 1

            return to_return

        def processos(read):
            return [
                {
                    'assunto': i['assunto'],
                    'classe': i['classe'] + '/' + i['classe_area'],
                    'processo_number': i['processo_number'],
                    'partes': [
                        {'parte_name': j['parte_name'], 'parte_type': j['parte_type']}
                        for j in i['partes']
                    ]
                }
                for i in read['esaj_processo']
            ]

        return (
            {'column_name': 'aliados_juridicos', 'how': aliados_juridicos},
            {'column_name': 'processos', 'how': processos},
        )

    @staticmethod
    def name():
        return 'esaj'

    @staticmethod
    def dependencies():
        return ('name',), ('razao_social',)

    @staticmethod
    def crop():
        return 'aliados_juridicos', 'processos'

    @staticmethod
    def entity_required():
        return 'entity_person', 'entity_firm'

    @classmethod
    def harvest(cls, entity_person=None, entity_firm=None, dependencies=None):
        phantom = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true'])

        # todo: por enquanto, só recolhe processos do primeiro grau; implementar para colher de segundo grau também?
        # todo: como lidar com nomes abreviados? tais como em http://www2.tjal.jus.br/cpopg/show.do?processo.codigo=1M00014OI0000&processo.foro=58&conversationId=&dadosConsulta.localPesquisa.cdLocal=-1&cbPesquisa=NMPARTE&dadosConsulta.tipoNuProcesso=UNIFICADO&dadosConsulta.valorConsulta=anchieta&paginaConsulta=1
        # todo: será bom me preocupar em casos como em http://www2.tjal.jus.br/cpopg/search.do?conversationId=&dadosConsulta.localPesquisa.cdLocal=58&cbPesquisa=NMPARTE&dadosConsulta.tipoNuProcesso=UNIFICADO&dadosConsulta.valorConsulta=Cristiano+de+Oliveira+Gomes
        #  da qual o Cristiano de Oliveira Gomes tem um apelido, "CACO"
        #  http://www2.tjal.jus.br/cpopg/show.do?processo.codigo=1X00000TR0000&processo.foro=58

        # recolher lista de processos
        urls_esaj = [
            'http://esaj.tjce.jus.br/cpopg/open.do',  # ceará
            'http://esaj.tjrn.jus.br/cpo/pg/open.do',  # rio grande do norte
            # http://www.21varacivel.com.br/cpo/pg/open.do,  # paraná # todo: precisa lidar com o captchar
            #'https://esaj.tjsc.jus.br/cpopg/open.do', # santa catarina # todo: precisa lidar com o captchar
            #'https://esaj.tjsp.jus.br/cpopg/open.do',  # são paulo # todo: usa abreviações na seção "partes"
            'http://esaj.tjac.jus.br/cpopg/open.do',  # acre
            'http://www2.tjal.jus.br/cpopg/open.do',  # alagoas
            # todo: falta adicionar os demais estados
            # todo: o do Rio de Janeiro realmente é diferente? http://www4.tjrj.jus.br/ConsultaUnificada/consulta.do#tabs-numero-indice0
        ]

        def get_element_next_page():
            element_next_page = phantom.execute_script("return $('a:contains(\">\")').eq(0)")
            if len(element_next_page) > 0:
                return element_next_page[0]
            else:
                return None

        #href_list = ['https://esaj.tjsp.jus.br/cpopg/show.do?processo.codigo=1HZX2RK010000&processo.foro=53&dadosConsulta.localPesquisa.cdLocal=-1&cbPesquisa=NMPARTE&dadosConsulta.tipoNuProcesso=UNIFICADO&dadosConsulta.valorConsulta=Petrobras&chNmCompleto=true&paginaConsulta=1']
        #for i in []:
        href_list = []
        for i in urls_esaj:
            phantom.get(i)
            phantom.find_element_by_css_selector('[value=NMPARTE]').click()
            phantom.find_element_by_id('NMPARTE').find_elements_by_tag_name('input')[0].send_keys(dependencies['name'])
            #phantom.find_element_by_id('NMPARTE').find_elements_by_tag_name('input')[0].send_keys('Petrobras')
            phantom.find_element_by_id('NMPARTE').find_elements_by_tag_name('input')[1].click()
            phantom.find_element_by_name('pbEnviar').click()

            if len(phantom.find_elements_by_id('paginacaoSuperior')) == 0:
                # caso só haja um único resultado, não é mostrada a listagem de processos; mas sim vai direto para a página do processo
                href_list.append(phantom.current_url)
                continue

            while True:
                href_list.extend([i.find_element_by_tag_name('a').get_attribute('href') for i in phantom.find_elements_by_class_name('nuProcesso')])

                element_next_page = get_element_next_page()
                if element_next_page is not None:
                    element_next_page.click()
                else:
                    break

        if len(href_list) == 0:
            cls.update_crawler_status(False)
            return

        cls.update_my_table({})

        # parse em todas as listas de processos colhidas nos sites
        for url in href_list:
            phantom.get('about:blank')
            while phantom.current_url == 'about:blank':
                phantom.get(url)

            # dados do processo
            processo_number =\
                phantom.execute_script("return $('.labelClass:contains(\"Processo\")').closest('tr').find('span').slice(0, 2).text()").replace('\n', '').replace('\t', '').replace('(', ' (')

            processo_grau =\
                phantom.execute_script("return $(\"span[style*=red], span[style*='#FF0000']\").text()").replace('(', '').replace(')', '')

            classe =\
                phantom.execute_script("return $('.labelClass:contains(\"Classe\")').closest('tr').find('span').eq(0).text()")

            classe_area =\
                phantom.execute_script("return $('.labelClass:contains(\"Área\")').closest('td').text()").strip()[len('Área: '):]

            assunto =\
                phantom.execute_script("return $('.labelClass:contains(\"Assunto\")').closest('tr').find('span').text()")

            juiz =\
                phantom.execute_script("return $('.labelClass:contains(\"Juiz\")').closest('tr').find('span').text()")
            juiz = (juiz, None)[juiz == '']

            valor_acao =\
                phantom.execute_script("return $('.labelClass:contains(\"Valor da ação\")').closest('tr').find('span').text()")
            valor_acao = (valor_acao, None)[valor_acao == '']

            cls.update_my_table({
                'processo_number': processo_number,
                'processo_grau': processo_grau,
                'classe': classe,
                'classe_area': classe_area,
                'assunto': assunto,
                'entity_person_id_juiz': cls.db.update_entity_row({}, entity_filter={'name': juiz}, entity_name='entity_person'),
                'valor_acao': valor_acao,
                'url': url}, table='processo'
            )
            reference_processo = cls.db.lastrowid()

            # partes do processo
            table_principais = phantom.find_element_by_id('tablePartesPrincipais')
            if len(phantom.find_elements_by_id('tableTodasPartes')) == 1:
                table_todos = phantom.find_element_by_id('tableTodasPartes')
            else:
                table_todos = None

            if table_todos is not None:
                principais_name = [i.text.split('\n')[0] for i in table_principais.find_elements_by_css_selector('[align="left"]')]
                phantom.find_element_by_id('linkpartes').click()

                for i in table_todos.find_elements_by_class_name('fundoClaro'):
                    parte_type = i.find_element_by_css_selector('[align="right"]').text[:-2]

                    left_text = i.find_element_by_css_selector('[align="left"]').text
                    parte_name = left_text.split('\n')[0]
                    left_text = left_text.split('\n')[1:]

                    # todo: lidar com abreviações em parte_type tais como
                    #   "Ministério Púb" -> "Mininsério Público"

                    cls.update_my_table({'principal': (0, 1)[parte_name in principais_name], 'parte_type': parte_type, 'parte_name': parte_name.title(), 'reference_processo': reference_processo}, table='partes')
                    reference_partes = cls.db.lastrowid()

                    for i in left_text:
                        justiciario_type, justiciario_name = i.split(': ')
                        # todo: unificar gêneros em justiciario_type, tais como
                        #   "advogada" -> "advogado"
                        #   "Devedora" -> "Devedor"
                        # também há sites que ficam com inicial maiúscula enquanto outros não
                        # e abreviações, tais como
                        #   "Ministério Púb" -> "Mininsério Público"
                        #   "Defensor P" -> Defensor Público
                        cls.update_my_table({
                            'justiciario_type': justiciario_type,
                            'entity_person_id_justiciario_name': cls.db.update_entity_row({}, entity_filter={'name': justiciario_name.strip().title()}, entity_name='entity_person'),
                            'reference_partes': reference_partes
                        }, table='partes_justiciario')
            else:
                for i in table_principais.find_elements_by_class_name('fundoClaro'):
                    parte_type = i.find_element_by_css_selector('[align="right"]').text[:-2]

                    left_text = i.find_element_by_css_selector('[align="left"]').text
                    parte_name = left_text.split('\n')[0]
                    left_text = left_text.split('\n')[1:]

                    reu_preso = 0
                    if parte_name[-1 * len(' Réu Preso'):] == ' Réu Preso':
                        parte_name = parte_name[:len(parte_name) - len(' Réu Preso')]
                        reu_preso = 1

                    cls.update_my_table({'principal': 1, 'parte_type': parte_type, 'parte_name': parte_name, 'reu_preso': reu_preso, 'reference_processo': reference_processo}, table='partes')
                    reference_partes = cls.db.lastrowid()

                    for i in left_text:
                        justiciario_type, justiciario_name = i.split(': ')
                        cls.update_my_table({
                            'justiciario_type': justiciario_type,
                            'entity_person_id_justiciario_name': cls.db.update_entity_row({}, entity_filter={'name': justiciario_name.strip().title()}, entity_name='entity_person'),
                            'reference_partes': reference_partes
                        }, table='partes_justiciario')

            # movimentações
            phantom.find_element_by_id('linkmovimentacoes').click()

            movimentacoes = phantom.find_element_by_id('tabelaTodasMovimentacoes').find_elements_by_tag_name('tr')
            for i in movimentacoes:
                data, descricao = [i.find_elements_by_tag_name('td')[0].text, i.find_elements_by_tag_name('td')[2].text]
                data_day, data_month, data_year = data.split('/')
                # todo: documento_url = ... pegar o texto do documento, se disponível
                cls.update_my_table({'data_day': data_day, 'data_month': data_month, 'data_year': data_year, 'descricao': descricao, 'reference_processo': reference_processo}, table='movimentacoes')

            # petições diversas
            peticoes = phantom.execute_script('return $(\'div:contains("Petições diversas")\').next().next()')[0]

            for i in peticoes.find_elements_by_tag_name('tr'):
                data, descricao = [i.find_elements_by_tag_name('td')[0].text, i.find_elements_by_tag_name('td')[1].text]
                data_day, data_month, data_year = data.split('/')
                cls.update_my_table({'data_day': data_day, 'data_month': data_month, 'data_year': data_year, 'descricao': descricao, 'reference_processo': reference_processo}, table='peticoes')

            # incidentes
            incidentes = phantom.execute_script('return $(\'div:contains("Incidentes, ações incidentais, recursos e execuções de sentenças")\').next().next()')[0]

            for i in incidentes.find_elements_by_tag_name('tr'):
                data = i.find_elements_by_tag_name('td')[0].text
                data_day, data_month, data_year = data.split('/')
                classe, href = i.find_elements_by_tag_name('td')[1].find_element_by_tag_name('a').text, i.find_elements_by_tag_name('td')[1].find_element_by_tag_name('a').get_attribute('href')
                cls.update_my_table({'data_day': data_day, 'data_month': data_month, 'data_year': data_year, 'classe': classe, 'href': href, 'reference_processo': reference_processo}, table='incidentes')

            # audiências
            if 'tjrn' in phantom.current_url:
                audiencias = phantom.execute_script('return $(\'div:contains("Audiências")\').next().next()')[0]
            else:
                audiencias = phantom.execute_script('return $(\'div:contains("Audiências")\').next().next().next()')[0]

            for i in audiencias.find_elements_by_tag_name('tr'):
                data, audiencia, situacao, quantidade_pessoas =\
                    [i.find_elements_by_tag_name('td')[0].text, i.find_elements_by_tag_name('td')[1].text, i.find_elements_by_tag_name('td')[2].text, i.find_elements_by_tag_name('td')[3].text]
                data_day, data_month, data_year = data.split('/')
                cls.update_my_table({'data_day': data_day, 'data_month': data_month, 'data_year': data_year, 'audiencia': audiencia, 'situacao': situacao, 'quantidade_pessoas': int(quantidade_pessoas), 'reference_processo': reference_processo}, table='audiencia')

            # dados da delegacia
            if len(phantom.find_elements_by_id('dadosDaDelegacia')) > 0:
                dados_delegacia = phantom.find_element_by_id('dadosDaDelegacia').find_elements_by_tag_name('tr')
                for i in dados_delegacia:
                    documento, numero, distrito_policial, municipio =\
                        [i.find_elements_by_tag_name('td')[0].text, i.find_elements_by_tag_name('td')[1].text, i.find_elements_by_tag_name('td')[2].text, i.find_elements_by_tag_name('td')[3].text]
                    city, state = municipio.rsplit('-', 1)
                    cls.update_my_table({'documento': documento, 'numero': numero, 'distrito_policial': distrito_policial, 'city': city, 'state': state, 'reference_processo': reference_processo}, table='dados_delegacia')

        cls.update_crawler_status(True)
