if __name__ == '__main__':
    from database import ManagerDatabase
    import analysis

    db = ManagerDatabase(trigger=False)

    db.crawler_qselecao.harvest(specifc_concurso=2871)
    db.crawler_etufor.harvest(entity_person=1)
    db.crawler_sspds.harvest(entity_person=1)

#    x = db.crawler_test1.harvest()
#    x = db.crawler_test2.harvest(entity_person=1)
#    x = db.crawler_test3.harvest(entity_person=1)

# é melhor escrever do primeiro ou do segundo modo?
#for cls in Crawler.__subclasses__():
#    if cls.have_dependencies() is False:
#        continue
#
#    for current_crop in cls.crop():
#        dict_info_to_crawlers[current_crop].append(cls)

#[
#    dict_info_to_crawlers[current_crop].append(cls)
#    for cls in Crawler.__subclasses__()
#    if cls.have_dependencies() is True
#
#    for current_crop in cls.crop()
#]

    exit()

    import requests
    import json

    for year, month, day in [(y, m, d) for y in range(2009, 2011) for m in range(1, 13) for d in range(1, (32, 31)[m % 2 == 0])]:
        print(year, month, day)
        r = requests.get('http://compras.dados.gov.br/licitacoes/v1/licitacoes.json?data_publicacao={}-{:0>2}-{:0>2}'.format(
                year, month, day
            ), verify=False)
        while True:
            try:
                json_return = json.loads(r.text)
            except:
                # virá para cá caso o dia seja inexistente no calendário, como no caso do dia 30 de fevereiro
                break

            for i in json_return['_embedded']['licitacoes']:
                print('http://compras.dados.gov.br' + i['_links']['self']['href'] + '.json')
                if i['modalidade'] == 3:
                    db.crawler_compras_governamentais_concorrencia.harvest(licitacao_url='http://compras.dados.gov.br' + i['_links']['self']['href'] + '.json')
                elif i['modalidade'] == 5:
                    db.crawler_compras_governamentais_pregao.harvest(licitacao_url='http://compras.dados.gov.br' + i['_links']['self']['href'] + '.json')

            if 'next' in json_return['_links']:
                r = requests.get('http://compras.dados.gov.br' + json_return['_links']['next']['href'], verify=False)
            else:
                break

    exit()

    #analysis.make_pie(partido)

    #db.crawler_esaj.harvest(entity_person=4)
    #x = db.get_entity_row_info(1, 'person')
    #print(x)

    #graphdependencies.GraphDependencies.entity_graphs['person'].draw()

    #db.crawler_etufor.harvest(entity_person=546)

    #x = db.get_entity_row_info(546, 'person')
    #print(x)

    #db.get_entity_row_info(1, 'person')

    #db.crawler_aylien_concept.harvest(entity_news=1)
    #db.crawler_aylien_summarize.harvest(entity_news=2, total_sentences=2)

    #db.crawler_portal_transparencia.harvest(specific_siteid=1884980)
    #x = db.get_entity_row_info(1, 'person')
    #print(x)

    #preciso dizer que me inspirei na parte do driver maltego no código do paolo

    # fazer crawler de diretório, para analisar pendrives alheio

    # todo
    #  para resolver casos como o do esaj
    #  <column>
    #    <name>parte_name</name>
    #    <type>TEXT</type> <!-- todo: o ideal seria que aqui fosse uma referência para entity id de person ou então de empresa; pode ser um ou outro -->
    #  </column>
    #  eu poderia criar duas colunas "entity_person_id_parte_name" e "entity_firm_id_parte_name", e somente uma das duas poderia ser preenchida
    #  e eu verificaria no esaj se é person ou firm, para escrever na coluna correta
    #  a verificação de que só uma foi escrita seri feita na própria criação da tabela

    # todo
    #  da para melhorar a parte da tabela "partes" do "easj", pois a coluna "people_name" poderia se relacionar com um id de people na verdade

    # todo
    #  a parte da API de conexão ao mundo externo será feito na forma de driver, recebendo um GET e retornando JSON ou então XML,
    #  pode ser feita usando http://bottlepy.org/docs/dev/index.html

    ###
    # Exemplos

    # Coletar os dados das pessoas dessa página: http://qselecao.ifce.edu.br/listagem.aspx?idconcurso=2600&etapa=1
    # O crawler qselecao é um exemplo de crawler populador
    #db.crawler_qselecao.harvest(specifc_concurso=2600)

    # Coletar os dados da etufor da entity row person de id 1
    #db.crawler_etufor.harvest(entity_person=1)

    # Exibir o grafo de dependências da entity person
    #graphdependencies.GraphDependencies.entity_graphs['person'].draw()

    # Exibir o grafo de dependêncais da entity row person de id 1
    #graphdependencies.GraphDependenciesOfentityRow(db, 1, 'person').draw()

    ############

    # todo: http://empresasdobrasil.com/

    # lista de cnpj úteis para testes:
    #  antes usvam o simples nacional -> 00592174000514, 13154567000164, 03231589000127
    #  usam o simples nacional -> 12772600000157, 14948401000182, 18999322000151
    #  usam o simei -> 18218555000170, 15788951000144

    # todo:  https://sncr.serpro.gov.br/ccir/emissao;jsessionid=MGijK1RW2N2llgH4QNLH8h7o.ccir2?windowId=011

    # todo: a coincidência de nomes de 'voter_registration' entre os dois crawlers bagunça um pouco o grafo de dependências, pois
    # ele só salva uma das edges para o 'voter_registration'. Há duas possíveis partindo do nome, por exemplo, mas ele só salva uma delas
    # Eu preciso fazer com que no campo 'crawler' das edges possa salvar uma lista de crawlers e deixar assim mesmo

    # todo: depois fazer um mapa com os lugares frequentados por tal pessoa

    # todo: depois implementar checagem no Diário Oficial
    #       http://pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp?jornal=3&pagina=69&data=06/11/2015
    #       http://portal.in.gov.br/

    # todo: mais dados aqui http://www2.camara.leg.br/transpnet/consulta
    # todo: aqui mostra dados a serem extraídos de uma url http://sbseg2015.univali.br/anais/SBSegCompletos/artigoCompleto08.pdf

    # todo: adicionar crem(sp|mg|...)

    # todo: no futuro, o spyck poderia monitorar sites de lojas para encontrar produtos roubados, como obras de artes
    #  2:10 http://g1.globo.com/fantastico/noticia/2016/03/se-achadas-obras-sumidas-poderiam-formar-maior-museu-de-arte-do-brasil.html