if __name__ == '__main__':
    from database import ManagerDatabase
    import graphdependencies

    db = ManagerDatabase(trigger=False)
    #x = db.get_primitive_row_info_all(1, 'peoples')
    #print(x)
    #db.get_primitive_row_info_all(1, 'peoples')
    #db.crawler_fak.harvest(primitive_peoples=1)
    #db.crawler_qselecao.harvest(specifc_concurso=2600)

    #db.crawler_portal_transparencia.harvest(primitive_peoples=40)

    # todo: seria viável no projeto do bepid fazer um maltego open source? no começo, eu posso começar a desenvolver uma ferramenta de geração dos XML de forma gráfica

    # todo: fazer no futuro
    # no arquivo
    # java -jar classfileanalyzer.jar '/home/macabeus/ApenasMeu/Maltego bckap (another copy)/v3.0/maltego-core-platform/modules/com-paterva-maltego-graph/com/paterva/maltego/graph/MaltegoGraph.class'
    # há o método
    # .method public abstract add(Lcom/paterva/maltego/core/MaltegoEntity;)V
    # que é implementado em GraphWrapper
    # eu posso fazer com que liste todas as primitives row já existentes no banco e clicando nela adicioná a entidade ao grafo, já com o table_id setado

    # por alguma razão, PRECISA ter o java 1.7
    # sudo apt-get install oracle-java7-installer
    # sudo apt-get install oracle-java7-set-default
    # e se quiser trocar depois a versão
    # update-alternatives --config java

    #pegar o fork do paolo https://github.com/brunomacabeusbr/casefile-extender
    #e começar a jogar as minhas coisas nele, documentar tudo em inglês, colocar manual do pyKyourinrin no macalogs para depois traduzir para o inglês

    exit()

    # todo: http://empresasdobrasil.com/
    # todo: fazer o gerador do XML em UML, e usando aplicação web (flask + javascript = s2)
    # todo: talvez seja bom mudar o help() dos crawlers, para da infromações da chamada e retornos, dentre outras coisas

    # todo: no script cfpatch, posso automatizar o processo de alterar as entidades que se localizam no com/paterva/maltego/entities/common/maltego.Unknown.entity

    # lista de cnpj úteis para testes:
    #  antes usvam o simples nacional -> 00592174000514, 13154567000164, 03231589000127
    #  usam o simples nacional -> 12772600000157, 14948401000182, 18999322000151
    #  usam o simei -> 18218555000170, 15788951000144

    # todo:  https://sncr.serpro.gov.br/ccir/emissao;jsessionid=MGijK1RW2N2llgH4QNLH8h7o.ccir2?windowId=011

    # todo: a coincidência de nomes de 'voter_registration' entre os dois crawlers bagunça um pouco o grafo de dependências, pois
    # ele só salva uma das edges para o 'voter_registration'. Há duas possíveis partindo do nome, por exemplo, mas ele só salva uma delas
    # Eu preciso fazer com que no campo 'crawler' das edges possa salvar uma lista de crawlers e deixar assim mesmo

    # todo: depois fazer um mapa com os lugares frequentados por tal pessoa

    # todo: ver sobre o grupo Infraestrutura Nacional de Dados Abertos https://groups.google.com/forum/#!forum/lista-inda-gt3
    # todo: depois implementar checagem no Diário Oficial
    #       http://pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp?jornal=3&pagina=69&data=06/11/2015
    #       http://portal.in.gov.br/

    # todo: mais dados aqui http://www2.camara.leg.br/transpnet/consulta
    # todo: aqui mostra dados a serem extraídos de uma url http://sbseg2015.univali.br/anais/SBSegCompletos/artigoCompleto08.pdf

    # todo: adicionar crem(sp|mg|...)