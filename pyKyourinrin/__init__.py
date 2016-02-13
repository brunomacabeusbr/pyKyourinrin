if __name__ == '__main__':
    from database import ManagerDatabase
    import graphdependencies

    db = ManagerDatabase(trigger=False)

    ###
    # Exemplos

    # Coletar os dados das pessoas dessa página: http://qselecao.ifce.edu.br/listagem.aspx?idconcurso=2600&etapa=1
    # O crawler qselecao é um exemplo de crawler populador
    #db.crawler_qselecao.harvest(specifc_concurso=2600)

    # Coletar os dados da etufor da primitive row peoples de id 1
    #db.crawler_etufor.harvest(primitive_peoples=1)

    # Exibir o grafo de dependências da primitive peoples
    #graphdependencies.GraphDependencies.primitive_graphs['peoples'].draw()

    # Exibir o grafo de dependêncais da primitive row peoples de id 1
    graphdependencies.GraphDependenciesOfPrimitiveRow(db, 1, 'peoples').draw()
