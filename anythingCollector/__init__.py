if __name__ == '__main__':
    from database import ManagerDatabase
    db = ManagerDatabase()

    db.crawler_sspds.harvest(id=1)

    exit()

    from database import ManagerDatabase
    db = ManagerDatabase()

    db.crawler_qselecao.harvest()
    db.crawler_etufor.harvest(id=1)

    exit()


    from graphdependencies import GraphDependenciesOfThisPeople

    gdp = GraphDependenciesOfThisPeople(db, 1)
    gdp.draw()

    #gdp.find_routes('name_monther')

    exit()

    #db.crawler_sspds.harvest(1)
    #db.commit()

    #db.crawler_qselecao.harvest(specifc_concurso=2871)
    #db.commit()
