from db import spatialite

database = spatialite.Spatialite('spatialite.db')

database.test()
