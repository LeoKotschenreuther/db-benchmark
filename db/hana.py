import jaydebeapi
import hanaCredentials
import re
import math

class Hana:

    def __init__(self):
        self.HOST = hanaCredentials.host()
        self.PASSWORD = hanaCredentials.pw()
        self.PORT = hanaCredentials.port()
        self.USER = hanaCredentials.user()
        self.con = self.connect()
        self.cursor = self.con.cursor()

    def connect(self):
        url = 'jdbc:sap://%s:%s' %(self.HOST, self.PORT)
        return jaydebeapi.connect(
            'com.sap.db.jdbc.Driver',
            [url, self.USER, self.PASSWORD],
            'ngdbc.jar')


    def disconnect(self):
        self.con.close()

    def polygonString(self, polygon):
        # NEW ST_POLYGON('Polygon((-0.8 0.7, -0.6 0.7, -0.6 0.4, -0.8 0.4, -0.8 0.7))')
        string = "NEW ST_Polygon('Polygon(("
        for point in polygon:
            string += str(point['x']) + " " + str(point['y']) + ","
        string += str(polygon[0]['x']) + " " + str(polygon[0]['y']) + "))')"
        return string

    def isPolygonValid(self, polygon):
        query = "SELECT " + self.polygonString(polygon) + ".ST_IsValid() FROM dummy"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            if row[0] == 1 : return True
            else: return False

    def checkIntersection(self, polygons):
        query = "SELECT " + self.polygonString(polygons[0]) + ".ST_Intersects(" + self.polygonString(polygons[1]) + ") FROM dummy"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        flag = False
        for row in result:
            if row[0] == 1 : flag = True
            elif row[0] == 0 : flag = False
        return flag

    def insertPolygons(self, polygons):
        try:
            dropPolygons = "DROP TABLE BENCHMARK.POLYGONS"
            self.cursor.execute(dropPolygons)
            print("\tDropped Table Polygons")
        except:
            print("\tCould not drop table polygons as it doesn't exist")
        createPolygonTable = "CREATE COLUMN TABLE BENCHMARK.POLYGONS (ID INTEGER, POLYGON ST_GEOMETRY)"
        self.cursor.execute(createPolygonTable)
        print("\tCreated Table Polygons")
        for i, polygon in enumerate(polygons):
            insert = "INSERT INTO BENCHMARK.POLYGONS (ID, polygon) VALUES (" + str(i) + ", " + self.polygonString(polygon) + ")"
            self.cursor.execute(insert)
        print("\tInserted Polygons into polygons table")

    def runQueriesPoly(self, queries, numberOfExecutions, polygonSize):
        print '\tPolygonsize: ' + str(polygonSize)
        results = self.runQueries(queries, numberOfExecutions)
        results['polygonSize'] = polygonSize
        return results

    def runQueries(self, queries, numberOfExecutions):
        clearPlanCacheSQL = "ALTER SYSTEM CLEAR SQL PLAN CACHE"
        self.cursor.execute(clearPlanCacheSQL)
        results = {'database': 'hana', 'queries': list()}
        allQueries = len(queries) * numberOfExecutions
        n = 0
        for query in queries:
            queryObject = {'name': query, 'executions': 0, 'avg': 0}
            for x in range(0, numberOfExecutions):
                self.cursor.execute(query)
                result = self.cursor.fetchall()

            # exchange every ' in the query string with '' for escaping
            preparedQuery = re.sub("'", "''", query)
            getAvgTime = "SELECT EXECUTION_COUNT, AVG_EXECUTION_TIME FROM PUBLIC.M_SQL_PLAN_CACHE WHERE STATEMENT_STRING LIKE '" + preparedQuery + "'"
            self.cursor.execute(getAvgTime)
            avgTimeResult = self.cursor.fetchall()
            for row in avgTimeResult:
                # print int(str(row[1]))
                queryObject['avg'] = float(int(str(row[1]))/1000)
                queryObject['executions'] = row[0]

            results['queries'].append(queryObject)
            n = n + 1
            if n % (math.ceil(allQueries/10.0)) == 0:
                print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')

        return results

    def runTest(self):
        query = "SELECT * FROM BENCHMARK.POINTS limit 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        for r in result:
            print r[0]