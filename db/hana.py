import jaydebeapi
import xml.etree.ElementTree as ET
import hanaCredentials
import math
import time

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

    def pointString(self, point):
        return "POINT(" + str(point['x']) + " " + str(point['y']) + ")"

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

    def dropCreateTable(self, table):
        try:
            dropPolygons = "DROP TABLE " + table
            self.cursor.execute(dropPolygons)
            print("\tDropped Table")
        except:
            print("\tCould not drop table as it doesn't exist")
        createTable = ""
        if table == 'BENCHMARK.POLYGONS':
            createTable = "CREATE TABLE " + table + " (ID INTEGER, size INTEGER, POLYGON ST_GEOMETRY)"
        elif table == 'BENCHMARK.B_POINTS':
            createTable = "CREATE COLUMN TABLE BENCHMARK.B_POINTS (ID INTEGER, X FLOAT, Y FLOAT, POINT ST_POINT)"
        self.cursor.execute(createTable)
        print("\tCreated Table")

    def insertPolygons(self, polygons, polygonSize):
        for i, polygon in enumerate(polygons):
            insert = "INSERT INTO BENCHMARK.POLYGONS (ID, SIZE, polygon) VALUES (" + str(i) + ", " + str(polygonSize) + ", " + self.polygonString(polygon) + ")"
            self.cursor.execute(insert)
        print("\tInserted Polygons into polygons table")

    def insertPoints(self, points):
        for i, point in enumerate(points):
            # print self.pointString(point)
            insert = '''INSERT INTO BENCHMARK.B_POINTS (ID, X, Y, POINT) VALUES (?, ?, ?, NEW ST_POINT(?))'''
            self.cursor.execute(insert, (i, point['x'], point['y'], self.pointString(point)))
            # self.cursor.execute(insert, (i, point['x'], point['y'], 'POINT(1 2)'))
            if i % 1000 == 999:
                print "finished: " + str(i+1)
        print("\tInserted Points into Points table")

    def runQueriesPoly(self, queries, numberOfExecutions, polygonSize):
        print '\tPolygonsize: ' + str(polygonSize)
        results = self.runQueries(queries, numberOfExecutions)
        results['polygonSize'] = polygonSize
        return results

    def runQueries(self, queries, numberOfExecutions):
        results = {'database': 'hana', 'queries': list()}
        allQueries = len(queries) * numberOfExecutions
        n = 0
        for query in queries:
            queryObject = {'name': query, 'times': list(), 'avg': 0}
            for x in range(0, numberOfExecutions):
                # print query
                self.get_planviz_data(query, 'hana')
                n = n + 1
                if n % (math.ceil(allQueries/10.0)) == 0:
                    print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')

                tree = ET.parse("./results/hana/hana.xml") #xml output vom planviz_call
                queryObject['times'].append(float(tree.find(".//{http://www.sap.com/ndb/planviz}RootRelation//{http://www.sap.com/ndb/planviz}ExecutionTime//{http://www.sap.com/ndb/planviz}Inclusive").text)/1000)

            results['queries'].append(queryObject)

        for query in results['queries']:
            avg = 0
            x = 0.0
            for val in query['times']:
                avg = avg + val
                x = x + 1

            avg = avg / x
            query['avg'] = avg
        return results

    def get_planviz_data(self, query, resultname):
        self.cursor.execute(''' {CALL PLANVIZ_ACTION(?,?)} ''', (103, None)) #enable planviz mode
        self.cursor.execute(''' {CALL PLANVIZ_ACTION(?,?)} ''', (201, query)) #get query id
        queryid = u''''''
        for row in self.cursor.fetchall():
            queryid =  unicode(row[0])
        # cursor.execute(''' {CALL PLANVIZ_ACTION(?,?)} ''', (304, queryid)) # get optimizer plan
        # with open("planviz_data/range_test/304/"+resultname+".xml", "w") as xmlout:
        #     for row in cursor.fetchall():
        #         xmlout.write(row[0])
        string = "EXECUTE PLANVIZ STATEMENT ID '" + str(queryid) +"'" # exceute the query
        with open("execution_time_range_test.csv", "a") as out:
            start = time.time()
            try:
                self.cursor.execute(string)
            except:
                print "Error during execution of query" + query
                return
            out.write(str(time.time()-start)+'\t' + resultname + '\n')
        self.cursor.execute(''' {CALL PLANVIZ_ACTION(?,?)} ''', (401, queryid)) # get plan after execution
        with open("results/hana/"+ resultname+".xml", "w") as xmlout:
            for row in self.cursor.fetchall():
                xmlout.write(row[0])

    def runTest(self):
        query = "SELECT * FROM BENCHMARK.POINTS limit 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        for r in result:
            print r[0]