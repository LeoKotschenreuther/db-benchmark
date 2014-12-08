import jaydebeapi
import xml.etree.ElementTree as ET
import math
import time

class Hana:

    def __init__(self):
        self.HOST = '172.16.19.226'
        self.PASSWORD = 'manager'
        self.PORT = 31415
        # self.PORT = 30015
        self.USER = 'SYSTEM'
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        url = 'jdbc:sap://%s:%s' %(self.HOST, self.PORT)
        return jaydebeapi.connect(
            'com.sap.db.jdbc.Driver',
            [url, self.USER, self.PASSWORD],
            'ngdbc.jar')

    def disconnect(self):
        self.connection.close()

    def reconnect(self):
        self.disconnect()
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def polygonString(self, polygon):
        string = "Polygon(("
        for point in polygon:
            string += str(point['x']) + " " + str(point['y']) + ","
        string += str(polygon[0]['x']) + " " + str(polygon[0]['y']) + "))"
        return string

    def lineString(self, line):
        string = "Linestring("
        for point in line:
            string += str(point['x']) + " " + str(point['y']) + ","
        string = string[:-1] + ")"
        return string

    def pointString(self, point):
        return "POINT(" + str(point['x']) + " " + str(point['y']) + ")"

    def isPolygonValid(self, polygon):
        query = "SELECT NEW ST_POLYGON('" + self.polygonString(polygon) + "').ST_IsValid() FROM dummy"
        # query = '''SELECT NEW ST_POLYGON(?).ST_IsValid() FROM dummy'''
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                if row[0] == 1 : return True
                else: return False
        except Exception as e:
            print e
            return False

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
            createTable = "CREATE COLUMN TABLE BENCHMARK.POLYGONS (ID INTEGER, size INTEGER, POLYGON ST_GEOMETRY)"
        elif table == 'BENCHMARK.LINES':
            createTable = "CREATE COLUMN TABLE BENCHMARK.LINES (ID INTEGER, size INTEGER, line ST_GEOMETRY)"
        elif table == 'BENCHMARK.B_POINTS':
            createTable = "CREATE COLUMN TABLE BENCHMARK.B_POINTS (ID INTEGER, X FLOAT, Y FLOAT, POINT ST_POINT)"
        self.cursor.execute(createTable)
        print("\tCreated Table")

    def insertPolygons(self, polygons, offset):
        for i, polygon in enumerate(polygons):
            size = len(polygon)
            insert = '''INSERT INTO BENCHMARK.POLYGONS (ID, SIZE, polygon) VALUES (?, ?, New ST_POLYGON(?))'''
            # print self.polygonString(polygon)
            self.cursor.execute(insert, (i + offset, size, self.polygonString(polygon)))
            if i % 1000 == 999:
                self.reconnect()
                print "finished: " + str(i+1)
        self.reconnect()
        print("\tInserted Polygons into polygons table")

    def insertLines(self, lines, offset):
        for i, line in enumerate(lines):
            size = len(line)
            insert = '''INSERT INTO BENCHMARK.LINES (ID, SIZE, line) VALUES (?, ?, New ST_LINESTRING(?))'''
            self.cursor.execute(insert, (i + offset, size, self.lineString(line)))
            if i % 1000 == 999:
                self.reconnect()
                print "finished: " + str(i+1)
        self.reconnect()
        print("\tInserted Lines into lines table")

    def removePolygons(self, size):
        query = "DELETE FROM BENCHMARK.POLYGONS WHERE SIZE = " + str(size)
        self.cursor.execute(query)

    def insertPoints(self, points):
        for i, point in enumerate(points):
            # print self.pointString(point)
            insert = '''INSERT INTO BENCHMARK.B_POINTS (ID, X, Y, POINT) VALUES (?, ?, ?, NEW ST_POINT(?))'''
            self.cursor.execute(insert, (i, point['x'], point['y'], self.pointString(point)))
            # self.cursor.execute(insert, (i, point['x'], point['y'], 'POINT(1 2)'))
            if i % 1000 == 999:
                print "finished: " + str(i+1)
        print("\tInserted Points into Points table")

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
            except Exception as e:
                print "Error during execution of query"
                print query
                print e
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