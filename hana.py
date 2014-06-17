import jaydebeapi
import time
import hanaCredentials
# import dbapi

class Hana:

    def __init__(self):
        self.HOST = hanaCredentials.host()
        self.PASSWORD = hanaCredentials.pw()
        self.PORT = hanaCredentials.port()
        self.USER = hanaCredentials.user()
        # self.con = self.connect()
        # self.cursor = self.con.cursor()

    def connect(self):
        url = 'jdbc:sap://%s:%s' %(self.HOST, self.PORT)
        return jaydebeapi.connect(
            'com.sap.db.jdbc.Driver',
            [url, self.USER, self.PASSWORD],
            'ngdbc.jar')

    def disconnect(self):
        self.con.close()

    def runQueries(self, queries, numberOfExecutions):
        results = {'database': 'hana', 'queries': list()}
        for query in queries:
            queryObject = {'name': query, 'times': list(), 'avg': 0}
            for x in range(0, numberOfExecutions):
                # print query
                self.con = self.connect()
                self.cursor = self.con.cursor()
                startTime = time.clock()
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                executionTime = 1000 * (time.clock() - startTime) # milliseconds
                self.disconnect()
                print "Execution time:" + str(executionTime)
                for row in result:
                    print row[0]
                queryObject['times'].append(executionTime)

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

    def runTest(self):
        query = "SELECT * FROM BENCHMARK.POINTS limit 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        for r in result:
            print r[0]