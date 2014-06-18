import jaydebeapi
import hanaCredentials
import re

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

    def runQueries(self, queries, numberOfExecutions):
        clearPlanCacheSQL = "ALTER SYSTEM CLEAR SQL PLAN CACHE"
        self.cursor.execute(clearPlanCacheSQL)
        results = {'database': 'hana', 'queries': list()}
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

        return results

    def runTest(self):
        query = "SELECT * FROM BENCHMARK.POINTS limit 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        for r in result:
            print r[0]