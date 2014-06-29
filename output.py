import time

def printSingleResult(data):
	f = open('results/'+ data['database'] + time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Performance of ' + data['database'] + ':\n\n\n')
	f.write('all times in milliseconds\n')
	for query in data['queries']:
		f.write(query['name'] + '\n')
		f.write('avg: ' + str(query['avg']) + '\n')
		if query.has_key('times'):
			f.write('values:\n')
			for value in query['times']:
				f.write(str(value) + '\n')
		elif query.has_key('executions'):
			f.write('Number of executions: ' + str(query['executions']) + '\n')

		f.write('\n')

	f.close()


def printSummary(datas):
	f = open('results/Summary'+time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Summary of all measurements:\n\n\n')

	for data in datas:
		f.write('Performance of ' + data['database'] + ':\n\n')
		if data.has_key('polygonSize'):
			f.write('PolygonSize: ' + str(data['polygonSize']) + '\n')
		f.write('all times in milliseconds\n')
		for query in data['queries']:
			f.write(query['name'] + '\n')
			f.write('avg: ' + str(query['avg']) + '\n\n')
		f.write('\n')

	f.close()

def printSoccerSummary(datas):
	f = open('results/SoccerSummary'+time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Summary of all soccer measurements:\n\n\n')

	for data in datas:
		f.write('Performance of ' + data['database'] + ':\n')
		f.write('all times in milliseconds\n')
		for query in data['queries']:
			f.write(query['name'] + '\t' + str(query['avg']) + '\n')
		f.write('\n')

	f.close()
