import time

def printSingleResult(data):
	f = open('results/'+ data['database'] + time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Performance of ' + data['database'] + ':\n\n\n')
	f.write('all times in milliseconds\n')
	for query in data['queries']:
		result = data['queries'][query]
		f.write(query + '\n')
		f.write('avg: ' + str(result['avg']) + '\n')
		f.write('values:\n')
		for value in result['times']:
			f.write(str(value) + '\n')
		f.write('\n\n')

	f.close()


def printSummary(datas):
	f = open('results/Summary'+time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Summary of all measurements:\n\n\n')

	for data in datas:
		f.write('Performance of ' + data['database'] + ':\n\n\n')
		f.write('all times in milliseconds\n')
		for query in data['queries']:
			result = data['queries'][query]
			f.write(query + '\n')
			f.write('avg: ' + str(result['avg']) + '\n')
			f.write('values:\n')
			for value in result['times']:
				f.write(str(value) + '\n')
			f.write('\n\n\n')

	f.close()
