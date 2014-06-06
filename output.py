import time

def printMySQL(data):

	f = open('results/MySQL'+time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Performance of MySQL:\n\n\n')
	f.write('all times in milliseconds\n')
	for query in data:
		result = data[query]
		f.write(query + '\n')
		f.write('avg: ' + str(1000 * result['avg']) + '\n')
		f.write('values:\n')
		for value in result['times']:
			f.write(str(1000 * value) + '\n')
		f.write('\n\n')

	f.close()


def printPostgis(data):

	f = open('results/Postgis'+time.strftime("%Y-%m-%d")+'-'+time.strftime("%H:%M:%S")+'.txt', 'w')

	f.write('Performance of Postgis:\n\n\n')
	f.write('all times in milliseconds\n')
	for query in data:
		result = data[query]
		f.write(query + '\n')
		f.write('avg: ' + str(result['avg']) + '\n')
		f.write('values:\n')
		for value in result['times']:
			f.write(str(value) + '\n')
		f.write('\n\n')

	f.close()