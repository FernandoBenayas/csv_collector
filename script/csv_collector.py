import ConfigParser
from subprocess import Popen
from os import chmod
from time import sleep
import stat

def get_indices(es_url):

	with open("list_indices", "wb") as out, open("log_indices", "wb") as err:
		Popen(["curl", "http://" + es_url + "/_cat/indices?v"], stdout = out, stderr = err)

	print "Downloaded list of indices"
	
	out.close()
	err.close()
	
	sleep(1)

	chmod("list_indices", stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
	chmod("log_indices", stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)

	print "Changed files permissions"
	
	text_indices = open("list_indices", "r")
	response = text_indices.read()
	text_indices.close()

	array_response = response.split(' ')
	
	print "Getting simulation indices"

	for words in array_response[:]:
		if words.find("simulation") == -1:
			array_response.remove(words)

	return array_response


if __name__ == '__main__':

	config = ConfigParser.ConfigParser()
	config.readfp(open('/home/script/config', 'r'))
	es_url = str(config.get('main', 'elasticsearch_url'))

	index_list = get_indices(es_url)

	print "Getting data - It's going to take a while!"

	for index in index_list:
		print "Converting index {}".format(index)
		Popen(["es2csv", "-q", "_type:node", "-i", index, "-o", "../csv/" + index + + "_node" + ".csv"])
		Popen(["es2csv", "-q", "_type:sim_state", "-i", index, "-o", "../csv/" + index + + "_simstate" + ".csv"])
		Popen(["es2csv", "-q", "_type:topology", "-i", index, "-o", "../csv/" + index + + "_topology" + ".csv"])