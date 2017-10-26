import ConfigParser
from subprocess import Popen
from subprocess import 	PIPE
from os import chmod
from time import sleep
import stat
import trimmer

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
	config.readfp(open('config', 'r'))
	es_url = str(config.get('main', 'elasticsearch_url'))

	index_list = get_indices(es_url)

	print "Getting data - It's going to take a while!"

	for index in index_list:
		print "Converting index {}".format(index)
		a = Popen(["es2csv", "-u", "http://"+str(es_url), "-r", "-q", "@./query_node.json", "-i", index, "-o", "../csv/" + index + "_node" + ".csv"], stdout=PIPE)
		a.communicate()
		b = Popen(["es2csv", "-u", "http://"+str(es_url), "-r", "-q", "@./query_simstate.json", "-i", index, "-o", "../csv/" + index + "_simstate" + ".csv"], stdout=PIPE)
		b.communicate()
		c = Popen(["es2csv", "-u", "http://"+str(es_url), "-r", "-q", "@./query_topo.json", "-i", index, "-o", "../csv/" + index + "_topology" + ".csv"], stdout=PIPE)
		c.communicate()

		#trimmer.start(window_time)

