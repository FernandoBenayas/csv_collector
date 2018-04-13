import ConfigParser
from subprocess import Popen
from subprocess import 	PIPE
from os import chmod
from time import sleep
import stat
import trimmer

# -*- coding: utf-8 -*-
#~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*
#
# Copyright (c) 2018  Fernando Benayas  <ferbenayas94@gmail.com>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the GNU Public License v2.0
# which accompanies this distribution, and is available at
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.html.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
#~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*
#title           : collector.py
#date created    : 12/01/2018
#python_version  : 3.5.1
#notes           :
__author__ = "Fernando Benayas"
__license__ = "GPLv2"
__version__ = "0.1.0"
__maintainer__ = "Fernando Benayas"
__email__ = "ferbenayas94@gmail.com"

"""This program can change the license header inside files.
"""
#~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*

"""
Collector script. Collects data from elasticsarch.

"""

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

	trimmer.start()
