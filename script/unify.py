import pandas as pd
import ConfigParser
import datetime as dt
import os
import glob
import sys
import ConfigParser
import math
import shutil
from collections import OrderedDict
from nodedatachunks import NodeDataChunks


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
#title           : unify.py
#date created    : 23/03/2018
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
Unifies all fields related to flow change.

"""

def unify(df):

	flow_list = ['changed_output','changed_priority','changed_inport']

	df["changed_flow"] = "none"
	
	for index, row in df[flow_list].iloc[::-1].iterrows():
		print "		Processing row %s" % index

		if row['changed_output'] == 'True' or row['changed_inport'] == 'True' or row['changed_priority'] == 'True':
			df.at[index, 'changed_flow'] = 'True'
		elif row['changed_inport'] == 'First':
			df.at[index, 'changed_flow'] = 'True'
		else:
			df.at[index, 'changed_flow'] = 'False'
	return

def final_trimmer(df):

	df.drop('changed_priority', axis=1, inplace=True)
	df.drop('changed_inport', axis=1, inplace=True)
	df.rename(columns={'changed_output': 'changed_flow'}, inplace=True)

	return

#CHANGE TO START
if __name__ == '__main__':


	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	training = str(config.get('main', 'training'))
	bufferTimeWindow = dt.timedelta(seconds=int(config.get('main', 'buffer_time_window'))).total_seconds()

	os.chdir('/root/csv')
	csv_dict = {}
	for file in glob.glob('*_node_modified_modified.csv'):
		sim_id = file.replace("_node_modified_modified.csv", "")
		csv_dict[sim_id] = ["/root/csv/" + str(sim_id) + "_node_modified_modified.csv"]

	for sim_id in csv_dict:
		node_data = NodeDataChunks(csv_dict[sim_id][0], sim_id, bufferTimeWindow)

		for index, df in enumerate(node_data.shards):
			unify(df)
			#final_trimmer(df)

		node_data.join()
