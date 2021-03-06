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
#title           : trace.py
#date created    : 24/03/2018
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
Script that applies a time window.

"""

INDEX_NEAREST = {}

def datetimefy(timestamp):

	list_timestamp = timestamp.split("T")
	date_list = list_timestamp[0].split("-")
	time_list = list_timestamp[1].split(":")
	seconds_list = time_list[2].split(".")
	copy = seconds_list[1]
	seconds_list[1] = int(copy.replace("Z", ""))/1000

 	return dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))

# Adds a field containing the index of the nearest entry of the same
# switch at the node csv. If a time window is given, it returns a dictionary
# of entries that match the time window
def add_nearest(df, df_buffer, timeWindow = 0):

	global INDEX_NEAREST
	print "Storing nearest entries"

	for index, row in df[['id', '@timestamp']].iterrows():
		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'index': 'First', 'delta': dt.timedelta.max.total_seconds()}
		is_buffer = False
		df_trimmed = df.ix[df.id == str(row['id'])]
		buffer_trimmed = df_buffer.ix[df_buffer.id == str(row['id'])]
		# Checking for the closest entry of the same switch in the
		# current dataframe and the buffer
		if timeWindow == 0:
			for index2, row2 in df_trimmed.iloc[0:index].iterrows():
				state_datetime = datetimefy(str(row2['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < min_diff['delta']:
					min_diff['index'] = str(index2)
					min_diff['delta'] = diff
			for index_buffer, row_buffer  in buffer_trimmed[['id', '@timestamp']].iterrows():
				state_datetime = datetimefy(str(row_buffer['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < min_diff['delta']:
					min_diff['index'] = str(index_buffer)
					min_diff['delta'] = diff
					is_buffer = True
			INDEX_NEAREST[index] = {min_diff['index']: is_buffer}
		else:
			timewindow_dictionary = OrderedDict()
			for index2, row2 in df_trimmed.iloc[index::-1].iterrows():
				state_datetime = datetimefy(str(row2['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < timeWindow:
					timewindow_dictionary[str(index2)] = False
			for index_buffer, row_buffer  in buffer_trimmed[['id', '@timestamp']].iloc[::-1].iterrows():
				state_datetime = datetimefy(str(row_buffer['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < timeWindow:
					timewindow_dictionary[str(index_buffer)] = True
			if not timewindow_dictionary:
				INDEX_NEAREST[index] = {'First': False}
			else:
				INDEX_NEAREST[index] = timewindow_dictionary
	return

def trace_changes(df, df_buffer):

	# flow_list = ['changed_output', 'changed_priority', 'changed_inport', 'not_dropping_lldp', 'changed_order']
	flow_list = ['changed_output', 'changed_priority', 'changed_inport', 'not_dropping_lldp']

	for index, row in df[flow_list].iloc[::-1].iterrows():
		print "		Processing row %s" % index

		pastreport_dict = INDEX_NEAREST.get(index)
		indexlist = pastreport_dict.keys()
		nearestreport = indexlist[0]

		if row['changed_output'] == 'True':
			df.at[index, 'changed_output'] = 'True'
		else:
			if nearestreport != 'First':
				has_changed = False
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.loc[[int(i)]]
					else:
						row2 = df_buffer.loc[[int(i)]]
					if row2['changed_output'].item() != 'True':
						continue
					else:
						has_changed = True
						break
				df.at[index, 'changed_output'] = str(has_changed)
			else:
				df.at[index, 'changed_output'] = 'First'

		
		if row['changed_priority'] == 'True':
			df.at[index, 'changed_priority'] = 'True'
		else:
			if nearestreport != 'First':
				has_changed = False
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.loc[[int(i)]]
					else:
						row2 = df_buffer.loc[[int(i)]]
					if row2['changed_priority'].item() != 'True':
						continue
					else:
						has_changed = True
						break
				df.at[index, 'changed_priority'] = str(has_changed)
			else:
				df.at[index, 'changed_priority'] = 'First'
		

		if row['changed_inport'] == 'True':
			df.at[index, 'changed_inport'] = 'True'
		else:
			if nearestreport != 'First':
				has_changed = False
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.loc[[int(i)]]
					else:
						row2 = df_buffer.loc[[int(i)]]
					if row2['changed_inport'].item() != 'True':
						continue
					else:
						has_changed = True
						break
				df.at[index, 'changed_inport'] = str(has_changed)
			else:
				df.at[index, 'changed_inport'] = 'First'
 
		if row['not_dropping_lldp'] == False:
			df.at[index, 'not_dropping_lldp'] = False
		else:
			if nearestreport != 'First':
				has_changed = False
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.loc[[int(i)]]
					else:
						row2 = df_buffer.loc[[int(i)]]
					if row2['not_dropping_lldp'].item() != False:
						continue
					else:
						has_changed = True
						break
				df.at[index, 'not_dropping_lldp'] = not has_changed
			else:
				df.at[index, 'not_dropping_lldp'] = 'First'
		#
		# if row['changed_order'] == 'True':
		# 	df.at[index, 'changed_order'] = 'True'
		# else:
		# 	if nearestreport != 'First':
		# 		has_changed = False
		# 		for i in indexlist:
		# 			if pastreport_dict[i] == False:
		# 				row2 = df.loc[[int(i)]]
		# 			else:
		# 				row2 = df_buffer.loc[[int(i)]]
		# 			if row2['changed_order'].item() != 'True':
		# 				continue
		# 			else:
		# 				has_changed = True
		# 				break
		# 		df.at[index, 'changed_order'] = str(has_changed)
		# 	else:
		# 		df.at[index, 'changed_order'] = 'First'

	df.drop(['id', '@timestamp'], axis=1, inplace=True)
	return


def start():

	config = ConfigParser.ConfigParser()
	config.readfp(open('/root/script/config', 'r'))
	training = str(config.get('main', 'training'))
	bufferTimeWindow = dt.timedelta(seconds=int(config.get('main', 'buffer_time_window'))).total_seconds()
	timeWindow = dt.timedelta(seconds=int(config.get('main', 'time_window'))).total_seconds()

	os.chdir('/root/csv')
	csv_dict = {}
	for file in glob.glob('*_node_modified.csv'):
		sim_id = file.replace("_node_modified.csv", "")
		csv_dict[sim_id] = ["/root/csv/" + str(sim_id) + "_node_modified.csv"]

	for sim_id in csv_dict:
		node_data = NodeDataChunks(csv_dict[sim_id][0], sim_id, bufferTimeWindow)

		for index, df in enumerate(node_data.shards):
			add_nearest(df, node_data.buffers[index], timeWindow)
			trace_changes(df, node_data.buffers[index])

		node_data.join()

	return