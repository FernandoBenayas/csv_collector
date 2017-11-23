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

def datetimefy(timestamp):

	list_timestamp = timestamp.split("T")
	date_list = list_timestamp[0].split("-")
	time_list = list_timestamp[1].split(":")
	seconds_list = time_list[2].split(".")
	copy = seconds_list[1]
	seconds_list[1] = int(copy.replace("Z", ""))/1000

 	return dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))

def limit_size(sim_list, sim_id):
	global SIM_SIZE
	global NUMBER_OF_SPLITS
	#Dictionary of node csvs; each key will be the original node csv, and each value will be 
	#a list of the files in which the original csv has been splitted
	nodes_dict = {}
	#Same for topo csvs dictionary
	topo_dict = {}
	#Time window for packet average 
	os.chdir('/root/script')
	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	timewindow = dt.timedelta(seconds=int(config.get('main', 'buffer_time_window'))).total_seconds()

	for csv in sim_list:
		print 'csv'
		print csv
		df = pd.read_csv(str(csv))

		#Searching for the size of the simulation
		if 'node' in csv:
			df_size = df[["id"]]
			for row in df_size.iterrows():
				of_number = int(row[1][0].split("w")[1])
				if of_number > SIM_SIZE:
					SIM_SIZE = of_number
			del df_size

		#Splitting into 5000 rows dataframes
		csv_length = df.shape[0]
		print 'Number of rows: %s' % csv_length
		print 'Splitting csv...'
		shards = []
		NUMBER_OF_SPLITS = int(csv_length / 5000)
		print NUMBER_OF_SPLITS
		if csv_length == 5000:
			NUMBER_OF_SPLITS = 0
		for i in range(NUMBER_OF_SPLITS+1):
			new_df = df[5000*i:5000*(i+1)]
			buffer_df = create_buffer(df, 5000*i, timewindow)
			csv_name = str(csv).replace('.csv', '_'+str(i)+'.csv').replace('/csv', '/csv/shards')
			print csv_name
			csv_buffer = str(csv).replace('.csv', '_'+str(i)+'_buffer'+'.csv').replace('/csv', '/csv/shards')
			shards.append(csv_name)
			new_df.to_csv(csv_name, index=False)
			buffer_df.to_csv(csv_buffer, index=False)
		if 'node' in csv:
			nodes_dict[str(csv)] = shards
		elif 'topo' in csv:
			topo_dict[str(csv)] = shards
			del new_df
			del buffer_df

		#Releasing memory
		del df
		#sys.exit()

	return (nodes_dict, topo_dict)

def create_buffer(dataframe, breaking_point, timewindow):
	global SIM_SIZE

	if breaking_point == 0:
		return dataframe[-SIM_SIZE:0]

	first_entry_time = datetimefy(dataframe.iloc[breaking_point]['@timestamp'])
	comparate_time = datetimefy(dataframe.iloc[breaking_point-SIM_SIZE]['@timestamp'])
	j = 1
	while (first_entry_time - comparate_time).total_seconds() < timewindow:
		j += 1
		comparate_time = datetimefy(dataframe.iloc[breaking_point-SIM_SIZE*j]['@timestamp'])
	buffer_dataframe = dataframe[breaking_point-SIM_SIZE*(j+1):breaking_point]

	return buffer_dataframe

# Adds a field containing the index of the nearest entry of the same
# switch at the node csv. If a time window is given, it returns a dictionary
# of entries that match the time window
def add_nearest2(sim_csv, sim_id, timewindow = 0):
	df_node = pd.read_csv(str(sim_csv))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	global INDEX_NEAREST
	#INDEX_NEAREST = {}


	for index, row in df_node[['id', '@timestamp']].iterrows():
		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'index': 'First', 'delta': dt.timedelta.max.total_seconds()}
		is_buffer = False
		df_trimmed = df_node.ix[df_node.id == str(row['id'])]
		buffer_trimmed = buffer_df.ix[buffer_df.id == str(row['id'])]
		# Checking for the closest entry of the same switch in the
		# current dataframe and the buffer
		if timewindow == 0:
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
			INDEX_NEAREST[index] = {int(min_diff['index']): is_buffer}
		else:
			timewindow_dictionary = OrderedDict()
			for index2, row2 in df_trimmed.iloc[index::-1].iterrows():
				state_datetime = datetimefy(str(row2['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < timewindow:
					timewindow_dictionary[str(index2)] = False
			for index_buffer, row_buffer  in buffer_trimmed[['id', '@timestamp']].iloc[::-1].iterrows():
				print index_buffer
				state_datetime = datetimefy(str(row_buffer['@timestamp']))
				diff = (origin_datetime - state_datetime).total_seconds()
				if diff > 0 and diff < timewindow:
					timewindow_dictionary[str(index2)] = True
			if not timewindow_dictionary:
				INDEX_NEAREST[index] = {'First': False}
			else:
				INDEX_NEAREST[index] = timewindow_dictionary


	return

def progressive_changes(sim_csv, sim_id, buffertimewindow):
	df = pd.read_csv(str(sim_csv))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	global INDEX_NEAREST
	print INDEX_NEAREST

	flow_list = ['changed_output','changed_priority','changed_inport']
	for index, row in df[flow_list].iloc[::-1].iterrows():
		print row
		pastreport_dict = INDEX_NEAREST.get(index)
		indexlist = pastreport_dict.keys()
		nearestreport = indexlist[0]

		if row['changed_output'] == 'True':
			df.at[index, 'changed_output'] = 0
		else:
			if nearestreport != 'First':
				j = 1
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.iloc[[int(i)]]
					else:
						row2 = buffer_df.iloc[[int(i)]]
					if row2['changed_output'].item() != 'True':
						j += 1
					else:
						break
				df.at[index, 'changed_output'] = j
			else:
				df.at[index, 'changed_output'] = 'First'

		
		if row['changed_priority'] == 'True':
			df.at[index, 'changed_priority'] = 0
		else:
			if nearestreport != 'First':
				j = 1
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.iloc[[int(i)]]
					else:
						row2 = buffer_df.iloc[[int(i)]]
					if row2['changed_priority'].item() != 'True':
						j += 1
					else:
						break
				df.at[index, 'changed_priority'] = j
			else:
				df.at[index, 'changed_priority'] = 'First'
		


		if row['changed_inport'] == 'True':
			df.at[index, 'changed_inport'] = 0
		else:
			if nearestreport != 'First':
				j = 1
				for i in indexlist:
					if pastreport_dict[i] == False:
						row2 = df.iloc[[int(i)]]
					else:
						row2 = buffer_df.iloc[[int(i)]]
					if row2['changed_inport'].item() != 'True':
						j += 1
					else:
						break
				df.at[index, 'changed_inport'] = j
			else:
				df.at[index, 'changed_inport'] = 'First'

	df.to_csv(str(sim_csv), index=False)
	del df
	del buffer_df
	return

def final_trimmer2(sim_csv, sim_id, training_dataset = 'False'):
	global INDEX_NEAREST
	global node_columns_list
	df = pd.read_csv(str(sim_csv)) 
	node_columns_list = df.columns.values.tolist()

	df['err_type'] = df.err_type.astype(str)
	if training_dataset == 'True':
		for index, row, in df[['id']].iterrows():
			if row['id'] != 'openflow2':
				df.drop(index, inplace=True)

	for i in range(0, len(node_columns_list) - 15):
		if node_columns_list[i] == 'id' or node_columns_list[i] == '@timestamp' or node_columns_list[i] == 'changed_priority':
			continue
		df.drop(node_columns_list[i], axis=1, inplace=True)

	df.to_csv(str(sim_csv), index=False)
	INDEX_NEAREST = {}
	del df
	return

#CHANGE TO START
if __name__ == '__main__':

	#Move this up
	NODE_NUMBER_OF_FLOWS = 0
	NUMBER_OF_SPLITS = 0
	node_columns_list = []
	INDEX_NEAREST = {}
	SIM_SIZE = 0

	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	training = str(config.get('main', 'training'))
	timewindow = int(config.get('main', 'time_window'))
	buffertimewindow = int(config.get('main', 'buffer_time_window'))
	csv_dict = {}

	files = glob.glob('/root/csv/shards/*')
	for f in files:
	    os.remove(f)

	os.chdir('/root/csv')
	for file in glob.glob('*_modified_node.csv'):
		sim_index = file.replace("_modified_node.csv", "")
		csv_dict[sim_index] = ["/root/csv/" + str(sim_index) + "_modified_node.csv", "/root/csv/" + str(sim_index) + "_simstate.csv", "/root/csv/" + str(sim_index) + "_topology.csv"]

	for sim_index in csv_dict:

		csv_splitted = limit_size(csv_dict[sim_index], sim_index)

		for key, value in csv_splitted[0].iteritems():
			for csv in value:
				add_nearest2(csv, sim_index, timewindow = timewindow)
				progressive_changes(csv, sim_index, buffertimewindow)
				final_trimmer2(csv, sim_index, training_dataset=training)

			if len(value) > 1:
				print 'Joining topo shards...'
				join_shards(value, sim_index, 'topo')

			else:
				os.rename('/root/csv/shards/'+sim_index+'_modified_node_0.csv', '/root/csv/'+sim_index+'_modified_node.csv')

