import pandas as pd
import ConfigParser
import datetime as dt
import os
import glob
import sys
import ConfigParser
import math


def datetimefy(timestamp):

	list_timestamp = timestamp.split("T")
	date_list = list_timestamp[0].split("-")
	time_list = list_timestamp[1].split(":")
	seconds_list = time_list[2].split(".")
	copy = seconds_list[1]
	seconds_list[1] = int(copy.replace("Z", ""))/1000

 	return dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))


def limit_size(sim_list, sim_id):

	#Dictionary of node csvs; each key will be the original node csv, and each value will be 
	#a list of the files in which the original csv has been splitted
	nodes_dict = {}
	#Same for topo csvs dictionary
	topo_dict = {}
	#Time window for packet average 
	os.chdir('/root/script')
	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	timewindow = dt.timedelta(seconds=int(config.get('main', 'time_window'))).total_seconds()

	for csv in sim_list:
		df = pd.read_csv(str(csv))

		#Searching for the size of the simulation
		if 'node' in csv:
			df_size = df[["id"]]
			size = 0
			for row in df_size.iterrows():
				of_number = int(row[1][0].split("w")[1])
				if of_number > size:
					size = of_number
			del df_size

		#Splitting into 5000 rows dataframes
		csv_length = df.shape[0]
		print 'Number of rows: %s' % csv_length
		print 'Splitting csv...'
		shards = []
		number_of_splits = int(csv_length / 5000)
		if csv_length == 5000:
			number_of_splits = 0
		for i in range(number_of_splits+1):
			new_df = df[5000*i:5000*(i+1)]
			buffer_df = create_buffer(df, new_df, size, i, timewindow)
			csv_name = str(csv).replace('.csv', '_'+str(i)+'.csv').replace('/csv', '/csv/shards')
			csv_buffer = str(csv).replace('.csv', '_'+str(i)+'_buffer'+'.csv').replace('/csv', '/csv/shards')
			shards.append(csv_name)
			new_df.to_csv(csv_name)
			buffer_df.to_csv(csv_buffer)
		if 'node' in csv:
			nodes_dict[str(csv)] = shards
		elif 'topo' in csv:
			topo_dict[str(csv)] = shards
			del new_df
			del buffer_df

		#Releasing memory
		del df
	return (nodes_dict, topo_dict, size)

def create_buffer(dataframe, new_df, size, index_df, timewindow):
	if index_df == 0:
		return dataframe[-size:0]

	first_entry_time = datetimefy(new_df.iloc[0]['@timestamp'])
	comparate_time = datetimefy(dataframe.iloc[5000*index_df-size]['@timestamp'])
	j = 1
	while (first_entry_time - comparate_time).total_seconds() < timewindow:
		j += 1
		comparate_time = datetimefy(dataframe.iloc[5000*index_df-size*j]['@timestamp'])
	buffer_dataframe = dataframe[5000*index_df-size*(j+1):5000*index_df]

	return buffer_dataframe

def general_trimmer(sim_csv, sim_id):

	df = pd.read_csv(str(sim_csv))

	#Dropping useless data DEBUGGING: COULD USE ALL()? TOO TIME-CONSUMING
	print 'Dropping useless columns'

	columns_list = df.columns.values.tolist()
	for column in columns_list:
		if 'flow-node-inventory:table' in column and 'flow-node-inventory:table.68' not in column:
			df.drop(column, axis=1, inplace=True)
		elif 'duration' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'opendaylight-group-statistics' in column or 'opendaylight-meter-statistics' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'node-connector.' in column and (('state' not in column) and ('packets' not in column) and ('id' not in column)):
			df.drop(column, axis=1, inplace=True)
		elif 'manufacturer' in column or 'hardware' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'flow-node-inventory:ip-address' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'flow-node-inventory:switch-features' in column or 'opendaylight-group-statistics:group-features' in column:
			df.drop(column, axis=1, inplace=True)

	print 'Creating flow list'

	flow_columns_list = [s for s in columns_list if 'flow-node-inventory:table.68.flow.' in s and '.id' in s and '_table' not in s and 'time' not in s ]
	df["existence_of_flows"] = ""

	for index, row in df[flow_columns_list].iterrows():
		counter = 0
		for column in flow_columns_list:
			if "UF" not in str(row[column])	and str(row[column]) != 'nan':
				counter += 1
		if counter > 0:
			df.set_value(index, "existence_of_flows", True)
		else:
			df.set_value(index, "existence_of_flows", False)

	print 'Creating lldp list'
	type_columns_list = [s for s in columns_list if 'match.ethernet-type' in s]
	df['not_dropping_lldp'] = ""

	for index, row in df.iterrows():
		for column in type_columns_list:
			if "35020"in str(row[column]):
				column_text = str(column).split('.')
				flow_id = column_text[3]
				output_node_connector = "flow-node-inventory:table.68.flow." + str(flow_id) + ".instructions.instruction.0.apply-actions.action.0.output-action.output-node-connector"
				if str(row[output_node_connector]) != 'nan':
					df.set_value(index, "not_dropping_lldp", 'True')
					break
				else:
					df.set_value(index, "not_dropping_lldp", 'False')
					break
			else:
				df.set_value(index, "not_dropping_lldp", 'Not LLDP flow')

	print 'Creating hard timeout field'
	h_timeout_list = [s for s in columns_list if 'hard-timeout' in s]
	df['modified_h_timeout'] = ""

	for index, row in df[h_timeout_list].iterrows():
		for column in h_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) != 300:
				df.set_value(index, "modified_h_timeout", True)
				break
			else:
				df.set_value(index, "modified_h_timeout", False)

	print 'Creating idle timeout field'

	i_timeout_list = [s for s in columns_list if 'idle-timeout' in s]
	df['modified_i_timeout'] = ""

	for index, row in df[i_timeout_list].iterrows():
		for column in i_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) <= 15:
				df.set_value(index, "modified_i_timeout", True)
				break
			else:
				df.set_value(index, "modified_i_timeout", False)

	print 'Creating node connector list'
	node_connector_list = [s for s in columns_list if 'node-connector.' in s and '.id' in s and 'address' not in s]
	df['node_connector_down'] = ""
	node_connector_counter = 0

	for index, row in df.iterrows():
		for column in node_connector_list:
			if 'LOCAL' not in str(row[column]) and str(row[column]) != 'nan':
				node_connector_counter += 1
				column = column.replace('.id', '.flow-node-inventory:state.link-down')
				if str(row[column]) != 'nan':
					if str(row[column]) != 'True':
						df.set_value(index, "node_connector_down", False)
					else:
						df.set_value(index, "node_connector_down", True)
						break
		if node_connector_counter == 0:
			df.set_value(index, "node_connector_down", 'isolated')
		node_connector_counter = 0

	df['err_type'] = "sample"
	df['action'] = "sample"
	df.to_csv(sim_csv.replace('_node', '_modified_node'))
	del df
	return

def topology_csv_trimmer(sim_csv, sim_id):

	df2 = pd.read_csv(str(sim_csv), engine='python')
	print 'Selecting ip columns in topology csv'
	columns_list = df2.columns.values.tolist()

	for column in columns_list:
		if not all(x in str(column) for x in ['host-tracker-service:addresses.', '.ip']) and '@timestamp' not in column:
			df2.drop(column, axis=1, inplace=True)

	df2['err_type'] = "sample"
	df2['action'] = "sample"
	df2.to_csv(sim_csv.replace('_topology', '_modified_topo'))

	del df2
	return

# Adds a field in the topology csv that indicates
# if there are hosts missing
def add_topology_field(sim_csv, sim_id):

	# Getting all the 'modified_topo' shards
	os.chdir('/root/csv/shards')
	topo_list = []
	for file in glob.glob('*_modified_topo*'):
		topo_list.append(file)

	first_df = pd.read_csv(str(topo_list[0]))
	node_df = pd.read_csv(str(sim_csv.replace('_node', '_modified_node')))
	columns_list = first_df.columns.values.tolist()

	ip_column_list = [s for s in columns_list if 'ip' in s]
	ip_start_list = []
	ip_current_list = []
	node_df['modified_hosts'] = False

	# Capturing the first report of hosts in the network
	# and inserting int in ip_start_list
	for column in ip_column_list:
		ip = first_df.iloc[0][column]
		if 'nan' not in str(ip):
			ip_start_list.append(ip)

	# For each row in node csv, we look for the nearest (in time)
	# topology report in all the topology shards, and compare it with the 'start' 
	# report on the topology (when no errors where started), to see if something
	# is missing
	for index, row in node_df[['@timestamp']].iterrows():
		node_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': dt.timedelta.max.total_seconds()}
		for topo_csv in topo_list:
			df = pd.read_csv(str(topo_csv))
			for index2, row2 in df.iterrows():
				topo_datetime = datetimefy(str(row2['@timestamp']))
				diff = (node_datetime - topo_datetime).total_seconds()
				if diff >= 0 and diff < min_diff['delta']:
					min_diff['delta'] = diff
					min_diff['row'] = row2
		if isinstance(min_diff['row'], basestring):
			node_df.set_value(index, 'modified_hosts', 'First')
			continue
		for column in ip_column_list:
			ip = min_diff['row'][column]
			if 'nan' not in str(ip):
				ip_current_list.append(ip)
		if set(ip_start_list) == set(ip_current_list):
			node_df.set_value(index, 'modified_hosts', False)
		else:
			node_df.set_value(index, 'modified_hosts', True)
		ip_current_list = []

	node_df.to_csv(sim_csv.replace('_node', '_modified_node'), index=False)
	return
	
def time_sync_node(sim_csv, sim_id):

	df_state = pd.read_csv('/root/csv/' + sim_id + '_simstate.csv')
	df_node = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	columns_list_state = df_state.columns.values.tolist()
	columns_list_node = df_node.columns.values.tolist()

	for index, row in df_node[['@timestamp']].iterrows():
		print '		Syncing node: processing row %s' % index
		node_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

		df2_state = df_state[['@timestamp', 'err.err_type', 'action']]
		for index2, row2 in df2_state.iterrows():
			state_datetime = datetimefy(str(row2['@timestamp']))
			diff = (node_datetime - state_datetime).total_seconds()

			if diff >= 0 and diff < min_diff['delta']:
				min_diff['delta'] = diff
				if "start" in str(row2['action']):
					min_diff['err'] = '-'
				else:
					min_diff['err'] = str(row2['err.err_type'])
				min_diff['action'] = str(row2['action'])

		df_node.set_value(index, 'err_type', str(min_diff['err']))
		df_node.set_value(index, 'action', str(min_diff['action']))

	df_node.to_csv(sim_csv.replace('_node', '_modified_node'), index=False)
	del df_node
	del df_state
	del df2_state
	return

def time_sync_topo(sim_csv, sim_id):

	df_state = pd.read_csv('/root/csv/' + sim_id + '_simstate.csv')
	df_topo = pd.read_csv(str(sim_csv.replace('_topology', '_modified_topo')))
	columns_list_state = df_state.columns.values.tolist()
	columns_list_topo = df_topo.columns.values.tolist()

	for index, row in df_topo[['@timestamp']].iterrows():
		print '		Syncing topo: processing row %s' % index
		topo_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

		df2_state = df_state[['@timestamp', 'err.err_type', 'action']]
		for index2, row2 in df2_state.iterrows():
			state_datetime = datetimefy(str(row2['@timestamp']))
			diff = (topo_datetime - state_datetime).total_seconds()

			if diff >= 0 and diff < min_diff['delta']:
				min_diff['delta'] = diff
				if "start" in str(row2['action']):
					min_diff['err'] = '-'
				else:
					min_diff['err'] = str(row2['err.err_type'])
				min_diff['action'] = str(row2['action'])

		df_topo.set_value(index, 'err_type', str(min_diff['err']))
		df_topo.set_value(index, 'action', str(min_diff['action']))

	df_topo.to_csv(sim_csv.replace('_topology', '_modified_topo'), index=False)
	del df_topo
	del df_state
	del df2_state
	return

# Checks if the in-port fields in the match rules were modified
def modified_inport_columns(sim_csv, sim_id):

	df3 = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	columns_list = df3.columns.values.tolist()
	df3["changed_inport"] = "sample"
	# Even when no changes have been made, after a change we keep
	# marking the field as 'True' until a fix happens. This is
	# done using keep_true_bit.
	keep_true_bit = 0

	# Looking for the number of flows in the csv
	flow_list = [s for s in columns_list if 'flow-node-inventory:table.68.flow.' in s]
	number_of_flows = 0
	for column in flow_list:
		flow_number = int(column.split('.')[3])
		if flow_number > number_of_flows:
			number_of_flows = flow_number

	# For each row (switch), we create a {flow : list-of-inports} dictionary. Then, we look
	# for the closest previous entry of that switch, create another dictionary, and compare
	# them
	flow_list.extend(['@timestamp', 'id'])
	for index, row in df3[flow_list].iterrows():
		print '		Checking in-ports: processing row %s' % index
		in_port_dictionary = {}
		for i in range(number_of_flows + 1):
			try:
				in_port = str(row['flow-node-inventory:table.68.flow.' + str(i) + '.match.in-port'])
				in_port_dictionary[str(row['flow-node-inventory:table.68.flow.' + str(i) + '.id'])] = in_port
			except KeyError as err:
				print 'Ignoring flow: %s' % err

		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': dt.timedelta.max.total_seconds()}

		# Getting all rows of the same switch in the csv
		df4 = df3.ix[df3.id == str(row['id'])]
		df4buffer = buffer_df.ix[buffer_df.id == str(row['id'])]

		# Checking for the closest entry of the same switch in the
		# current dataframe and the buffer
		for index2, row2, in df4[flow_list].iterrows():
			state_datetime = datetimefy(str(row2['@timestamp']))
			diff = (origin_datetime - state_datetime).total_seconds()
			if diff > 0 and diff < min_diff['delta']:
				min_diff['row'] = row2
				min_diff['delta'] = diff

		for index_buffer, row_buffer  in df4buffer[flow_list].iterrows():
			state_datetime = datetimefy(str(row_buffer['@timestamp']))
			diff = (origin_datetime - state_datetime).total_seconds()
			if diff > 0 and diff < min_diff['delta']:
				min_diff['row'] = row_buffer
				min_diff['delta'] = diff

		# Once we have the closest entry (if there is one), we create a 'b' dictionary,
		# insert in it the flow : list-of-inports of that entry, and compare dictionaries
		if str(min_diff['row']) == 'sample':
			df3.set_value(index, 'changed_inport', 'First')
		else:
			in_port_dictionary_b = {}
			row2 = min_diff['row']
			for i in range(number_of_flows + 1):
				try:
					in_port = str(row2['flow-node-inventory:table.68.flow.' + str(i) + '.match.in-port'])
					#DEBUGGING : row or row2?
					in_port_dictionary_b[str(row2['flow-node-inventory:table.68.flow.' + str(i) + '.id'])] = in_port
				except KeyError as err:
					print "Ignoring flow: %s" % err
			for key in in_port_dictionary:
				try:
					if in_port_dictionary[key] != in_port_dictionary_b[key]:
						df3.set_value(index, 'changed_inport', 'True')
						keep_true_bit = keep_true_bit ^ 1
						break
					else:
						if keep_true_bit == 1:
							df3.set_value(index, 'changed_inport', 'True')
						else:
							df3.set_value(index, 'changed_inport', 'False')
				except KeyError as err:
					print '		Flow unpaired: %s' % key
					continue
	df3.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df3
	del df4
	del buffer_df
	return

# Returns the number of packets send and received in the
# timeslot between two entries of the same switch
def packets_delta(sim_csv, sim_id):
	df5 = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	columns_list = df5.columns.values.tolist()

	df5["packets_transmitted"] = 0
	df5["packets_received"] = 0
	df5["transmitted_delta"] = '0'
	df5["received_delta"] = '0'
	buffer_df["packets_transmitted"] = 0
	buffer_df["packets_received"] = 0

	# Adding all the packages transmited and received, and inserting
	# the sum in the table
	packets_column_list = [s for s in columns_list if 'packets' in s]
	for index, row in df5[packets_column_list].iterrows():
		packets_received = 0
		packets_transmitted = 0
		for column in columns_list:
			if 'node-connector' in column and 'packets.received' in column:
				if str(row[column]) != 'nan':
					packets_received += int(row[column])
			elif 'node-connector' in column and 'packets.transmitted' in column:
				if str(row[column]) != 'nan':
					packets_transmitted += int(row[column])
		df5.set_value(index, 'packets_transmitted', packets_transmitted)
		df5.set_value(index, 'packets_received', packets_received)

	# Doing the same for the buffer
	for index, row in buffer_df[packets_column_list].iterrows():
		packets_received = 0
		packets_transmitted = 0
		for column in columns_list:
			if 'node-connector' in column and 'packets.received' in column:
				if str(row[column]) != 'nan':
					packets_received += int(row[column])
			elif 'node-connector' in column and 'packets.transmitted' in column:
				if str(row[column]) != 'nan':
					packets_transmitted += int(row[column])
		buffer_df.set_value(index, 'packets_transmitted', packets_transmitted)
		buffer_df.set_value(index, 'packets_received', packets_received)
	buffer_df.to_csv(str(sim_csv).replace('.csv', '_buffer.csv'), index=False)

	# Now, we find the closest previous entry of each row (switch entry),
	# compare the packets_transmitted and received fields, and
	# calculate the differences, inserting them into the csv
	packets_column_list.extend(['id','@timestamp', 'packets_transmitted', 'packets_received'])
	for index, row in df5[packets_column_list].iterrows():
		print '		Counting and comparing packets: processing row %s' % index
		end_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': sys.maxint }

		df6 = df5.ix[df5.id == str(row['id'])]
		df6buffer = buffer_df.ix[buffer_df.id == str(row['id'])]

		for index2, row2 in df6[packets_column_list].iterrows():
			origin_datetime = datetimefy(str(row2['@timestamp']))
			diff = (end_datetime - origin_datetime).total_seconds()
			if diff > 0 and min_diff['delta'] > diff:
				min_diff['row'] = row2
				min_diff['delta'] = diff

		for index_buffer, row_buffer in df6buffer[packets_column_list].iterrows():
			origin_datetime = datetimefy(str(row_buffer['@timestamp']))
			diff = (end_datetime - origin_datetime).total_seconds()
			if diff > 0 and min_diff['delta'] > diff:
				min_diff['row'] = row_buffer
				min_diff['delta'] = diff

		if str(min_diff['row']) == 'sample':
			df5.set_value(index, 'transmitted_delta', 'First')
			df5.set_value(index, 'received_delta', 'First')
		else:
			transmitted_delta = int(row["packets_transmitted"]) - int(min_diff['row']["packets_transmitted"])
			received_delta = int(row["packets_received"]) - int(min_diff['row']["packets_received"])
			df5.set_value(index, 'transmitted_delta', str(transmitted_delta))
			df5.set_value(index, 'received_delta', str(received_delta))

	df5.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df5
	del df6
	return

# Same as packets_delta, but for the buffer
def packets_delta_buffer(sim_csv, sim_id):
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	columns_list = buffer_df.columns.values.tolist()
	buffer_df["transmitted_delta"] = '0'
	buffer_df["received_delta"] = '0'

	packets_column_list = [s for s in columns_list if 'packets' in s]
	packets_column_list.extend(['id', '@timestamp'])
	for index, row in buffer_df[packets_column_list].iterrows():
		# Getting the timestamp of the current row (switch)
		end_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': sys.maxint }
		# Getting the rows from the same switch
		df = buffer_df.ix[buffer_df.id == str(row['id'])]
		# Iterating over them, and selecting the previous entrance 
		for index2, row2 in df[packets_column_list].iterrows():
			origin_datetime = datetimefy(str(row2['@timestamp']))
			diff = (end_datetime - origin_datetime).total_seconds()
			if diff > 0 and min_diff['delta'] > diff:
				min_diff['row'] = row2
				min_diff['delta'] = diff
		del df
		# Comparating the current row and the previous entrance
		if str(min_diff['row']) == 'sample':
			buffer_df.set_value(index, 'transmitted_delta', 'First')
			buffer_df.set_value(index, 'received_delta', 'First')
		else:
			transmitted_delta = int(row["packets_transmitted"]) - int(min_diff['row']["packets_transmitted"])
			received_delta = int(row["packets_received"]) - int(min_diff['row']["packets_received"])
			buffer_df.set_value(index, 'transmitted_delta', str(transmitted_delta))
			buffer_df.set_value(index, 'received_delta', str(received_delta))

	buffer_df.to_csv(str(sim_csv).replace('.csv', '_buffer.csv'), index=False)
	del buffer_df
	return

# Calculating average and standard deviation over 'transmitted_delta'
# and 'received_delta' given a timeslot
def packets_average(sim_csv, sim_id, window_time = 30):

	df1 = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	buffer_df1 = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	df1["transmitted_average"] = '0'
	df1["received_average"] = '0'
	df1["transmitted_deviation"] = '0'
	df1["received_deviation"] = '0'

	packets_list = ([],[]) # Hey, look, an owl in a python!
	deviation_list = ([],[]) #Look, another one
	transmitted_packets_count = 0
	received_packets_count = 0
	count = 0

	# Iterating over the list of nodes
	buffer_columns_list = ['id', '@timestamp', 'transmitted_delta', 'received_delta']
	columns_list = ['id', '@timestamp', 'transmitted_delta', 'received_delta', 'transmitted_deviation', 'received_deviation']
	for index, row in df1[columns_list].iterrows():
		print '		Calculating averages and deviations: processing row %s' % index
		# Getting the timestamp of the current row (switch)
		orig_time = datetimefy(str(row['@timestamp']))
		# Getting the rows of the same switch and storing them in ad hoc dataframes
		row_id_df = df1.ix[df1.id == str(row['id'])]
		row_id_bufferdf = buffer_df1.ix[buffer_df1.id == str(row['id'])]
		# Getting all the received and transmitted deltas of previous entrances of the 
		# same switch, and storing them in packets_list
		for index2, row2 in row_id_df[columns_list].iterrows():
			time = datetimefy(str(row2['@timestamp']))
			if 0 <= (orig_time-time).total_seconds() < window_time:
				if row2['transmitted_delta'] != 'First':
					count += 1
					transmitted_packets_count += int(row2['transmitted_delta'])
					received_packets_count += int(row2['received_delta'])
					packets_list[0].append(int(row2['transmitted_delta']))
					packets_list[1].append(int(row2['received_delta']))
		for index2, row2 in row_id_bufferdf[buffer_columns_list].iterrows():
			time = datetimefy(str(row2['@timestamp']))
			if 0 <= (orig_time-time).total_seconds() < window_time:
				if row2['transmitted_delta'] != 'First':
					count += 1
					transmitted_packets_count += int(row2['transmitted_delta'])
					received_packets_count += int(row2['received_delta'])
					packets_list[0].append(int(row2['transmitted_delta']))
					packets_list[1].append(int(row2['received_delta']))
		# If there isn't any previous entrances of this switch
		if count == 0:
			df1.set_value(index, 'transmitted_average', 'First')
			df1.set_value(index, 'received_average', 'First')
			df1.set_value(index, 'transmitted_deviation', 'First')
			df1.set_value(index, 'received_deviation', 'First')
		else:
			# Processing averages and standard deviations
			transmitted_average = transmitted_packets_count/count
			received_average = received_packets_count/count
			df1.set_value(index, 'transmitted_average', str(transmitted_average))
			df1.set_value(index, 'received_average', str(received_average))
			for i in range(len(packets_list[0])):
				deviation_list[0].append((packets_list[0][i] - transmitted_average) ** 2)
				deviation_list[1].append((packets_list[1][i] - received_average) ** 2)
			df1.set_value(index, 'transmitted_deviation', str(math.sqrt(sum(deviation_list[0])/count)))
			df1.set_value(index, 'received_deviation', str(math.sqrt(sum(deviation_list[1])/count)))

		transmitted_packets_count = 0
		received_packets_count = 0
		count = 0
		packets_list = ([],[])
		deviation_list = ([],[])

	df1.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df1
	return

# Checks if the 'output-node-connector' field in each flow has been modified
def modified_output_columns(sim_csv, sim_id):

	df3 = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	columns_list = df3.columns.values.tolist()
	# Even when no changes have been made, after a change we keep
	# marking the field as 'True' until a fix happens. This is
	# done using keep_true_bit.
	keep_true_bit = 0

	df3["changed_output"] = "sample"
	flows_and_actions = {}

	flow_columns_list = [s for s in columns_list if 'flow-node-inventory:table.68.flow.' in s and '.id' in s and 'idle' not in s ]
	for column in flow_columns_list:
		flow_id = int(column.split('.')[3])
		action_list = [s for s in columns_list if 'flow-node-inventory:table.68.flow.' + str(flow_id) in s and 'output-action.output-node-connector' in s]
		flows_and_actions[flow_id] = len(action_list)
	# DEBUGGING
	# Limit this df3!!!
	for index, row in df3.iterrows():
		print '		Checking output node connectors: processing row %s' % index
		out_conn_dictionary = {}
		for key, value in flows_and_actions.iteritems():
			for i in range(value + 1):
				try:
					action_id = 'flow-node-inventory:table.68.flow.' + str(key) + '.instructions.instruction.0.apply-actions.action.'+ str(i) +'.output-action.output-node-connector'
					out_conn = str(row[action_id]).replace('.0', '')
					out_conn_dictionary[action_id] = out_conn
					break
				except KeyError as err:
					print 'Ignoring flow: %s' % err

		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': dt.timedelta.max.total_seconds()}

		df4 = df3.ix[df3.id == str(row['id'])]
		df4buffer = buffer_df.ix[buffer_df.id == str(row['id'])]
		for index2, row2, in df4.iterrows():
			state_datetime = datetimefy(str(row2['@timestamp']))
			diff = (origin_datetime - state_datetime).total_seconds()
			if diff > 0 and diff < min_diff['delta']:
				min_diff['row'] = row2
				min_diff['delta'] = diff

		for index_buffer, row_buffer, in df4buffer.iterrows():
			state_datetime = datetimefy(str(row_buffer['@timestamp']))
			diff = (origin_datetime - state_datetime).total_seconds()
			if diff > 0 and diff < min_diff['delta']:
				min_diff['row'] = row_buffer
				min_diff['delta'] = diff

		if str(min_diff['row']) == 'sample':
			df3.set_value(index, 'changed_output', 'First')

		else:
			out_conn_dictionary_b = {}
			row2 = min_diff['row']
			for key, value in flows_and_actions.iteritems():
				for i in range(value + 1):
					try:
						action_id = 'flow-node-inventory:table.68.flow.' + str(key) + '.instructions.instruction.0.apply-actions.action.'+ str(i) +'.output-action.output-node-connector'
						out_conn = str(row2[action_id]).replace('.0', '')
						out_conn_dictionary_b[action_id] = out_conn
						break
					except KeyError as err:
						print 'Ignoring flow: %s' % err
			for key in out_conn_dictionary:
				if out_conn_dictionary[key] != out_conn_dictionary_b[key]:
					df3.set_value(index, 'changed_output', 'True')
					keep_true_bit = keep_true_bit ^ 1
				else:
					if keep_true_bit == 1:
						df3.set_value(index, 'changed_output', 'True')
					else:
						df3.set_value(index, 'changed_output', 'False')

	df3.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df3
	del df4
	del df4buffer
	return

def final_trimmer(sim_csv, sim_id, training_dataset = 'False'):

	df = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	df['err_type'] = df.err_type.astype(str)
	columns_list = df.columns.values.tolist()
	number_of_columns = len(columns_list)

	if training_dataset == 'True':
		for index, row, in df[['id']].iterrows():
			if row['id'] != 'openflow2':
				df.drop(index, inplace=True)

	for i in range(0, number_of_columns - 10):
		if columns_list[i] == 'id' or columns_list[i] == '@timestamp':
			#df.drop(columns_list[i], axis=1, inplace=True)
			continue
		df.drop(columns_list[i], axis=1, inplace=True)
	for index, row, in df[['action', 'err_type']].iterrows():
		if row['action'] == 'fix' or row['err_type'] == '-':
			df.set_value(index, 'err_type', 'ok')
	df.drop('action', axis=1, inplace=True)
	df.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df
	return

def join_shards(csv_shards, sim_id, csv_type):

	last_csv = None
	for i in range(len(csv_shards)-1):
		df1 = pd.read_csv(str(csv_shards[i]).replace(str(csv_type), 'modified_'+str(csv_type)))		
		df2 = pd.read_csv(str(csv_shards[i+1]).replace(str(csv_type), 'modified_'+str(csv_type)))
		df = pd.concat([df1, df2])
		df.to_csv(str(csv_shards[i+1]).replace(str(csv_type), 'modified_'+str(csv_type)), index=False)
		last_csv = str(csv_shards[i+1]).replace(str(csv_type), 'modified_'+str(csv_type))
	os.rename(str(last_csv), '/root/csv/'+sim_id+'_modified_'+str(csv_type)+'.csv')
	del df
	del df1
	del df2
	return

#CHANGE TO START
if __name__ == '__main__':

	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	training = str(config.get('main', 'training'))

	os.chdir('/root/csv')
	csv_dict = {}
	for file in glob.glob('*node.csv'):
		if 'modified' not in file:
			sim_index = file.replace("_node.csv", "")
			csv_dict[sim_index] = ["/root/csv/" + str(sim_index) + "_node.csv", "/root/csv/" + str(sim_index) + "_simstate.csv", "/root/csv/" + str(sim_index) + "_topology.csv"]

	for sim_index in csv_dict:
		print 'Limiting size...'
		csv_splitted = limit_size(csv_dict[sim_index], sim_index)

		for key, value in csv_splitted[1].iteritems():
			for csv in value:
				print 'Trimming and syncing topo csv...'
				topology_csv_trimmer(csv, sim_index)
				time_sync_topo(csv, sim_index)
			if len(value) > 1:
				print 'Joining topo shards...'
				join_shards(value, sim_index, 'topo')

		for key, value in csv_splitted[0].iteritems():
			for csv in value:
				print "Working on node csv"
				print "Trimming..."
				general_trimmer(csv, sim_index)
				print "Time syncing..."
				time_sync_node(csv, sim_index)
				print "Checking in-ports..."
				modified_inport_columns(csv, sim_index)
				print "Checking output-node-connectors..."
				modified_output_columns(csv, sim_index)
				# Traffic errors not supported for the time being
				'''
				print "Checking packets count..."
				packets_delta(csv, sim_index)
				packets_delta_buffer(csv, sim_index)
				print "Processing packet average..."
				packets_average(csv, sim_index)
				'''
				print "Adding topology field..."
				add_topology_field(csv, sim_index)
				print "Final trimming..."
				final_trimmer(csv, sim_index, training_dataset=training)
			if len(value) > 1:
				print 'Joining node shards...'
				join_shards(value, sim_index, 'node')