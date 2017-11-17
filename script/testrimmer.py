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

# Adds a field containing the index of the nearest entry of the same
# switch at the node csv
def add_nearest(sim_csv, sim_id):

	df_node = pd.read_csv(str(sim_csv).replace('_node', '_modified_node'))
	buffer_df = pd.read_csv(str(sim_csv).replace('.csv', '_buffer.csv'))
	df_node['index_nearest'] = 'sample'
	df_node['is_buffer'] = 'False'
	
	for index, row in df_node[['id', '@timestamp']].iterrows():
		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'index': 'First', 'delta': dt.timedelta.max.total_seconds()}
		is_buffer = False
		df_trimmed = df_node.ix[df_node.id == str(row['id'])]
		buffer_trimmed = buffer_df.ix[buffer_df.id == str(row['id'])]
		# Checking for the closest entry of the same switch in the
		# current dataframe and the buffer
		for index2, row2 in df_trimmed.loc[0:index].iterrows():
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
		df_node.set_value(index, 'index_nearest', min_diff['index'])
		df_node.set_value(index, 'is_buffer', str(is_buffer))

	df_node.to_csv(sim_csv.replace('_node', '_modified_node'), index=False)
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

# We look for the closest network report (simstate message) and assign
# its state to the correspondent row in the node csv
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

# Same for the topo csv
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
	buffer_flow_list = flow_list[:]
	flow_list.extend(['@timestamp', 'id', 'index_nearest', 'is_buffer'])
	for index, row in df3[flow_list].iterrows():
		print '		Checking in-ports: processing row %s' % index
		in_port_dictionary = {}
		for i in range(number_of_flows + 1):
			try:
				in_port = str(row['flow-node-inventory:table.68.flow.' + str(i) + '.match.in-port'])
				in_port_dictionary[str(row['flow-node-inventory:table.68.flow.' + str(i) + '.id'])] = in_port
			except KeyError as err:
				print 'Ignoring flow: %s' % err

		past_report = str(row['index_nearest'])

		# Once we have the closest entry (if there is one), we create a 'b' dictionary,
		# insert in it the flow : list-of-inports of that entry, and compare dictionaries
		if past_report == 'First':
			df3.set_value(index, 'changed_inport', 'First')
		else: 
			in_port_dictionary_b = {}
			if row['is_buffer'] == False:
				row2 = df3[flow_list].iloc[[int(past_report)]]
			else:
				row2 = buffer_df[buffer_flow_list].iloc[[int(past_report)]]
			for i in range(number_of_flows + 1):
				try:
					in_port = str(row2['flow-node-inventory:table.68.flow.' + str(i) + '.match.in-port'].item())
					in_port_dictionary_b[str(row2['flow-node-inventory:table.68.flow.' + str(i) + '.id'].item())] = in_port
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
	del buffer_df
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

		past_report = str(row['index_nearest'])

		if past_report == 'First':
			df3.set_value(index, 'changed_output', 'First')

		else:
			out_conn_dictionary_b = {}
			if row['is_buffer'] == False:
				row2 = df3.iloc[[int(past_report)]]
			else:
				row2 = buffer_df.iloc[[int(past_report)]]
			for key, value in flows_and_actions.iteritems():
				for i in range(value + 1):
					try:
						action_id = 'flow-node-inventory:table.68.flow.' + str(key) + '.instructions.instruction.0.apply-actions.action.'+ str(i) +'.output-action.output-node-connector'
						out_conn = str(row2[action_id].item()).replace('.0', '')
						out_conn_dictionary_b[action_id] = out_conn
						break
					except KeyError as err:
						print 'Ignoring flow: %s' % err
			for key in out_conn_dictionary:
				if out_conn_dictionary[key] != out_conn_dictionary_b[key] and out_conn_dictionary[key] != 'nan' and out_conn_dictionary_b[key] != 'nan':
					df3.set_value(index, 'changed_output', 'True')
					#keep_true_bit = keep_true_bit ^ 1
					break
				else:
					if keep_true_bit == 1:
						df3.set_value(index, 'changed_output', 'True')
					else:
						df3.set_value(index, 'changed_output', 'False')

	df3.to_csv(str(sim_csv).replace('_node', '_modified_node'), index=False)
	del df3
	del buffer_df
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

	for i in range(0, number_of_columns - 12):
		if columns_list[i] == 'id' or columns_list[i] == '@timestamp':
			#df.drop(columns_list[i], axis=1, inplace=True)
			continue
		df.drop(columns_list[i], axis=1, inplace=True)
	for index, row, in df[['action', 'err_type']].iterrows():
		if row['action'] == 'fix' or row['err_type'] == '-':
			df.set_value(index, 'err_type', 'ok')
	df.drop(['action', 'index_nearest', 'is_buffer'], axis=1, inplace=True)
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
				add_nearest(csv, sim_index)
				time_sync_node(csv, sim_index)
				print "Checking in-ports..."
				modified_inport_columns(csv, sim_index)
				print "Checking output-node-connectors..."
				modified_output_columns(csv, sim_index)
				print "Adding topology field..."
				add_topology_field(csv, sim_index)
				print "Final trimming..."
				final_trimmer(csv, sim_index, training_dataset=training)
			if len(value) > 1:
				print 'Joining node shards...'
				join_shards(value, sim_index, 'node')