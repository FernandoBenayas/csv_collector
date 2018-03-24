import pandas as pd
import ConfigParser
import datetime as dt
import os
import glob
import sys
import ConfigParser
import math
from datachunks import DataChunks
from nodedatachunks import NodeDataChunks
from topodatachunks import TopoDataChunks

def datetimefy(timestamp):

	list_timestamp = timestamp.split("T")
	date_list = list_timestamp[0].split("-")
	time_list = list_timestamp[1].split(":")
	seconds_list = time_list[2].split(".")
	copy = seconds_list[1]
	seconds_list[1] = int(copy.replace("Z", ""))/1000

 	return dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))

def general_trimmer(df, df_buffer):

	#Dropping useless data DEBUGGING: COULD USE ALL()? TOO TIME-CONSUMING
	print 'Dropping useless columns'

	node_columns_list = df.columns.values.tolist()
	for column in node_columns_list:
		if 'flow-node-inventory:table' in column and 'flow-node-inventory:table.68' not in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'duration' in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'opendaylight-group-statistics' in column or 'opendaylight-meter-statistics' in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'node-connector.' in column and (('state' not in column) and ('packets' not in column) and ('id' not in column)):
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'manufacturer' in column or 'hardware' in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'flow-node-inventory:ip-address' in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)
		elif 'flow-node-inventory:switch-features' in column or 'opendaylight-group-statistics:group-features' in column:
			df.drop(column, axis=1, inplace=True)
			node_columns_list.remove(column)

	print 'Creating flow list'

	flow_columns_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s and '.id' in s and '_table' not in s and 'time' not in s ]
	df["existence_of_flows"] = ""

	for index, row in df[flow_columns_list].iterrows():
		counter = 0
		for column in flow_columns_list:
			if "UF" not in str(row[column])	and str(row[column]) != 'nan':
				counter += 1
		if counter > 0:
			df.at[index, "existence_of_flows"] = True
		else:
			df.at[index, "existence_of_flows"] = False

	print 'Creating lldp list'
	type_columns_list = [s for s in node_columns_list if 'match.ethernet-type' in s]
	df['not_dropping_lldp'] = ""

	for index, row in df.iterrows():
		for column in type_columns_list:
			if "35020"in str(row[column]):
				column_text = str(column).split('.')
				flow_id = column_text[3]
				output_node_connector = "flow-node-inventory:table.68.flow." + str(flow_id) + ".instructions.instruction.0.apply-actions.action.0.output-action.output-node-connector"
				if str(row[output_node_connector]) != 'nan':
					df.at[index, "not_dropping_lldp"] = 'True'
					break
				else:
					df.at[index, "not_dropping_lldp"] = 'False'
					break
			else:
				df.at[index, "not_dropping_lldp"] = 'False'

	print 'Creating timeout field'
	h_timeout_list = [s for s in node_columns_list if 'hard-timeout' in s]
	df['modified_timeout'] = False

	for index, row in df[h_timeout_list].iterrows():
		for column in h_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) != 300:
				df.at[index, "modified_timeout"] = True
				break

	i_timeout_list = [s for s in node_columns_list if 'idle-timeout' in s]

	for index, row in df[i_timeout_list].iterrows():
		for column in i_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) <= 15:
				df.at[index, "modified_timeout"] = True
				break

	print 'Creating node connector list'
	node_connector_list = [s for s in node_columns_list if 'node-connector.' in s and '.id' in s and 'address' not in s]
	df['node_connector_down'] = ""
	node_connector_counter = 0

	for index, row in df.iterrows():
		for column in node_connector_list:
			if 'LOCAL' not in str(row[column]) and str(row[column]) != 'nan':
				node_connector_counter += 1
				column = column.replace('.id', '.flow-node-inventory:state.link-down')
				if str(row[column]) != 'nan':
					if str(row[column]) != 'True':
						df.at[index, "node_connector_down"] = False
					else:
						df.at[index, "node_connector_down"] = True
						break
		if node_connector_counter == 0:
			df.at[index, "node_connector_down"] = 'isolated'
		node_connector_counter = 0

	global NODE_NUMBER_OF_FLOWS_MATCHIN
	# Looking for the number of flows in the node csv
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s and '.match.in-port' in s]
	for column in flow_list:
		NODE_NUMBER_OF_FLOWS_MATCHIN.append(column)

	global NODE_NUMBER_OF_FLOWS_PRIORITY
	# Looking for the number of flows in the node csv
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s and '.priority' in s]
	for column in flow_list:
		NODE_NUMBER_OF_FLOWS_PRIORITY.append(column)


	global NODE_NUMBER_OF_FLOWS
	# Looking for the number of flows in the node csv
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s and '.id' in s and 'idle' not in s]
	for column in flow_list:
		flow_number = int(column.split('.')[3])
		if flow_number > NODE_NUMBER_OF_FLOWS:
			NODE_NUMBER_OF_FLOWS = flow_number


	df['err_type'] = "sample"
	df['action'] = "sample"
	return

def topology_csv_trimmer(df, df_buffer):

	print 'Selecting ip columns in topology csv'
	columns_list = df.columns.values.tolist()

	for column in columns_list:
		if not all(x in str(column) for x in ['host-tracker-service:addresses.', '.ip']) and '@timestamp' not in column:
			df.drop(column, axis=1, inplace=True)
	df['err_type'] = "sample"
	df['action'] = "sample"
	return

# Adds a field containing the index of the nearest entry of the same
# switch at the node csv
def add_nearest(df, df_buffer):

	df['index_nearest'] = 'sample'
	df['is_buffer'] = 'False'
	
	for index, row in df[['id', '@timestamp']].iterrows():
		print '		Adding nearest field: row %s' % index
		origin_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'index': 'First', 'delta': dt.timedelta.max.total_seconds()}
		is_buffer = False
		df_trimmed = df.ix[df.id == str(row['id'])]
		buffer_trimmed = df_buffer.ix[df_buffer.id == str(row['id'])]
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
		df.at[index, 'index_nearest'] = min_diff['index']
		df.at[index, 'is_buffer'] = str(is_buffer)

	return

# Adds a field in the topology csv that indicates
# if there are hosts missing
def add_topology_field(df_node, topo_data):

	first_topo_df = topo_data.shards[0]
	columns_list = first_topo_df.columns.values.tolist()

	ip_column_list = [s for s in columns_list if 'ip' in s]
	ip_start_list = []
	ip_current_list = []
	df_node['modified_hosts'] = 'sample'

	# Capturing the first report of hosts in the network
	# and inserting int in ip_start_list
	for column in ip_column_list:
		ip = first_topo_df.iloc[0][column]
		if 'nan' not in str(ip):
			ip_start_list.append(ip)

	# For each row in node csv, we look for the nearest (in time)
	# topology report in all the topology shards, and compare it with the 'start' 
	# report on the topology (when no errors where started), to see if something
	# is missing
	for index, row in df_node[['@timestamp']].iterrows():
		print '		Adding modified hosts field: row %s' % index
		node_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'row': 'sample', 'delta': dt.timedelta.max.total_seconds()}
		for index2, df_topo in enumerate(topo_data.shards):
			for index3, row3 in df_topo.iterrows():
				topo_datetime = datetimefy(str(row3['@timestamp']))
				diff = (node_datetime - topo_datetime).total_seconds()
				if diff >= 0 and diff < min_diff['delta']:
					min_diff['delta'] = diff
					min_diff['row'] = row3
				elif diff < 0:
					break
		if isinstance(min_diff['row'], basestring):
			df_node.at[index, 'modified_hosts'] = 'First'
			continue
		for column in ip_column_list:
			ip = min_diff['row'][column]
			if 'nan' not in str(ip):
				ip_current_list.append(ip)
		if set(ip_start_list) == set(ip_current_list):
			df_node.at[index, 'modified_hosts'] = 'False'
		else:
			df_node.at[index, 'modified_hosts'] = 'True'
		ip_current_list = []

	return

# We look for the closest network report (simstate message) and assign
# its state to the correspondent row in the node csv
def time_sync_node(df_node, df_state_list):

	for index, row in df_node[['@timestamp']].iterrows():
		print '		Syncing node: processing row %s' % index
		node_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

		for df_state in df_state_list:
			for index2, row2 in df_state[['@timestamp', 'err.err_type', 'action']].iterrows():
				state_datetime = datetimefy(str(row2['@timestamp']))
				diff = (node_datetime - state_datetime).total_seconds()

				if diff >= 0 and diff < min_diff['delta']:
					min_diff['delta'] = diff
					if "start" in str(row2['action']):
						min_diff['err'] = '-'
					else:
						min_diff['err'] = str(row2['err.err_type'])
					min_diff['action'] = str(row2['action'])

				elif diff < 0:
					break


		df_node.at[index, 'err_type'] = str(min_diff['err'])
		df_node.at[index, 'action'] = str(min_diff['action'])

	return

# Same for the topo csv
def time_sync_topo(df_topo, df_state_list):

	for index, row in df_topo[['@timestamp']].iterrows():
		print '		Syncing topo: processing row %s' % index
		topo_datetime = datetimefy(str(row['@timestamp']))
		min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

		for df_state in df_state_list:
			for index2, row2 in df_state[['@timestamp', 'err.err_type', 'action']].iterrows():
				state_datetime = datetimefy(str(row2['@timestamp']))
				diff = (topo_datetime - state_datetime).total_seconds()

				if diff >= 0 and diff < min_diff['delta']:
					min_diff['delta'] = diff
					if "start" in str(row2['action']):
						min_diff['err'] = '-'
					else:
						min_diff['err'] = str(row2['err.err_type'])
					min_diff['action'] = str(row2['action'])

				elif diff < 0:
					break

		df_topo.at[index, 'err_type'] = str(min_diff['err'])
		df_topo.at[index, 'action'] = str(min_diff['action'])

	return


# Checks if the order fields were modified
def order_columns(df, df_buffer):

	df['changed_order'] = 'nan'

	flow_list = []
	for i in range(0, NODE_NUMBER_OF_FLOWS + 1):
		print i
		flow_list.append('flow-node-inventory:table.68.flow.'+str(i)+'.id')

	columns_list = flow_list[:]
	columns_list.extend(['@timestamp', 'id', 'index_nearest', 'is_buffer'])
	print columns_list

	for index, row in df[columns_list].iterrows():
		order_dict = {}
		for column in flow_list:
			order_dict[column] = str(row[column])

		past_report = str(row['index_nearest'])

		if past_report == 'First':
				df.at[index, 'changed_order'] = 'First'
		else:
			order_dict_b = {}
			if row['is_buffer'] == 'False':
				row2 = df[columns_list].loc[[int(past_report)]]
			else:
				row2 = df_buffer[columns_list].loc[[int(past_report)]]

			for column in flow_list:
				order_dict_b[column] = str(row2[column].item())

			for key in order_dict:
				if order_dict[key] != order_dict_b[key]:
					df.at[index, 'changed_order'] = 'True'
					break
				else:
					df.at[index, 'changed_order'] = 'False'
				


	return

# Checks if the in-port fields in the match rules were modified
def modified_inport_columns(df, df_buffer):

	node_columns_list = df.columns.values.tolist()
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s]
	df["changed_inport"] = "sample"

	# For each row (switch), we create a {flow : list-of-inports} dictionary. Then, we look
	# for the closest previous entry of that switch, create another dictionary, and compare
	# them
	buffer_flow_list = flow_list[:]
	flow_list.extend(['@timestamp', 'id', 'index_nearest', 'is_buffer'])

	for index, row in df[flow_list].iterrows():
		print '		Checking in-ports: processing row %s' % index
		in_port_dictionary = {}
		for i in NODE_NUMBER_OF_FLOWS_MATCHIN:
			in_port = str(row[str(i)])
			flow_id = str(i).split('.')[3]
			in_port_dictionary[str(row['flow-node-inventory:table.68.flow.' + flow_id + '.id'])] = in_port

		past_report = str(row['index_nearest'])
		# Once we have the closest entry (if there is one), we create a 'b' dictionary,
		# insert in it the flow : list-of-inports of that entry, and compare dictionaries
		if past_report == 'First':
			df.at[index, 'changed_inport'] = 'First'
		else: 
			in_port_dictionary_b = {}
			if row['is_buffer'] == 'False':
				row2 = df[flow_list].loc[[int(past_report)]]
			else:
				row2 = df_buffer[buffer_flow_list].loc[[int(past_report)]]

			for i in NODE_NUMBER_OF_FLOWS_MATCHIN:
				in_port = str(row2[str(i)].item())
				flow_id = str(i).split('.')[3]
				in_port_dictionary_b[str(row2['flow-node-inventory:table.68.flow.' + flow_id + '.id'].item())] = in_port

			if in_port_dictionary.keys() != in_port_dictionary_b.keys():
				df.at[index, 'changed_inport'] = 'True'
				continue
			for key in in_port_dictionary:
				try:
					if in_port_dictionary[key] != in_port_dictionary_b[key]:
						df.at[index, 'changed_inport'] = 'True'
						break
					else:
						df.at[index, 'changed_inport'] = 'False'
				except KeyError as err:
					print '		Flow unpaired: %s' % key
					df.at[index, 'changed_inport'] = 'True'
					break
	return

def priorities(df, df_buffer):

	node_columns_list = df.columns.values.tolist()
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s]
	df['changed_priority'] = 'sample'

	buffer_flow_list = flow_list[:]
	flow_list.extend(['index_nearest', 'is_buffer'])
	for index, row in df[flow_list].iterrows():
		print '		Checking priority: processing row %s' % index
		priority_dictionary = {}
		for i in NODE_NUMBER_OF_FLOWS_PRIORITY:
			priority = str(row[str(i)])
			flow_id = str(i).split('.')[3]
			priority_dictionary[str(row['flow-node-inventory:table.68.flow.' + flow_id + '.id'])] = priority

		past_report = str(row['index_nearest'])
		
		if past_report == 'First':
			df.at[index, 'changed_priority'] = 'First'
		else: 
			priority_dictionary_b = {}
			if row['is_buffer'] == 'False':
				row2 = df[flow_list].loc[[int(past_report)]]
			else:
				row2 = df_buffer[buffer_flow_list].loc[[int(past_report)]]

			for i in NODE_NUMBER_OF_FLOWS_PRIORITY:
				priority = str(row2[str(i)].item())
				flow_id = str(i).split('.')[3]
				priority_dictionary_b[str(row2['flow-node-inventory:table.68.flow.' + flow_id + '.id'].item())] = priority


			if priority_dictionary.keys() != priority_dictionary_b.keys():
				df.at[index, 'changed_priority'] = 'True'
				continue

			for key in priority_dictionary:
				try:
					if priority_dictionary[key] != priority_dictionary_b[key]:
						df.at[index, 'changed_priority'] = 'True'
						break
					else:
						df.at[index, 'changed_priority'] = 'False'
				except KeyError as err:
					print '		Flow unpaired: %s' % key
					df.at[index, 'changed_priority'] = 'True'
					break
	return

# Checks if the 'output-node-connector' field in each flow has been modified
def modified_output_columns(df, df_buffer):

	node_columns_list = df.columns.values.tolist()
	flow_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s]
	df["changed_output"] = "sample"
	flows_and_actions = {}

	flow_columns_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' in s and '.id' in s and 'idle' not in s ]
	for column in flow_columns_list:
		flow_id = int(column.split('.')[3])
		action_list = [s for s in node_columns_list if 'flow-node-inventory:table.68.flow.' + str(flow_id) in s and 'output-action.output-node-connector' in s]
		if action_list:
			flows_and_actions[flow_id] = action_list

	flow_list.extend(['index_nearest', 'is_buffer'])
	for index, row in df[flow_list].iterrows():
		print '		Checking output node connectors: processing row %s' % index
		out_conn_dictionary = {}
		for key, value in flows_and_actions.iteritems():
			for i in value:
				out_conn = str(row[i]).replace('.0', '')
				out_conn_dictionary[i] = out_conn

		past_report = str(row['index_nearest'])
		if past_report == 'First':
			df.at[index, 'changed_output'] = 'First'
		else:
			out_conn_dictionary_b = {}
			if row['is_buffer'] == 'False':
				row2 = df.loc[[int(past_report)]]
			else:
				row2 = df_buffer.loc[[int(past_report)]]
			for key, value in flows_and_actions.iteritems():
				for i in value:
					out_conn = str(row2[i].item()).replace('.0', '')
					out_conn_dictionary_b[i] = out_conn

			if out_conn_dictionary.keys() != out_conn_dictionary_b.keys():
				df.at[index, 'changed_output'] = 'True'
				continue

			for key in out_conn_dictionary:
				if out_conn_dictionary[key] != out_conn_dictionary_b[key]:
					df.at[index, 'changed_output'] = 'True'
					break
				else:
					df.at[index, 'changed_output'] = 'False'
	return

def final_trimmer(df):

	df['err_type'] = df.err_type.astype(str)
	columns_list = df.columns.values.tolist()

	for i in range(0, len(columns_list) - 13):
		if columns_list[i] == 'id' or columns_list[i] == '@timestamp' or columns_list[i] == 'changed_priority':
			continue
		df.drop(columns_list[i], axis=1, inplace=True)

	for index, row, in df[['action', 'err_type']].iterrows():
		if (row['action'] == 'fix' and row['err_type'] != 'buffer') or row['err_type'] == '-':
			df.at[index, 'err_type'] = 'ok'
		if row['err_type'] == 'buffer':
			df.drop(index, inplace = True)
	df.drop(['action', 'index_nearest', 'is_buffer'], axis=1, inplace=True)
	return


#CHANGE TO START
if __name__ == '__main__':

	#Move this up
	NODE_NUMBER_OF_FLOWS_PRIORITY = []
	NODE_NUMBER_OF_FLOWS_MATCHIN = []
	NODE_NUMBER_OF_FLOWS = 0

	config = ConfigParser.ConfigParser()
	config.readfp(open('config', 'r'))
	training = str(config.get('main', 'training'))
	bufferTimeWindow = dt.timedelta(seconds=int(config.get('main', 'buffer_time_window'))).total_seconds()

	if training == 'False':
		training = False

	os.chdir('/root/csv')
	csv_dict = {}
	for file in glob.glob('*node.csv'):
		if 'modified' not in file:
			sim_id = file.replace("_node.csv", "")
			csv_dict[sim_id] = ["/root/csv/" + str(sim_id) + "_node.csv", "/root/csv/" + str(sim_id) + "_simstate.csv", "/root/csv/" + str(sim_id) + "_topology.csv"]

	for sim_id in csv_dict:
		print 'Limiting size...'
		node_data = NodeDataChunks(csv_dict[sim_id][0], sim_id, bufferTimeWindow, training)
		topo_data = TopoDataChunks(csv_dict[sim_id][2], sim_id, bufferTimeWindow)
		state_data = DataChunks(csv_dict[sim_id][1], sim_id)

		for index, df in enumerate(topo_data.shards):
			topology_csv_trimmer(df, topo_data.buffers[index])
			time_sync_topo(df, state_data.shards)

		for index, df in enumerate(node_data.shards):
			general_trimmer(df, node_data.buffers[index])
			add_nearest(df, node_data.buffers[index])
			time_sync_node(df, state_data.shards)
			order_columns(df, node_data.buffers[index])
			modified_inport_columns(df, node_data.buffers[index])
			priorities(df, node_data.buffers[index])
			modified_output_columns(df, node_data.buffers[index])
			add_topology_field(df, topo_data)
			final_trimmer(df)

		node_data.join()
		topo_data.join()
		state_data.join()