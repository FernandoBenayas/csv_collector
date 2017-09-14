import pandas as pd
import datetime as dt
import os


def trimmer(sim_list, sim_id):

	df = pd.read_csv(str(sim_list[0]))

	#Dropping useless data
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

	flow_columns_list = []

	for column in columns_list:
		#USE REGEXP
		if 'flow-node-inventory:table.68.flow.' in column and '.id' in column and '_table' not in column and 'time' not in column :
			flow_columns_list.append(column)

	df["existence_of_flows"] = ""

	for index, row in df.iterrows():
		counter = 0
		for column in flow_columns_list:
			if "UF" not in str(row[column])	and str(row[column]) != 'nan':
				counter += 1
		if counter > 0:
			df.set_value(index, "existence_of_flows", True)
		else:
			df.set_value(index, "existence_of_flows", False)

	print 'Creating lldp list'

	type_columns_list = []
	
	for column in columns_list:
	#USE REGEXP
		if 'match.ethernet-type' in column:
			type_columns_list.append(column)

	df['not_dropping_lldp'] = ""

	for index, row in df.iterrows():
		for column in type_columns_list:
			if "35020"in str(row[column]):
				column_text = str(column).split('.')
				flow_id = column_text[3]
				output_node_connector = "flow-node-inventory:table.68.flow." + str(flow_id) + ".instructions.instruction.0.apply-actions.action.0.output-action.output-node-connector"
				if str(row[output_node_connector]) != 'nan':
					df.set_value(index, "not_dropping_lldp", True)
				else:
					df.set_value(index, "not_dropping_lldp", False)


	print 'Creating hard timeout field'

	h_timeout_list = []

	for column in columns_list:
	#USE REGEXP
		if 'hard-timeout' in column:
			h_timeout_list.append(column)

	df['modified_h_timeout'] = ""

	for index, row in df.iterrows():
		for column in h_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) < 15:
				df.set_value(index, "modified_h_timeout", True)
			else:
				df.set_value(index, "modified_h_timeout", False)

	print 'Creating idle timeout field'

	i_timeout_list = []

	for column in columns_list:
	#USE REGEXP
		if 'idle-timeout' in column:
			i_timeout_list.append(column)

	df['modified_i_timeout'] = ""

	for index, row in df.iterrows():
		for column in i_timeout_list:
			if str(row[column]) != 'nan' and int(row[column]) > 0 and int(row[column]) < 15:
				df.set_value(index, "modified_i_timeout", True)
			else:
				df.set_value(index, "modified_i_timeout", False)

	print 'Creating node connector list'

	node_connector_list = []

	for column in columns_list:
	#USE REGEXP
		if 'node-connector.' in column and '.id' in column and 'address' not in column:
			node_connector_list.append(column)

	df['node_connector_down'] = ""
	node_connector_counter = 0

	for index, row in df.iterrows():
		for column in node_connector_list:
			if 'LOCAL' not in str(row[column]) and str(row[column]) != 'nan':
				node_connector_counter += 1
				column.replace('.id', '.flow-node-inventory:state.link-down')
				if str(row[column]) != 'nan':
					if str(row[column]) != 'True':
						df.set_value(index, "node_connector_down", False)
					else:
						df.set_value(index, "node_connector_down", True)
		if node_connector_counter == 0:
			df.set_value(index, "node_connector_down", 'isolated')
		node_connector_counter = 0

	df['err_type'] = ""
	df['action'] = "sample"

	df.to_csv(sim_id + "_modified_node.csv")
	
	print 'Modifying topology csv'

	df2 = pd.read_csv(str(sim_list[2]))

	print 'Selecting ip columns in topology csv'

	columns_list = df2.columns.values.tolist()

	for column in columns_list:
		if any( x not in column for x in ('host-tracker-service:addresses.', '.ip')):
			df2.drop(column, axis=1, inplace=True)

	df2['err_type'] = ""
	df2['action'] = "sample"

	df2.to_csv(sim_id + "_modified_topo.csv")

	return
	
def time_sync_node(sim_list, sim_id):

	df_state = pd.read_csv(str(sim_list[1]))
	df_node = pd.read_csv(sim_id + '_modified_node.csv')
	columns_list_state = df_state.columns.values.tolist()
	columns_list_node = df_node.columns.values.tolist()

	for index, row in df_node.iterrows():
		for column in columns_list_node:
			if '@timestamp' in str(column):
				timestamp = str(row[column])
				list_timestamp = timestamp.split("T")
				date_list = list_timestamp[0].split("-")
				time_list = list_timestamp[1].split(":")
				seconds_list = time_list[2].split(".")
				copy = seconds_list[1]
				seconds_list[1] = int(copy.replace("Z", ""))/1000

				node_datetime = dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))


				min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

				for index2, row2 in df_state.iterrows():
					for column in columns_list_state:
						if '@timestamp' in str(column):
							timestamp = str(row2[column])
							list_timestamp = timestamp.split("T")
							date_list = list_timestamp[0].split("-")
							time_list = list_timestamp[1].split(":")
							seconds_list = time_list[2].split(".")
							copy = seconds_list[1]
							seconds_list[1] = int(copy.replace("Z", ""))/1000
							state_datetime = dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))
							diff = (node_datetime - state_datetime).total_seconds()

							if diff >= 0 and diff < min_diff['delta']:

								min_diff['delta'] = diff
								min_diff['err'] = str(row2['err.err_type'])
								min_diff['action'] = str(row2['action'])

				df_node.set_value(index, 'err_type', min_diff['err'])
				df_node.set_value(index, 'action', str(min_diff['action']))
				df_node.to_csv(sim_id + "_modified_node.csv")
	return

def time_sync_topo(sim_list, sim_id):

	df_state = pd.read_csv(str(sim_list[1]))
	df_topo = pd.read_csv(sim_id + '_modified_topo.csv')
	columns_list_state = df_state.columns.values.tolist()
	columns_list_topo = df_topo.columns.values.tolist()

	for index, row in df_topo.iterrows():
		for column in columns_list_topo:
			if '@timestamp' in str(column):
				timestamp = str(row[column])
				list_timestamp = timestamp.split("T")
				date_list = list_timestamp[0].split("-")
				time_list = list_timestamp[1].split(":")
				seconds_list = time_list[2].split(".")
				copy = seconds_list[1]
				seconds_list[1] = int(copy.replace("Z", ""))/1000

				node_datetime = dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))

				min_diff = {'err': '', 'delta': dt.timedelta.max.total_seconds(), 'action': ''}

				for index2, row2 in df_state.iterrows():
					for column in columns_list_state:
						if '@timestamp' in str(column):
							timestamp = str(row2[column])
							list_timestamp = timestamp.split("T")
							date_list = list_timestamp[0].split("-")
							time_list = list_timestamp[1].split(":")
							seconds_list = time_list[2].split(".")
							copy = seconds_list[1]
							seconds_list[1] = int(copy.replace("Z", ""))/1000
							state_datetime = dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))
							diff = (node_datetime - state_datetime).total_seconds()

							if diff >= 0 and diff < min_diff['delta']:

								min_diff['delta'] = diff
								min_diff['err'] = str(row2['err.err_type'])
								min_diff['action'] = str(row2['action'])

				df_topo.set_value(index, 'err_type', min_diff['err'])
				df_topo.set_value(index, 'action', str(min_diff['action']))
				df_topo.to_csv(sim_id + "_modified_topo.csv")
	return

if __name__ == '__main__':

	csv_dict = {}

	for file in os.listdir("/root/csv"):
		if file.endswith("node.csv"):
			sim_index = file.replace("_node.csv", "")
			csv_dict[sim_index] = ["/root/csv/" + str(sim_index) + "_node.csv", "/root/csv/" + str(sim_index) + "_simstate.csv", "/root/csv/" + str(sim_index) + "_topology.csv"]

	for key in csv_dict:
		print "Trimming..."
		trimmer(csv_dict[key], key)
		print "Time syncing..."
		if 'node' in csv_dict[key]:
			time_sync_node(csv_dict[key], key)
		elif 'topo' in csv_dict[key]:
			#time_sync_topo(csv_dict[key], key)