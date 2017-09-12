import pandas as pd
import os

csv_list_node = []

for file in os.listdir("/root/csv"):
	if file.endswith("node.csv"):
		csv_list_node.append(os.path.join("/root/csv", file))

for csv in csv_list_node:
	df = pd.read_csv(str(csv))

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

	columns_list = df.columns.values.tolist()

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

	df.to_csv("modified_node.csv")

csv_list_topo = []

for file in os.listdir("/root/csv"):
	if file.endswith("topology.csv"):
		csv_list_topo.append(os.path.join("/root/csv", file))

for csv in csv_list_topo:
	df = pd.read_csv(str(csv))

	print 'Selecting ip columns in topology csv'

	columns_list = df.columns.values.tolist()

	for column in columns_list:
		if any( x not in column for x in ('host-tracker-service:addresses.', '.ip')):
			df.drop(column, axis=1, inplace=True)

	df.to_csv("modified_topo.csv")