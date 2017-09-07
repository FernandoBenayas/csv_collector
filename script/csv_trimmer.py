import pandas as pd
import os

csv_list = []

for file in os.listdir("/root/csv"):
	if file.endswith(".csv"):
		csv_list.append(os.path.join("/root/csv", file))

for csv in csv_list:
	df = pd.read_csv(str(csv))
	columns_list = df.columns.values.tolist()

	for column in columns_list:
		if 'flow-node-inventory:table' in column and 'flow-node-inventory:table.68' not in column:
			df.drop(column, axis=1, inplace=True)
		elif 'duration' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'opendaylight-group-statistics' in column or 'opendaylight-meter-statistics' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'node-connector.' in column and (('state' not in column) and ('packets' not in column)):
			df.drop(column, axis=1, inplace=True)
		elif 'manufacturer.' in column or 'hardware' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'flow-node-inventory:ip-address' in column:
			df.drop(column, axis=1, inplace=True)
		elif 'flow-node-inventory:switch-features' in column or 'opendaylight-group-statistics:group-features' in column:
			df.drop(column, axis=1, inplace=True)

	print len(df.columns)

	df.to_csv("modified.csv")