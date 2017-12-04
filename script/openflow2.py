import pandas as pd
import os
import glob

def final_trimmer(df):
	
	for index, row, in df[['id']].iterrows():
		if row['id'] != 'openflow2':
			df.drop(index, inplace=True)
		print "Processing row %s" % index

	return

if __name__ == '__main__':

	os.chdir('/root/csv')
	for file in glob.glob('*_node.csv'):
		df = pd.read_csv(file)
		final_trimmer(df)
		df.to_csv(file, index=False)