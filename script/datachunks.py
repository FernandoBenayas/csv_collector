import pandas as pd
import datetime as dt
import os



class DataChunks(object):

	def __init__(self, source, sim_id, training = False):

		self.sim_id = sim_id
		self.source = source

		if bool(training) != False:
			df = pd.read_csv(source)
			self.original_df = df.ix[df.id == 'openflow'+str(training)]
		else:
			self.original_df = pd.read_csv(source)

		splits = int(self.original_df.shape[0] / 5000)
		if self.original_df.shape[0] == 5000:
			splits = 0
		self.shards = []

		for i in range(splits + 1):
			df = self.original_df[5000*i:5000*(i+1)]
			self.shards.append(df)

	def join(self):

		final_df = pd.concat(self.shards)
		final_df.to_csv(self.source.replace('.csv', '_modified.csv'), index=False)

		return