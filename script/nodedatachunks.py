import pandas as pd
from datachunks import DataChunks
from collections import OrderedDict
import datetime as dt
import os

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
#title           : nodedatachunks.py
#date created    : 12/01/2018
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
NodeDataChunks class. Completes DataChunks to reflect info about nodes.

"""

class NodeDataChunks(DataChunks):


	def __init__(self, source, sim_id, bufferTimeWindow = 30, training = False):

		DataChunks.__init__(self, source, sim_id, training)
		self.buffers = []
		self.size = 0

		for row in self.original_df[['id']].iterrows():
			node_number = int(row[1][0].split("w")[1])
			if node_number > self.size:
				self.size = node_number

		for index, shard in enumerate(self.shards):
			shard_buffer = self.buffer(bufferTimeWindow, index)
			self.buffers.append(shard_buffer)

	def buffer(self, bufferTimeWindow, index):

		if index == 0:
			return self.shards[index][0:0]

		first_entry_time = self.datetimefy(self.shards[index].iloc[0]['@timestamp'])
		comparate_time = self.datetimefy(self.original_df.iloc[5000*index-self.size]['@timestamp'])
		j = 1
		while (first_entry_time - comparate_time).total_seconds() < bufferTimeWindow:
			j += 1
			comparate_time = self.datetimefy(self.original_df.iloc[5000*index-self.size*j]['@timestamp'])
		buffer_dataframe = self.original_df[5000*index-self.size*(j+1):5000*index]

		return buffer_dataframe

	def datetimefy(self, timestamp):

		list_timestamp = timestamp.split("T")
		date_list = list_timestamp[0].split("-")
		time_list = list_timestamp[1].split(":")
		seconds_list = time_list[2].split(".")
		copy = seconds_list[1]
		seconds_list[1] = int(copy.replace("Z", ""))/1000

	 	return dt.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2]), int(time_list[0]), int(time_list[1]), int(seconds_list[0]), int(seconds_list[1]))

	def join(self):

		final_df = pd.concat(self.shards)
		final_df.to_csv(self.source.replace('.csv', '_modified.csv'), index=False)

		return