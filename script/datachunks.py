import pandas as pd
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
#title           : datachunks.py
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
Datachunks class. Divides the data in parts with a max size of 5000 entries.

"""

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