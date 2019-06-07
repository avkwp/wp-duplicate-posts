import numpy as np
import pandas as pd
import re
from XMLRPC.database import Writer

MODE_TITLES = 0
MODE_IDS = 1
MODE_ATTRS = 2
DEFAULT_SCORE = 20
CHUNK_SIZE = 10

POSTCODE_REGULAR_EXPRESSION = r"\s*(([A-Z]{1,2})[0-9][0-9A-Z]?\s*([0-9])[A-Z]{2})\s*"
POSTCODE_REGEX = r"(([A-Z][0-9]{1,2})|(([A-Z][A-HJ-Y][0-9]{1,2})|(([A-Z][0-9][A-Z])|([A-Z][A-HJ-Y][0-9]?[A-Z]))))\s*([0-9][A-Z]{2})"

class ResolveDuplicates(object):

  def __init__(self, score_df, original_df, mode):
    self.score_df = score_df
    self.original_df = original_df
    self.mode = mode

  def reduce_score(self, score = DEFAULT_SCORE):
    self.score_df = self.score_df.loc[self.score_df.value >= score, :]

  # if id is not available append_df
  def score_append_original_df(self):
    self.score_df = self.score_df.join(self.original_df, on='variable', how='inner')

  # returns appended data, exact match
  def join_by_title(self, contains_id = False):
    df = self.score_df.join(self.original_df.set_index('Title'), on='Title1', how='right', lsuffix='_score', rsuffix='_original')
    df['Company Name'] = df.index.values
    if(contains_id):
      return df.loc[df.ID1 != df.ID]
    else:
      return df
  
  def original_append_id_df(self, reducers, joiners, contains_id = False):
    self.score_append_original_df()
    def join_list(a):
      return "|".join(self.score_df.loc[self.score_df.variable == a, 'ID1'].values.astype(str).tolist())
    for reducer in reducers:
      reducer(contains_id)
    join_vector = np.vectorize(join_list)
    self.original_df['reducer_post_ids'] = join_vector(self.original_df.index.values)
    
    for joiner in joiners:
      self.original_df = joiner(contains_id)
    self.original_df['join_post_ids'] = self.original_df.loc[:, 'ID1']
    self.original_df.loc[(self.original_df.loc[:, 'ID1'].astype(str) == 'nan'), 'join_post_ids'] = 0
    self.original_df['join_post_ids'] = self.original_df['join_post_ids'].astype(np.int64)
    
    return None

  # returns appended data, description comparison and terms comparison to merge
  def reduce_by_constraints(self, original_df = None, contains_id = False):
    if(contains_id):
      # setting term constraints
      self.score_df['term1_constraint'] = 1
      self.score_df['term2_constraint'] = 1
      self.score_df.loc[self.score_df.Terms1.astype(str) == 'nan', 'term1_constraint'] = 0
      self.score_df.loc[self.score_df.Terms2.astype(str) == 'nan', 'term2_constraint'] = 0

      # setting description constraints
      self.score_df['desc1_constraint'] = 0
      self.score_df['desc2_constraint'] = 0
      self.score_df.loc[self.score_df.Description_length1 > self.score_df.Description_length2, 'desc1_constraint'] = 1
      self.score_df.loc[self.score_df.Description_length2 >= self.score_df.Description_length1, 'desc2_constraint'] = 1

    else:
      l = np.vectorize(len)
      # assumes the score_df to be appended
      self.score_df['Description_length'] = l(self.score_df.loc[:, 'Description'])
      self.score_df['desc1_constraint'] = 0
      self.score_df['desc2_constraint'] = 0
      self.score_df.loc[self.score_df.Description_length1 > self.score_df.Description_length, 'desc1_constraint'] = 1
      self.score_df.loc[self.score_df.Description_length >= self.score_df.Description_length1, 'desc2_constraint'] = 1

    return None

  # to report
  def reduce_by_matching_attrs(self, contains_id = False):
    postcode_regex = re.compile(POSTCODE_REGULAR_EXPRESSION)

    postcodes = []
    for address in self.score_df.address_1:
      r = postcode_regex.findall(address)
      if(len(r) == 0):
        postcodes.append("")
      else:
        postcodes.append(r[0])
    self.score_df['postcodes1'] = postcodes
    postcodes = [address[p.start():p.end()] if p is not None else "" for address in self.score_df.address_2 for p in postcode_regex.finditer(address)]
    postcodes = []
    for address in self.score_df.address_2:
      r = postcode_regex.findall(address)
      if(len(r) == 0):
        postcodes.append("")
      else:
        postcodes.append(r[0])
    self.score_df['postcodes2'] = postcodes

    self.score_df = self.score_df.loc[((self.score_df.postcodes1 == self.score_df.postcodes2) | \
      (self.score_df.website_1 == self.score_df.website_2)), :]

  def resolve_duplicates(self, writer, contingency_write=False):
    cols = ['ID1', 'Title1', 'ID2', 'Title2', 'Terms1', 'Terms2', 
    'Description_length1', 'Description_length2', 'Description_length', 'Terms',
    'address_1', 'website_1', 'address_2', 'website_2', 
    'postcodes1', 'postcodes2', 'term1_constraint', 'term2_constraint', 'desc1_constraint', 
    'desc2_constraint']
    
    i = 0
    rows = []
    for idx, row in self.score_df.loc[:, cols].iterrows():
      _cols = row.index.intersection(cols).values.tolist()
      values = row.values
      np.place(values, row.values.astype(str) == 'nan', None)
      if (i % CHUNK_SIZE == 0):
        if(len(rows) != 0):
          writer.write_contingency(rows, _cols, 'table_contingency_duplicates') # write executemany
        rows = []
        rows.append(tuple(values.tolist()))
      else:
        rows.append(tuple(values.tolist()))
      i += 1
    if(len(rows) != 0):
      writer.write_contingency(rows, _cols, 'table_contingency_duplicates') # write executemany

    cols = ['ID', 'Title', 'Company_Name', 'reducer_post_ids', 'join_post_ids', 'ID1', 'Terms', 
    'Title1', 'Description_length1', 'Terms1', 'address_1', 'address_2', 'website_1', 'website_2']

    i = 0
    rows = []
    for idx, row in self.original_df.loc[:, cols].iterrows():
      _cols = row.index.intersection(cols).values.tolist()
      values = row.values
      np.place(values, row.values.astype(str) == 'nan', None)
      if (i % CHUNK_SIZE == 0):
        if(len(rows) != 0):
          writer.write_contingency(rows, _cols, 'table_contingency_entries') # write executemany
        rows = []
        rows.append(tuple(values.tolist()))
      else:
        rows.append(tuple(values.tolist()))
      i += 1
    if(len(rows) != 0):
      writer.write_contingency(rows, _cols, 'table_contingency_entries') # write executemany
    
    return None
  
def create_original_df(filename):
  return pd.read_csv(filename)

def create_score_df_title(filename, original_df, num_cols_start):
  score_df = pd.read_csv(filename)
  score_df.columns.values[num_cols_start:] = range(0,score_df.columns.values.shape[0]-num_cols_start)
  return score_df

def create_score_df_posts(filename, original_df, num_cols_start):
  score_df = pd.read_csv(filename)
  score_df.columns.values[num_cols_start:] = range(0,score_df.columns.values.shape[0]-num_cols_start)
  return score_df

def create_score_df_attrs(filename, original_df, num_cols_start):
  score_df = pd.read_csv(filename)
  score_df.columns.values[num_cols_start:] = range(0,score_df.columns.values.shape[0]-num_cols_start)
  return score_df

def melt_df(score_df, num_cols_start, score = DEFAULT_SCORE):
  cols = score_df.columns
  score_big_df = pd.melt(score_df, id_vars=score_df.columns.values[0:num_cols_start], value_vars=range(0,len(cols) - num_cols_start))
  score_big_df.drop(index=np.where(score_big_df.value < DEFAULT_SCORE)[0], axis='columns', inplace=True)
  return score_big_df

def get_reduced_original(original_df, score_big_df, score = DEFAULT_SCORE):
  return original_df.iloc[score_big_df.iloc[score_big_df.value >= score, :].variable, :] # by index

def set_original_and_score(num_cols_start, original_filename, score_filename):
  original_df = create_original_df(original_filename)
  score_df = create_score_df_title(score_filename, original_df, num_cols_start)
  score_big_df = melt_df(score_df, num_cols_start)
  def ret(resolver=ResolveDuplicates):
    resolve = resolver(score_big_df, original_df, mode = 'None')
    resolve.reduce_score()
    resolve.original_append_id_df(reducers=[resolve.reduce_by_constraints, resolve.reduce_by_matching_attrs], 
    joiners=[resolve.join_by_title])
    return resolve
  return ret

