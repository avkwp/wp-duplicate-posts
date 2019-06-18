import numpy as np
import pandas as pd
import re
from XMLRPC.database import Writer

MODE_TITLES = 0
MODE_IDS = 1
MODE_ATTRS = 2
DEFAULT_SCORE = 20
MELT_SCORE = 12
CHUNK_SIZE = 30
POSTCODE_REGULAR_EXPRESSION = r"\s*(([A-Z]{1,2})[0-9][0-9A-Z]?\s*([0-9])[A-Z]{2})\s*"
POSTCODE_REGEX = r"(([A-Z][0-9]{1,2})|(([A-Z][A-HJ-Y][0-9]{1,2})|(([A-Z][0-9][A-Z])|([A-Z][A-HJ-Y][0-9]?[A-Z]))))\s*([0-9][A-Z]{2})"

class ResolveDuplicates(object):

  def __init__(self, score_df, original_df, mode):
    self.score_df = score_df
    self.original_df = original_df
    self.mode = mode

  # reduce by join_post_ids and score
  def reduce_score(self, score = DEFAULT_SCORE):
    idxs = np.intersect1d(np.where(self.original_df['join_post_ids'] == 0)[0], 
    self.score_df.loc[self.score_df.value < score, 'variable'])
    mask = np.isin(self.score_df.variable, idxs)
    self.score_df.drop(labels=self.score_df[mask].index, axis='index', inplace=True)
    
  # if id is not available append_df
  def score_append_original_df(self):
    self.score_df = self.score_df.join(self.original_df, on='variable', how='inner')

  # returns appended data, exact match
  def join_by_title(self, contains_id = False):
    self.original_df.Title = self.original_df.Title.str.lower()
    self.score_df.Title1 = self.score_df.Title1.str.lower()
    df = self.score_df.join(self.original_df.set_index('Title'), on='Title1', how='right', lsuffix='_score', rsuffix='_original')
    df['Company Name'] = df.index.values
    if(contains_id):
      return df.loc[df.ID1 != df.ID]
    else:
      return df
  
  def original_append_id_df(self, reducers, joiners, post_reducers, contains_id = False):
    self.score_append_original_df()
    def join_list(a):
      return "|".join(self.score_df.loc[self.score_df.variable == a, 'ID1'].values.astype(str).tolist())
    if len(self.score_df) > 0:
      for reducer in reducers:
        reducer(contains_id)
    join_vector = np.vectorize(join_list)
    self.original_df['reducer_post_ids'] = join_vector(range(self.original_df.shape[0]))
    
    for joiner in joiners:
      self.original_df = joiner(contains_id)
    self.original_df['join_post_ids'] = self.original_df.loc[:, 'ID1']
    self.original_df.loc[(self.original_df.loc[:, 'ID1'].astype(str) == 'nan'), 'join_post_ids'] = 0
    self.original_df['join_post_ids'] = self.original_df['join_post_ids'].astype(np.int64)

    if len(self.score_df) > 0:
      for post_reducer in post_reducers:
        post_reducer(contains_id)
    
    return None

  # returns appended data, description comparison and terms comparison to merge
  def reduce_by_constraints(self, original_df = None, contains_id = False):
    if(contains_id):
      if len(self.score_df) > 0:
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
      if len(self.score_df) > 0:
        l = np.vectorize(len)
        desc = self.score_df.loc[:, 'Description'].astype(str)
        desc.loc[desc == 'nan'] = ""
        # assumes the score_df to be appended
        self.score_df['Description_length'] = l(desc)
        self.score_df['desc1_constraint'] = 0
        self.score_df['desc2_constraint'] = 0
        self.score_df.loc[self.score_df.Description_length1 > self.score_df.Description_length, 'desc1_constraint'] = 1
        self.score_df.loc[self.score_df.Description_length >= self.score_df.Description_length1, 'desc2_constraint'] = 1

    return None

  def reduce_post_by_constraints(self, original_df = None, contains_id = False):
    def split_reducer(a):
      t = tuple(a.split("|"))
      cond = np.ones(self.score_df.shape[0]).astype(bool)
      for t1 in t:
        cond |= (self.score_df.ID1 == t1)
      max_desc = self.score_df.loc[cond, 'Description_length1'].max()
      self.score_df.loc[cond, :].loc[self.score_df.Description_length1 == max_desc, 'desc1_constraint'] = 1
      self.score_df.loc[cond, :].loc[self.score_df.Description_length1 != max_desc, 'desc1_constraint'] = 0
      
      return max_desc

    def matching_attrs_reducer(w, p):
      return "|".join(self.score_df.loc[
        ((w != "") & (w is not None) & (w != 'nan') & (self.score_df.website_1 == w)) | 
        ((p != "") & (p is not None) & (p != 'nan') & (self.score_df.postcodes1 == p)), 
        'ID1'].values.astype(str).tolist())
      
    if not contains_id:
      s = np.vectorize(split_reducer)
      s(self.original_df.reducer_post_ids.values)
      mattr = np.vectorize(matching_attrs_reducer)
      self.original_df['matching_attrs'] = \
      mattr(self.original_df.Company_URL_original.astype(str), self.original_df.Postcode_original.astype(str))
    

  # to report
  def reduce_by_matching_attrs(self, contains_id = False):
    postcode_regex = re.compile(POSTCODE_REGULAR_EXPRESSION)

    postcodes = []
    for address in self.score_df.address_1:
      r = postcode_regex.findall(address)
      if(len(r) == 0):
        postcodes.append("")
      else:
        postcodes.append(r[0][0])
    self.score_df['postcodes1'] = postcodes
    if('address_2' in self.score_df.columns.values):
      postcodes = []
      for address in self.score_df.address_2:
        r = postcode_regex.findall(address)
        if(len(r) == 0):
          postcodes.append("")
        else:
          postcodes.append(r[0][0])
      self.score_df['postcodes2'] = postcodes

    self.score_df['matching_attrs'] = 0

  def resolve_duplicates(self, session_id, writer, contingency_write=False, feed=''):
    cols = ['ID1', 'Title1', 'ID2', 'Title2', 'Terms1', 'Terms2', 
    'Description_length1', 'Description_length2', 'Description_length', 'Terms',
    'address_1', 'website_1', 'address_2', 'website_2', 
    'postcodes1', 'postcodes2', 'term1_constraint', 'term2_constraint', 'desc1_constraint', 
    'desc2_constraint', 'value']
    
    writer.delete_contingency(session_id, 'table_contingency_duplicates')
    writer.delete_contingency(session_id, 'table_contingency_entries')

    i = 0
    rows = []
    writer.delete_contingency(session_id, 'table_contingency_duplicates')
    for idx, row in self.score_df.loc[:, cols].iterrows():
      _cols = row.index.intersection(cols).values.tolist() + ['session_token', 'feed']
      values = row.values
      np.place(values, values.astype(str) == 'nan', None)
      if (i % CHUNK_SIZE == 0):
        if(len(rows) != 0):
          writer.write_contingency(rows, _cols, 'table_contingency_duplicates') # write executemany
        rows = []
        rows.append(tuple(values.tolist() + [session_id, feed]))
      else:
        rows.append(tuple(values.tolist() + [session_id, feed]))
      i += 1
    if(len(rows) != 0):
      writer.write_contingency(rows, _cols, 'table_contingency_duplicates') # write executemany

    cols = ['ID', 'Title', 'Company_Name', 'reducer_post_ids', 'join_post_ids', 'ID1', 'Terms', 
    'Title1', 'Description_length1', 'Terms1', 'address_1', 
    'address_2', 'website_1', 'website_2', 'matching_attrs']

    i = 0
    rows = []
    writer.delete_contingency(session_id, 'table_contingency_entries')
    for idx, row in self.original_df.loc[:, cols].iterrows():
      _cols = row.index.intersection(cols).values.tolist() + ['session_token', 'feed']
      values = row.values
      np.place(values, values.astype(str) == 'nan', None)
      if (i % CHUNK_SIZE == 0):
        if(len(rows) != 0):
          writer.write_contingency(rows, _cols, 'table_contingency_entries') # write executemany
        rows = []
        rows.append(tuple(values.tolist() + [session_id, feed]))
      else:
        rows.append(tuple(values.tolist() + [session_id, feed]))
      i += 1
    if(len(rows) != 0):
      writer.write_contingency(rows, _cols, 'table_contingency_entries') # write executemany
    
    return None
  
def create_original_df(filename):
  return pd.read_csv(filename)

def create_score_df_title(score_df, original_df, num_cols_start):
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

def melt_df(score_df, num_cols_start, score = MELT_SCORE):
  cols = score_df.columns
  score_big_df = pd.melt(score_df, id_vars=score_df.columns.values[0:num_cols_start], value_vars=range(0,len(cols) - num_cols_start))
  score_big_df.drop(index=np.where(score_big_df.value.astype(np.float32) < score)[0], axis='columns', inplace=True)
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

def set_feed_and_score(num_cols_start, original_filename, score_df, score=MELT_SCORE):
  original_df = create_original_df(original_filename)
  score_df = create_score_df_title(score_df, original_df, num_cols_start)
  score_big_df = melt_df(score_df, num_cols_start, score)
  def ret(resolver=ResolveDuplicates):
    resolve = resolver(score_big_df, original_df, mode = 'None')
    resolve.original_append_id_df(
      reducers=[resolve.reduce_by_constraints, resolve.reduce_by_matching_attrs], 
      joiners=[resolve.join_by_title], 
      post_reducers=[resolve.reduce_post_by_constraints])
    resolve.reduce_score()
    return resolve
  return ret