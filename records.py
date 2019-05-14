import numpy as np
import pandas as pd

def obtain_reduced_score(score_df, score=10):
  reduced_score_df = (score_df.iloc[:, 2:] < score).astype(bool)
  idx = np.where(reduced_score_df)
  for i, j in zip(idx[0].tolist(), idx[1].tolist()):
      reduced_score_df.iloc[i, j] = np.nan
  return reduced_score_df

def create_score_df(filename, original_df):
  score_df = pd.read_csv(filename)
  score_df.columns.values[:] = np.append(['ID', 'Title1'], original_df.index.values.tolist())
  return score_df

def replace_reduced_score(score_df):
  values = score_df.iloc[:, 4:].values
  np.place(values, score_df.iloc[:, 4:] < 10, 0)

