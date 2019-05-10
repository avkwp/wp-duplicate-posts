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

def create_match_df(reduced_score_df):
  matches_df = pd.DataFrame(columns=['ID2', 'Title1', 'Title2', 'Original_Index'])
  for _, row in reduced_score_df.iterrows():
      new_row = reduced_score_df.loc[_, :].dropna()
      for i in new_row.index.values.astype(int):
          matches_df = matches_df.append({
              'ID2': score_df.loc[_, 'ID'],
              'Title1': original_df.iloc[i, 3],
              'Title2': score_df.loc[_, 'Title1'],
              'Original_Index': i,
              'Score': score_df.iloc[_, i+2],
          }, ignore_index=True)
  return matches_df

