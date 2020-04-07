import numpy as np

# add filter and xyz renaming
def clean_XYZ(df,cols):
  """
  :param df: pandas DataFrame
  :param cols: list of columns to cleaned
  :return: pandas DataFrame
  """
  for i in cols:
    df = df.replace({i:{1:'X',2:'Y',3:'Z',0:'Z'}})
  return df

def xyz_label(df, dependent, splits):
    """
    :param df: pandas dataframe
    :param dependent: dependent variable
    :param splits: array of splits
    :return labeled: pandas column
    """
    labeled = np.searchsorted(splits, df[dependent].values)

return(labeled)

def filter_columns():
    "Mary: Python version"
    return(df)
