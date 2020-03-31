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

def xyz_label(df, name, dependent, splits):
    """
    :param df: pandas dataframe
    :param name: name of dependent variable
    :param dependent: dependent variable
    :param splits: array of splits
    :return df: pandas dataframe with bucketed dependent variables
    """
    df[name] = np.searchsorted(splits, df[dependent].values)

return(df)

def filter_columns():
    "Mary: Python version"
    return(df)
