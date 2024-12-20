
from csverve.core import CsverveInput
import pandas as pd


standard_hmmcopy_reads_cols = [
    'chr',
    'start',
    'end',
    'cell_id',
    'gc',
    'reads',
    'copy',
    'state',
]

_categorical_cols_align = [
    'cell_id',
    'sample_id',
    'library_id',
]

_categorical_cols_hmmcopy = [
    'cell_id',
    'chr',
    'sample_id',
    'library_id',
]


def union_categories(dfs, cat_cols=None):
    """ Recreate specified categoricals on the union of categories inplace.

    Args:
        dfs (list of pandas.DataFrame): pandas dataframes to unify categoricals in-place.
    
    KwArgs:
        cat_cols (list of str): columns to unify categoricals, default None for any categorical.
    """

    # Infer all categorical columns if not given
    if cat_cols is None:
        cat_cols = set()
        for df in dfs:
            for col in df:
                if df[col].dtype.name == 'category':
                    cat_cols.add(col)

    # Get a list of categories for each column
    col_categories = collections.defaultdict(set)
    for col in cat_cols:
        for df in dfs:
            if col in df:
                col_categories[col].update(df[col].values)

    # Remove None and nan as they cannot be in the list of categories
    for col in col_categories:
        col_categories[col] = col_categories[col] - set([None, np.nan])

    # Create a pandas index for each set of categories
    for col, categories in col_categories.items():
        col_categories[col] = pd.Index(categories)

    # Set all categorical columns as having teh same set of categories
    for col in cat_cols:
        for df in dfs:
            if col in df:
                df[col] = df[col].astype('category')
                df[col] = df[col].cat.set_categories(col_categories[col])


def process_data(filepath, _categorical_cols , usecols=None):
    data = CsverveInput(filepath).read_csv(usecols=usecols)

    data.query(f"cell_id != 'reference'", inplace=True)

    #data['library_id'] = [a.split('-')[-3] for a in data['cell_id']]

    for col in _categorical_cols:
        if col in data:
            data[col] = pd.Categorical(data[col])

    return data