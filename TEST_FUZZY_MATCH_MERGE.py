# # -*- coding: utf-8 -*-
# """
# Created on Thu Aug 11 06:31:29 2022

# @author: onais

import pandas as pd
from thefuzz import process

def fuzzy_match(
    df_left, df_right, column_left, column_right, threshold=60, limit=1):
    # Create a series
    series_matches = df_left[column_left].apply(
        lambda x: process.extract(x, df_right[column_right], limit=limit) )           # Creates a series with id from df_left and column name _column_left_, with _limit_ matches per item)

    # Convert matches to a tidy dataframe
    df_matches = series_matches.to_frame()
    df_matches = df_matches.explode(column_left)     # Convert list of matches to rows
    df_matches[['match_string', 'match_score', 'df_right_id']] = pd.DataFrame(df_matches[column_left].tolist(), index=df_matches.index)       # Convert match tuple to columns
    df_matches.drop(column_left, axis=1, inplace=True)      # Drop column of match tuples

    # Reset index, as in creating a tidy dataframe we've introduced multiple rows per id, so that no longer functions well as the index
    if df_matches.index.name:
        index_name = df_matches.index.name     # Stash index name
    else:
        index_name = 'index'        # Default used by pandas
    df_matches.reset_index(inplace=True)
    df_matches.rename(columns={index_name: 'df_left_id'}, inplace=True)       # The previous index has now become a column: rename for ease of reference

    # Drop matches below threshold
    df_matches.drop(
        df_matches.loc[df_matches['match_score'] < threshold].index,
        inplace=True)

    return df_matches




