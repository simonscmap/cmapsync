from cmapingest import DB
import SOT_relations as SOT
import pandas as pd

"""Future code:
When ran, will pull dataframes from SOT.table_specific_reports_dir as well as the general report df
For each of these, a basic summary (# of row mismatch, server, table_name etc.) will be shown to user.
User will then OK or Reject a table update to the child table
"""
# test table on rainier: tblSyncTest


def read_sync_df(Table_Name):
    df = pd.read_csv(SOT.table_specific_reports_dir + Table_Name + ".csv")
    return df


def update_table(diff_df, table_name, server):
    """Function will append missing rows to table in database

    Args:
        diff_df (Pandas DataFrame): DF containing rows missing from child table
        table_name (string): Table_Name in CMAP
        server (string): CMAP server
    """
    DB.toSQLpandas(diff_df, table_name, server)


Table_Name = "tblSensors"
server = SOT.Children[0]

diff_df = read_sync_df(Table_Name)
# update_table(diff_df, Table_Name, server)
