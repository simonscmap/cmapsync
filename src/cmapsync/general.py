"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-29-2021

cmapsync - general.Py - General function for DB table/index scanning.
Requires functionality from cmapdata/ingest
"""

from cmapdata.ingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import numpy as np
import pandas as pd
from datetime import date
from tqdm import tqdm


def scan_db_table(Table_Name, child_server, report_df):
    """Compares tables from parent/child DB's and generates a report.

    Args:
        Table_Name (str): CMAP table name
        child_server (str): Valid CMAP server name. Designate in SOT relations.
        report_df (Pandas DataFrame): report dataframe to append information

    Returns:
        report_df (Pandas DataFrame): report dataframe with information appended
    """
    # check that table exists on parent/child DBs
    table_exists_bool_parent = TR.check_table_exists(Table_Name, SOT.Parent)
    table_exists_bool_child = TR.check_table_exists(Table_Name, child_server)
    # if table exists on parent/child DB..
    if table_exists_bool_parent == True and table_exists_bool_child == True:
        checksum_tables = TR.checksum(Table_Name, SOT.Parent, child_server)
        # if checksum of tables disagrees
        if checksum_tables is not None:
            # retrieve parentDB_df and childDB_df
            parent_df = TR.retrieve_table(Table_Name, SOT.Parent)
            child_df = TR.retrieve_table(Table_Name, child_server)
            # retrieve parentDB and childDB index
            parent_index = TR.retrieve_index_constraints(Table_Name, SOT.Parent)
            child_index = TR.retrieve_index_constraints(Table_Name, child_server)
            # check for differing rows between parent and child DB tables.
            missing_rows_from_child = TR.diff_between_parent_child_df(
                parent_df, child_df
            )
            # check for differing indicies between parent and child DB tables
            missing_index_from_child = TR.diff_between_parent_child_df(
                parent_index, child_index
            )

            if missing_rows_from_child.empty == False:
                missing_rows_from_child.to_csv(
                    f"""{SOT.table_specific_reports_dir}{Table_Name}.csv""",
                    index=False,
                )
                report_df.loc[len(report_df)] = [
                    Table_Name,
                    child_server,
                    date.today(),
                    f"""Row diff of {len(missing_rows_from_child)} between parent and child DBs""",
                ]

            if missing_index_from_child.empty == False:
                missing_index_from_child.to_csv(
                    f"""{SOT.table_specific_reports_dir}{Table_Name}_{SOT.Parent}_{child_server}_index_mismatch.csv""",
                    index=False,
                )
                report_df.loc[len(report_df)] = [
                    Table_Name,
                    child_server,
                    date.today(),
                    f"""Indicies between parent and child DBs differ: {missing_index_from_child.to_string()}""",
                ]
    else:
        report_df.loc[len(report_df)] = [
            Table_Name,
            child_server,
            date.today(),
            "Table does not exist",
        ]
    return report_df


def scan_db_data_table(Table_Name, child_server, report_df):
    # check that table exists on parent/child DBs
    table_exists_bool_parent = TR.check_table_exists(Table_Name, SOT.Parent)
    table_exists_bool_child = TR.check_table_exists(Table_Name, child_server)
    if table_exists_bool_parent == False:
        report_df.loc[len(report_df)] = [
            Table_Name,
            SOT.parent,
            date.today(),
            "Table does not exist",
        ]
    if table_exists_bool_child == False:
        report_df.loc[len(report_df)] = [
            Table_Name,
            child_server,
            date.today(),
            "Table does not exist",
        ]
    return report_df


def scan_all_tables_all_dB():
    """Wrapper function for scan_db_table function. Iterates through tables, generating comparative reports. 
    """
    report_df = pd.DataFrame(columns=["Table_Name", "Server", "Date", "Message"])
    metadata_tables = TR.get_metadata_tables()
    print("scanning metadata tables")
    for Table_Name in tqdm(metadata_tables):
        print(Table_Name)
        report_df = scan_db_metadata_table(Table_Name, SOT.Children[0], report_df)

    print("scanning data tables")
    data_tables = TR.get_catalog_datasets()
    for Table_Name in tqdm(data_tables):
        print(Table_Name)
        report_df = scan_db_data_table(Table_Name, SOT.Children[0], report_df)

    report_df.to_csv(SOT.report_dir + SOT.report_name, index=False)


# scan_all_tables_all_dB()
