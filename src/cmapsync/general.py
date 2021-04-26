from cmapingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import numpy as np
import pandas as pd
from datetime import date
from tqdm import tqdm


def scan_db_table(Table_Name, child_server, report_df):
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


def scan_all_tables_all_dB():
    report_df = pd.DataFrame(columns=["Table_Name", "Server", "date", "Message"])
    # metadata_tables = TR.get_metadata_tables()

    # for Table_Name in tqdm(metadata_tables):
    for Table_Name in tqdm(["tblUsers"]):

        print(Table_Name)
        for child_server in SOT.Children:
            report_df = scan_db_table(Table_Name, child_server, report_df)
    report_df.to_csv(SOT.report_dir + SOT.report_name, index=False)


scan_all_tables_all_dB()
