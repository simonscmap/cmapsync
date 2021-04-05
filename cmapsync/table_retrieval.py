from cmapingest import DB
import SOT_relations as SOT
import numpy as np
import pandas as pd


def checksum_tables(Table_Name):
    if len(SOT.Children) > 1:
        pass
        # checksum for all tables


def checksum(Table_Name, Parent_Server, Child_Server):
    """Takes Table name and name of two servers, computes checksum and returns dict if

    Returns:
        Dict: Dictionary containing Table_Name as well as server names, or None.
    """

    qry = f"""SELECT SUM(CAST(CHECKSUM(*) AS BIGINT)) from [{Table_Name}]"""
    parent_checksum = DB.dbRead(qry, server=Parent_Server)
    child_checksum = DB.dbRead(qry, server=Child_Server)
    if parent_checksum.iloc[0][0] != child_checksum.iloc[0][0]:
        checksum_result_dict = {
            "Table_Name": Table_Name,
            "Parent_Server": Parent_Server,
            "Child_Server": Child_Server,
        }
    else:
        checksum_result_dict = None
    return checksum_result_dict


def retrieve_diff_between_tables(Table_Name, Parent_Server, Child_Server):
    """Retrieves table from parent and child databases, finds differance between both
    Returns:
        Pandas DataFrame: Result diff DataFrame
    """
    qry = f"""SELECT * FROM {Table_Name}"""
    parent_df = DB.dbRead(qry, server=Parent_Server)
    child_df = DB.dbRead(qry, server=Child_Server)
    merged = parent_df.merge(child_df, indicator=True, how="left")
    missing_rows_from_child = merged.loc[merged["_merge"] != "both"].drop(
        ["_merge"], axis=1
    )
    return missing_rows_from_child


def check_table_exists(Table_Name, server):
    """Returns a bool depending if table exists on specified server

    Returns:
        Boolean: True/False
    """
    qry = (
        f"""SELECT * FROM information_schema.tables WHERE table_name = '{Table_Name}'"""
    )
    df = DB.dbRead(qry, server=server)
    if df.empty == True:
        table_exists_bool = False
    else:
        table_exists_bool = True
    return table_exists_bool


def get_catalog_datasets():
    """This function returns a list of datasets from uspCatalog. Server/SOT is supplied from SOT_relations.py
    Returns:
        Pandas Series : Series of all tables from uspCatalog. 
    """
    SOT_catalog_datasets = DB.dbRead("""EXEC uspCatalog""", server=SOT.Parent)[
        "Table_Name"
    ].unique()
    return SOT_catalog_datasets


def get_DB_tables():
    """Returns all tables in database
    Returns:
        Pandas Series: Series of all tables in DB
    """
    SOT_tables = DB.dbRead(
        f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='{SOT.db}' AND TABLE_NAME <> 'sysdiagrams'""",
        server=SOT.Parent,
    ).iloc[:, 0]
    return SOT_tables


def get_metadata_tables():
    """Returns Series of all metadata only tables in DB

    Returns:
        Pandas Series: Returns Series of metadata only tables
    """
    SOT_catalog_datasets = get_catalog_datasets()
    SOT_tables = get_DB_tables()
    metadata_tables = np.sort(list(set(SOT_catalog_datasets) ^ set(SOT_tables)))
    return metadata_tables


metadata_tables = get_metadata_tables()
