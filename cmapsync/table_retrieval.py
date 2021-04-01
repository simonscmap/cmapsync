from cmapingest import DB
import SOT_relations as SOT
import numpy as np


def check_table_exists(Table_Name, Server):
    """Returns a bool depending if table exists on specified server

    Returns:
        Boolean: True/False
    """
    qry = (
        f"""SELECT * FROM information_schema.tables WHERE table_name = '{Table_Name}'"""
    )
    df = DB.dbRead(qry, server=Server)
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
