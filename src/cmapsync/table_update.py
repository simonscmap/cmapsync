"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-29-2021

cmapsync - table_update - Some table update functionallity. Fragment.
Requires DB functionality/connection information from cmapdata/ingest
"""


from cmapdata.ingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import pandas as pd
import os
import sqlalchemy
import numpy as np
from tqdm import tqdm
import pandas.io.sql as sql


def remove_csv(Table_Name):
    os.remove(SOT.table_specific_reports_dir + Table_Name + ".csv")


def read_sync_df(Table_Name):
    df = pd.read_csv(
        SOT.table_specific_reports_dir + Table_Name + ".csv", lineterminator="\n"
    )
    return df


def update_table(diff_df, table_name, server):
    """Function will append missing rows to table in database

    Args:
        diff_df (Pandas DataFrame): DF containing rows missing from child table
        table_name (string): Table_Name in CMAP
        server (string): CMAP server
    """

    DB.DB_modify(f"""SET IDENTITY_INSERT [{table_name}] ON""", server)
    DB.toSQLpandas(diff_df, table_name, server)
    DB.DB_modify(f"""SET IDENTITY_INSERT [{table_name}] OFF""", server)

    print(table_name, " updated")


def SQL_Merge():
    """
    MSSQL upsert solution seems to be merge

    INSERT tbl_A (col, col2)
    SELECT col, col2
    FROM tbl_B
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col);


    INSERT {table_name} tuple(list(df))
    SELECT *
    FROM {temp_table_name}
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col);




    take in diff_df?
    create temp table from diff_df

    MERGE child_table AS TARGET
    USING temp_table_diff_df AS SOURCE
    ON (TARGET.PRIMARY_KEY = SOURCE.PRIMARY_KEY)
    --When records are matched, update the records if there is any change
    WHEN MATCHED AND TARGET.ProductName <> SOURCE.ProductName OR TARGET.Rate <> SOURCE.Rate
    THEN UPDATE SET TARGET.ProductName = SOURCE.ProductName, TARGET.Rate = SOURCE.Rate
    --When no records are matched, insert the incoming records from source table to target table
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ProductID, ProductName, Rate) VALUES (SOURCE.ProductID, SOURCE.ProductName, SOURCE.Rate)

    INSERT tbl_A (col, col2)  
    SELECT col, col2
    FROM tbl_B
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col); 
    
    
    INSERT {table_name} tuple(list(df))
    SELECT *
    FROM {temp_table_name}
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col); """


DB.toSQLpandas(diff_df, f"""temp_{table_name}""", "Mariana")
tblA = "tblUsers"
tblB = "temp_tblUsers"
cols = str(tuple(list(diff_df))).replace("'", "")

sql_qry = f"""
INSERT INTO {tblA} {cols}
SELECT {cols.replace("(","",")")}
FROM {tblB}
WHERE NOT EXISTS (SELECT {cols} FROM {tblA} A2 WHERE


)"""


# r_users = DB.dbRead(f"""SELECT * FROM {table_name}""", server='Rainier')
# m_users = DB.dbRead(f"""SELECT * FROM {table_name}""", server='Mariana')

