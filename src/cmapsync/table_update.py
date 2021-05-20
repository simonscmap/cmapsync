# devnote:apr23,2021: When fixing tblVariables, there is still a disagreement betwen parent/child tables
# The floats seem to be slightly diff. May need a tolerence added to the join: https://stackoverflow.com/questions/48790335/how-to-merge-efficiently-two-dataframes-with-a-tolerance-in-python

# dev note apr26th:
# change retrieval logic
# if checksum doesn't match...
# does len(df1) == len(df2) (compute on DB side)
# if len doesn't match, update missing rows
# idea #1, get primary key of table from sys.information, get left outer join on ID key.
# get rows with missing ID's from parent, update child tale
#
# if len matches, one of the entries needs updating (use pandas.DataFrame.compare)
#      then find a way to use 'update' set as formatting


from cmapingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import pandas as pd
import os
import sqlalchemy
import numpy as np
from tqdm import tqdm
import pandas.io.sql as sql


def dbRead(query, server="Rainier"):
    conn, cursor = DB.dbConnect(server)
    df = sql.read_sql(query, conn, coerce_float=True)
    conn.close()
    return df


def toSQLpandas(df, tableName, server):
    conn_str = DB.pyodbc_connection_string(server)
    quoted_conn_str = DB.urllib_pyodbc_format(conn_str)
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(quoted_conn_str),
        fast_executemany=True,
    )
    df.to_sql(
        tableName,
        con=engine,
        if_exists="append",
        method="multi",
        chunksize=100,
        index=False,
    )

    # df.to_sql(
    #     tableName,
    #     con=engine,
    #     if_exists="replace",
    #     method="multi",
    #     chunksize=100,
    #     index=False,
    # )


def toSQLbcp_wrapper(df, tableName, server):
    export_path = "temp_bcp.csv"
    df.to_csv(export_path, index=False, line_terminator="\n")
    toSQLbcp(export_path, tableName, server)
    os.remove(export_path)


def toSQLbcp(export_path, tableName, server):

    usr, psw, ip, port, db_name, TDS_Version = DB.server_select_credentials(server)
    bcp_str = (
        """bcp Opedia.dbo."""
        + tableName
        + """ in """
        + """'"""
        + export_path
        + """'"""
        # + """ -e error -F 2 -c -t, -U  """
        + """ -E -e error -F 2 -c -t, -U  """
        + usr
        + """ -P """
        + psw
        + """ -S """
        + ip
        + ""","""
        + port
    )
    os.system(bcp_str)


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
    toSQLpandas(diff_df, table_name, server)
    DB.DB_modify(f"""SET IDENTITY_INSERT [{table_name}] OFF""", server)

    print(table_name, " updated")


table_name = "tblUsers"
server = SOT.Children[0]
diff_df = read_sync_df(table_name)
diff_df["Date"] = pd.to_datetime(diff_df["Date"])
# asdf = diff_df.drop('UserID',axis=1)
update_table(asdf, table_name, server)


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
    
    
    # --When there is a row that exists in target and same record does not exist in source then delete this record target
    # WHEN NOT MATCHED BY SOURCE 
    # THEN DELETE 
        
    """


# str_start = """WHERE """
# for col in list(diff_df):
#     sql_str = f"""A2.{col} = tblB.{col} AND """
#     str_start += sql_str
#     print(col)


"""
   INSERT tbl_A (col, col2)  
    SELECT col, col2
    FROM tbl_B
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col); 
    
    
    INSERT {table_name} tuple(list(df))
    SELECT *
    FROM {temp_table_name}
    WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col); 
"""
toSQLpandas(diff_df, f"""temp_{table_name}""", "Mariana")
tblA = "tblUsers"
tblB = "temp_tblUsers"
cols = str(tuple(list(diff_df))).replace("'", "")

sql_qry = f"""
INSERT INTO {tblA} {cols}
SELECT {cols.replace("(","",")")}
FROM {tblB}
WHERE NOT EXISTS (SELECT {cols} FROM {tblA} A2 WHERE


)

"""

# r_users = DB.dbRead(f"""SELECT * FROM {table_name}""", server='Rainier')
# m_users = DB.dbRead(f"""SELECT * FROM {table_name}""", server='Mariana')

# parent_df = r_users[r_users["UserID"] == 31]
# child_df = m_users[m_users["UserID"] == 31]
