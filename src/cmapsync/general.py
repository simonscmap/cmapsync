from cmapingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import numpy as np


"""
1. get list of all catalog tables (TR.get_metadata_tables())
2. check if table exists on server (TR.check_table_exists(table_name, server))
3. checksum tables(TR.)
4. if checksum does not match
    5.pandas df diff 

    
    """

metadata_tables = TR.get_metadata_tables()

Table_Name = "tblSensors"
table_exists_bool_parent = TR.check_table_exists(Table_Name, SOT.Parent)
table_exists_bool_child = TR.check_table_exists(Table_Name, SOT.Children[0])

mismatch_dict = TR.checksum(Table_Name, SOT.Parent, SOT.Children[0])

parent_df = TR.retrieve_table(Table_Name, SOT.Parent)
child_df = TR.retrieve_table(Table_Name, SOT.Children[0])

parent_index = TR.retrieve_index_constraints(Table_Name, SOT.Parent)
child_index = TR.retrieve_index_constraints(Table_Name, SOT.Children[0])

missing_rows_from_child = TR.diff_between_parent_child_df(parent_df, child_df)
