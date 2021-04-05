from cmapingest import DB
import SOT_relations as SOT
import table_retrieval as TR
import numpy as np


"""
1. get list of all catalog tables (TR.get_metadata_tables())
2. check if table exists on server (TR.check_table_exists(table_name, server))
3. checksum tables
4. if checksum does not match, pandas df diff """

metadata_tables = TR.get_metadata_tables()

Table_Name = "tblTemporal_Resolutions"
