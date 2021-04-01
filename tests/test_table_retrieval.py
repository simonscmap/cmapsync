import pandas as pd
from pandas._testing import assert_frame_equal
import SOT_relations as SOT


"""example from cmapingest"""


def test_check_table_exists():
    success_table_bool = check_table_exists("tblDatasets", server=SOT.Parent)
    fail_table_bool = check_table_exists("tblDOESNOTEXIST", server=SOT.Parent)
    assert success_table_bool == True, "test check table exist success case failed."
    assert fail_table_bool == False, "test check table exist fail case failed."
