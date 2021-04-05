import pandas as pd
from pandas._testing import assert_frame_equal
from cmapsync import table_retrieval as TR
from cmapsync import SOT_relations as SOT


def test_check_table_exists():
    success_table_bool = TR.check_table_exists("tblDatasets", server=SOT.Parent)
    fail_table_bool = TR.check_table_exists("tblDOESNOTEXIST", server=SOT.Parent)
    assert success_table_bool == True, "test check table exist success case failed."
    assert fail_table_bool == False, "test check table exist fail case failed."


def test_diff_between_parent_child_df():
    parent_df = pd.DataFrame({"ID": [0, 1, 2, 3], "value": ["a", "b", "c", "d"]})
    child_df = pd.DataFrame({"ID": [0, 3], "value": ["a", "d"]})
    expected_df = pd.DataFrame({"ID": [1, 2], "value": ["b", "c"]})
    func_df = TR.diff_between_parent_child_df(parent_df, child_df)
    assert_frame_equal(
        expected_df, func_df, obj="test_diff_between_parent_child_df failed"
    )
