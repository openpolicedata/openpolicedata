
def test_all_datasets_tested(is_excel, is_api):
    x = (is_excel+is_api)!=1
    numleft = x.sum()
    assert numleft==0, f'{numleft} datasets not used or used more than once'