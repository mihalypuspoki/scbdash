import pytest
from scbapi.scbstat import QueryInfo


@pytest.fixture
def varaibles():
    from scbapi.scbstat import QueryVariable
    variable_a = QueryVariable(code='A', text='T_A',values=['C', 'C1', 'Z'], valueTexts=['T_C','T_C1','T_Z'])
    variable_b = QueryVariable(code='B', text='T_B', values=['D', 'D1', 'Y'], valueTexts=['T_D', 'T_D1', 'T_Y'])
    variables = [variable_a,variable_b]
    return variables


@pytest.mark.parametrize(
    'texts,exp',
    [
        (['T_A', 'T_B'], ['A', 'B']),
        (['T_A'], ['A']),
        (['T_A', 'T_A'], ['A']),
        (['T_C'], []),
        ([''], []),
        ([], []),
        (None, ['A', 'B']),
    ],
)
def test_text_to_code(varaibles, texts, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.get_codes(texts=texts) == exp


@pytest.mark.parametrize(
    'codes,exp',
    [
        (['A', 'B'], ['T_A', 'T_B']),
        (['A'], ['T_A']),
        (['A', 'A'], ['T_A']),
        (['C'], []),
        ([''], []),
        ([], []),
        (None, ['T_A', 'T_B']),
    ],
)
def test_code_to_text(varaibles, codes, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.get_texts(codes=codes) == exp


@pytest.mark.parametrize(
    'value_texts,code,text,exp',
    [
        (['T_C', 'T_C1'],'A',None,['C', 'C1']),
        (['T_D'],None,'T_B',['D']),
        (['T_D'],None,None,[]),
        (['T_E'],'A',None,[]),
        ([''],None,'T_B',[]),
        ([],'A',None,[]),
        (None,None,'T_B',['D','D1','Y']),
    ],
)
def test_get_values(varaibles, value_texts, code, text, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.get_values(value_texts,code,text) == exp

@pytest.mark.parametrize(
    'values,code,text,exp',
    [
        (['C', 'C1'],'A',None,['T_C', 'T_C1']),
        (['D'],None,'T_B',['T_D']),
        (['D'],None,None,[]),
        (['E'],'A',None,[]),
        ([''],None,'T_B',[]),
        ([],'A',None,[]),
        (None,None,'T_B',['T_D','T_D1','T_Y']),
    ],
)
def test_get_value_texts(varaibles, values, code, text, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.get_value_texts(values,code,text) == exp


@pytest.mark.parametrize(
    'codes,exp',
    [
        (['A', 'B'], []),
        (['C'], ['C']),
        (['C','D'], ['C','D']),
        (['A', 'A'], []),
        ([''], []),
        ([], []),
        (None, []),
    ],
)
def test_check_codes(varaibles, codes, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.check_codes(codes=codes) == exp

@pytest.mark.parametrize(
    'texts,exp',
    [
        (['T_A', 'T_B'], []),
        (['T_C'], ['T_C']),
        (['T_C','T_D'], ['T_C','T_D']),
        (['T_A', 'T_A'], []),
        ([''], []),
        ([], []),
        (None, []),
    ],
)
def test_check_texts(varaibles, texts, exp):
    query_info = QueryInfo(title='test', variables=varaibles)
    assert query_info.check_texts(texts=texts) == exp