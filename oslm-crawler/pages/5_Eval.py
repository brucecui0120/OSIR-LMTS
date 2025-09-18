import json
import streamlit as st
import pandas as pd
from pathlib import Path

def set_multi_level_columns(df, mapping):
    new_columns = []
    for col in df.columns:
        level1, level2 = mapping.get(col, ('合计', col))
        new_columns.append((level1, level2))
    
    df.columns = pd.MultiIndex.from_tuples(new_columns)
    return df

root_path = Path(__file__).parents[1]
key_path = Path(__file__).parents[1] / 'config/ranking-key-zh.json'
with key_path.open('r', encoding='utf-8') as f:
    mapping = json.load(f)['Evaluation']

choices = []

for path in (root_path / 'data').glob("????-??-??"):
    overall_rank_path = path / 'eval-rank.csv'
    if overall_rank_path.exists():
        choices.append(path.name)
        
option = st.selectbox(
    "Select date",
    list(sorted(choices, reverse=True))
)
table_type = st.selectbox(
    "Select table type",
    ['rank', 'metrics']
)

match table_type:
    case 'rank':
        cur_path = root_path / 'data' / option / 'eval-rank.csv'
    case 'metrics':
        cur_path = root_path / 'data' / option / 'eval-summary.csv'

data = pd.read_csv(cur_path, index_col='org')
data = set_multi_level_columns(data, mapping)
data