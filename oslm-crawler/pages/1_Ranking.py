import streamlit as st
import pandas as pd
from pathlib import Path

root_path = Path(__file__).parents[1]
choices = []

for path in (root_path / 'data').glob("????-??-??"):
    overall_rank_path = path / 'overall-rank.csv'
    if overall_rank_path.exists():
        choices.append(path.name)
        
option = st.selectbox(
    "Select date",
    list(sorted(choices, reverse=True))
)
accumulate = st.toggle("Accumulate")

if accumulate:
    cur_path = root_path / 'data' / option / 'overall-accumulated-rank.csv'
else:
    cur_path = root_path / 'data' / option / 'overall-rank.csv'

data = pd.read_csv(cur_path, index_col='org')
data