import csv
import yaml
import argparse
from itertools import dropwhile
from pathlib import Path

parser = argparse.ArgumentParser()

parser.add_argument("src", type=str)
parser.add_argument('-o', required=True, type=str)

args = parser.parse_args()

src_path = Path(args.src)
tgt_path = Path(args.o)

with src_path.open('r', encoding='utf-8') as f:
    csv_reader = csv.reader(f)
    next(csv_reader)
    next(csv_reader)
    
    data = []
    for row in csv_reader:
        row = list(dropwhile(lambda x: x in ('', None), reversed(row)))[::-1]
        data.append(row)

config_path = Path(__file__).parents[1] / 'config/default_task.yaml'
with config_path.open('r') as f:
    config = yaml.safe_load(f)
header = ['org']
header.extend(list(config['MergeAndRankingPipeline']['ranking']['eval_config'][1].keys()))

tgt_path = tgt_path / 'eval-summary.csv'
with tgt_path.open('w', newline='', encoding='utf-8') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(header)
    csv_writer.writerows(data)
