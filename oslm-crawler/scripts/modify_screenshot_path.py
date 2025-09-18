import argparse
import jsonlines
from pathlib import Path

def main(ss_path):
    data_base_path = Path(__file__).parents[1] / 'data'
    for file in data_base_path.rglob('*.jsonl'):
        data = []
        with jsonlines.open(file, 'r') as f:
            for item in f:
                if item.get('img_path') is not None:
                    if ss_path:
                        item['img_path'] = str(ss_path / item['img_path'].split('screenshots')[-1])
                    else:
                        item['img_path'] = None
                data.append(item)
        with jsonlines.open(file, 'w') as f:
            f.write_all(data)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, help='`screenshots` dir path, for example: /root/oslm-crawler/screenshots')
    parser.add_argument("--null", action="store_true", help='Set all img_path fields to null')
    args = parser.parse_args()
    
    if args.null:
        screenshot_path = None
    elif args.src:
        screenshot_path = Path(args.src)
    else:
        screenshot_path = Path(__file__).parents[2] / 'screenshots'
        
    main(screenshot_path)
