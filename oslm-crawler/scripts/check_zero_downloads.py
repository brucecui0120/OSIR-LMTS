import re
import jsonlines
import argparse
from loguru import logger
from pathlib import Path
from tqdm import tqdm
from oslm_crawler.ai.screenshot_checker import check_image_info, CheckRequest


parser = argparse.ArgumentParser()
parser.add_argument("--from-log", help="Recover from log files instead of using AI to check.")
parser.add_argument("path", help="The location of the files to be checked must be the raw files crawled by ModelScope.")
args = parser.parse_args()

if args.from_log:
    log_path = Path(args.from_log)
    with log_path.open('r', encoding='utf-8') as f:
        logs = f.readlines()
    buffer = []
    path = Path(args.path)
    with jsonlines.open(path, 'r') as f:
        for item in f:
            assert 'total_downloads' in item
            if item['total_downloads'] == 0 and item['img_path'] and Path(item['img_path']).exists():
                for log in logs:
                    if re.search("WARNING", log) and re.search(item['link'], log):
                        downloads = re.search(
                            r"downloads corrected from 0 to (\d*)", log).group(1)
                        downloads = int(downloads)
                        item['total_downloads'] = downloads
            buffer.append(item)
    with jsonlines.open(path, 'w') as f:
        f.write_all(buffer)
                
else:
    path = Path(args.path)
    logger.remove()
    logger.add(path.parent / 'check.log', level="DEBUG")
    buffer = []
    count = 0
    total = 0
    with jsonlines.open(path, 'r') as f:
        for item in f:
            if item['total_downloads'] == 0 and item['img_path'] and Path(item['img_path']).exists():
                total += 1
    pbar = tqdm(total=total, desc="Error correction...")
    with jsonlines.open(path, 'r') as f:
        for item in f:
            assert 'total_downloads' in item
            if item['total_downloads'] == 0 and item['img_path'] and Path(item['img_path']).exists():
                request = CheckRequest(item['img_path'], item['link'], 'ModelScope')
                response = check_image_info([request])[0]
                pbar.update(1)
                if response.downloads is not None and response.downloads > 0:
                    downloads = response.downloads
                    logger.warning(f"Data error: {item}, downloads corrected from {item['total_downloads']} to {downloads}")
                    count += 1
                    item['total_downloads'] = downloads
                elif response.downloads is not None and response.downloads == 0:
                    count += 1
                    logger.info("zero downloads.")
                else:
                    logger.error(f"Generate error: {response}")
            buffer.append(item)
    pbar.close()
    print(f'successful: {count}, total: {total}')

    with jsonlines.open(path, 'w') as f:
        f.write_all(buffer)
