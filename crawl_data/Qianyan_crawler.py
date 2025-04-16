# -*- coding: utf-8 -*-
# @Time       : 2024/11/11 14:38
# @Author     : Marverlises
# @File       : Qianyan_crawler.py
# @Description: 爬取千言数据集信息
import os
import time
import json
import logging
from string import Template
from selenium.webdriver.common.by import By
from utils import init_driver, extract_pdf_link, extract_arxiv_link


class QianyanCrawler:
    """
    爬取千言数据集信息
    """

    def __init__(self, driver: object, base_url: str, dataset_num: int, screenshot_save_dir: str = ''):
        self.driver = driver
        self.base_url = base_url
        self.dataset_num = dataset_num
        self.screen_shot_save_path = screenshot_save_dir
        self._init_logger()
        self._init_relevant_element_xpath()
        if not os.path.exists(screenshot_save_dir):
            os.makedirs(screenshot_save_dir)

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = '../../gabage/logs/Qianyan'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'Qianyan_crawl_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling Qianyan dataset info")

    def _init_relevant_element_xpath(self) -> None:
        # 任务, 下载次数, 大小, 使用限制, 特征 的xpath模板字符串
        self.small_tags_info_xpath = Template('//*[@id="root"]/div[2]/div/div[1]/ul/li[$index]/span/strong')

    def crawl(self):
        result = {}
        for i in range(1, self.dataset_num):
            try:
                link = f'{self.base_url}?id={i}'
                driver.get(link)

                time.sleep(0.5)
                div = driver.find_element(By.CSS_SELECTOR, 'div.detail_top_left')
                title = div.find_element(By.CSS_SELECTOR, 'strong.detail_title').text
                description = div.find_element(By.CSS_SELECTOR, 'span.detail_desc').text
                release_time = div.find_element(By.CSS_SELECTOR, 'span.detail_time').text

                # 一些数据集相关的信息
                driver.save_screenshot(f"{self.screen_shot_save_path}/{title}.png")
                task = driver.find_element(By.XPATH, self.small_tags_info_xpath.substitute(index=1)).text
                download_count = driver.find_element(By.XPATH, self.small_tags_info_xpath.substitute(index=2)).text
                size = driver.find_element(By.XPATH, self.small_tags_info_xpath.substitute(index=3)).text
                use_restriction = driver.find_element(By.XPATH, self.small_tags_info_xpath.substitute(index=4)).text
                feature = driver.find_element(By.XPATH, self.small_tags_info_xpath.substitute(index=5)).text

                dataset_information = driver.find_element(By.XPATH, r'//*[@id="root"]/div[2]/div/div[2]').text
                pdf_link = extract_pdf_link(dataset_information) if extract_pdf_link(
                    dataset_information) else extract_arxiv_link(dataset_information)
                # 截取相关论文的pdf图片
                if pdf_link:
                    driver.get(pdf_link)
                    time.sleep(7)
                    driver.save_screenshot(f"{self.screen_shot_save_path}/{title}_pdf.png")

                result[i] = {
                    'link': link,
                    'title': title,
                    'description': description,
                    'release_time': release_time,
                    'task': task,
                    'download_count': download_count,
                    'size': size,
                    'use_restriction': use_restriction,
                    'feature': feature,
                    'pdf_link': pdf_link,
                    'dataset_information': dataset_information.replace('"', "").replace("'", ""),
                    'paper_screenshot_save_path': f"result/qianyan/qianyan_dataset_info_screenshots/{title}_pdf.png" if pdf_link else '',
                    'dataset_screenshot_save_path': f"result/qianyan/qianyan_dataset_info_screenshots/{title}.png"
                }
                logging.info(f"Got dataset: {link}")
            except Exception as e:
                logging.error(f"Error: {e}, when crawling {link}")
        return result


if __name__ == '__main__':
    # 发布的数据集总个数
    end_point = 74
    base_url = r'https://www.luge.ai/#/luge/dataDetail'
    driver = init_driver(headless=False)
    crawler = QianyanCrawler(driver, base_url, end_point,
                             screenshot_save_dir='../../gabage/result/qianyan/qianyan_dataset_info_screenshots')
    result = crawler.crawl()

    # 保存数据集链接
    if not os.path.exists('../../gabage/result/qianyan'):
        os.makedirs('../../gabage/result/qianyan')
    with open('../../gabage/result/qianyan/qianyan.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=4)
