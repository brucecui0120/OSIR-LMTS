# -*- coding: utf-8 -*-
# @Time       : 2024/11/21 11:27
# @Author     : Marverlises
# @File       : Github_crawler.py
# @Description: 算子相关GitHub爬虫
import json
import os
import logging
import tqdm
from selenium.webdriver.common.by import By
from utils import init_driver, save_json_data


class GithubCrawler:
    """
    爬取算子相关的GitHub仓库
    """

    def __init__(self, headless=True, base_dir='./result/Github', log_level=logging.INFO,
                 need_crawl_links_file='./organization_links/github_links.json'):
        """
        初始化
        :param headless:                    是否使用无头浏览器
        :param base_dir:                    结果存储目录
        :param log_level:                   日志级别
        :param need_crawl_links_file:       需要爬取的数据集链接文件
        """
        self.need_crawl_links_file = need_crawl_links_file
        self.driver = init_driver(headless=headless)
        self.base_dir = base_dir
        self.screen_shot_save_path = self.base_dir + '/Github_info_screenshots'
        self._init_logger(log_level=log_level)
        self._init_relevant_element_xpath()
        if os.path.exists(self.screen_shot_save_path) is False:
            os.makedirs(self.screen_shot_save_path)

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = './logs/Github'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'Github_crawl_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling Github info")

    def _init_relevant_element_xpath(self) -> None:
        """
        初始化相关元素的Xpath
        :return:
        """
        # 右侧边栏base xpath
        self.right_sidebar_base_xpath = '//*[@id="repo-content-pjax-container"]/div/div/div/div[2]/div/div[1]/div/div'
        # star
        self.related_star_xpath = '//*[@id="repository-details-container"]/ul'
        # readme
        self.readme_xpath = '//*[@id="repo-content-pjax-container"]/div/div/div/div[1]/react-partial/div/div/div[3]/div[2]/div/div[2]'
        # issues
        self.issue_xpath = '//*[@id="repository-container-header"]/nav/ul/li[2]'
        # pull requests
        self.pull_requests_xpath = '//*[@id="repository-container-header"]/nav/ul/li[3]'
        # last commit
        self.last_commit_xpath = '//*[@id="repo-content-pjax-container"]/div/div/div/div[1]/react-partial/div/div/div[3]/div[1]/table/tbody/tr[1]/td/div/div[2]/div[1]'
        # commits
        self.commits_xpath = '//*[@id="repo-content-pjax-container"]/div/div/div/div[1]/react-partial/div/div/div[3]/div[1]/table/tbody/tr[1]/td/div/div[2]/div[2]'

    def _read_github_links(self):
        """
        读取需要爬取的数据集链接
        :return:    数据集链接
        """
        with open(self.need_crawl_links_file, 'r') as f:
            links = json.load(f)
        return links

    def crawl_github_info(self):
        """
        爬取数据集信息
        :return:
        """
        result = {}
        exception_links = []
        # 读取需要爬取的数据集链接
        links = self._read_github_links()
        # 爬取数据集信息
        for link in tqdm.tqdm(links):
            try:
                self.driver.get(link)
                # 截图
                self.driver.save_screenshot(f'{self.screen_shot_save_path}/{link.split("/")[-1]}.png')
                # 爬取数据集信息
                result[link] = {}
                commits, issues, last_commit, pull_requests, readme, sidebar_data, star_watch_fork = self._crawl_data_items()
                result[link]['star_watch_fork'] = star_watch_fork
                result[link]['sidebar_data'] = sidebar_data
                result[link]['issues'] = issues
                result[link]['pull_requests'] = pull_requests
                result[link]['last_commit'] = last_commit
                result[link]['commits'] = commits
                result[link]['readme'] = readme
                logging.info(f"Finish crawling {link}")
            except Exception as e:
                logging.error(f"Error: {e}, when crawling {link}")
                exception_links.append(link)

        return result, exception_links

    def _crawl_data_items(self):
        """
        爬取每个数据项
        """
        # 遍历顶部的star、fork、watch——可能有些没有
        star_watch_fork_elements = self.driver.find_element(By.XPATH, self.related_star_xpath)
        each_element = star_watch_fork_elements.find_elements(By.XPATH, 'li')  # 三个元素
        star_watch_fork = [element.text for element in each_element]
        # 遍历右侧边栏，取出每一对h3和div作为key和value
        right_sidebar_elements = self.driver.find_element(By.XPATH, self.right_sidebar_base_xpath)
        h3_elements = right_sidebar_elements.find_elements(By.XPATH, 'h3')
        # 遍历每个h3标签，获取它后面紧随的div的文本
        sidebar_data = {}
        for h3 in h3_elements:
            # 对于每个h3，找到紧接着它的div
            div = h3.find_element(By.XPATH, 'following-sibling::div')
            sidebar_data[h3.text] = div.text.strip()  # 去掉可能的多余空白字符
        # 爬取issues, pull requests, last commit, commits
        issues = self.driver.find_element(By.XPATH, self.issue_xpath).text
        pull_requests = self.driver.find_element(By.XPATH, self.pull_requests_xpath).text
        last_commit = self.driver.find_element(By.XPATH, self.last_commit_xpath).text
        commits = self.driver.find_element(By.XPATH, self.commits_xpath).text
        # 爬取整个readme
        readme = self.driver.find_element(By.XPATH, self.readme_xpath).text
        return commits, issues, last_commit, pull_requests, readme, sidebar_data, star_watch_fork


if __name__ == '__main__':
    # 爬取Github信息
    Github_crawler = GithubCrawler(headless=True, base_dir='./result/Github')
    Github_info, exception_links = Github_crawler.crawl_github_info()

    logging.info(f"Total Github info: {len(Github_info)}")
    logging.info(f"Exception links: {exception_links}")
    # 存储
    save_json_data(Github_info, './result/Github/Github_info.json')
    save_json_data(exception_links, './result/Github/exception_links.json')
