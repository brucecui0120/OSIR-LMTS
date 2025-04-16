# -*- coding: utf-8 -*-
# @Time       : 2024/11/20 15:25
# @Author     : Marverlises
# @File       : Huggingface_crawler.py
# @Description: 爬取Huggingface数据集信息

import os
import logging
import json
import re
import time
import pandas as pd
import tqdm
import pickle

from typing_extensions import override
from crawl_data.Data_post_process import DataPostProcess
from utils import init_driver, clean_text
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


class HuggingfaceCrawler:
    """
    爬取Huggingface数据集信息，首先需要爬取数据集链接，然后再爬取数据集信息，如果已经爬取了数据集链接，则直接爬取数据集信息
    数据集链接文件：hugging_face_organization_datasets_links.json
    """

    def __init__(self, headless=True,
                 organization_links_file_path='organization_links/hugging_face_organization_links.json',
                 sort_method='downloads', save_dir='result/huggingface',
                 organization_datasets_links_save_file='hugging_face_organization_datasets_links.json',
                 logging_cookie_file_path='./huggingface_cookies.pkl', get_arxiv=False):
        """
        初始化
        :param headless:                        是否启用无头模式
        :param organization_links_file_path:    机构链接文件路径
        :param sort_method:                     排序方法-[updated, created, alphabetical, likes, downloads, rowsMost, rowsLeast]
        :param save_dir:                        保存目录
        :param organization_datasets_links_save_file:  机构数据集链接保存文件
        :param logging_cookie_file_path:        登录cookie文件路径
        :param get_arxiv:                       是否获取arxiv论文信息
        """
        self.driver = init_driver(headless)
        self.organization_links_file_path = organization_links_file_path
        self.sort_method = sort_method
        self.save_dir = save_dir
        self.organization_datasets_links_save_file = organization_datasets_links_save_file
        self.logging_cookie_file_path = logging_cookie_file_path
        self.get_arxiv = get_arxiv
        # 初始化logger，初始化相关元素的Xpath
        self._init_logger(log_level=logging.INFO)
        self._init_relevant_element_xpath()
        # 创建保存截图的文件夹
        if not os.path.exists(f'{self.save_dir}/hugging_face_dataset_info_screenshots'):
            os.makedirs('./result/huggingface/hugging_face_dataset_info_screenshots')
        self.screen_shot_save_path = f'{self.save_dir}/hugging_face_dataset_info_screenshots'

    def _init_relevant_element_xpath(self) -> None:
        """
        初始化相关元素的Xpath
        :return:
        """
        # =========================== _crawl_dataset_links相关的 ================================
        # expand all按钮
        self.expand_all_button_xpath = '//*[@id="datasets"]/div/div[2]/div/button'
        # 每一个数据集的div
        self.dataset_item_xpath = '//*[@id="datasets"]/div/div/article'
        # =========================== _crawl_dataset_info相关的 ================================
        # 如果需要填写表单，则填写表单的元素
        self.need_finish_form_xpath = '/html/body/div/main/div[2]/section[1]/div[1]/div/form'
        # 如果需要填写表单，则填写表单的元素
        self.form_items_xpath = '/html/body/div/main/div[2]/section[1]/div[1]/div/form/label'
        # 如果获取数据集详情需要填写表单并点击按钮
        self.finish_form_button_xpath = '/html/body/div/main/div[2]/section[1]/div[1]/div/form/div/button'
        # tags信息
        self.tags_info_xpath = '/html/body/div/main/div[1]/header/div/div[1]/div'
        # 右侧面板信息
        self.data_info_div_xpath = '//div[@class="flex flex-col flex-wrap xl:flex-row"]'
        # 下载量
        self.download_count_xpath = '/html/body/div/main/div[2]/section[2]/dl/dd'
        # 社区活跃
        self.community_xpath = '/html/body/div/main/div[1]/header/div/div[2]/div/a[last()]'
        # 点赞数
        self.like_xpath = '/html/body/div/main/div[1]/header/div/h1/div[3]/button[2]'

    def crawl_dataset_links(self) -> None:
        """
        爬取数据集链接
        :return:
        """
        # 读取机构链接
        with open(self.organization_links_file_path, 'r', encoding='utf-8') as f:
            crawl_targets = json.load(f)

        organization_datasets_links = {}
        # 遍历所有机构链接，获取机构发布数据集链接
        for index, target in crawl_targets.items():
            time.sleep(10)
            organization_datasets_links[index] = []
            try:
                if target is None or target == '':
                    continue
                logging.info(f"Scrawling {index}: {target}")
                target = self._get_related_links(target)
                self.driver.get(target)

                # 如果存在按钮，则点击——expand all
                time.sleep(5)
                try:
                    self.driver.find_element(By.XPATH, self.expand_all_button_xpath).click()
                except:
                    logging.info(f"Don't have expand all button.")

                # 获取所有数据集相关信息
                articles = self.driver.find_elements(By.XPATH, self.dataset_item_xpath)
                logging.info(f"current organization have datasets: {len(articles)}")
                for article in articles:
                    a_tag = article.find_element(By.TAG_NAME, 'a')
                    dataset_link = a_tag.get_attribute('href')
                    if index not in organization_datasets_links:
                        organization_datasets_links[index] = []
                    organization_datasets_links[index].append(dataset_link)
                    logging.info(f"Got dataset link: {dataset_link}")
            except Exception as e:
                logging.error(f"Error: {e}, when crawling {index}: {target}")

        # 保存数据集链接
        with open(f'{self.save_dir}/{self.organization_datasets_links_save_file}', 'w', encoding='utf-8') as f:
            json.dump(organization_datasets_links, f, indent=4)

    def _get_related_links(self, current_link=None) -> str:
        """
        获取相关数据集链接
        :param current_link:    当前链接
        """
        if current_link is None:
            raise ValueError("current_link cannot be None.")
        # 排序方法映射
        sort_map = {
            'downloads': 'downloads',
            'updated': 'modified',
            'created': 'created',
            'alphabetical': 'alphabetical',
            'likes': 'likes',
            'rowsMost': 'most_rows',
            'rowsLeast': 'least_rows'
        }
        if self.sort_method not in sort_map:
            raise ValueError("Invalid sort_method.")

        target_url = f"{current_link}?sort_datasets={sort_map[self.sort_method]}#datasets"
        return target_url

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = './logs/HF'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'HF_crawl_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling HF dataset info")

    def crawl_dataset_info(self):
        # 如果不存在数据集链接文件，则先爬取数据集链接
        if not os.path.exists(f'{self.save_dir}/{self.organization_datasets_links_save_file}'):
            self.crawl_dataset_links()

        # 读取数据集链接
        with open(f'{self.save_dir}/{self.organization_datasets_links_save_file}', 'r', encoding='utf-8') as f:
            organization_datasets_links = json.load(f)

        # 获取所有数据集的链接
        all_dataset_links = []
        for organization, links in organization_datasets_links.items():
            all_dataset_links.extend(links)

        # 获取所有数据集的详细信息
        dataset_details, exception_links = self._get_all_link_data(all_dataset_links)
        return dataset_details, exception_links

    def _get_all_link_data(self, all_dataset_links):
        """
        获取所有数据集的详细信息
        最终结果：{组织名：{数据集名：{详细信息}}}
        :param all_dataset_links:   所有数据集的链接
        :return:                    {组织名：{数据集名：{详细信息}}}
        """
        dataset_details = {}
        exception_links = []

        # 首先登录
        self.driver.get('https://huggingface.co/login')
        with open(self.logging_cookie_file_path, "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                self.driver.add_cookie(cookie)
        self.driver.refresh()
        time.sleep(1)

        # 遍历所有数据集链接，爬取数据集详细信息
        for link in tqdm.tqdm(all_dataset_links):
            try:
                self.driver.get(link)
                time.sleep(1)

                logging.info(f"Start crawling dataset: {link}")
                # 如果查看数据集具体行数信息需要填写相关信息
                if self.driver.find_elements(By.XPATH, self.need_finish_form_xpath):
                    labels = self.driver.find_elements(By.XPATH, self.form_items_xpath)
                    for label in labels:
                        label.find_element(By.TAG_NAME, 'input').send_keys('asd')
                    self.driver.find_element(By.XPATH, self.finish_form_button_xpath).click()

                organization = link.split("/")[-2]
                dataset_name = link.split("/")[-1]
                if organization not in dataset_details:
                    dataset_details[organization] = {}
                if dataset_name not in dataset_details[organization]:
                    dataset_details[organization][dataset_name] = {}
                # 对当前页面进行截图
                self.driver.save_screenshot(f'{self.screen_shot_save_path}/{dataset_name}.png')
                # 存储部分信息并获得arxiv_id
                arxiv_id = self._extract_related_data(dataset_details, dataset_name, organization)
                if self.get_arxiv:
                    # 获取arxiv链接
                    if arxiv_id:
                        arxiv_link = f'https://arxiv.org/pdf/{arxiv_id}'
                        self.driver.get(arxiv_link)
                        # TODO 这里需要等待页面加载完成，否则会出现截图不完整的情况，但是等待时间过长
                        time.sleep(5)
                        # 对arxiv页面进行截图
                        self.driver.save_screenshot(
                            f'{self.screen_shot_save_path}/{dataset_name}_pdf.png')

                dataset_details[organization][dataset_name]["link"] = link
                dataset_details[organization][dataset_name][
                    "paper_screenshot_save_path"] = f'{self.screen_shot_save_path}/{dataset_name}_pdf.png' if arxiv_id else ''
                dataset_details[organization][dataset_name][
                    "dataset_screenshot_save_path"] = f'{self.screen_shot_save_path}/{dataset_name}.png'
            except Exception as e:
                logging.info(f"Error: {e}, when crawling {link}")
                exception_links.append(link)
        return dataset_details, exception_links

    def _extract_related_data(self, dataset_details, dataset_name, organization):
        """
        提取相关数据
        :param dataset_details:         数据集详细信息，存储对象
        :param dataset_name:            数据集名称
        :param organization:            组织名称
        :return:                        arxiv_id
        """
        try:
            time.sleep(1)
            # 获取下载量
            download_count_last_month = self.driver.find_element(By.XPATH, self.download_count_xpath).text
            # 获取community社交活跃量——/html/body/div/main/div[1]/header/div/div[2]/div/的最后一个a
            community = self.driver.find_element(By.XPATH, self.community_xpath).text
            # 获取like数量
            like = self.driver.find_element(By.XPATH, self.like_xpath).text
            # 获取当前链接下所有tags信息
            dataset_tags_info_divs = self.driver.find_elements(By.XPATH, self.tags_info_xpath)
            dataset_tags_info_map = {}
            # 每个div下的span为key,div中除了span的其余标签部分的文本作为value，同时获取arxiv链接
            arxiv_id = ''
            for div in dataset_tags_info_divs:
                key = div.find_element(By.TAG_NAME, 'span').text.replace(':', '').replace("'", '').replace('"', '')
                value = clean_text(div.text.replace(key, '').split(':')[-1]).replace("'", '').replace('"', '')
                dataset_tags_info_map[key] = value
                if key.lower() == 'arxiv':
                    arxiv_id = clean_text(value.split(':')[-1])
                    # arxiv_id: 2107.06499 + 4 对于这种多篇文章的arxiv_id，只取第一篇
                    arxiv_id = arxiv_id.split(' ')[0]
            # 获取数据集一些相关的信息
            try:
                data_info_div = self.driver.find_element(By.XPATH, self.data_info_div_xpath)
                data_info = {}
                if data_info_div:
                    a_tags = data_info_div.find_elements(By.TAG_NAME, 'a')
                    # 遍历a标签，获取每个a标签的第一个div作为key，第二个div作为value
                    for a_tag in a_tags:
                        divs = a_tag.find_elements(By.TAG_NAME, 'div')
                        if len(divs) == 2:  # 确保每个a标签下有两个div
                            key = divs[0].text.strip()  # 第一个div的文本作为key
                            value = divs[1].text.strip()  # 第二个div的文本作为value
                            data_info[key] = value

                # 将获取到的数据量等信息保存到dataset_details中
                dataset_details[organization][dataset_name]["dataset_info"] = data_info
                logging.info(f"Get dataset info: {data_info}")
            except Exception as e:
                logging.info(f"Get dataset info failed.")
            # 获取数据集面板信息
            if self.driver.find_elements(By.CSS_SELECTOR, "div[class='2xl\\:pr-6']"):
                dataset_panel_info = self.driver.find_element(By.CSS_SELECTOR,
                                                              "div[class='2xl\\:pr-6']").text.replace(
                    '"',
                    '').replace(
                    "'", '')
                dataset_details[organization][dataset_name]['dataset_panel_info'] = dataset_panel_info
            # 获取数据集右侧 Collection 与相关 Model 信息
            self._crawl_related_models_or_collections(self.driver, dataset_details, dataset_name, organization)
            dataset_details[organization][dataset_name]['dataset_tags_info'] = dataset_tags_info_map
            dataset_details[organization][dataset_name]["download_count_last_month"] = download_count_last_month
            dataset_details[organization][dataset_name]['community'] = clean_text(
                community.replace('Community', '')) if clean_text(community.replace('Community', '')) else '0'
            dataset_details[organization][dataset_name]['like'] = clean_text(
                like.replace('Like', '')) if clean_text(
                like.replace('Like', '')) else '0'
        except Exception as e:
            raise e

        return arxiv_id

    def _crawl_related_models_or_collections(self, driver, dataset_details, dataset_name, organization):
        """
        获取数据集右侧 Collection 与相关 Model 信息
        :param driver:              浏览器驱动
        :param dataset_details:     保留信息集合
        :param dataset_name:        数据集名称
        :param organization:        组织名称
        :return:                    None
        """
        result = {}
        try:
            # 找到指定的 section 元素
            section_element = driver.find_element(By.XPATH, "/html/body/div/main/div[2]/section[2]")
            section_html = section_element.get_attribute('outerHTML')
            soup = BeautifulSoup(section_html, 'html.parser')
            # 找到所有的h2标签
            h2_tags = soup.find_all('h2')
            # 遍历所有的h2标签
            for i in range(len(h2_tags)):
                current_h2 = h2_tags[i]
                sub_title = clean_text(current_h2.text)
                result[sub_title] = {}
                # 如果还没到最后一个h2标签，获取当前h2到下一个h2之间的内容
                if i + 1 < len(h2_tags):
                    next_h2 = h2_tags[i + 1]
                    contents = current_h2.find_next_siblings()  # 获取当前h2标签之后的所有兄弟元素
                    for sibling in contents:
                        if sibling == next_h2:
                            break
                        a_elements = sibling.find_all('a', href=True)
                        self._extract_models_or_collections_items(a_elements, result, sub_title)
                # 如果是最后一个h2标签，获取从当前h2到文档结束的所有内容
                else:
                    contents = current_h2.find_next_siblings()
                    for sibling in contents:
                        a_elements = sibling.find_all('a', href=True)
                        self._extract_models_or_collections_items(a_elements, result, sub_title)
        except Exception as e:
            print(f"获取相关 Model 或 Collection 信息失败：{e}")
        dataset_details[organization][dataset_name]['related_models_collections'] = result

    def _extract_models_or_collections_items(self, elements, result, sub_title):
        """
        提取相关 Model 或 Collection 每个条目的信息
        :param elements:            当前h2下的所有的a标签元素
        :param result:              保存结果的字典
        :param sub_title:           当前标题
        :return:                    None
        """
        for a_tag in elements:
            header = clean_text(a_tag.find('header').text) if a_tag.find('header') else 'expand'
            result[sub_title][header] = {}
            if header != 'expand' and a_tag.find('div') and a_tag.find('div').find('div'):
                result[sub_title][header]['href'] = a_tag['href']
                result[sub_title][header]['text'] = clean_text(a_tag.find('div').find('div').text)
            else:
                result[sub_title][header]['other'] = clean_text(a_tag.text).replace('"', '').replace("'", '')

    def crawl_exception_links(self, exception_links):
        """
        重新爬取出错的链接
        :param exception_links:     出错的链接
        :return:
        """
        add_dataset_details, new_exception_links = self._get_all_link_data(exception_links)
        return add_dataset_details, new_exception_links


class HuggingfaceDataPostProcess(DataPostProcess):
    """
    该类用来处理爬取的Huggingface数据集信息
    提取出格式为：
    组织 | 机构 | 数据集名称 | 模态 | 生命周期 | 链接 | 上月下载量 | 统计渠道 | 统计方法 | 是否新增 | 发布时间
    """

    organization_type = "HF"  # 用于获取映射

    def __init__(self, dataset_info_file_path='./result/huggingface/huggingface_dataset_details.json'):
        super().__init__(dataset_info_file_path)

    @override
    def _extract_need_info(self, dataset_tags_info=None, related_models_collections=None, dataset_link=None):
        """
        从爬取的数据中对部分数据进行处理，提取需要信息
        :param dataset_tags_info:               数据集tags信息
        :param related_models_collections:      相关模型或集合信息
        :param dataset_link:                    数据集链接
        :return:                                数据集license, 数据集被使用量
        """
        dataset_license = ''
        dataset_used_count = 0
        sub_organization_name = ''
        organization_name = ''
        # 处理协议
        if 'License' in dataset_tags_info:
            dataset_license = dataset_tags_info['License']
        elif 'license' in dataset_tags_info:
            dataset_license = dataset_tags_info['license']
        elif 'Licence' in dataset_tags_info:
            dataset_license = dataset_tags_info['Licence']
        elif 'licence' in dataset_tags_info:
            dataset_license = dataset_tags_info['licence']
        # 处理数据集被使用量
        for key, value in related_models_collections.items():
            if 'trained' in key or 'fine-tuned' in key:
                # 查看是否有expand键
                for k, v in value.items():
                    if 'expand' in k:
                        # Browse 106 models trained on this dataset 取出其中的数字
                        dataset_used_count = int(re.findall(r'\d+', v['other'])[0])
                    else:
                        dataset_used_count += 1
        # 处理组织与子组织
        extracted_organization = re.findall(r'datasets/(.*?)/', dataset_link)
        for organization, sub_organizations in self.organization_sub_organization_map.items():
            for sub_organization in sub_organizations:
                if sub_organization.lower() == extracted_organization[0].lower():
                    sub_organization_name = sub_organization
                    organization_name = organization
                else:
                    sub_organization_name = extracted_organization[0]
                    organization_name = extracted_organization[0]
        return dataset_license, dataset_used_count, sub_organization_name, organization_name

    @override
    def post_process(self):
        """
        后处理数据, 将获取到的数据转化为对应的格式
        :return:
        """
        # 取出上月的数据 —— 统计渠道列对应的值为huggingface
        hf_data = self.last_month_data[self.last_month_data['统计渠道'].str.strip() == 'huggingface']
        # 构造一个按照链接为key，其它信息为value的字典
        hf_data_dict = {}
        for index, row in hf_data.iterrows():
            hf_data_dict[row['链接']] = row

        result = []
        for organization, datasets in self.dataset_info.items():
            for dataset_name, dataset_info in datasets.items():
                if not dataset_info:
                    logging.info(f"dataset_info is empty: {dataset_name}")
                    continue
                # 获取数据集信息
                dataset_size_related = dataset_info.get('dataset_info', {})
                dataset_link = dataset_info.get('link', '')
                download_count_last_month = int(dataset_info.get('download_count_last_month', '').replace(',', ''))
                community = int(dataset_info.get('community', '')) if dataset_info.get('community', '') and dataset_info.get('community', '').isdigit() else 0
                like = self.parse_download_num(dataset_info.get('like', '')) if dataset_info.get('like', '') else 0
                dataset_tags_info = dataset_info.get('dataset_tags_info', {})
                dataset_panel_info = self.remove_illegal_characters(dataset_info.get('dataset_panel_info', ''))
                related_models_collections = dataset_info.get('related_models_collections', {})
                # 从爬取的数据中对部分数据进行处理，提取需要信息
                dataset_license, dataset_used_count, sub_organization, organization = self._extract_need_info(
                    dataset_tags_info,
                    related_models_collections,
                    dataset_link)
                # 截图保存路径
                paper_screenshot_save_path = dataset_info.get('paper_screenshot_save_path', '')
                dataset_screenshot_save_path = dataset_info.get('dataset_screenshot_save_path', '')
                # 汇总 成组织 | 机构 | 数据集名称 | 模态 | 生命周期 | 链接 | 上月下载量 | 统计渠道 | 统计方法 | 是否新增 | 发布时间
                if dataset_link in hf_data_dict:
                    is_new = '否'
                    modality = hf_data_dict[dataset_link]['模态']
                    life_cycle = hf_data_dict[dataset_link]['生命周期']
                else:
                    is_new = '是'
                    modality = ''
                    life_cycle = ''

                result.append(
                    {'组织': organization, '机构': sub_organization, '数据集名称': dataset_name, '模态': modality,
                     '生命周期': life_cycle,
                     '链接': dataset_link, '上月下载量': download_count_last_month,
                     '数据集被使用量': dataset_used_count,
                     '统计渠道': 'huggingface',
                     '是否新增': is_new, '发布时间': '',
                     '下载量': '',
                     'license': dataset_license,
                     'dataset_panel_info': dataset_panel_info,
                     'community': community, 'like': like,
                     'dataset_size_related': dataset_size_related,
                     'paper_screenshot_save_path': paper_screenshot_save_path,
                     'dataset_screenshot_save_path': dataset_screenshot_save_path})

        # 将结果保存到excel中
        self.save_to_excel(result, f'./result/huggingface/huggingface_dataset_info.xlsx')

    def check_with_last_month(self, data):
        """ 与上月数据进行对比 """
        pass


if __name__ == '__main__':
    # =========================== 爬取数据集信息 ================================
    huggingface_crawler = HuggingfaceCrawler(headless=True,
                                             organization_links_file_path='organization_links/hugging_face_organization_links.json',
                                             sort_method='downloads',
                                             save_dir='./result/huggingface')

    dataset_details, exception_links = huggingface_crawler.crawl_dataset_info()
    # save
    with open('./result/huggingface/huggingface_dataset_details.json', 'w', encoding='utf-8') as f:
        json.dump(dataset_details, f, indent=4)
    if exception_links:
        with open('./result/huggingface/huggingface_exception_links.json', 'w', encoding='utf-8') as f:
            json.dump(exception_links, f, indent=4)
    # =========================== 后处理数据 ================================
    if exception_links:
        with open('./result/huggingface/huggingface_exception_links.json', 'r', encoding='utf-8') as f:
            exception_links = json.load(f)
        add_dataset_details, new_exception_links = huggingface_crawler.crawl_exception_links(exception_links)
        # save
        with open('./result/huggingface/huggingface_dataset_details_add.json', 'w', encoding='utf-8') as f:
            json.dump(add_dataset_details, f, indent=4)
        if new_exception_links:
            with open('./result/huggingface/huggingface_exception_links_add.json', 'w', encoding='utf-8') as f:
                json.dump(new_exception_links, f, indent=4)

    assert len(exception_links) == 0, "Some links are not crawled successfully"
    huggingface_data_post_process = HuggingfaceDataPostProcess(
        dataset_info_file_path='./result/huggingface/huggingface_dataset_details.json')
    huggingface_data_post_process.post_process()
