# -*- coding: utf-8 -*-
# @Time       : 2024/11/21 9:04
# @Author     : Marverlises
# @File       : Modelscope_crawler.py
# @Description: 爬取Modelscope数据集信息
import json
import os
import logging
import re
import time
import tqdm
from typing_extensions import override
from crawl_data.Data_post_process import DataPostProcess
from utils import extract_arxiv_link, extract_pdf_link, read_json_file, init_driver, parse_string
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class ModelscopeCrawler:
    def __init__(self, sort_method='downloads', headless=True, base_save_path='./result/modelscope',
                 target_json_path='./organization_links/model_scope_organization_links.json', log_level=logging.INFO,
                 organization_datasets_links_save_file='modelscope_organization_datasets_links.json', get_arxiv=False):
        """
        初始化
        :param sort_method:                         排序方式
        :param headless:                            是否无头模式
        :param base_save_path:                      保存路径
        :param target_json_path:                    目标json文件路径
        :param log_level:                           日志级别
        :param organization_datasets_links_save_file:  机构数据集链接保存文件
        :param get_arxiv:                           是否获取arxiv链接
        """
        sort_method_map = {'synthesis': "综合排序", 'downloads': "下载量排序", 'likes': "收藏量排序",
                           'updated': "最近更新"}
        self.chosen_sort_method = sort_method_map[sort_method]
        self.driver = init_driver(headless=headless)
        self.screen_shot_save_path = base_save_path + '/modelscope_dataset_info_screenshots'
        self.crawl_targets = read_json_file(target_json_path)
        self.organization_datasets_links_save_file = base_save_path + '/' + organization_datasets_links_save_file
        self.get_arxiv = get_arxiv
        self._init_logger(log_level=log_level)
        self._init_relevant_element_xpath()
        if os.path.exists(self.screen_shot_save_path) is False:
            os.makedirs(self.screen_shot_save_path)

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = './logs/MS'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'MS_crawl_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling MS dataset info")

    def _init_relevant_element_xpath(self) -> None:
        """
        初始化相关元素的Xpath
        :return:
        """
        # ================================= get_dataset_links相关元素的Xpath =================================
        # 所有数据集元素div
        self.dataset_elements_xpath = '//*[@id="normal_tab_dataset"]/div/div[3]/div/div/div/div[1]/div'
        # 数据集链接
        self.dataset_link_xpath = '//*[@id="organization_rightContent"]/div/div/div[1]/div/div[4]'
        # 开始选择排序方式
        self.sort_method_xpath = '//*[@id="normal_tab_dataset"]/div/div[2]/div[2]/div/span[2]'
        # 排序方式的Xpath
        self.sort_xpath = f"//div[text()='{self.chosen_sort_method}']"
        # 下一页
        self.next_page_xpath = '//*[@id="normal_tab_dataset"]/div/div[3]/div/div/div/div[2]/ul/li[last()-1]'
        # 总页数
        self.total_page_xpath = '//*[@id="normal_tab_dataset"]/div/div[3]/div/div/div/div[2]/ul/li[last()-2]'
        # ================================= get_dataset_info相关元素的Xpath =================================
        # 开源协议
        """
        有好几种，直接获取//*[@id="root"]/div/div/main/div[1]/div[1]/div[1]/div/div/div[2]/div/div 所有内容，后续处理
        """
        self.dataset_license_xpath = '//*[@id="root"]/div/div/main/div[1]/div[1]/div[1]/div/div/div[2]/div/div'
        # 相关信息
        self.related_info_xpath = '//*[@id="root"]/div/div/main/div[1]/div[1]/div[1]/div/div/div[3]/div[1]'
        # introduction
        self.introduction_xpath = '//*[@id="modelDetail_bottom"]/div/div[1]'
        # community activities
        self.community_activities_xpath = '//*[@id="modelDetail_bottom"]/div/div/div[2]/div[1]/div'

    def get_dataset_links(self):
        """
        获取机构发布的数据集链接
        :return:    机构发布的数据集链接
        """
        organization_datasets_links = {}
        # 遍历所有机构链接，获取机构发布数据集链接
        for index, target in self.crawl_targets.items():
            try:
                if target is None or target == '':
                    continue
                logging.info(f"Start crawling {index} datasets")
                self.driver.get(target)
                # 进入数据集页面
                time.sleep(5)
                self.driver.find_element(By.XPATH, self.dataset_link_xpath).click()
                # 排序方式
                time.sleep(3)
                self.driver.find_element(By.XPATH, self.sort_method_xpath).click()
                # 选择排序方式
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, self.sort_xpath))
                    ).click()
                except:
                    raise Exception(f"Can't find sort method: {self.chosen_sort_method}")

                # 等待页面全部加载完成
                time.sleep(3)
                dataset_elements = self.driver.find_elements(By.XPATH, self.dataset_elements_xpath)
                # 获取当前页面的所有数据集链接
                logging.info(f"Current page has {len(dataset_elements)} datasets")
                if not dataset_elements or len(dataset_elements) == 0 or any(
                        "无数据集" in element.text for element in dataset_elements):
                    logging.info(f"No dataset found for {index}")
                    continue
                # 爬取首页数据
                dataset_links, dataset_last_update_time, dataset_download_num, dataset_like_num = self._get_page_info(
                    dataset_elements)
                if index not in organization_datasets_links:
                    organization_datasets_links[index] = {
                        "dataset_links": dataset_links,
                        "dataset_last_update_time": dataset_last_update_time,
                        "dataset_download_num": dataset_download_num,
                        "dataset_like_num": dataset_like_num
                    }
                else:
                    organization_datasets_links[index]["dataset_links"].extend(dataset_links)
                    organization_datasets_links[index]["dataset_last_update_time"].extend(dataset_last_update_time)
                    organization_datasets_links[index]["dataset_download_num"].extend(dataset_download_num)
                    organization_datasets_links[index]["dataset_like_num"].extend(dataset_like_num)

                # 多页情况的处理
                try:
                    WebDriverWait(self.driver, 2).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[@id="normal_tab_dataset"]/div/div[3]/div/div/div/div[2]/ul')))
                except:
                    logging.info("Only one page")
                    logging.info(f"Finish crawling {index} datasets")
                    continue
                self._iterate_other_pages(index, organization_datasets_links)
            except Exception as e:
                logging.error(f"Error: {e}, when crawling page {index} dataset")
                raise e

        logging.info(f"Finish crawling all datasets")
        # 保存数据集链接
        with open(self.organization_datasets_links_save_file, 'w', encoding='utf-8') as f:
            json.dump(organization_datasets_links, f, ensure_ascii=False)
        return organization_datasets_links

    def _iterate_other_pages(self, index, organization_datasets_links):
        """
        遍历其余页面中数据集的信息
        :param index:                       当前机构的索引
        :param organization_datasets_links: 结果集
        :return:                            None
        """
        # 获取总页数
        total_page = self.driver.find_element(By.XPATH, self.total_page_xpath).get_attribute('title')
        logging.info(f"Total page: {total_page}")
        # 遍历所有页
        for page in range(2, int(total_page) + 1):
            try:
                self.driver.find_element(By.XPATH, self.next_page_xpath).click()
                time.sleep(3)
                # 获取当前页面的所有数据集链接
                dataset_elements = self.driver.find_elements(By.XPATH, self.dataset_elements_xpath)
                logging.info(f"current page datasets have: {len(dataset_elements)}")

                dataset_links, dataset_last_update_time, dataset_download_num, dataset_like_num = self._get_page_info(
                    dataset_elements)

                organization_datasets_links[index]["dataset_links"].extend(dataset_links)
                organization_datasets_links[index]["dataset_last_update_time"].extend(dataset_last_update_time)
                organization_datasets_links[index]["dataset_download_num"].extend(dataset_download_num)
                organization_datasets_links[index]["dataset_like_num"].extend(dataset_like_num)
            except Exception as e:
                logging.error(f"Error: {e}, when crawling page {page} dataset, current index: {index}")

    def _get_page_info(self, dataset_elements):
        """
        获取当前页面的数据集信息
        :param dataset_elements:    当前页面的数据集元素
        :return:                    数据集链接、最后更新时间、下载量、收藏量
        """
        dataset_links = [element.find_element(By.TAG_NAME, 'a').get_attribute('href') for
                         element in dataset_elements]
        dataset_last_update_time = [element.find_element(By.XPATH, 'a/div/div[3]/div[2]/div[1]').text for
                                    element in dataset_elements]
        dataset_download_num = [element.find_element(By.XPATH, 'a/div/div[3]/div[2]/div[2]').text for
                                element in dataset_elements]
        dataset_like_num = [element.find_element(By.XPATH, 'a/div/div[3]/div[2]/div[3]').text for
                            element in dataset_elements]
        assert len(dataset_elements) == len(dataset_links) == len(dataset_last_update_time) == len(
            dataset_download_num) == len(dataset_like_num)
        logging.info(f"dataset_links: {dataset_links}")
        return dataset_links, dataset_last_update_time, dataset_download_num, dataset_like_num

    def preprocess_dataset_info(self):
        """
        预处理数据集信息——获取所有数据集的链接
        :return:
        """
        # 首先查看是否爬取了数据集链接
        if not os.path.exists(self.organization_datasets_links_save_file):
            self.get_dataset_links()
        # 读取数据集链接
        targets = read_json_file(self.organization_datasets_links_save_file)
        all_dataset_links = []
        all_dataset_download_num = []
        all_dataset_like_num = []
        all_dataset_last_update_time = []
        # 获取所有数据集的链接
        for organization, info in targets.items():
            all_dataset_links.extend(info['dataset_links'])
            all_dataset_download_num.extend(info['dataset_download_num'])
            all_dataset_like_num.extend(info['dataset_like_num'])
            all_dataset_last_update_time.extend(info['dataset_last_update_time'])
        logging.info(f"Total dataset: {len(all_dataset_links)}")
        assert len(all_dataset_links) == len(all_dataset_download_num) == len(all_dataset_like_num) == len(
            all_dataset_last_update_time), "数据集链接、下载量、收藏量、最后更新时间数量不一致,需要检查"
        return all_dataset_links, all_dataset_download_num, all_dataset_like_num, all_dataset_last_update_time

    def crawl_dataset_info(self):
        """
        获取所有数据集的详细信息
        :return:                                {组织名：{数据集名：{详细信息}}}，异常链接
        """
        # 获取所有数据集的链接以及下载量、收藏量、最后更新时间
        all_dataset_links, all_dataset_download_num, all_dataset_like_num, all_dataset_last_update_time = self.preprocess_dataset_info()
        # 结果集
        dataset_details = {}
        exception_links = []
        # 遍历所有数据集链接
        for i in tqdm.tqdm(range(len(all_dataset_links))):
            link = all_dataset_links[i]
            try:
                self.driver.get(link)
                time.sleep(4)
                download_num = all_dataset_download_num[i]
                like_num = all_dataset_like_num[i]
                last_update_time = all_dataset_last_update_time[i].split(" ")[0]
                organization = link.split("/")[-2]
                dataset_name = link.split("/")[-1]
                logging.info(f"正在获取数据集详细信息：{link}")
                # 首先对当前页面进行截图，保存到model_scope_dataset_info_screenshots文件夹下
                self.driver.save_screenshot(f"{self.screen_shot_save_path}/{dataset_name}.png")
                # 获取开源协议，下载量，介绍面板显示数据集大小（不一定有用，很多和实际大小有很大差异）,以及整个介绍面板的内容
                dataset_license_and_task_spans = self.driver.find_elements(By.XPATH,
                                                                           self.dataset_license_xpath + '/span')
                dataset_license_and_task = [span.text for span in dataset_license_and_task_spans]
                related_info = self.driver.find_element(By.XPATH, self.related_info_xpath).text
                related_info = parse_string(related_info)
                introduction = self.driver.find_element(By.XPATH, self.introduction_xpath).text
                # 从introduction中提取相关论文链接
                arxiv_pdf_link = extract_arxiv_link(introduction)
                only_pdf_link = extract_pdf_link(introduction)
                # 进入feedback页面，获取社区活跃度
                self.driver.get(link + '/feedback')
                time.sleep(1)
                community_activities = self.driver.find_element(By.XPATH, self.community_activities_xpath).text
                if self.get_arxiv:
                    # 获取pdf的截图
                    if arxiv_pdf_link:
                        self.get_pdf_screenshots(arxiv_pdf_link, dataset_name)
                    elif only_pdf_link:
                        self.get_pdf_screenshots(only_pdf_link, dataset_name)
                    else:
                        logging.info("没有pdf链接")

                dataset_details.setdefault(organization, {})[dataset_name] = {
                    "dataset_license": dataset_license_and_task,
                    "related_info": related_info,
                    "introduction": introduction,
                    "community_activities": community_activities,
                    "dataset_screenshot_save_path": f"result/modelscope/model_scope_dataset_info_screenshots/{dataset_name}.png",
                    "paper_screenshot_save_path": f"result/modelscope/model_scope_dataset_info_screenshots/{dataset_name}_pdf.png" if arxiv_pdf_link or only_pdf_link else '',
                    "download_num": download_num,
                    "like_num": like_num,
                    "last_update_time": last_update_time,
                    "link": link}

                logging.info(f"Got dataset: {link}, dataset name: {dataset_name}")
            except:
                exception_links.append(link)
                logging.error(f"Error when crawling page dataset {i}")
                continue
        return dataset_details, exception_links

    def get_pdf_screenshots(self, pdf_link, dataset_name):
        """
        获取pdf的截图
        :param pdf_link:        pdf链接
        :param dataset_name:    数据集名
        :return:                None
        """
        if pdf_link:
            logging.info(f"正在获取pdf截图：{pdf_link}")
            self.driver.get(pdf_link)
            time.sleep(8)
            self.driver.save_screenshot(
                f"{self.screen_shot_save_path}/{dataset_name}_pdf.png")


class ModelscopeDataPostProcess(DataPostProcess):
    """ 数据后处理 """

    organization_type = "MS"  # 用于获取映射

    def __init__(self, dataset_info_file_path='./result/modelscope/model_scope_dataset_details.json'):
        super().__init__(dataset_info_file_path)

    @override
    def _extract_need_info(self, dataset_license=None, related_info=None, community_activities=None,
                           dataset_link=None):
        """
        提取需要的信息
        :param dataset_license:         开源协议                          ['开源协议：cc-by-nc-sa-4.0']
        :param related_info:            相关信息 - 提取上次更新时间          {'downloads': None, 'last_update': '2024-11-21', 'size': '7.90GB'}
        :param community_activities:    社区活跃度                        '活跃中（0）已完结（0）'         ->  0 + 0 = 0
        :param download_num:            下载量                            '2.5k'                       ->  2500 | '1,879' -> 1879 | '1.2w' -> 12000
        :param dataset_link:            数据集链接                         https://modelscope.cn/datasets/OpenDataLab/STN_PLAD
        :return:                        提取后的数据
        """
        organization_name = ''

        # license ['开源协议：cc-by-nc-sa-4.0']
        dataset_license = dataset_license[0].split("：")[-1] if dataset_license else ''
        # related_info {'downloads': None, 'last_update': '2024-11-21', 'size': '7.90GB'}
        download_num = related_info.get('downloads', 0)
        last_update = related_info.get('last_update', '').replace('-', '.')
        size = related_info.get('size', '')
        # community_activities 活跃中（\d+）\n已完结（\d+） 取出两个数字
        # 使用正则提取数字
        numbers = re.findall(r'活跃中（(\d+)）|已完结（(\d+)）', community_activities)
        community_activities_count = sum(int(num) for match in numbers for num in match if num)
        # download_num
        download_num = self.parse_download_num(download_num) if download_num else 0
        # 处理组织与子组织
        extracted_organization = re.findall(r'datasets/(.*?)/', dataset_link)
        for organization, sub_organizations in self.organization_sub_organization_map.items():
            for sub_organization in sub_organizations:
                if sub_organization.lower() == extracted_organization[0].lower():
                    organization_name = organization
        return dataset_license, download_num, last_update, size, community_activities_count, organization_name

    @override
    def post_process(self):
        # 取出上月的数据 —— 统计渠道列对应的值为Modelscope
        ms_data = self.last_month_data[self.last_month_data['统计渠道'].str.strip() == 'modelscope']
        # 构造一个按照链接为key，其它信息为value的字典
        ms_data_dict = {}
        for index, row in ms_data.iterrows():
            ms_data_dict[row['链接']] = row

        result = []
        for sub_organization, datasets in self.dataset_info.items():
            for dataset_name, dataset_info in datasets.items():
                dataset_link = dataset_info.get('link', '')
                dataset_license = dataset_info.get('dataset_license', '')
                related_info = dataset_info.get('related_info', '')
                introduction = dataset_info.get('introduction', '')
                community_activities = dataset_info.get('community_activities', '')
                like_num = dataset_info.get('like_num', '')
                download_num_from_card = dataset_info.get('download_num', '')

                # 截图路径
                dataset_screenshot_save_path = dataset_info.get('dataset_screenshot_save_path', '')
                paper_screenshot_save_path = dataset_info.get('paper_screenshot_save_path', '')

                # 提取需要的信息
                dataset_license, download_num, last_update, size, community_activities, organization = self._extract_need_info(
                    dataset_license, related_info, community_activities, dataset_link)

                # 是否新增
                if dataset_link in ms_data_dict:
                    is_new = '否'
                    # 上月下载量——总数
                    last_month_download_num = ms_data_dict[dataset_link]['下载量']
                    # 本月下载量——差值
                    download_num_last_month = self.parse_download_num(
                        download_num) - self.parse_download_num(
                        last_month_download_num)
                    modality = ms_data_dict[dataset_link]['模态']
                    life_cycle = ms_data_dict[dataset_link]['生命周期']
                else:
                    is_new = '是'
                    # 本月下载量——差值
                    download_num_last_month = self.parse_download_num(download_num)
                    modality = ''
                    life_cycle = ''

                result.append({
                    '组织': organization,
                    '机构': sub_organization,
                    '数据集名称': dataset_name,
                    '模态': modality,
                    '生命周期': life_cycle,
                    '链接': dataset_link,
                    '上月下载量': download_num_last_month,
                    '统计渠道': 'modelscope',
                    '是否新增': is_new, '发布时间': last_update, 'license': dataset_license,
                    '社区活跃度': community_activities,
                    '下载量': download_num,
                    'download_num': self.parse_download_num(download_num_from_card),
                    'like_num': like_num,
                    'introduction': introduction,
                    'dataset_screenshot_save_path': dataset_screenshot_save_path,
                    'paper_screenshot_save_path': paper_screenshot_save_path
                })
        # 保存数据
        save_path = f'./result/modelscope/modelscope_dataset_info.xlsx'
        self.save_to_excel(result, save_path)

    def check_with_last_month(self, data):
        """ 与上月数据进行对比 """
        pass


if __name__ == '__main__':
    # # =========================== 爬取数据集信息 ================================
    # modelscope_crawler = ModelscopeCrawler(headless=True, get_arxiv=False, organization_datasets_links_save_file='modelscope_organization_datasets_links2.json',
    #                                        target_json_path='./organization_links/model_scope_organization_links.json')
    # dataset_details, exception_links = modelscope_crawler.crawl_dataset_info()
    # # save
    # with open('./result/modelscope/model_scope_dataset_details.json', 'w', encoding='utf-8') as f:
    #     json.dump(dataset_details, f, ensure_ascii=False)
    # if exception_links:
    #     with open('./result/modelscope/model_scope_exception_links.json', 'w', encoding='utf-8') as f:
    #         json.dump(exception_links, f, ensure_ascii=False)
    # # =========================== 数据后处理 ================================
    # assert len(exception_links) == 0, "Some links are not crawled successfully"
    modelscope_data_post_process = ModelscopeDataPostProcess(
        dataset_info_file_path='./result/modelscope/model_scope_dataset_details.json')
    modelscope_data_post_process.post_process()
