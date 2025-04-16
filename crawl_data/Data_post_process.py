# -*- coding: utf-8 -*-
# @Time       : 2024/12/13 10:17
# @Author     : Marverlises
# @File       : Data_post_process.py
# @Description: PyCharm
"""
基类，用于数据后处理
"""
import re
import time
import pandas as pd
from abc import abstractmethod
from crawl_data.utils import read_json_file


class DataPostProcess:
    """
    数据后处理基类
    """
    # 防止保存为excel出错
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
    # 组织-子组织映射
    HF_ORGANIZATION_SUB_ORGANIZATION_MAP = {
        "智源": ["BAAI"],
        "OpenXLab": ["OpenGVLab", "internlm", "opendatalab", "AI4Chem", "OpenDriveLab", "OpenScienceLab",
                     "OpenDILabCommunity", "OpenMEDLab", "openmmlab"],
        "Baidu": ["baidu", "PaddlePaddle"],
        "Baichuan": ["baichuan-inc"],
        "Zhipu": ["THUDM", "THUDM-HF-SPACE"],
        "Ali": ["modelscope", "Qwen", "alibabasglab", "alibaba-pai"],
        "Huawei": ["HWERI", "huawei-noah"],
        "LMSYS": ["lmsys"],
        "Falcon": ["tiiuae"],
        "EleutherAI": ["EleutherAI"],
        "Meta": ["facebook", "meta-llama"],
        "Google": ["google", "google-bert", "google-t5"],
        "HuggingFace": ["HuggingFaceH4", "open-r1", "HuggingFaceTB"],
    }
    MS_ORGANIZATION_SUB_ORGANIZATION_MAP = {
        "智源": ["BAAI"],
        "OpenXLab": ["OpenGVLab", "Shanghai_AI_Laboratory", "OpenDataLab"],
        "Baidu": ["baidu"],
        "Baichuan": ["baichuan-inc"],
        "Zhipu": ["ZhipuAI"],
        "Ali": ["Qwen", "iic"],
        "Huawei": [],
        "LMSYS": [],
        "Falcon": [],
        "EleutherAI": [],
        "Meta": [],
        "Google": [],
        "Modelscope": ["Modelscope"]  # 单独列出，防止计入Ali
    }
    # OpenDataLab 对应的组织的ID
    DL_ORGANIZATION_SUB_ORGANIZATION_MAP = {"OpenDataLab": [['1678533', '12199', '11828', '12157', '12589']]}

    def __init__(self, dataset_info_file_path):
        self.dataset_info_file_path = dataset_info_file_path
        self.dataset_info = read_json_file(self.dataset_info_file_path)
        # 得到当前的月份的整数
        current_month = int(time.strftime("%m", time.localtime()))
        if current_month == 1:
            current_month = 13
        # 获取上月的数据
        self.last_month_path = f'./data_each_month/month{current_month - 1}.xlsx'
        self.last_month_data = pd.read_excel(self.last_month_path)
        # 组织-子组织映射
        self.organization_sub_organization_map = self.get_mapping()

    # 定义抽象方法post_process
    @abstractmethod
    def post_process(self):
        """ 数据后处理 """
        pass

    @abstractmethod
    def _extract_need_info(self, data):
        """ 从爬取的数据中对部分数据进行处理，提取需要信息 """
        pass

    @abstractmethod
    def check_with_last_month(self, data):
        """ 与上月数据进行对比 """
        pass

    @staticmethod
    def remove_illegal_characters(text):
        """ 去除文本中的非法字符 """
        return DataPostProcess.ILLEGAL_CHARACTERS_RE.sub(r'', text)

    @staticmethod
    def save_to_excel(result, save_path):
        """ 保存数据到excel """
        result_df = pd.DataFrame(result)
        result_df.to_excel(save_path, index=False)

    def parse_download_num(self, download_num):
        """ 解析下载量 """
        if not download_num == 0 and not download_num:
            raise Exception(f"download_num is None")
        try:
            download_num = int(download_num)
            return download_num
        except:
            try:
                if 'k' in download_num:
                    download_num = download_num.replace('k', '')
                    download_num = int(float(download_num) * 1000)
                elif 'w' in download_num:
                    download_num = download_num.replace('w', '')
                    download_num = int(float(download_num) * 10000)
                elif ',' in download_num:
                    download_num = download_num.replace(',', '')
                    download_num = int(download_num)
                return download_num
            except:
                raise Exception(f"Can't parse download_num: {download_num}")

    # 定义get方法获取映射,根据调用的子类不同，返回不同的映射
    def get_mapping(self):
        if hasattr(self, "organization_type"):
            # Huggingface
            if self.organization_type == "HF":
                return self.HF_ORGANIZATION_SUB_ORGANIZATION_MAP
            # Modelscope
            elif self.organization_type == "MS":
                return self.MS_ORGANIZATION_SUB_ORGANIZATION_MAP
            # OpenDataLab
            elif self.organization_type == 'DL':
                return self.DL_ORGANIZATION_SUB_ORGANIZATION_MAP
            else:
                return {}
