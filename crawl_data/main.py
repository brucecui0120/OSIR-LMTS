# -*- coding: utf-8 -*-
# @Time       : 2024/12/14 12:28
# @Author     : Marverlises
# @File       : main.py
# @Description: PyCharm
import os
import time

import pandas as pd

# 组织 | 机构 | 数据集名称 | 模态 | 生命周期 | 链接 | 上月下载量 | 统计渠道 | 是否新增 | 下载量
desired_columns = ['组织', '机构', '数据集名称', '模态', '生命周期', '链接',
                   '上月下载量', '统计渠道', '是否新增', '下载量']


# Function to read and process each Excel file
def read_and_select_columns(file_path):
    try:
        data = pd.read_excel(file_path)
        # Select the desired columns if they exist
        existing_columns = [col for col in desired_columns if col in data.columns]
        missing_columns = [col for col in desired_columns if col not in data.columns]
        if missing_columns:
            print(
                f"Warning: The following columns are missing in {file_path} and will be filled with NaN: {missing_columns}")
            for col in missing_columns:
                data[col] = pd.NA  # Add missing columns with NaN values
        # Select and reorder the columns
        data = data[desired_columns]
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame(columns=desired_columns + ['来源'])


def combine_excel(BAAI_excel, DL_excel, HF_excel, MS_excel):
    # 合并数据 —— 取出以下几列，把所有数据汇总到一张表中
    # Read and process each dataset
    BAAI_data = read_and_select_columns(BAAI_excel)
    HF_data = read_and_select_columns(HF_excel)
    MS_data = read_and_select_columns(MS_excel)
    DL_data = read_and_select_columns(DL_excel)
    # Concatenate all DataFrames
    combined_data = pd.concat([BAAI_data, HF_data, MS_data, DL_data], ignore_index=True)
    download_columns = ['上月下载量', '下载量']
    for col in download_columns:
        combined_data[col] = pd.to_numeric(combined_data[col], errors='coerce')
    # Ensure the output directory exists
    output_dir = './result/combined'
    os.makedirs(output_dir, exist_ok=True)
    # Define the output file path
    output_file = os.path.join(output_dir, 'combined_dataset_info.xlsx')
    # Save the combined DataFrame to an Excel file
    try:
        combined_data.to_excel(output_file, index=False)
        print(f"Combined dataset saved successfully to {output_file}")
    except Exception as e:
        print(f"Error saving combined dataset: {e}")


class StatisticAll:
    """
    所有数据的统计类
    """
    # company_name_list = ['BAAI', 'OpenXLab', 'Baidu', 'Baichuan', 'Zhipu', 'Ali', 'Huawei', 'LMSYS', 'Falcon',
    #                      'EleutherAI', 'Meta', 'Google']
    modality_name_list = ['语言', '语音', '视觉', '多模态', '具身']
    life_cycle_list = ['预训练', '微调', '偏好']
    result = []

    def __init__(self, excel_path):
        self.entry_list = None
        self.company_name_list = None
        self.excel_path = excel_path
        self.data = pd.read_excel(excel_path)
        self._pre_process()

    @staticmethod
    def replace_organization(organization):
        mapping = {
            'opengvlab': 'OpenXLab',
            'internlm': 'OpenXLab',
            'opendatalab': 'OpenXLab',
            'ai4chem': 'OpenXLab',
            'opendrivelab': 'OpenXLab',
            'opensciencelab': 'OpenXLab',
            'opendilab': 'OpenXLab',
            'openmedlab': 'OpenXLab',
            'openmmlab': 'OpenXLab',
            'opendilabcommunity' : 'OpenXLab',
            'lmsys': 'LMSYS',
            'tiiuae': 'Falcon',
            'facebook': 'Meta',
            'meta-llama': 'Meta',
            'thudm': 'Zhipu',
            'thudm-hf-space': 'Zhipu',
            'modelscope': 'Ali',
            'qwen': 'Ali',
            'alibabasglab': 'Ali',
            'alibaba-pai': 'Ali',
            'hweri': 'Huawei',
            'huawei-noah': 'Huawei',
            'google-research-datasets': 'Google',
            'google': 'Google',
            'google-bert': 'Google',
            'google-t5': 'Google',
            'baichuan-inc': 'Baichuan',
            'eleutherai': 'EleutherAI',
            '智源': 'BAAI',
            'baidu': 'Baidu',
            'paddlepaddle': 'Baidu'
        }
        # 将组织名称转换为小写并进行替换
        return mapping.get(organization.lower(), organization)

    def _pre_process(self):
        # 构建一个条目列表
        self.entry_list = {}
        count = ((self.data['组织'] == 'Modelscope') & (self.data['统计渠道'] == 'modelscope')).sum()
        print(f"Modelscope数据集数量为:{count}")
        self.data.loc[(self.data['组织'] == 'Modelscope') & (
                self.data['统计渠道'] == 'modelscope'), '组织'] = 'Modelscope_modelscope'
        # 对组织列的名字进行替换——使用 apply() 进行替换
        self.data['组织'] = self.data['组织'].apply(self.replace_organization)
        # save
        self.data.to_excel('current_month.xlsx', index=False)

    def process_data(self):
        """
        按照模态和生命周期统计数据集数量和下载量
        """
        self.company_name_list = set(self.data['组织'])
        self.entry_list = {company_name: [] for company_name in self.company_name_list}
        print(self.company_name_list)
        # 首先根据‘组织’列进行分组，将每组数据存储在entry_list中
        for company_name in self.company_name_list:
            company_data = self.data[self.data['组织'] == company_name]
            for index, row in company_data.iterrows():
                entry = {'组织': row['组织'], '机构': row['机构'], '数据集名称': row['数据集名称'], '模态': row['模态'],
                         '生命周期': row['生命周期'], '链接': row['链接'], '上月下载量': row['上月下载量'],
                         '统计渠道': row['统计渠道'],
                         '统计方法': row['统计方法'], '是否新增': row['是否新增'], '发布时间': row['发布时间']}
                self.entry_list[company_name].append(entry)

        # # 存储这个新的数据到新的excel表格
        # new_data = []
        # for company_name in self.company_name_list:
        #     new_data.extend(self.entry_list[company_name])
        # new_data = pd.DataFrame(new_data)
        # new_data.to_excel('temp_data.xlsx', index=False)

        # 分别查看每个组织的每个模态的数据集数量，及其汇总的下载量，统计
        for company_name in self.company_name_list:
            for modality_name in self.modality_name_list:
                modality_data = [entry for entry in self.entry_list[company_name] if
                                 entry['模态'] == modality_name]
                modality_download = sum([entry['上月下载量'] for entry in modality_data])
                # 根据数据集名称去重
                modality_data = pd.DataFrame(modality_data).drop_duplicates(subset='数据集名称')
                modality_count = len(modality_data)
                print(
                    f"{company_name}的{modality_name}模态数据集数量为:{modality_count}，汇总下载量为:{modality_download}")
                self.result.append(
                    {company_name: {modality_name: {'数据集数量': modality_count, '下载量': modality_download}}})
        # 分别查看每个组织的每个生命周期的数据集数量，及其汇总的下载量，统计
        for company_name in self.company_name_list:
            for life_cycle in self.life_cycle_list:
                life_cycle_data = [entry for entry in self.entry_list[company_name] if
                                   entry['生命周期'] == life_cycle]
                life_cycle_download = sum([entry['上月下载量'] for entry in life_cycle_data])
                # 根据数据集名称去重
                life_cycle_data = pd.DataFrame(life_cycle_data).drop_duplicates(subset='数据集名称')
                life_cycle_count = len(life_cycle_data)
                print(
                    f"{company_name}的{life_cycle}生命周期数据集数量为:{life_cycle_count}，汇总下载量为:{life_cycle_download}")
                self.result.append(
                    {company_name: {life_cycle: {'数据集数量': life_cycle_count, '下载量': life_cycle_download}}})
        # 将结果写入到excel表格
        result_data = []
        for item in self.result:
            for key, value in item.items():
                for k, v in value.items():
                    result_data.append(
                        {'组织': key, '模态/生命周期': k, '数据集数量': v['数据集数量'], '下载量': v['下载量']})

        result_data = pd.DataFrame(result_data)
        # result_data.to_excel('result_data.xlsx', index=False)
        # 统计每个公司的每个模态的数据集数量和下载量
        new_result_data = {}
        # 存储成{公司名：{语言模态数据集数量: , 语言模态下载量: , 语音模态数据集数量: , 语音模态下载量: , 视觉模态数据集数量: , 视觉模态下载量: , 多模态模态数据集数量: , 多模态模态下载量: }}
        for company_name in self.company_name_list:
            company_data = result_data[result_data['组织'] == company_name]
            modality_data = {}
            for modality_name in self.modality_name_list:
                modality_count = company_data[company_data['模态/生命周期'] == modality_name]['数据集数量'].values
                modality_download = company_data[company_data['模态/生命周期'] == modality_name]['下载量'].values
                modality_data[modality_name + '模态数据集数量'] = modality_count[0]
                modality_data[modality_name + '模态下载量'] = modality_download[0]
            new_result_data[company_name] = modality_data
        # 存储到新的excel表格 按照 语言数据集个数	语音数据集个数	视觉数据集个数	多模态数据集个数	语言数据集下载量	语音数据集下载量	视觉数据集下载量	多模态数据集下载量
        modality_data = pd.DataFrame(new_result_data).T
        # 统计每个公司的每个生命周期的数据集数量和下载量
        new_result_data = {}
        # 存储成{公司名：{预训练生命周期数据集数量: , 预训练生命周期下载量: , 微调生命周期数据集数量: , 微调生命周期下载量: , 偏好生命周期数据集数量: , 偏好生命周期下载量: }}
        for company_name in self.company_name_list:
            company_data = result_data[result_data['组织'] == company_name]
            life_cycle_data = {}
            for life_cycle in self.life_cycle_list:
                life_cycle_count = company_data[company_data['模态/生命周期'] == life_cycle]['数据集数量'].values
                life_cycle_download = company_data[company_data['模态/生命周期'] == life_cycle]['下载量'].values
                life_cycle_data[life_cycle + '生命周期数据集数量'] = life_cycle_count[0]
                life_cycle_data[life_cycle + '生命周期下载量'] = life_cycle_download[0]
            new_result_data[company_name] = life_cycle_data
        # 存储到新的excel表格 按照 预训练数据集个数	微调数据集个数	偏好数据集个数	预训练数据集下载量	微调数据集下载量	偏好数据集下载量
        life_cycle_data = pd.DataFrame(new_result_data).T

        # 把二者列合并
        final_data = pd.concat([modality_data, life_cycle_data], axis=1)
        # 调整列的顺序
        # 创建映射字典
        rename_mapping = {
            '语言模态数据集数量': '语言数据集个数',
            '语音模态数据集数量': '语音数据集个数',
            '视觉模态数据集数量': '视觉数据集个数',
            '多模态模态数据集数量': '多模态数据集个数',
            '语言模态下载量': '语言数据集下载量',
            '语音模态下载量': '语音数据集下载量',
            '视觉模态下载量': '视觉数据集下载量',
            '多模态模态下载量': '多模态数据集下载量',
            '预训练生命周期数据集数量': '预训练数据集个数',
            '微调生命周期数据集数量': '微调数据集个数',
            '偏好生命周期数据集数量': '偏好数据集个数',
            '预训练生命周期下载量': '预训练数据集下载量',
            '微调生命周期下载量': '微调数据集下载量',
            '偏好生命周期下载量': '偏好数据集下载量'
        }

        # 重命名列
        final_data = final_data.rename(columns=rename_mapping)

        # 重新排列列顺序
        desired_order = [
            '语言数据集个数',
            '语音数据集个数',
            '视觉数据集个数',
            '多模态数据集个数',
            '语言数据集下载量',
            '语音数据集下载量',
            '视觉数据集下载量',
            '多模态数据集下载量',
            '预训练数据集个数',
            '微调数据集个数',
            '偏好数据集个数',
            '预训练数据集下载量',
            '微调数据集下载量',
            '偏好数据集下载量'
        ]

        final_data = final_data[desired_order]
        desired_order = [
            'BAAI',
            'OpenXLab',
            'Baidu',
            'Baichuan',
            'Zhipu',
            'Ali',
            'Huawei',
            'LMSYS',
            'Falcon',
            'EleutherAI',
            'Meta',
            'Google'
        ]

        # 获取未在 desired_order 中的其他索引
        other_indices = [idx for idx in final_data.index if idx not in desired_order]

        # 创建新的索引顺序：先是 desired_order 中的，后是其他的
        new_index = desired_order + other_indices

        # 重新索引 `DataFrame`，缺失的索引会被填充为 NaN
        df_sorted = final_data.reindex(new_index)

        # 如果您只想保留存在于原 `DataFrame` 中的索引，可以使用 `dropna`
        df_sorted = df_sorted.dropna(how='all')
        used_num = self.dataset_used_num()
        for row in df_sorted.iterrows():
            index, data = row
            df_sorted.loc[index, '数据集被使用数量'] = used_num[index]

        # 显示排序后的 `DataFrame`
        df_sorted.to_excel('final_data.xlsx')



    def dataset_used_num(self):
        """ 统计数据集被使用数量 —— 来自huggingface """
        hf_info_path = r'./result/huggingface/huggingface_dataset_info.xlsx'
        hf_info = pd.read_excel(hf_info_path)
        hf_info['组织'] = hf_info['组织'].apply(self.replace_organization)

        result = {}
        for company_name in self.company_name_list:
            company_data = hf_info[hf_info['组织'] == company_name]
            used_num = sum(company_data['数据集被使用量'])
            print(f"{company_name}数据集被使用数量为:{used_num}")
            result[company_name] = used_num
        return result

def crawl_data():
    """
    单独运行几个爬虫，获取数据
    TODO 并未整合到该函数中统一处理，因为每个爬虫的数据xml可能变化
    """
    # ==================== 数据爬取与处理 ====================
    pass


def combine_data():
    """
    对获取到的数据进行合并，数据来源为几个经过后处理后的excel表格
    """
    # ======================= 数据合并 ======================
    BAAI_excel = r'./result/BAAI/BAAI_dataset_info.xlsx'
    HF_excel = r'./result/huggingface/huggingface_dataset_info.xlsx'
    MS_excel = r'./result/modelscope/modelscope_dataset_info.xlsx'
    DL_excel = r'./result/OpenDataLab/OpenDataLab_dataset_info.xlsx'
    combine_excel(BAAI_excel, DL_excel, HF_excel, MS_excel)
    # ===================== 找到当前的excel表没有但是上个月的表有的部分单独汇总到一个表格 ======================
    current_month = int(time.strftime("%m", time.localtime()))
    if current_month == 1:
        current_month = 13
    last_month_path = f'./data_each_month/month{current_month - 1}.xlsx'
    last_month_data = pd.read_excel(last_month_path)
    # Find the datasets that are present in the last month but not in the current month, find by '链接'
    last_month_links = last_month_data['链接']
    current_month_data = pd.read_excel('./result/combined/combined_dataset_info.xlsx')
    current_month_links = current_month_data['链接']
    missing_links = last_month_links[~last_month_links.isin(current_month_links)]
    missing_datasets = last_month_data[last_month_data['链接'].isin(missing_links)]
    # change missing_datasets '是否新增' to '是'
    missing_datasets.loc[:, '是否新增'] = '是'
    # Save the missing datasets to a separate Excel file
    output_dir = './result/combined/'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'missing_datasets_info.xlsx')
    missing_datasets.to_excel(output_file, index=False)
    print(f"Missing datasets saved successfully to {output_file}")
    # =====================================================
    # 将二者合并
    combined_data = pd.concat([current_month_data, missing_datasets], ignore_index=True)
    # =====================================================
    # 单独列出 '模态' 或 '生命周期' 为空的行，存储为新的表格
    df = combined_data
    filtered_df = df[df['模态'].isna() | df['生命周期'].isna()]
    # 存储到新的表格
    output_filtered_file = os.path.join(output_dir, 'filtered_datasets_info.xlsx')
    filtered_df.to_excel(output_filtered_file, index=False)
    # 去除 '模态' 或 '生命周期' 为空的行，存储到另一个表格
    remaining_df = df.dropna(subset=['模态', '生命周期'])
    # 存储剩余的数据
    output_remaining_file = os.path.join(output_dir, 'remaining_datasets_info.xlsx')
    remaining_df.to_excel(output_remaining_file, index=False)
    print(f"过滤后的表格已保存至: {output_filtered_file}")
    print(f"剩余数据的表格已保存至: {output_remaining_file}")
    # =====================================================
    # 强制报错，需要先手动对模态和生命周期进行填充 TODO: 此处需要手动填充
    raise Exception("请先手动填充模态和生命周期，再运行下一步！")

def get_final_data():
    """
    读取filtered_datasets_info和remaining_datasets_info，合并后保存到final_datasets_info
    """
    output_dir = './result/combined/'
    output_filtered_file = os.path.join(output_dir, 'filtered_datasets_info_annotated.xlsx')
    output_remaining_file = os.path.join(output_dir, 'remaining_datasets_info.xlsx')
    # =====================================================
    # 生成最终的数据表格 —— 合并filtered_datasets_info和remaining_datasets_info
    filtered_df = pd.read_excel(output_filtered_file)
    remaining_df = pd.read_excel(output_remaining_file)
    final_df = pd.concat([filtered_df, remaining_df], ignore_index=True)
    # Save the final DataFrame to an Excel file
    output_final_file = os.path.join(output_dir, 'final_datasets_info.xlsx')
    final_df.to_excel(output_final_file, index=False)
    print(f"Final dataset saved successfully to {output_final_file}")


if __name__ == '__main__':
    # crawl_data()
    # 汇总数据
    # combine_data()
    # 生成最终的数据表格
    # get_final_data()
    # 统计数据
    statistic = StatisticAll('D:\Workspace\ProgramWorkspace\Python\BAAI\Workspace\data_dispose\month3_processed.xlsx')
    statistic.process_data()
    # print("数据统计完成！")
    statistic.dataset_used_num()
