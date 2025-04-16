## crawl_data

项目功能说明：**本项目基于Selenium自动化脚本实现一些开源平台中各个机构发布数据集的相关信息收集**，目前实现`huggingface`，
`github`，`modelscope`，`opendatalab`，`qianyan`，`BAAI`等平台的数据集信息收集。

### 1. 项目文件及其功能

```
crawl_data/
|=======================================================================
├── Data_post_process.py            # 数据后处理程序基类
├── BAAI_crawler.py                 # 爬虫主程序 + 数据后处理
├── Github_crawler.py               # Github 爬虫主程序 + 数据后处理
├── Huggingface_crawler.py          # Huggingface 爬虫主程序 + 数据后处理
├── Modelscope_crawler.py           # Modelscope 爬虫主程序 + 数据后处理
├── Opendatalab_crawler.py          # Opendatalab 爬虫主程序 + 数据后处理
├── Qianyan_crawler.py              # Qianyan 爬虫主程序 + 数据后处理
├── huggingface_cookies.pkl         # Huggingface 的 Cookie 信息 pkl文件
|=======================================================================
├── logs/                           # 日志文件夹
├── main.py                         # 主程序 (主要为数据统计脚本)
|=======================================================================
├── organization_links/             # 组织链接 (需要爬取的目标地址，根据需求设置)
│   ├── github_links.json           # Github 组织链接
│   ├── hugging_face_organization_links.json  # Huggingface 组织链接
│   ├── model_scope_organization_links.json  # Modelscope 组织链接
│   └── opendatalab_organization_links.json  # Opendatalab 组织链接
├── requirements.txt                # 项目依赖文件
|=======================================================================
├── result/                         # 爬取结果文件夹
│   ├── BAAI/                       # BAAI 数据集信息
│   ├── Github/                     # Github 爬取结果
│   ├── OpenDataLab/                # Opendatalab 爬取结果
│   ├── combined/                   # 最终的汇总输出结果
│   ├── huggingface/                # Huggingface 爬取结果
│   └── modelscope/                 # Modelscope 爬取结果
|=======================================================================
├── utils.py                        # 工具文件
└── utils_get_huggingface_cookie.py # 获取 Huggingface 账号 Cookie 信息 (需要设置 USERNAME 和 PASSWORD)
|=======================================================================
data_each_month  					# 往期统计的数据
├── month11.xlsx					# *月份统计的最终结果
└── month12.xlsx

```

**result文件夹说明：**

```
result/
├── combined/
│   ├── combined_dataset_info.xlsx           # 根据原始的数据来源初步汇总的所有数据集信息
│   ├── missing_datasets_info.xlsx           # combined_dataset_info 表没有但上个月的表有的部分（由于有些数据集链接失效）
│   ├── filtered_datasets_info.xlsx          # '模态' 或 '生命周期' 为空的行——需要先手动标注
│   ├── remaining_datasets_info.xlsx         # 除去 '模态' 或 '生命周期' 为空的行后的剩余部分
│   └── final_datasets_info.xlsx             # 当月最终的数据表格
├── huggingface/
│   ├── hugging_face_dataset_info_screenshots/ # 信息获取页面截图存储文件夹
│   ├── hugging_face_organization_datasets_links.json # 每个组织下对应的爬取部分链接
│   ├── huggingface_dataset_details.json      # 爬取到的所有数据集详细信息
│   ├── huggingface_dataset_info.xlsx         # 经过数据后处理后的 Excel 表格
│   └── huggingface_exception_links.json      # 爬取过程中出现异常的链接地址

```

### 2. 使用方法
---

#### **安装依赖**

```shell
pip install -r requirements.txt
```

#### 设置爬取目标

`crawl_data/organization_links/hugging_face_organization_links.json` 中设置需要爬取的 **组织** 链接，作为爬虫起始位置，后续会自动解析组织下的datasets条目

#### **分别运行爬虫** —— 以`huggingface`为例

为了确保huggingface爬取字段正常，请先运行`utils_get_huggingface_cookie.py`获取cookie信息，脚本会将cookie信息保存到
`huggingface_cookies.pkl`文件中。

```shell
# 设置 USERNAME 和 PASSWORD 环境变量
export HF_USERNAME="your_username"
export HF_PASSWORD="your_password"
python utils_get_huggingface_cookie.py
```

#### **运行爬虫**

```shell
python Huggingface_crawler.py
```

- 爬取的**最终结果数据集信息**会保存在`result/huggingface/huggingface_dataset_info.xlsx`文件中
- 爬取过程中出现**异常的链接**地址会保存在`result/huggingface/huggingface_exception_links.json`文件中
- 爬取的所有**数据集详细信息**会保存在`result/huggingface/huggingface_dataset_details.json`文件中
- 每个**组织下对应的爬取链接**会保存在`result/huggingface/hugging_face_organization_datasets_links.json`文件中
- 信息获取**页面截图**会保存在`result/huggingface/hugging_face_dataset_info_screenshots/`文件夹中
- 按照**下载量降序排序**爬取数据集详细信息。

#### **爬取内容示例：**

- json格式

```json
{
  "OpenGVLab": {
    "OmniCorpus-CC": {
      "dataset_info": {
        "Size of downloaded dataset files:": "2.9 TB",
        "Size of the auto-converted Parquet files:": "2.9 TB",
        "Number of rows:": "985,514,699"
      },
      "dataset_panel_info": "\u2b50\ufe0f NOTE: Several parquet files were marked unsafe (viruses) by official ......... terleaved with Text},\n  author={Li, Qingyun and Chen, Zhe and Wang, Weiyun and Wang, Wenhai and Ye, Shenglong and Jin, Zhenjiang and others},\n  journal={arXiv preprint arXiv:2406.08418},\n  year={2024}\n}",
      "related_models_collections": {
        "Collection including OpenGVLab/OmniCosrpus-CC": {
          "OmniCorpus Collection": {
            "other": "OmniCorpus Collection OmniCorpus: A Unified Multimodal Corpus of 10 Billion-Level Images Interleaved with Text \u2022 6 items \u2022 Updated 2 minutes ago \u2022 1"
          }
        }
      },
      "dataset_tags_info": {
        "Tasks": "Image-to-Text Visual Question Answering",
        "Modalities": "Text",
        "Formats": "parquet",
        "Languages": "English",
        "Size": "100M - 1B",
        "ArXiv": "2406.08418",
        "Libraries": "Datasets Dask Croissant + 1",
        "License": "cc-by-4.0"
      },
      "download_count_last_month": "21,832",
      "community": "1",
      "like": "11",
      "link": "https://huggingface.co/datasets/OpenGVLab/OmniCorpus-CC",
      "paper_screenshot_save_path": "./result/huggingface/hugging_face_dataset_info_screenshots/OmniCorpus-CC_pdf.png",
      "dataset_screenshot_save_path": "./result/huggingface/hugging_face_dataset_info_screenshots/OmniCorpus-CC.png"
    }
  }
}
```

- 经过数据后处理后的`huggingface_dataset_info.xlsx`表格（会解析上月数据）

```python
# 获取上月的数据
self.last_month_path = f'../data_each_month/month{current_month - 1}.xlsx'
```

表格示例：

| 组织     | 机构      | 数据集名称    | 模态   | 生命周期 | 链接                                                    | 上月下载量 | 数据集被使用量 | 统计渠道    | 是否新增 | 发布时间 | 下载量 | license   | dataset_panel_info                                           | community | like | dataset_size_related                                         | paper_screenshot_save_path                                   | dataset_screenshot_save_path                                 |
| -------- | --------- | ------------- | ------ | -------- | ------------------------------------------------------- | ---------- | -------------- | ----------- | -------- | -------- | ------ | --------- | ------------------------------------------------------------ | --------- | ---- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| OpenXLab | OpenGVLab | OmniCorpus-CC | 多模态 | 预训练   | https://huggingface.co/datasets/OpenGVLab/OmniCorpus-CC | 21832      | 0              | huggingface | 否       |          |        | cc-by-4.0 | ⭐️ NOTE: Several parquet files were marked unsafe (viruses) by official  scanin ........ Qingyun and Chen, Zhe  and Wang, Weiyun and Wang, Wenhai and Ye, Shenglong and Jin, Zhenjiang and  others},      journal={arXiv preprint  arXiv:2406.08418},      year={2024}     } | 1         | 11   | {'Size of downloaded dataset files:': '2.9 TB', 'Size of the  auto-converted Parquet files:': '2.9 TB', 'Number of rows:': '985,514,699'} | ./result/huggingface/hugging_face_dataset_info_screenshots/OmniCorpus-CC_pdf.png | ./result/huggingface/hugging_face_dataset_info_screenshots/OmniCorpus-CC.png |

获取完毕所有需要处理的数据并得到类似huggingface_dataset_info.xlsx的格式。

#### 运行main.py

1. 运行`combine_data`函数

```
def combine_data():
    """
    对获取到的数据进行合并，数据来源为几个经过后处理后的excel表格
    """
    # ======================= 数据合并 ======================
    BAAI_excel = r'./result/BAAI/BAAI_dataset_info.xlsx'
    HF_excel = r'./result/huggingface/huggingface_dataset_info.xlsx'
    MS_excel = r'./result/modelscope/modelscope_dataset_info.xlsx'
    DL_excel = r'./result/OpenDataLab/OpenDataLab_dataset_info.xlsx'
```

得到`filtered_datasets_info.xlsx`和`remaining_datasets_info.xlsx`两个文件

2. **手动处理**—— 对 `filtered_datasets_info.xlsx` 中的模态和生命周期进行标注
3. 运行 `get_final_data` 函数

```
def get_final_data():
    """
    读取filtered_datasets_info.xlsx和remaining_datasets_info.xlsx，合并后保存到final_datasets_info.xlsx
    """
```

得到 `final_datasets_info.xlsx`

4. 运行统计数据相关代码

```
# 统计数据
statistic = StatisticAll('./result/combined/final_datasets_info.xlsx')
statistic.process_data()
# 单独打印数据集被使用量——针对huggingface平台
statistic.dataset_used_num()
```

得到 `result_data.xlsx` 和打印的输出，将对应内容填入需要的表格
