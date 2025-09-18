[简体中文](./Readme.md)   [English](./Readme_en.md)   

# OSIR-LMTS（Open Source Influence Ranking of Large Model Technology Stack）

This project is dedicated to building a systematic evaluation framework for assessing the open-source influence of large model technology ecosystems. Through scientific and actionable evaluation criteria, we aim to help developers, researchers, and enterprises better understand and measure the real-world value of open-source large model technologies. Our goal is to comprehensively evaluate their impact across four key technical dimensions: **Data**, **Models**, **Infrastructure**, and **Evaluation Platforms**.

## **Evaluation Dimensions**


Currently, the following evaluation dimensions are available:

1. **Data Dimension**: Evaluation based on dataset modality coverage, coverage of the large model lifecycle, and data processing tools.
2. **Model Dimension**: Evaluation includes model usage, modality coverage, model scale, contributor activity, openness, and the number of compatible hardware chips.
3. **Infrastructure Dimension**: Evaluation based on operator libraries, parallel training and inference frameworks, deep learning frameworks, open-source AI compilers, communication libraries, and contributor activity.
4. **Evaluation Platform Dimension**: Includes metrics like evaluation leaderboards, evaluated models, datasets, and evaluation methodologies.

This comprehensive coverage ensures the **systematic and scientific nature** of the framework when assessing the open-source large model technology ecosystem.

## **Data Sources and Evaluation Methods**

### **Data Sources**

- **Data Dimension**: Hugging Face, ModelScope, GitHub, GitCode, Gitee, BAAI Data Platform, OpenDataLab, Google Official Website, META Official Website, OpenI (启智), etc.
- **Model Dimension**: Hugging Face, ModelScope, GitHub, GitCode, Gitee, OpenI (启智), etc.
- **Infrastructure Dimension**: GitHub, GitCode, PaddlePaddle, MindSpore, TensorFlow, PyTorch, etc.
- **Evaluation Platform Dimension**: Hugging Face, GitHub, Gitee, GitCode, and various institutional websites (e.g., OpenCompass).

## **Statistical Methods**

- **Data Indicators**: For projects with multiple repositories, modality and lifecycle stages are determined based on README files and related papers.
- **Model Indicators**: We count all organization/research group repositories under each institution. Only models with **monthly downloads over 50** are considered. Only large models based on **Transformer architecture or later** are included, excluding traditional deep learning models like CNNs and RNNs, and excluding language models with fewer than **500M parameters**.
- **Infrastructure Indicators**: Support for heterogeneous training, number of hardware chip vendors integrated, and lifecycle coverage are gathered from platforms such as GitHub, PaddlePaddle, and MindSpore.
- **Evaluation Platform Indicators**: Statistics on evaluation models and datasets are collected **from 2023 onwards**. Only publicly available models are included, and models evaluated solely for dataset release purposes are excluded. Similarly, only **actively maintained evaluation platforms** are included—temporary leaderboards tied to dataset releases are excluded. All data is based on the evaluation platform’s official records.

We collected a wide range of indicators from **17 platforms** and **13,541 links**. The data collection is updated **monthly**, and the current dataset is up to date as of **September 7, 2025**.

## **Scoring Method**

All indicators are scored using **Min-Max normalization**, and the final influence score is the average of these normalized scores.

## **Community Engagement and Feedback**

We encourage community participation—please feel free to submit an Issue with suggestions or feedback. Your input helps us continually improve our evaluation methods and enhance the completeness and quality of the data.

Thank you for your attention and support of the Open-Source Large Model Technology Ecosystem Influence Rankings. We look forward to working with you to advance open-source technology and innovation.