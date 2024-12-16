[简体中文](./Readme.md)   [English](./Readme_en.md)   

# OSIR-LMTS（Open Source Influence Ranking of Large Model Technology Stack）

This project aims to establish a systematic framework for evaluating the open-source impact of large model technology ecosystems. By defining scientific and actionable evaluation criteria, we strive to help developers, researchers, and enterprises better understand and assess the practical value of these systems. Our approach focuses on four key technical dimensions: data, models, systems, and evaluation platforms.



## **Evaluation Dimensions**

The evaluation framework includes the following dimensions:

- **Data Dimension:** Includes dataset coverage, large model lifecycle coverage, and data processing tools as evaluation criteria.
- **Model Dimension:** Includes metrics such as model usage, modality coverage, model size, contributor activity, and openness of the models.
- **System Dimension:** Focuses on operator libraries, parallel training and inference frameworks, deep learning frameworks, and contributor activity.
- **Evaluation Platform Dimension:** Covers evaluation leaderboards, models, data, and methods.



This comprehensive coverage of the dimensions of the large-model technology ecosystem ensures the evaluation framework's systematic and scientific rigor in assessing the open-source impact of large-model technologies.

## **Data Sources and Evaluation Methods**

### **Data Sources:**

- **Data Dimension**: HuggingFace, ModelScope, GitHub, GitCode, OpenDataLab, Google Official Website, META Official Website, OpenI, etc.
- **Model Dimension**: HuggingFace, ModelScope, GitHub, GitCode, OpenI, etc.
- **System Dimension**: GitHub, GitCode, Paddle, Mindspore, TensorFlow, PyTorch, etc.
- **Evaluation Platform Dimension**: Hugging Face, GitHub, Gitee, GitCode and official websites of various organizations (OpenCompass, LMArena, HELM, FlagEval, etc.).

*Note*: Supplemented by Google search results and some additional platforms.

### **Evaluation Methods**

- **Data Metrics**: Multiple repositories under the same project are analyzed based on their README files and associated papers to classify each repository's modality and lifecycle stage.
- **Model Metrics**: Only repositories with a monthly download count exceeding 200 and ranking within the top 100 across the organization are included.
- **System Metrics**: Metrics such as support for heterogeneous training, the number of supported training chip vendors, and lifecycle support for large models are collected from platforms including GitHub, Paddle, and Mindspore.
- **Evaluation Platform Metrics**: Metrics for evaluation models and datasets are collected starting from 2023. Only publicly accessible models are considered, excluding those evaluated solely for the purpose of dataset publication.

#### **Data Collection Timeline**

- Download-related data represents the total downloads for the given month, while other data reflects the values as of the end of that month.

#### **Summary of Data Collection**

A total of 44 indicators were collected from 7,025 links. Data collection is conducted around the 15th of each month. This is the first dataset, with the initial collection completed on November 15, 2024.

## Calculation Method

All metrics are normalized using the Min-Max method, and the influence score is calculated as the average of the normalized values.

## **Engagement and Feedback**

We encourage active participation from the community.The public announcement period is from December 16, 2024, to January 16, 2025.

 You can share your suggestions and feedback by scanning the QR code below or submitting an issue directly. Your input will help us refine the evaluation methodology and enhance data accuracy and completeness.

<div align=center>
<img src="./contract_logo.jpg" width="30%" height="30%">
</div>

Thank you for your attention and support for the Open Source Large Model Technology Influence Leaderboard. Together, we can drive innovation and advancement in open-source technologies!