# -*- coding: utf-8 -*-
# @Time       : 2024/11/7 16:18
# @Author     : Marverlises
# @File       : get_huggingface_cookie.py
# @Description: PyCharm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from utils import init_driver
import time
import pickle  # 用于保存和加载 cookies
import os

# 从环境变量中读取用户名和密码
USERNAME = os.getenv("HF_USERNAME")
PASSWORD = os.getenv("HF_PASSWORD")

# 检查是否获取到环境变量中的值
if not USERNAME or not PASSWORD:
    print("请确保环境变量 'HF_USERNAME' 和 'HF_PASSWORD' 已设置")
    exit(1)

driver = init_driver()
driver.get("https://huggingface.co/login")
time.sleep(2)

# 输入用户名和密码
username = driver.find_element(By.NAME, "username")
password = driver.find_element(By.NAME, "password")

# 将 Hugging Face 账号的用户名和密码输入到对应的输入框
username.send_keys(USERNAME)  # 从环境变量获取用户名
password.send_keys(PASSWORD)  # 从环境变量获取密码
password.send_keys(Keys.RETURN)

# 等待登录完成
time.sleep(5)  # 根据网络情况调整等待时间

# 获取 cookies 并保存到文件中
cookies = driver.get_cookies()
with open("huggingface_cookies.pkl", "wb") as file:
    pickle.dump(cookies, file)
print("Cookies 已保存为 huggingface_cookies.pkl")

# 关闭浏览器
driver.quit()
