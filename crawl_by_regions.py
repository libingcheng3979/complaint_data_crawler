import requests
import json
import pandas as pd
import time
from datetime import datetime
import random
from tqdm import tqdm
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import urllib3

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def create_session():
    """创建带有重试机制的会话"""
    session = requests.Session()
    retry_strategy = Retry(
        total=10,  # 最大重试次数
        backoff_factor=2,  # 重试间隔
        status_forcelist=[500, 502, 503, 504],  # 需要重试的HTTP状态码
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.verify = False  # 禁用SSL验证
    return session


def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def get_last_processed_page():
    """获取上次处理到的页码"""
    try:
        if os.path.exists('checkpoint.json'):
            with open('checkpoint.json', 'r') as f:
                return json.load(f)['last_page']
    except Exception as e:
        logging.warning(f"读取断点文件失败: {e}")
    return 1


def save_checkpoint(page):
    """保存当前处理的页码"""
    try:
        with open('checkpoint.json', 'w') as f:
            json.dump({'last_page': page}, f)
    except Exception as e:
        logging.warning(f"保存断点失败: {e}")


def fetch_data():
    # 设置请求URL和headers
    url = 'https://api1.liuyan.cjn.cn/messageboard/internetUserInterface/selectThreadsByGroup'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://liuyan.cjn.cn',
        'Referer': 'https://liuyan.cjn.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    session = create_session()
    all_data = []
    records_count = 0

    try:
        # 获取起始页码（断点续传）
        start_page = get_last_processed_page()

        # 首先获取总数据量
        data = {
            'pageSize': '100',
            'pageNum': '1',
            'fid': '6',  # 修改
            'handleState': '',
            'threadState': ''
        }

        response = session.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        first_page = response.json()
        total_records = first_page['total']
        total_pages = (total_records + 99) // 100

        logging.info(f"总记录数: {total_records}, 总页数: {total_pages}, 从第 {start_page} 页继续")

        # 创建进度条
        with tqdm(total=total_records, initial=(start_page - 1) * 100) as pbar:
            for page in range(start_page, total_pages + 1):
                retry_count = 0
                max_retries = 10

                while retry_count < max_retries:
                    try:
                        data['pageNum'] = str(page)
                        response = session.post(url, headers=headers, data=data, timeout=30)
                        response.raise_for_status()
                        json_data = response.json()

                        if 'rows' not in json_data:
                            raise ValueError(f"数据格式错误: {json_data}")

                        # 获取forum信息
                        forum_info = json_data.get('other', {}).get('forum', {})

                        # 处理每条记录
                        for row in json_data['rows']:
                            if row.get('dateline'):
                                row['dateline'] = timestamp_to_date(row['dateline'])

                            for key, value in forum_info.items():
                                row[f'forum_{key}'] = value

                            all_data.append(row)
                            records_count += 1
                            pbar.update(1)

                        # 每500条数据保存一次
                        if records_count % 500 == 0:
                            df = pd.DataFrame(all_data)
                            df.to_csv('6江岸区.csv', index=False, encoding='utf-8-sig',
                                      mode='a', header=(records_count == 500))
                            all_data = []

                        # 保存断点
                        save_checkpoint(page)

                        # 随机延迟
                        delay_time = random.uniform(12, 15)
                        time.sleep(delay_time)

                        break  # 成功获取数据，跳出重试循环

                    except requests.exceptions.RequestException as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            logging.error(f"第{page}页请求失败，已达到最大重试次数: {str(e)}")
                            continue
                        wait_time = 2 ** retry_count  # 指数退避
                        logging.warning(f"第{page}页请求失败，{wait_time}秒后重试: {str(e)}")
                        time.sleep(wait_time)
                    except Exception as e:
                        logging.error(f"处理第{page}页时发生错误: {str(e)}")
                        break

        # 保存剩余数据
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv('6江岸区.csv', index=False, encoding='utf-8-sig', mode='a', header=False)

    except Exception as e:
        logging.error(f"发生致命错误: {str(e)}")
    finally:
        logging.info(f"爬取完成！共获取 {records_count} 条记录")


if __name__ == "__main__":
    fetch_data()