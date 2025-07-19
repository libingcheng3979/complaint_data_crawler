import requests
import json
import csv
import time
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime
from tqdm import tqdm
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class RequestError(Exception):
    pass


def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504, 429],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.verify = False  # 禁用SSL验证
    return session


def get_search_results(session, keywords, pageNum, pageSize=100, max_retries=3):
    url = "https://api1.liuyan.cjn.cn/messageboard/api/essearch/querySearchMapByPage"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://liuyan.cjn.cn',
        'Referer': 'https://liuyan.cjn.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'
    }
    data = {
        'keywords': keywords,
        'pageSize': pageSize,
        'pageNum': pageNum
    }

    for attempt in range(max_retries):
        try:
            response = session.post(url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise RequestError(f"请求失败: {str(e)}")
            wait_time = (attempt + 1) * 15
            time.sleep(wait_time)


def convert_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def save_checkpoint(filename, last_page):
    with open(f"{filename}_checkpoint.txt", 'w') as f:
        f.write(str(last_page))


def load_checkpoint(filename):
    try:
        with open(f"{filename}_checkpoint.txt", 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 1


def main():
    keywords = "交通噪音"
    pageSize = 100
    filename = f"{keywords}.csv"
    session = create_session()

    try:
        # 加载断点
        start_page = load_checkpoint(filename)

        # 获取总数据量
        results = get_search_results(session, keywords, 1, pageSize)
        total = results['data']['total']
        total_pages = (total // pageSize) + (1 if total % pageSize else 0)

        # 计算剩余数据量
        remaining_records = total - ((start_page - 1) * pageSize)

        print(f"正在从第 {start_page} 页继续爬取，共 {total} 条数据")

        # 判断文件是否存在
        file_exists = os.path.exists(filename)

        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['fid', 'dateline', 'subject', 'typeId', 'userId', 'content']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            # 使用全局计数器
            global_count = 0

            # 创建全局进度条
            with tqdm(total=remaining_records, desc="爬取进度", unit="条") as pbar:
                extracted_data = []
                for page in range(start_page, total_pages + 1):
                    try:
                        results = get_search_results(session, keywords, page)
                        data = results['data']['rows']

                        for item in data:
                            row_data = {
                                'fid': item['source']['fid'],
                                'dateline': convert_timestamp(item['source']['dateline']),
                                'subject': item['source']['subject'],
                                'typeId': item['source']['typeId'],
                                'userId': item['source']['userId'],
                                'content': item['source']['content']
                            }
                            extracted_data.append(row_data)
                            global_count += 1
                            pbar.update(1)

                            if len(extracted_data) >= 100:
                                writer.writerows(extracted_data)
                                csvfile.flush()
                                extracted_data = []

                        # 保存断点
                        save_checkpoint(filename, page + 1)

                        # 随机延迟
                        time.sleep(random.uniform(7, 10))

                    except Exception as e:
                        if extracted_data:
                            writer.writerows(extracted_data)
                            csvfile.flush()
                        save_checkpoint(filename, page)
                        raise

                # 保存剩余数据
                if extracted_data:
                    writer.writerows(extracted_data)

        print(f"\n数据已保存到 {filename}")

        # 爬取完成后删除断点文件
        try:
            os.remove(f"{filename}_checkpoint.txt")
        except FileNotFoundError:
            pass

    except Exception as e:
        print(f"\n发生错误: {str(e)}")
        print("程序中断，已保存断点信息，下次运行将从中断处继续")


if __name__ == "__main__":
    main()