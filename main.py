import requests
import time
import pymysql
import hashlib
import json
from bs4 import BeautifulSoup


def get_top10_bilibili():
    """
    自动获取bilibili热门榜单上前10视频的网址并保存在一个字典中，
    每隔2小时检查榜单前10是否产生变化，如果发生变化则更新字典内容
    """
    # 获取当前时间
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print(f"正在获取bilibili热门榜单前10视频的网址，当前时间为 {current_time}...")

    # 请求热门榜单网页
    url = 'https://www.bilibili.com/v/popular/rank/all'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 获取前10视频的标题和链接，并存储到字典中
    top10_videos = {}
    video_list = soup.find_all('li', class_='rank-item')
    for index, video in enumerate(video_list[:10]):
        title = video.find('a', class_='title').get_text().strip()
        href = video.find('a', class_='title')['href']
        top10_videos[index + 1] = {'title': title, 'href': href}
    print(top10_videos[10])
    return top10_videos

def create_table(video_url):
    """
    检查数据库中是否有以当前爬取视频网址为名的数据库表，
    如果没有则在数据库中建立以此数据库表并以当前视频网址命名，
    此表单具有四个属性，分别是评论人，评论时间，评论内容，评论感情倾向，
    并返回1；如果存在则返回0。
    """
    # 连接MySQL数据库
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='20020516',
        database='bilbili'
    )
    # 判断是否存在同名表单
    table_name = hashlib.md5(video_url.encode('utf-8')).hexdigest()
    cursor = conn.cursor()
    exist_table = cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if exist_table == 0:
        # 不存在同名表单，创建表单
        sql_create = f"""
        CREATE TABLE '{table_name} '(
            id INT AUTO_INCREMENT PRIMARY KEY,
            commenter VARCHAR(255),
            comment_time DATETIME,
            comment_content TEXT,
            sentiment INT
        )
        """
        cursor.execute(sql_create)
        conn.commit()
        print(f"创建了新的数据库表 {table_name}")
        result = 1
    else:
        # 已存在同名表单
        print(f"数据库表 {table_name} 已存在")
        result = 0

    # 关闭数据库连接
    cursor.close()
    conn.close()

    return result
# 第三个函数，用于爬取指定视频的所有评论并存储到数据库表中
def crawl_comments(video_url):
    # 创建数据库表
    a=create_table(video_url)

    # 爬取评论数据并插入数据库表中
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='20020516', db='bilbili')
    cursor = conn.cursor()
    table_name = hashlib.md5(video_url.encode('utf-8')).hexdigest()
    # 查询该表中最新的评论时间
    if a==0:
        sql = "select MAX(comment_time) from %s" %table_name
        cursor.execute(sql)
        latest_time = cursor.fetchone()[0]
    else:
        latest_time=0

    # 调用bilibili API爬取指定视频下的所有评论
    api_url = 'https://api.bilibili.com/x/v2/reply'
    params = {
        'type': '1',
        'oid': video_url,
        'sort': '2',
        'pn': '1',
        'ps': '20'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'
    }
    response = requests.get(api_url, params=params, headers=headers)
    data = json.loads(response.text)
    comments = data['data']['replies']

    # 插入爬取到的评论数据到数据库表中
    for comment in comments:
        comment_time = int(comment['ctime'])
        if comment_time <= latest_time:
            break

        user = comment['member']['uname']
        content = comment['content']['message']
        sentiment = analyze_sentiment(content)

        sql = "INSERT INTO %s (comment_user, comment_time, comment_content, sentiment) VALUES ('%s', '%s', '%s', '%s')" % (
        video_url, user, comment_time, content, sentiment)
        cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()

    print('爬取并存储评论数据成功！')


# 分析评论情感倾向的函数
def analyze_sentiment(comment):
    # ...
    pass


# 主函数
if __name__ == '__main__':
    video_url = 'https://www.bilibili.com/video/BV1Kb411W75N'
    crawl_comments(video_url)
