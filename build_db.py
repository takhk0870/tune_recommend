import requests
import json
import sqlite3
import time
from tqdm import tqdm

# thesession.org APIの基本URL
BASE_URL = "https://thesession.org"
# データベースファイル名
DB_FILE = "thesession.db"
# リクエストのタイムアウト秒数
TIMEOUT_SECONDS = 15
# 連続で何回404エラーが出たら停止するか
CONSECUTIVE_404_LIMIT = 20
# ユーザーIDの最大値 (大体)
MAX_USER_ID = 187000

def initialize_database():
    # データベースとテーブルが存在しない場合のみ作成する。
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY)")
        c.execute('''
            CREATE TABLE IF NOT EXISTS rhythms (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS tunes (
                tune_id INTEGER PRIMARY KEY, name TEXT, tune_url TEXT,
                rhythm_id INTEGER, FOREIGN KEY (rhythm_id) REFERENCES rhythms(id))''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS tunebooks (
                user_id INTEGER, tune_id INTEGER, PRIMARY KEY (user_id, tune_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (tune_id) REFERENCES tunes(tune_id))''')
    print("Database schema is ready.")

def get_or_create_rhythm(conn, rhythm_name):
    # rhythmsテーブルにリズム名がなければ追加し、IDを返す。
    c = conn.cursor()
    c.execute("SELECT id FROM rhythms WHERE name = ?", (rhythm_name,))
    data = c.fetchone()
    if data:
        return data[0]
    else:
        c.execute("INSERT INTO rhythms (name) VALUES (?)", (rhythm_name,))
        return c.lastrowid

def fetch_data_continuously():
    # データベースの最後のユーザーIDから、404エラーが続くまで探索を続ける。
    start_user_id = 1
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(user_id) FROM users")
        last_user_id = c.fetchone()[0]
        if last_user_id is not None:
            start_user_id = last_user_id + 1
    
    print(f"Starting data collection from User ID {start_user_id}...")
    
    user_id_to_scan = start_user_id - 1
    consecutive_404_count = 0

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        processed_tunes = {row[0] for row in c.execute("SELECT tune_id FROM tunes")}
        
        # while Trueループで無限に探索
        while True:
            user_id_to_scan += 1
            # tqdmを使って現在のIDを表示
            pbar = tqdm(total=1, desc=f"Scanning ID: {user_id_to_scan}", bar_format='{desc}')

            try:
                # (APIリクエストとDB保存のロジックは同じ)
                all_tunes_for_user = []
                
                time.sleep(0.3)
                res = requests.get(
                    f"{BASE_URL}/members/{user_id_to_scan}/tunebook?format=json&perpage=50&page=1",
                    timeout=TIMEOUT_SECONDS
                )
                
                # ユーザーIDが最大値を超えた場合、404エラーが続く場合は自動停止
                if user_id_to_scan > MAX_USER_ID and res.status_code in [404, 410]:
                    consecutive_404_count += 1
                    tqdm.write(f"ID {user_id_to_scan} not found. (Consecutive 404s: {consecutive_404_count}/{CONSECUTIVE_404_LIMIT})")
                    if consecutive_404_count >= CONSECUTIVE_404_LIMIT:
                        print(f"\n{CONSECUTIVE_404_LIMIT} consecutive 404 errors. Stopping the process.")
                        break
                    continue
                
                # ユーザーが見つかればカウンターをリセット
                consecutive_404_count = 0
                res.raise_for_status()
                
                page1_data = res.json()
                all_tunes_for_user.extend(page1_data.get('tunes', []))
                total_pages = page1_data.get('pages', 1)

                if total_pages > 1:
                    for page_num in range(2, total_pages + 1):
                        time.sleep(0.3)
                        res = requests.get(
                            f"{BASE_URL}/members/{user_id_to_scan}/tunebook?format=json&perpage=50&page={page_num}",
                            timeout=TIMEOUT_SECONDS
                        )
                        res.raise_for_status()
                        page_data = res.json()
                        all_tunes_for_user.extend(page_data.get('tunes', []))

                if not all_tunes_for_user:
                    continue

                actual_user_id = page1_data['member']['id']
                tqdm.write(f"User ID {actual_user_id}: Found {len(all_tunes_for_user)} tunes. Storing...")
                
                c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (actual_user_id,))

                for tune_setting in all_tunes_for_user:
                    tune_id = tune_setting['id']
                    tune_name = tune_setting['name']
                    tune_url = tune_setting['url']
                    tune_type = tune_setting['type']
                    
                    c.execute("INSERT OR IGNORE INTO tunebooks (user_id, tune_id) VALUES (?, ?)", (actual_user_id, tune_id))
                    
                    if tune_id not in processed_tunes:
                        rhythm_id = get_or_create_rhythm(conn, tune_type)
                        c.execute("INSERT OR IGNORE INTO tunes (tune_id, name, tune_url, rhythm_id) VALUES (?, ?, ?, ?)",
                                  (tune_id, tune_name, tune_url, rhythm_id))
                        processed_tunes.add(tune_id)
                
                # ユーザー1人分の処理が終わるたびに、変更をデータベースにコミット（保存）する
                conn.commit()
                tqdm.write(f"User ID {actual_user_id}: Data committed to database.")

            except requests.exceptions.Timeout:
                tqdm.write(f"Timeout occurred for user {user_id_to_scan}. Skipping.")
            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code not in [404, 410]:
                    tqdm.write(f"An HTTP error occurred for user {user_id_to_scan}: {http_err}")
            except Exception as e:
                tqdm.write(f"An unexpected error occurred for user {user_id_to_scan}: {e}")
            finally:
                pbar.close() # 各IDのスキャン後にバーを閉じる
    
    print("\nData collection finished.")


if __name__ == '__main__':
    # DBがなければ作成する
    initialize_database()
    # 404が続くまでデータ収集を実行
    fetch_data_continuously()