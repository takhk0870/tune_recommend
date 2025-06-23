import requests
import time
import random
from bs4 import BeautifulSoup
import re
from gensim.models import Word2Vec
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity
from bs4.element import Tag
import csv
import sqlite3
import os

USER_AGENT = "music-tunebook-crawler/0.1 (contact: tak270604@gmail.com)"

def fetch_with_retry(url, max_retries=5):
    headers = {"User-Agent": USER_AGENT}
    backoff = 1
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 410:
            # 非公開または削除済みユーザー
            return None
        else:
            time.sleep(backoff)
            backoff *= 2
    return None

def scrape_tunebook(user_id):
    url = f"https://thesession.org/members/{user_id}/tunebook"
    response = fetch_with_retry(url)
    if response is None:
        print(f"ユーザーID {user_id} は非公開または取得失敗")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    tunes = []
    for li in soup.find_all("li", class_="manifest-item"):
        a = li.find("a", href=True)
        if not a or not a["href"].startswith("/tunes/"):
            continue
        tune_id = a["href"].split("/")[2]
        title = a.text.strip()
        rhythm = None
        rhythm_tag = li.find("a", class_="detail")
        if rhythm_tag:
            rhythm = rhythm_tag.text.strip()
        tunes.append({
            "tune_id": tune_id,
            "title": title,
            "rhythm": rhythm,
            "url": f"https://thesession.org{a['href']}"
        })
    # サーバー負荷対策
    time.sleep(random.uniform(1, 2))
    return tunes

def get_tune_detail(tune_id):
    url = f"https://thesession.org/tunes/{tune_id}"
    response = fetch_with_retry(url)
    if response is None:
        print(f"チューンID {tune_id} は非公開または取得失敗")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    # 作曲者名
    composer = None
    composer_tag = soup.find("a", href=re.compile("/composers/"))
    if not composer_tag:
        # 例: <span class="composer">by John Doe</span>
        composer_span = soup.find("span", class_="composer")
        if composer_span:
            composer = composer_span.text.replace("by", "").strip()
    else:
        composer = composer_tag.text.strip()
    # キー（key）
    key = None
    key_tag = soup.find("span", class_="key")
    if not key_tag:
        # 予備: 詳細テーブル内のKey
        detail_table = soup.find("div", class_="tune-details")
        if isinstance(detail_table, Tag) and hasattr(detail_table, 'find_all'):
            for row in detail_table.find_all("tr"):
                if not isinstance(row, Tag):
                    continue
                th = row.find("th")
                td = row.find("td")
                if th and td and "Key" in th.text:
                    key = td.text.strip()
                    break
    else:
        key = key_tag.text.strip()
    # ABC譜（最初のpre.abc）
    abc = None
    abc_tags = soup.find_all("pre", class_="abc")
    if abc_tags:
        abc = abc_tags[0].text.strip()
    # サーバー負荷対策
    time.sleep(random.uniform(1, 2))
    return {
        "tune_id": tune_id,
        "composer": composer,
        "key": key,
        "abc": abc,
        "url": url
    }

def tokenize_abc(abc_str):
    # 音符・記号・数字・長さ指定などをトークン化
    # 例: "A2 Bc | d2 z2 |" → ["A2", "Bc", "|", "d2", "z2", "|"]
    tokens = re.findall(r"[A-Ga-gzZ][,']*\d*|\|+|\[|\]|\(|\)|:|\d+|[\^_=]+|[<>]", abc_str)
    return tokens

def train_word2vec(tokenized_abc_list, vector_size=128, window=4, sg=1, min_count=1):
    model = Word2Vec(
        sentences=tokenized_abc_list,
        vector_size=vector_size,
        window=window,
        sg=sg,
        min_count=min_count
    )
    return model

def get_melody_vector(abc_str, w2v_model):
    tokens = tokenize_abc(abc_str)
    vectors = [w2v_model.wv[token] for token in tokens if token in w2v_model.wv]
    if not vectors:
        return np.zeros(w2v_model.vector_size)
    return np.mean(vectors, axis=0)

def build_feature_vector(tune, w2v_model, rhythm_encoder, key_encoder, composer_encoder):
    # melody
    melody_vec = get_melody_vector(tune["abc"], w2v_model)
    # rhythm, key, composer
    rhythm_vec = rhythm_encoder.transform([[tune["rhythm"]]])[0] if tune["rhythm"] else np.zeros(rhythm_encoder.categories_[0].shape[0])
    key_vec = key_encoder.transform([[tune["key"]]])[0] if tune["key"] else np.zeros(key_encoder.categories_[0].shape[0])
    composer_vec = composer_encoder.transform([[tune["composer"]]])[0] if tune["composer"] else np.zeros(composer_encoder.categories_[0].shape[0])
    # 結合
    return np.concatenate([melody_vec, rhythm_vec, key_vec, composer_vec])

def recommend_tunes(user_tune_vecs, all_tune_vecs, all_tune_ids, user_tune_ids, top_n=10):
    # ユーザーのTune Bookベクトルの平均
    user_vec = np.mean(user_tune_vecs, axis=0, keepdims=True)
    # ユーザーが既に持っている曲を除外
    mask = [i for i, tid in enumerate(all_tune_ids) if tid not in user_tune_ids]
    candidate_vecs = np.array([all_tune_vecs[i] for i in mask])
    candidate_ids = [all_tune_ids[i] for i in mask]
    # cos類似度計算
    sims = cosine_similarity(user_vec, candidate_vecs)[0]
    top_idx = np.argsort(sims)[::-1][:top_n]
    recommendations = [(candidate_ids[i], sims[i]) for i in top_idx]
    return recommendations

def get_tune_info_from_page(tune_id):
    import requests
    from bs4 import BeautifulSoup
    import time

    url = f"https://thesession.org/tunes/{tune_id}"
    headers = {"User-Agent": "music-tune-crawler/0.1 (contact: tak270604@gmail.com)"}
    response = requests.get(url, headers=headers)
    time.sleep(1)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    # タイトル
    title_tag = soup.find("h1", class_="tune-title")
    title = title_tag.text.strip() if title_tag else None
    # リズム種別
    rhythm_tag = soup.find("span", class_="rhythm")
    rhythm = rhythm_tag.text.strip() if rhythm_tag else None
    # メーター
    meter_tag = soup.find("span", class_="meter")
    meter = meter_tag.text.strip() if meter_tag else None
    # キー
    key = None
    key_tag = soup.find("span", class_="key")
    if not key_tag:
        # 予備: 詳細テーブル内のKey
        detail_table = soup.find("div", class_="tune-details")
        if isinstance(detail_table, Tag) and hasattr(detail_table, 'find_all'):
            for row in detail_table.find_all("tr"):
                if not isinstance(row, Tag):
                    continue
                th = row.find("th")
                td = row.find("td")
                if th and td and "Key" in th.text:
                    key = td.text.strip()
                    break
    else:
        key = key_tag.text.strip()
    # ABC譜（最初のpre.abc）
    abc = None
    abc_tags = soup.find_all("pre", class_="abc")
    if abc_tags:
        abc = abc_tags[0].text.strip()
    # 投稿者名
    submitter = None
    # 1. "Submitted by ..." テキストから抽出
    for s in soup.strings:
        if s.strip().startswith("Submitted by "):
            # 例: "Submitted by John Doe 2 years ago"
            submitter = s.strip().replace("Submitted by ", "").split(" ")[0].strip()
            break
    # 2. 予備: 投稿者リンク
    if not submitter:
        member_links = soup.find_all("a", href=True)
        for link in member_links:
            if isinstance(link, Tag) and "/members/" in link.get("href", ""):
                submitter = link.text.strip()
                break
    # 投稿コメント
    comment = None
    comment_tag = soup.find("div", class_="comment")
    if comment_tag:
        comment = comment_tag.text.strip()
    # バージョン数
    version_count = None
    version_tag = soup.find("span", class_="version-count")
    if version_tag:
        try:
            version_count = int(version_tag.text.strip())
        except:
            version_count = version_tag.text.strip()
    return {
        "tune_id": tune_id,
        "title": title,
        "rhythm": rhythm,
        "meter": meter,
        "key": key,
        "abc": abc,
        "submitter": submitter,
        "comment": comment,
        "version_count": version_count,
        "url": url
    }

def save_tunes_info(tune_ids, save_mode="csv", csv_path="tunes.csv", db_path="tunes.db"):
    """
    tune_ids: list of int or str
    save_mode: "csv" or "sqlite"
    """
    # 既存IDの読み込み
    existing_ids = set()
    if save_mode == "csv" and os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(str(row["id"]))
    elif save_mode == "sqlite" and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS tunes (id TEXT PRIMARY KEY, title TEXT, rhythm TEXT, meter TEXT, key TEXT, abc TEXT, submitter TEXT, comment TEXT, versions TEXT)")
        for row in cur.execute("SELECT id FROM tunes"):
            existing_ids.add(str(row[0]))
        conn.close()

    # 保存用の関数
    def save_csv(tune_info):
        file_exists = os.path.exists(csv_path)
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            fieldnames = ["id", "title", "rhythm", "meter", "key", "abc", "submitter", "comment", "versions"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "id": tune_info.get("tune_id"),
                "title": tune_info.get("title"),
                "rhythm": tune_info.get("rhythm"),
                "meter": tune_info.get("meter"),
                "key": tune_info.get("key"),
                "abc": tune_info.get("abc"),
                "submitter": tune_info.get("submitter"),
                "comment": tune_info.get("comment"),
                "versions": tune_info.get("version_count")
            })

    def save_sqlite(tune_info):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS tunes (id TEXT PRIMARY KEY, title TEXT, rhythm TEXT, meter TEXT, key TEXT, abc TEXT, submitter TEXT, comment TEXT, versions TEXT)")
        cur.execute("INSERT OR IGNORE INTO tunes (id, title, rhythm, meter, key, abc, submitter, comment, versions) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tune_info.get("tune_id"),
                tune_info.get("title"),
                tune_info.get("rhythm"),
                tune_info.get("meter"),
                tune_info.get("key"),
                tune_info.get("abc"),
                tune_info.get("submitter"),
                tune_info.get("comment"),
                tune_info.get("version_count")
            )
        )
        conn.commit()
        conn.close()

    for tid in tune_ids:
        tid_str = str(tid)
        if tid_str in existing_ids:
            print(f"[SKIP] {tid_str} は既に保存済み")
            continue
        try:
            info = get_tune_info_from_page(tid)
            if not info:
                print(f"[ERROR] {tid_str} の取得に失敗")
                continue
            if save_mode == "csv":
                save_csv(info)
            else:
                save_sqlite(info)
            print(f"[OK] {tid_str} を保存しました")
        except Exception as e:
            print(f"[ERROR] {tid_str} で例外発生: {e}")
        finally:
            time.sleep(1)

def save_user_tunes_info(user_ids, save_mode="csv", csv_path="user_tunes.csv", db_path="user_tunes.db"):
    import csv
    import sqlite3
    import os
    # 既存(user_id, tune_id)の組み合わせを読み込み
    existing_pairs = set()
    if save_mode == "csv" and os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_pairs.add((str(row["user_id"]), str(row["tune_id"])))
    elif save_mode == "sqlite" and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS user_tunes (user_id TEXT, tune_id TEXT, title TEXT, rhythm TEXT, key TEXT, abc TEXT, PRIMARY KEY(user_id, tune_id))")
        for row in cur.execute("SELECT user_id, tune_id FROM user_tunes"):
            existing_pairs.add((str(row[0]), str(row[1])))
        conn.close()
    # 保存用
    def save_csv(user_id, tune_info):
        file_exists = os.path.exists(csv_path)
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            fieldnames = ["user_id", "tune_id", "title", "rhythm", "key", "abc"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "user_id": user_id,
                "tune_id": tune_info.get("tune_id"),
                "title": tune_info.get("title"),
                "rhythm": tune_info.get("rhythm"),
                "key": tune_info.get("key"),
                "abc": tune_info.get("abc")
            })
    def save_sqlite(user_id, tune_info):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS user_tunes (user_id TEXT, tune_id TEXT, title TEXT, rhythm TEXT, key TEXT, abc TEXT, PRIMARY KEY(user_id, tune_id))")
        cur.execute("INSERT OR IGNORE INTO user_tunes (user_id, tune_id, title, rhythm, key, abc) VALUES (?, ?, ?, ?, ?, ?)",
            (
                user_id,
                tune_info.get("tune_id"),
                tune_info.get("title"),
                tune_info.get("rhythm"),
                tune_info.get("key"),
                tune_info.get("abc")
            )
        )
        conn.commit()
        conn.close()
    for user_id in user_ids:
        print(f"[USER] {user_id} のTune Bookを取得中...")
        try:
            tunes = scrape_tunebook(user_id)
        except Exception as e:
            print(f"[ERROR] ユーザー{user_id}のTune Book取得失敗: {e}")
            continue
        for tune in tunes:
            pair = (str(user_id), str(tune["tune_id"]))
            if pair in existing_pairs:
                print(f"[SKIP] user_id={user_id}, tune_id={tune['tune_id']} は既に保存済み")
                continue
            try:
                info = get_tune_info_from_page(tune["tune_id"])
                if not info:
                    print(f"[ERROR] user_id={user_id}, tune_id={tune['tune_id']} の取得に失敗")
                    continue
                if save_mode == "csv":
                    save_csv(user_id, info)
                else:
                    save_sqlite(user_id, info)
                print(f"[OK] user_id={user_id}, tune_id={tune['tune_id']} を保存しました")
            except Exception as e:
                print(f"[ERROR] user_id={user_id}, tune_id={tune['tune_id']} で例外発生: {e}")
            finally:
                time.sleep(1)

if __name__ == "__main__":
    user_ids = [186536, 18653]  # 例
    save_user_tunes_info(user_ids, save_mode="sqlite")
    # for tune in tunes:
    #     print(tune)
