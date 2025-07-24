import requests
import sqlite3
import sys
from collections import defaultdict

# データベースファイル名
DB_FILE = "thesession.db"
# thesession.org APIの基本URL
BASE_URL = "https://thesession.org"

def get_user_tunebook_from_api(user_id):
    # APIを叩いて、指定されたユーザーIDの最新チューンブック情報を取得する。
    try:
        print(f"APIからユーザーID {user_id} の最新チューンブックを取得中...")
        
        all_tunes = []
        page = 1
        while True:
            res = requests.get(
                f"{BASE_URL}/members/{user_id}/tunebook?format=json&perpage=50&page={page}",
                timeout=15
            )
            res.raise_for_status()
            data = res.json()
            
            tunes_on_page = data.get('tunes', [])
            all_tunes.extend(tunes_on_page)
            
            if page >= data.get('pages', 1):
                break
            page += 1
        
        print(f"-> {len(all_tunes)} 曲のブックマークが見つかりました。")
        return {tune['id'] for tune in all_tunes}

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [404, 410]:
            print(f"[エラー] ユーザーID {user_id} が見つからないか、チューンブックが非公開です。")
        else:
            print(f"[エラー] APIへのアクセスでHTTPエラーが発生しました: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[エラー] APIからのチューンブック取得に失敗しました: {e}")
        return None


def recommend_tunes(target_user_id, target_user_tunes, top_n_users=10):
    # ローカルDBの情報と、対象ユーザーのチューンを使って協調フィルタリングを行う。
    if not target_user_tunes:
        print("対象ユーザーのチューンブックが空です。推薦できません。")
        return {}

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()

        # 1. 類似ユーザーを探す際に、自分自身(target_user_id)を除外する
        placeholders = ','.join('?' for _ in target_user_tunes)
        query = f'''
            SELECT user_id, COUNT(tune_id) as common_tunes
            FROM tunebooks
            WHERE tune_id IN ({placeholders}) AND user_id != ?
            GROUP BY user_id
            ORDER BY common_tunes DESC
            LIMIT ?
        '''
        c.execute(query, list(target_user_tunes) + [target_user_id, top_n_users])
        similar_users = [row[0] for row in c.fetchall()]

        if not similar_users:
            print("DB内に似ているユーザーが見つかりませんでした。")
            return {}
        print(f"{len(similar_users)}人の類似ユーザーをDBから発見しました。")

        # 2. 推薦候補曲のスコア等を取得
        placeholders_similar = ','.join('?' for _ in similar_users)
        placeholders_target = ','.join('?' for _ in target_user_tunes)
        query_reco = f'''
            SELECT
                t.tune_id,
                t.name,
                r.name as rhythm_name,
                COUNT(t.tune_id) as overlap_score,
                (SELECT COUNT(*) FROM tunebooks WHERE tune_id = t.tune_id) as global_popularity
            FROM tunebooks tb
            JOIN tunes t ON tb.tune_id = t.tune_id
            JOIN rhythms r ON t.rhythm_id = r.id
            WHERE tb.user_id IN ({placeholders_similar})
              AND tb.tune_id NOT IN ({placeholders_target})
            GROUP BY t.tune_id, t.name, r.name
        '''
        c.execute(query_reco, similar_users + list(target_user_tunes))
        tunes_details = c.fetchall()

    # 3. リズム別に分類
    recommendations = defaultdict(list)
    for tune_id, name, rhythm, overlap, popularity in tunes_details:
        recommendations[rhythm].append({
            "name": name, 
            "tune_id": tune_id, 
            "overlap": overlap, 
            "popularity": popularity
        })

    # 4. 各リズム内でソート
    for rhythm in recommendations:
        recommendations[rhythm].sort(key=lambda x: (x['overlap'], x['popularity']), reverse=True)
        
    return recommendations

if __name__ == '__main__':
    # --- 引数の数をチェック ---
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("使い方: python recommender.py <ユーザーID> [表示する曲数]")
        print("例1: python recommender.py 1")
        print("例2: python recommender.py 1 10")
        sys.exit(1)

    # --- ユーザーIDと表示数を取得 ---
    try:
        target_user_id = int(sys.argv[1])
        # 3番目の引数があればそれを表示数に、なければデフォルトで5曲
        max_display = int(sys.argv[2]) if len(sys.argv) == 3 else 5
    except ValueError:
        print("[エラー] ユーザーIDと表示する曲数は数字で指定してください。")
        sys.exit(1)

    # --- メイン処理 ---
    my_latest_tunes = get_user_tunebook_from_api(target_user_id)
    
    if my_latest_tunes is not None:
        final_recommendations = recommend_tunes(target_user_id, my_latest_tunes, top_n_users=50)

        if not final_recommendations:
            print("\n申し訳ありませんが、現時点であなたへのおすすめは見つかりませんでした。")
        else:
            print(f"\n--- ユーザーID {target_user_id} さんへのおすすめ曲 ---")

            display_order = ["reel", "jig", "polka", "hornpipe"]
            
            for rhythm in display_order:
                if rhythm in final_recommendations:
                    print(f"\n--- {rhythm} ---")
                    # 引数で受け取った数だけ表示
                    for tune in final_recommendations[rhythm][:max_display]:
                        print(f"  - {tune['name']} (重複スコア: {tune['overlap']}, 総ブックマーク数: {tune['popularity']})")
            
            other_rhythms = sorted([r for r in final_recommendations if r not in display_order])
            for rhythm in other_rhythms:
                print(f"\n--- {rhythm} ---")
                # 引数で受け取った数だけ表示
                for tune in final_recommendations[rhythm][:max_display]:
                    print(f"  - {tune['name']} (重複スコア: {tune['overlap']}, 総ブックマーク数: {tune['popularity']})")