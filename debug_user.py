#!/usr/bin/env python3
import sqlite3
import sys

def debug_user(db_path: str, user_id: int):
    conn = None
    try:
        # データベースに接続
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) # 読み取り専用で安全に開く
        cur = conn.cursor()

        print(f"--- データベース '{db_path}' の状態 ---")

        # usersテーブルの総数をカウント
        cur.execute("SELECT COUNT(user_id) FROM users")
        total_users = cur.fetchone()[0]
        print(f"総ユーザー数 (usersテーブル): {total_users} 人")

        # tunesテーブルの総数をカウント
        cur.execute("SELECT COUNT(tune_id) FROM tunes")
        total_tunes = cur.fetchone()[0]
        print(f"総曲数 (tunesテーブル): {total_tunes} 曲")

        # tunebooksテーブルの総数（ブックマーク関係の総数）をカウント
        cur.execute("SELECT COUNT(*) FROM tunebooks")
        total_tunebooks = cur.fetchone()[0]
        print(f"総ブックマーク数 (tunebooksテーブル): {total_tunebooks} 件")

        print("-" * 35)

        # 指定されたユーザーのチューンリストを取得
        cur.execute("SELECT tune_id FROM tunebooks WHERE user_id=?", (user_id,))
        tunes_for_user = [row[0] for row in cur.fetchall()]

        if tunes_for_user:
            print(f"ユーザーID {user_id} の情報:")
            print(f"  ブックマークしている曲数: {len(tunes_for_user)}")
            # 曲リストが長い場合、最初の10件だけ表示
            if len(tunes_for_user) > 10:
                print(f"  曲IDリスト (最初の10件): {tunes_for_user[:10]}...")
            else:
                print(f"  曲IDリスト: {tunes_for_user}")
        else:
             # ユーザー自体が存在するか確認
            cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
            if cur.fetchone():
                 print(f"ユーザーID {user_id} は存在しますが、ブックマークは0件です。")
            else:
                 print(f"ユーザーID {user_id} はデータベース内に見つかりませんでした。")

    except sqlite3.OperationalError as e:
        print(f"[エラー] テーブルが見つからないか、DB操作に失敗しました: {e}")
        print("-> `fetch_tunes_db.py` を実行して、データベースを構築してください。")
        return
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python debug_user.py <データベースのパス> <ユーザーID>")
        print("例: python debug_user.py thesession.db 1")
        sys.exit(1)

    db_file_path = sys.argv[1]
    try:
        target_user_id = int(sys.argv[2])
        debug_user(db_file_path, target_user_id)
    except ValueError:
        print("[エラー] ユーザーIDは数字で指定してください。")
        sys.exit(1)
    except FileNotFoundError:
        print(f"[エラー] データベースファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)