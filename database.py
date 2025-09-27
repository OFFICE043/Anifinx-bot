import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from config import HEAD_ADMINS

load_dotenv()

db_pool = None

def get_pool():
    global db_pool
    if db_pool is None:
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=os.getenv("DATABASE_URL"))
            print("✅ PostgreSQL базасына қосылды!")
            init_db() # Бірінші рет қосылғанда кестелерді құру
        except psycopg2.OperationalError as e:
            print(f"❌ Базаға қосылу мүмкін болмады: {e}")
            db_pool = None
    return db_pool

def execute_query(query, params=None, fetch=None):
    conn = None
    pool = get_pool()
    if pool is None: return None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if fetch == "one": return cursor.fetchone()
            if fetch == "all": return cursor.fetchall()
            conn.commit()
    except psycopg2.Error as e:
        print(f"❌ Сұраныс орындауда қате: {e}")
        return None
    finally:
        if conn: pool.putconn(conn)

def init_db():
    """Барлық қажетті кестелерді құру"""
    commands = [
        """CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            status TEXT DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS admins (user_id BIGINT PRIMARY KEY)""",
        """CREATE TABLE IF NOT EXISTS kino_codes (
            code TEXT PRIMARY KEY, channel TEXT, message_id INTEGER, post_count INTEGER, title TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS stats (
            code TEXT PRIMARY KEY, searched INTEGER DEFAULT 0, viewed INTEGER DEFAULT 0
        )"""
    ]
    for command in commands:
        execute_query(command)
    print("База кестелері дайын.")

# --- Осы жерден бастап main.py-ға қажетті барлық функциялар ---

def add_user(user_id: int, username: str, first_name: str):
    execute_query("INSERT INTO users (user_id, username, first_name) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO NOTHING", (user_id, username, first_name))

def get_user_count():
    result = execute_query("SELECT COUNT(*) FROM users", fetch="one")
    return result[0] if result else 0
    
def get_today_users():
    result = execute_query("SELECT COUNT(*) FROM users WHERE DATE(created_at) = CURRENT_DATE", fetch="one")
    return result[0] if result else 0

def get_all_user_ids():
    rows = execute_query("SELECT user_id FROM users", fetch="all")
    return [row[0] for row in rows] if rows else []

def get_user_status(user_id: int) -> str:
    if user_id in HEAD_ADMINS: return 'bosh_admin'
    if execute_query("SELECT user_id FROM admins WHERE user_id = %s", (user_id,), fetch="one"): return 'oddiy_admin'
    user_data = execute_query("SELECT status FROM users WHERE user_id = %s", (user_id,), fetch="one")
    if user_data and user_data[0] == 'vip': return 'vip'
    return 'user'

def add_admin(user_id: int):
    execute_query("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (user_id,))

def remove_admin(user_id: int):
    execute_query("DELETE FROM admins WHERE user_id = %s", (user_id,))

def get_all_admins_from_db():
    rows = execute_query("SELECT user_id FROM admins", fetch="all")
    return [row[0] for row in rows] if rows else []

def add_kino_code(code, channel, message_id, post_count, title):
    execute_query("INSERT INTO kino_codes (code, channel, message_id, post_count, title) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (code) DO UPDATE SET channel=EXCLUDED.channel, message_id=EXCLUDED.message_id, post_count=EXCLUDED.post_count, title=EXCLUDED.title", (code, channel, message_id, post_count, title))
    execute_query("INSERT INTO stats (code) VALUES (%s) ON CONFLICT DO NOTHING", (code,))

def get_kino_by_code(code):
    row = execute_query("SELECT code, channel, message_id, post_count, title FROM kino_codes WHERE code = %s", (code,), fetch="one")
    return dict(zip(['code', 'channel', 'message_id', 'post_count', 'title'], row)) if row else None

def get_all_codes():
    rows = execute_query("SELECT code, title FROM kino_codes", fetch="all")
    return [dict(zip(['code', 'title'], row)) for row in rows] if rows else []

def delete_kino_code(code):
    execute_query("DELETE FROM stats WHERE code = %s", (code,))
    execute_query("DELETE FROM kino_codes WHERE code = %s", (code,))

def get_code_stat(code):
    row = execute_query("SELECT searched, viewed FROM stats WHERE code = %s", (code,), fetch="one")
    return dict(zip(['searched', 'viewed'], row)) if row else None

def increment_stat(code, field):
    if field not in ("searched", "viewed"): return
    execute_query(f"UPDATE stats SET {field} = {field} + 1 WHERE code = %s", (code,))

def update_anime_code(old_code, new_code, new_title):
    execute_query("UPDATE kino_codes SET code = %s, title = %s WHERE code = %s", (new_code, new_title, old_code))
