import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

db_pool = None

async def get_pool():
    """Байланыс пулын қайтару немесе қайта құру (егер байланыс үзілсе)"""
    global db_pool
    if db_pool is None or db_pool._closed:
        try:
            db_pool = await asyncpg.create_pool(
                dsn=os.getenv("DATABASE_URL"),
                ssl="require",
                statement_cache_size=0
            )
            print("✅ PostgreSQL базасына қосылу сәтті болды!")
        except Exception as e:
            print(f"❌ Базаға қосылу мүмкін болмады: {e}")
            db_pool = None
    return db_pool

async def execute_query(query, *params, fetch=None):
    """Базаға сұраныс жіберуге арналған негізгі функция (қателерді өңдеумен)"""
    pool = await get_pool()
    if not pool:
        return None
    
    try:
        async with pool.acquire() as conn:
            if fetch == "one":
                return await conn.fetchrow(query, *params)
            elif fetch == "all":
                return await conn.fetch(query, *params)
            else:
                return await conn.execute(query, *params)
    except (asyncpg.exceptions.InterfaceError, OSError) as e:
        print(f"⚠️ Байланыс үзілді: {e}. Қайта қосылуға тырысудамыз...")
        global db_pool
        db_pool = None # Пулды қайта құруға мәжбүрлеу
        # Сұранысты бір рет қайталап көру
        await asyncio.sleep(1)
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                if fetch == "one": return await conn.fetchrow(query, *params)
                if fetch == "all": return await conn.fetch(query, *params)
                await conn.execute(query, *params)
        else:
            print("❌ Қайта қосылу сәтсіз аяқталды.")
            return None

async def init_db():
    """Барлық қажетті кестелерді құру"""
    commands = [
        "CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS kino_codes (code TEXT PRIMARY KEY, channel TEXT, message_id INTEGER, post_count INTEGER, title TEXT);",
        "CREATE TABLE IF NOT EXISTS stats (code TEXT PRIMARY KEY, searched INTEGER DEFAULT 0, viewed INTEGER DEFAULT 0);",
        "CREATE TABLE IF NOT EXISTS admins (user_id BIGINT PRIMARY KEY);"
    ]
    for command in commands:
        await execute_query(command)
    print("База кестелері дайын.")

# === Қалған функциялардың барлығы execute_query арқылы жұмыс істейді ===

async def add_user(user_id):
    await execute_query("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

async def get_user_count():
    row = await execute_query("SELECT COUNT(*) FROM users", fetch="one")
    return row[0] if row else 0

async def get_today_users():
    today = date.today()
    row = await execute_query("SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1", today, fetch="one")
    return row[0] if row else 0

async def add_kino_code(code, channel, message_id, post_count, title):
    await execute_query("""
        INSERT INTO kino_codes (code, channel, message_id, post_count, title) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (code) DO UPDATE SET channel = EXCLUDED.channel, message_id = EXCLUDED.message_id, post_count = EXCLUDED.post_count, title = EXCLUDED.title;
    """, code, channel, message_id, post_count, title)
    await execute_query("INSERT INTO stats (code) VALUES ($1) ON CONFLICT DO NOTHING", code)

async def get_kino_by_code(code):
    row = await execute_query("SELECT code, channel, message_id, post_count, title FROM kino_codes WHERE code = $1", code, fetch="one")
    return dict(row) if row else None

async def get_all_codes():
    rows = await execute_query("SELECT code, title FROM kino_codes", fetch="all")
    return [{"code": row["code"], "title": row["title"]} for row in rows]

async def delete_kino_code(code):
    await execute_query("DELETE FROM stats WHERE code = $1", code)
    result = await execute_query("DELETE FROM kino_codes WHERE code = $1", code)
    return result and result.endswith("1")

async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"): return
    if field == "init":
        await execute_query("INSERT INTO stats (code, searched, viewed) VALUES ($1, 0, 0) ON CONFLICT DO NOTHING", code)
    else:
        await execute_query(f"UPDATE stats SET {field} = {field} + 1 WHERE code = $1", code)

async def get_code_stat(code):
    row = await execute_query("SELECT searched, viewed FROM stats WHERE code = $1", code, fetch="one")
    return dict(row) if row else None

async def update_anime_code(old_code, new_code, new_title):
    await execute_query("UPDATE kino_codes SET code = $1, title = $2 WHERE code = $3", new_code, new_title, old_code)

async def get_all_admins():
    rows = await execute_query("SELECT user_id FROM admins", fetch="all")
    return {row["user_id"] for row in rows}

async def add_admin(user_id: int):
    await execute_query("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

async def remove_admin(user_id: int):
    await execute_query("DELETE FROM admins WHERE user_id = $1", user_id)

async def get_all_user_ids():
    rows = await execute_query("SELECT user_id FROM users", fetch="all")
    return [row["user_id"] for row in rows]
