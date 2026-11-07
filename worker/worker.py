from celery import Celery
import psycopg2
import os

app = Celery(
    "tasks",
    broker=f"redis://{os.getenv('REDIS_HOST', 'redis')}:6379/0"
)

@app.task
def save_player_stats(player_id: str, score: int):
    """Сохраняет статистику игрока в БД"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        user=os.getenv("POSTGRES_USER", "game"),
        password=os.getenv("POSTGRES_PASSWORD", "game"),
        dbname=os.getenv("POSTGRES_DB", "game")
    )
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO stats (player_id, score, created_at) VALUES (%s, %s, NOW())",
        (player_id, score)
    )
    conn.commit()
    cur.close()
    conn.close()
    return f"Saved: {player_id} -> {score}"