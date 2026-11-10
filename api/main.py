from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
import redis
import psycopg2
import psycopg2.errors
import os
import time
from pydantic import BaseModel
from celery import Celery 

def init_db():
    max_retries = 5
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                user=os.getenv("POSTGRES_USER", "game"),
                password=os.getenv("POSTGRES_PASSWORD", "game"),
                dbname=os.getenv("POSTGRES_DB", "game")
            )
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    score INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    player_id INT REFERENCES players(id),
                    score INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("Таблица 'players' успешно создана или уже существует.")
            return
            
        except psycopg2.OperationalError as e:
            print(f"База данных пока не готова. Попытка {attempt + 1} из {max_retries}. Ждем {retry_delay} сек...")
            time.sleep(retry_delay)
        except Exception as e:
            print(f"Неожиданная ошибка при инициализации БД: {e}")
            return
            
    print("Не удалось подключиться к базе данных после нескольких попыток.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="Multiplayer Game API", lifespan=lifespan)

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

celery_app = Celery(
    "tasks",
    broker=f"redis://{os.getenv('REDIS_HOST', 'redis')}:6379/0"
)

def get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        user=os.getenv("POSTGRES_USER", "game"),
        password=os.getenv("POSTGRES_PASSWORD", "game"),
        dbname=os.getenv("POSTGRES_DB", "game")
    )

class PlayerCreate(BaseModel):
    username: str

@app.get("/")
def root():
    return {"status": "ok", "game": "multiplayer-demo"}

@app.get("/players/online")
def players_online():
    count = r.get("online_players") or 0
    return {"count": int(count)}

@app.get("/health")
def health():
    try:
        conn = get_db()
        conn.close()
        return {"db": "ok", "redis": "ok"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/players")
def create_player(player: PlayerCreate):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO players (username) VALUES (%s) RETURNING id, username, created_at",
            (player.username,)
        )
        new_player = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        celery_app.send_task(
            "worker.save_player_stats",
            args=[new_player[0], 0]  # player_id, начальный score
        )
        return {
            "message": "Игрок успешно добавлен",
            "player": {
                "id": new_player[0],
                "username": new_player[1],
                "created_at": str(new_player[2])
            }
        }
    except psycopg2.errors.UniqueViolation:
        return {"error": "Игрок с таким именем уже существует"}
    except Exception as e:
        return {"error": f"Ошибка базы данных: {str(e)}"}

@app.get("/admin")
def admin_dashboard():
    """Визуализация данных из базы данных"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Получаем всех игроков
        cur.execute("SELECT id, username, score, created_at FROM players ORDER BY id")
        players = cur.fetchall()
        
        # Получаем статистику
        cur.execute("SELECT id, player_id, score, created_at FROM stats ORDER BY id DESC LIMIT 50")
        stats = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Формируем HTML с таблицами
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Game Admin Dashboard</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 40px;
                    background-color: #f5f5f5;
                }
                h1 {
                    color: #333;
                    text-align: center;
                }
                h2 {
                    color: #555;
                    margin-top: 30px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    background-color: white;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    margin-bottom: 30px;
                }
                th, td {
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }
                th {
                    background-color: #4CAF50;
                    color: white;
                    font-weight: bold;
                }
                tr:hover {
                    background-color: #f5f5f5;
                }
                .stats {
                    font-size: 0.9em;
                    color: #666;
                }
            </style>
        </head>
        <body>
            <h1>Game Admin Dashboard</h1>
            
            <h2>Игроки (Всего: """ + str(len(players)) + """)</h2>
            <table>
                <tr>
                    <th>ID</th>
                    <th>Имя пользователя</th>
                    <th>Счёт</th>
                    <th>Дата регистрации</th>
                </tr>
        """
        
        # Добавляем строки с игроками
        for player in players:
            html += f"""
                <tr>
                    <td>{player[0]}</td>
                    <td>{player[1]}</td>
                    <td>{player[2]}</td>
                    <td>{player[3]}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Последние 50 записей статистики</h2>
            <table>
                <tr>
                    <th>ID</th>
                    <th>ID Игрока</th>
                    <th>Счёт</th>
                    <th>Дата</th>
                </tr>
        """
        
        # Добавляем строки со статистикой
        for stat in stats:
            html += f"""
                <tr>
                    <td>{stat[0]}</td>
                    <td>{stat[1]}</td>
                    <td>{stat[2]}</td>
                    <td>{stat[3]}</td>
                </tr>
            """
        
        html += """
            </table>
            <p class="stats">Данные обновляются в реальном времени при перезагрузке страницы</p>
        </body>
        </html>
        """
        
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=html)
        
    except Exception as e:
        return HTMLResponse(content=f"<h1>Ошибка</h1><p>{str(e)}</p>", status_code=500)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    r.incr("online_players")
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_json({"echo": data, "from": "server"})
    finally:
        r.decr("online_players")