from flask import Flask, request, jsonify
import sqlite3
import threading
import time
import requests

app = Flask(__name__)
DATABASE = 'tasks.db'

def init_db():
    with sqlite3.connect(DATABASE) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT,
                command TEXT,
                priority TEXT,
                status TEXT DEFAULT 'queued',
                worker_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                retries INTEGER DEFAULT 0
            )
        ''')

@app.route('/tasks', methods=['POST'])
def add_task():
    data = request.json
    task_id = data['id']
    name = data['name']
    command = data['command']
    priority = data.get('priority', 'medium')

    with sqlite3.connect(DATABASE) as conn:
        conn.execute('''
            INSERT INTO tasks (id, name, command, priority)
            VALUES (?, ?, ?, ?)
        ''', (task_id, name, command, priority))
    return jsonify({"status": "Task added"}), 201

@app.route('/tasks', methods=['GET'])
def get_tasks():
    with sqlite3.connect(DATABASE) as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM tasks')
        tasks = cur.fetchall()
    return jsonify(tasks)

def schedule_tasks():
    while True:
        with sqlite3.connect(DATABASE) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT id, command FROM tasks
                WHERE status = 'queued'
                ORDER BY 
                    CASE priority
                        WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2
                        ELSE 3
                    END
            ''')
            tasks = cur.fetchall()

            for task_id, cmd in tasks:
                try:
                    res = requests.post('http://worker:5001/run', json={'id': task_id, 'cmd': cmd}, timeout=10)
                    if res.status_code == 200:
                        conn.execute('UPDATE tasks SET status = "running", worker_id = "worker" WHERE id = ?', (task_id,))
                except Exception as e:
                    print(f"Worker unavailable for {task_id}: {e}")
        time.sleep(5)

threading.Thread(target=schedule_tasks, daemon=True).start()

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
