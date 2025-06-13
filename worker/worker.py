from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run', methods=['POST'])
def run_task():
    data = request.json
    task_id = data.get('id')
    cmd = data.get('cmd')

    try:
        result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=60)
        return jsonify({
            "status": "success",
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except Exception as e:
        return jsonify({"status": "failed", "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
