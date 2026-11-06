import os
# 必須放在最開頭，解決 eventlet 與 DNS/SSL 的衝突
os.environ["EVENTLET_NO_GREENDNS"] = "yes"

import eventlet
eventlet.monkey_patch()

import re
import uuid
from datetime import datetime
import certifi

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

# === MongoDB ===
from pymongo import MongoClient, ASCENDING, DESCENDING

from dotenv import load_dotenv # 使用讀取環境的套件（在使用MongoDB Atlas開啟）
load_dotenv() # 讀取.env的環境

app = Flask(__name__)

# 🔌 初始化 Socket.IO（使用 eventlet 異步模式）
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# === 根路由 ===
@app.route("/")
def index():
    return render_template("index.html")

# === 參數 ===
MAX_HISTORY = 100  # 取得歷史訊息時的最大筆數

# === MongoDB 連線設定 ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "chatapp")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "messages")

# 建立 MongoClient（加入 certifi 根憑證，確保雲端 SSL 握手正常）
mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = mongo_client[DB_NAME]
col = db[COLLECTION_NAME]

# 索引（啟動時確保存在）
col.create_index([("timestamp", ASCENDING)])

# === 工具：把 MongoDB 文件轉成前端要的 JSON 物件 ===
def _doc_to_message(doc):
    return {
        "id": doc.get("_id"),
        "username": doc.get("username"),
        "content": doc.get("content"),
        "timestamp": doc.get("timestamp").isoformat(timespec="seconds") + "Z" if doc.get("timestamp") else None,
    }

# === 使用者連線狀態（存在記憶體） ===
clients = {}

def broadcast_user_count():
    emit(
        "user_count",
        {"count": len([c for c in clients.values() if c["username"]])},
        broadcast=True,
    )

@socketio.on("connect")
def on_connect():
    clients[request.sid] = {"username": None}
    print("Client connect:", request.sid)

@socketio.on("disconnect")
def on_disconnect():
    info = clients.pop(request.sid, None)
    if info and info["username"]:
        emit("user_left", {"username": info["username"]}, broadcast=True)
        broadcast_user_count()
    print("Client disconnect:", request.sid)

@socketio.on("join")
def on_join(data):
    username = data.get("username", "匿名")
    clients[request.sid]["username"] = username
    emit("user_joined", {"username": username}, broadcast=True)
    broadcast_user_count()
    print(username, "joined")

@socketio.on("typing")
def on_typing(data):
    emit("typing", data, broadcast=True, include_self=False)

@socketio.on("change_username")
def on_change(data):
    old = data.get("oldUsername")
    new = data.get("newUsername")
    if request.sid in clients:
        clients[request.sid]["username"] = new
    emit("user_changed_name", {"oldUsername": old, "newUsername": new}, broadcast=True)

# === 核心：接收訊息 → 寫入 MongoDB → 廣播 ===
@socketio.on("send_message")
def on_message(data):
    try:
        username = (clients.get(request.sid, {}) or {}).get("username") or data.get("username") or "匿名"
        raw_content = str(data.get("content", "")).strip()
        cleaned_content = re.sub(r"user name is .*?\ncontent is ", "", raw_content, flags=re.IGNORECASE)

        msg_id = str(uuid.uuid4())
        now_utc = datetime.utcnow()

        doc = {
            "_id": msg_id,
            "username": username,
            "content": cleaned_content,
            "timestamp": now_utc,
        }

        col.insert_one(doc)
        message = _doc_to_message(doc)
        emit("chat_message", message, broadcast=True, include_self=False)

    except Exception as e:
        emit("chat_error", {"message": f"訊息處理失敗：{e}"}, to=request.sid)

# === 歷史 API：從 MongoDB 取最後 N 筆 ===