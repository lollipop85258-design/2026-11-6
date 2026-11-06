import os
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

# 🔌 初始化 Socket.IO
# 使用 threading 模式可避免 eventlet 與 MongoDB TLS/SSL 握手衝突
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# === 根路由 ===
@app.route("/")
def index():
    # 回傳 templates/index.html，前端會連上 Socket.IO 並渲染 UI
    return render_template("index.html")

# === 參數 ===
MAX_HISTORY = 100  # 取得歷史訊息時的最大筆數（避免一次查太多資料）

# === MongoDB 連線設定（可用環境變數覆蓋） ===
# - 本地開發預設連到本機 MongoDB。上雲（Atlas）時請在環境變數設定 MONGO_URI
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "chatapp")           # 資料庫名稱
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "messages")  # 集合名稱

# 建立 MongoClient（配置 TLS 與憑證容錯，解決 Render 上的 SSL Alert 錯誤）
mongo_client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where(),
    tlsAllowInvalidCertificates=True
)
db = mongo_client[DB_NAME]
col = db[COLLECTION_NAME]

# 索引（啟動時確保存在）
# 以 timestamp 排序取最新訊息會用到；若需要依 id 查找，_id 本身已是索引（這裡用自訂 uuid 字串）
col.create_index([("timestamp", ASCENDING)])

# === 工具：把 MongoDB 文件轉成前端要的 JSON 物件 ===
def _doc_to_message(doc):
    """
    將資料庫中的訊息文件轉為前端使用的格式：
    - _id: 我們用 uuid 字串當主鍵，傳給前端用作訊息唯一識別
    - timestamp: 以 datetime 儲存在 DB；回傳時轉成 ISO8601（UTC, 結尾 'Z'）
    """
    return {
        "id": doc.get("_id"),
        "username": doc.get("username"),
        "content": doc.get("content"),
        "timestamp": doc.get("timestamp").isoformat(timespec="seconds") + "Z" if doc.get("timestamp") else None,
    }

# === 使用者連線狀態（存在記憶體） ===
# 結構: { sid: {"username": "某人"} }
clients = {}

def broadcast_user_count():
    """
    廣播目前線上有「已設定 username」的連線數量。
    前端接收事件 'user_count' 後更新顯示。
    """
    emit(
        "user_count",
        {"count": len([c for c in clients.values() if c["username"]])},
        broadcast=True,
    )

@socketio.on("connect")
def on_connect():
    """
    新用戶連線時觸發：
    - 先註冊該連線的 SID，username 先放 None
    - 前端通常會在之後送 'join' 指定暱稱
    """
    clients[request.sid] = {"username": None}
    print("Client connect:", request.sid)

@socketio.on("disconnect")
def on_disconnect():
    """
    用戶離線時觸發：
    - 從 clients 移除該 SID
    - 如果他之前有 username，廣播 'user_left' 並更新線上人數
    """
    info = clients.pop(request.sid, None)
    if info and info["username"]:
        emit("user_left", {"username": info["username"]}, broadcast=True)
        broadcast_user_count()
    print("Client disconnect:", request.sid)

@socketio.on("join")
def on_join(data):
    """
    用戶加入聊天室：
    - 設定該 SID 對應的 username（預設 "匿名"）
    - 廣播 'user_joined' 給全部人，並更新線上人數
    """
    username = data.get("username", "匿名")
    clients[request.sid]["username"] = username
    emit("user_joined", {"username": username}, broadcast=True)
    broadcast_user_count()
    print(username, "joined")

@socketio.on("typing")
def on_typing(data):
    """
    用戶輸入中提示：
    - 直接把 'typing' 廣播給其他人（不包含自己）
    - 前端會顯示「某某正在輸入…」
    """
    emit("typing", data, broadcast=True, include_self=False)

@socketio.on("change_username")
def on_change(data):
    """
    用戶更改暱稱：
    - 更新記憶體中的 username
    - 廣播 'user_changed_name'，前端可以顯示「A 更名為 B」
    """
    old = data.get("oldUsername")
    new = data.get("newUsername")
    if request.sid in clients:
        clients[request.sid]["username"] = new
    emit("user_changed_name", {"oldUsername": old, "newUsername": new}, broadcast=True)

# === 核心：接收訊息 → 寫入 MongoDB → 廣播 ===
@socketio.on("send_message")
def on_message(data):
    """
    前端送來一則訊息（'send_message' 事件）：
    1) 取得送訊者的 username（優先從 server 端 SID 綁定，否則用 data.username，最後預設 "匿名"）
    2) 清理訊息內容：移除舊版格式 "user name is ...\ncontent is " 的前綴（如果有）
    3) 建立訊息文件（_id 用 uuid 字串、timestamp 用 datetime 以利排序）
    4) insert_one 寫入 MongoDB
    5) 把整理好的訊息（轉 ISO 字串時間）廣播給其他用戶（不包含自己）
    """
    try:
        # 從 server 記憶體找 username；找不到就 fallback 到 data；最後用 "匿名"
        username = (clients.get(request.sid, {}) or {}).get("username") or data.get("username") or "匿名"

        # 取出文字內容並去頭尾空白
        raw_content = str(data.get("content", "")).strip()

        # 把舊格式 "user name is xxx\ncontent is " 的前綴拿掉
        cleaned_content = re.sub(r"user name is .*?\ncontent is ", "", raw_content, flags=re.IGNORECASE)

        # 產生訊息主鍵（uuid 字串）與時間（UTC）
        msg_id = str(uuid.uuid4())
        now_utc = datetime.utcnow()

        # 準備寫入 MongoDB 的文件
        doc = {
            "_id": msg_id,        # 自訂主鍵；不用 ObjectId 是為了前端更好讀
            "username": username,
            "content": cleaned_content,
            "timestamp": now_utc, # 用 datetime 儲存，查詢/排序更直覺
        }

        # 寫入資料庫
        col.insert_one(doc)

        # 組成給前端的格式（把 datetime 轉成 ISO8601）
        message = _doc_to_message(doc)

        # 廣播給其他人（不含自己）
        emit("chat_message", message, broadcast=True, include_self=False)

    except Exception as e:
        # 任意錯誤只回給這位送訊息的人，不影響其他連線
        emit("chat_error", {"message": f"訊息處理失敗：{e}"}, to=request.sid)

# === 歷史 API：從 MongoDB 取最後 N 筆 ===
@app.route("/get_history", methods=["GET"])
def get_history():
    """
    取得歷史訊息：
    - 先依 timestamp 由新到舊排序，limit MAX_HISTORY
    - 再在程式端 reverse 成由舊到新，符合聊天視覺從上到下的習慣
    - 只挑必要欄位回傳，減少 payload
    """
    cursor = (
        col.find({}, {"_id": 1, "username": 1, "content": 1, "timestamp": 1})
           .sort("timestamp", DESCENDING)
           .limit(MAX_HISTORY)
    )
    docs = list(cursor)
    docs.reverse()
    return jsonify([_doc_to_message(d) for d in docs])

# === 清空歷史（刪除集合中的所有訊息） ===
@app.route("/clear_history", methods=["POST"])
def clear_history():
    """
    清除所有訊息（整個集合內容）：
    - 教學/重置用。正式環境請加權限保護或僅管理者可用。
    """
    try:
        col.delete_many({})
        return jsonify({"status": "success", "message": "歷史紀錄已清除"})
    except Exception as e:
        return jsonify({"status": "error", "message": f"刪除失敗: {e}"}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)