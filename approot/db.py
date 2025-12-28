import os
import requests
import threading
import queue
import logging
from databricks import sql

# 環境変数
HOST = os.environ["DATABRICKS_SERVER_HOSTNAME"]      # 例: "dbc-xxxx.cloud.databricks.com"
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
CLIENT_ID = os.environ["DATABRICKS_CLIENT_ID"]
CLIENT_SECRET = os.environ["DATABRICKS_CLIENT_SECRET"]

# プール設定（環境変数で上書き可能）
POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))

# モジュールレベルのプールとロック
_pool = None  # type: queue.Queue
_pool_lock = threading.Lock()
_initialized = False
logger = logging.getLogger(__name__)


def get_dbx_token():
    token_url = f"https://{HOST}/oidc/v1/token"
    resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _create_connection():
    access_token = get_dbx_token()
    return sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=access_token,
    )


def init_pool(size: int = POOL_SIZE):
    """コネクションプールを初期化します。複数回呼ばれても安全（最初の1回のみ実行）。"""
    global _pool, _initialized
    with _pool_lock:
        if _initialized:
            return
        logger.info("initializing pool size=%s", size)
        q = queue.Queue(maxsize=size)
        for _ in range(size):
            conn = _create_connection()
            q.put(conn)
        _pool = q
        _initialized = True


def get_connection(timeout: float | None = None):
    """プールからコネクションを取得します。プール未初期化なら初期化します。
    タイムアウト時は一時接続を返します（フォールバック）。"""
    global _pool, _initialized
    if not _initialized:
        init_pool()
    try:
        return _pool.get(timeout=timeout)
    except Exception:
        return _create_connection()


def release_connection(conn):
    """コネクションをプールに戻す。プールが満杯かエラー時はクローズする。"""
    global _pool, _initialized
    try:
        if _initialized and _pool is not None and _pool.qsize() < _pool.maxsize:
            _pool.put(conn)
        else:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def close_pool():
    """プール内の接続を全てクローズし、初期化状態をリセットする。"""
    global _pool, _initialized
    with _pool_lock:
        if not _initialized or _pool is None:
            return
        while not _pool.empty():
            try:
                conn = _pool.get_nowait()
                conn.close()
            except Exception:
                pass
        _pool = None
        _initialized = False


def get_customers(limit: int = 100):
    """顧客一覧を取得するヘルパー（プールされた接続を利用）。"""
    logger.info("start get_customers limit=%s", limit)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers LIMIT ?", (limit,))
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            logger.info("get_customers fetched rows=%s", len(rows))
            return rows
    except Exception:
        logger.exception("get_customers failed")
        raise
    finally:
        release_connection(conn)

def get_customer_detail(customer_id: int):
    """特定の顧客の詳細を取得するヘルパー（プールされた接続を利用）。"""
    logger.info("start get_customer_detail customer_id=%s", customer_id)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE customerid = ?", (customer_id,))
            cols = [c[0] for c in cur.description]
            row = cur.fetchone()
            if row:
                customer = dict(zip(cols, row))
                logger.info("get_customer_detail found customer_id=%s", customer_id)
                return customer
            else:
                logger.info("get_customer_detail no customer found for customer_id=%s", customer_id)
                return None
    except Exception:
        logger.exception("get_customer_detail failed for customer_id=%s", customer_id)
        raise
    finally:
        release_connection(conn)

if __name__ == "__main__":
    init_pool(2)
    print(get_customers(5))
