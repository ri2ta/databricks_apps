import os
import logging
import threading
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# モジュールレベルのエンジン、セッションファクトリ、ロック
_engine = None
_SessionFactory = None
_pool_lock = threading.Lock()
_initialized = False
logger = logging.getLogger(__name__)

# DSN と プール設定を環境変数から読み取る
# SQLALCHEMY_DATABASE_URL は必須、なければ KeyError でフェイルファスト
DATABASE_URL = os.environ["SQLALCHEMY_DATABASE_URL"]
POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.environ.get("DB_MAX_OVERFLOW", "10"))
POOL_TIMEOUT = int(os.environ.get("DB_POOL_TIMEOUT", "30"))


def get_engine():
    """SQLAlchemy エンジンを取得します。プール未初期化なら None を返します。"""
    global _engine
    return _engine


def get_session() -> Session:
    """SQLAlchemy Session を取得します。プール未初期化なら init_pool を呼び出します。"""
    global _SessionFactory, _initialized
    if not _initialized:
        init_pool()
    return _SessionFactory()


def init_pool(size: int = POOL_SIZE):
    """コネクションプールを初期化します。複数回呼ばれても安全（最初の1回のみ実行）。"""
    global _engine, _SessionFactory, _initialized
    with _pool_lock:
        if _initialized:
            return
        logger.info("initializing SQLAlchemy engine with pool_size=%s, max_overflow=%s, pool_timeout=%s",
                    size, MAX_OVERFLOW, POOL_TIMEOUT)
        
        # SQLAlchemy 2.x エンジンを作成（QueuePool をデフォルトで使用）
        _engine = create_engine(
            DATABASE_URL,
            poolclass=QueuePool,
            pool_size=size,
            max_overflow=MAX_OVERFLOW,
            pool_timeout=POOL_TIMEOUT,
            pool_pre_ping=True,  # 接続の健全性チェック
            echo=False,
        )
        
        # Session ファクトリを作成
        _SessionFactory = sessionmaker(bind=_engine, expire_on_commit=False)
        
        _initialized = True
        logger.info("SQLAlchemy engine initialized successfully")


def get_connection(timeout: float | None = None):
    """プールから raw DBAPI コネクションを取得します。プール未初期化なら初期化します。
    generic_repo との互換性のため、cursor() メソッドを持つ DBAPI 接続を返します。"""
    global _engine, _initialized
    if not _initialized:
        init_pool()
    
    # SQLAlchemy の raw connection を取得
    # これは DBAPI connection のラッパーで cursor() メソッドを持つ
    return _engine.raw_connection()


def release_connection(conn):
    """コネクションをプールに戻す（クローズすることで SQLAlchemy プールに返却）。"""
    try:
        conn.close()
    except Exception as e:
        logger.warning("Error releasing connection: %s", e)


def close_pool():
    """プール内の接続を全てクローズし、初期化状態をリセットする。"""
    global _engine, _SessionFactory, _initialized
    with _pool_lock:
        if not _initialized:
            return
        logger.info("Disposing SQLAlchemy engine and closing pool")
        try:
            if _engine is not None:
                _engine.dispose()
        except Exception as e:
            logger.warning("Error disposing engine: %s", e)
        finally:
            _engine = None
            _SessionFactory = None
            _initialized = False
            logger.info("Pool closed successfully")


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
