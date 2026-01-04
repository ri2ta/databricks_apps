import os
import logging
import threading
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url, Connection
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import DBAPIError
import requests

# モジュールレベルのエンジン、セッションファクトリ、ロック
_engine = None
_SessionFactory = None
_pool_lock = threading.Lock()
_initialized = False
_token_cache = None  # {"key": (host, client_id, scope), "token": str, "expires_at": float}
_uses_client_credentials = False
logger = logging.getLogger(__name__)

# Ensure environment variables from .env are available even in direct imports (tests, scripts)
load_dotenv()

# Provide commit()/rollback() on SQLAlchemy Connection for 1.4 (used in tests)
if not hasattr(Connection, "commit"):
    def _conn_commit(self):
        return self.connection.commit()
    def _conn_rollback(self):
        return self.connection.rollback()
    Connection.commit = _conn_commit  # type: ignore[attr-defined]
    Connection.rollback = _conn_rollback  # type: ignore[attr-defined]

def _load_config():
    """Load DB config from environment with a clear error if missing."""
    host = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not host:
        raise RuntimeError("DATABRICKS_HOST is required")
    if not http_path:
        raise RuntimeError("DATABRICKS_HTTP_PATH is required")
    if not client_id:
        raise RuntimeError("DATABRICKS_CLIENT_ID is required")
    if not client_secret:
        raise RuntimeError("DATABRICKS_CLIENT_SECRET is required")
    pool_size = int(os.environ.get("DB_POOL_SIZE", "1"))
    max_overflow = int(os.environ.get("DB_MAX_OVERFLOW", "1"))
    pool_timeout = int(os.environ.get("DB_POOL_TIMEOUT", "30"))
    url: str = f"databricks+connector://token:@{host}:443/default?http_path={http_path}&auth_type=databricks-oauth&client_id={client_id}&client_secret={client_secret}"
    return url, pool_size, max_overflow, pool_timeout


def _fetch_access_token(host: str, client_id: str, client_secret: str, scope: str | None = None) -> tuple[str, int]:
    """Fetch OAuth access token using client_credentials against workspace OIDC.

    Scope は Databricks 推奨の "all-apis" をデフォルトとし、指定が空なら付けない。
    400 発生時のレスポンス本文をログに出して原因を特定しやすくする。
    """
    token_endpoint = f"https://{host}/oidc/v1/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope
    resp = requests.post(token_endpoint, data=data, timeout=30)
    if resp.status_code >= 400:
        logger.error("token fetch failed status=%s body=%s", resp.status_code, resp.text)
    resp.raise_for_status()
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("access_token not returned from token endpoint")
    expires_in = int(payload.get("expires_in", 3600))
    return token, expires_in


def _get_cached_access_token(host: str, client_id: str, client_secret: str, scope: str) -> str:
    """Return cached access token or fetch a new one when expired."""
    global _token_cache
    cache_key = (host, client_id, scope)
    now = time.time()

    if _token_cache and _token_cache.get("key") == cache_key:
        if _token_cache["expires_at"] - 60 > now:
            return _token_cache["token"]

    token, expires_in = _fetch_access_token(host, client_id, client_secret, scope)
    # Refresh 10% early; minimum 60 seconds
    ttl = max(60, int(expires_in * 0.9))
    _token_cache = {
        "key": cache_key,
        "token": token,
        "expires_at": now + ttl,
    }
    return token


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


def init_pool(size: int | None = None):
    """コネクションプールを初期化します。
    Databricks Apps ではワーカーが短命のため、極小 QueuePool (size=1, overflow=0/1) で
    最低限の接続を温存しつつ毎リクエストのレイテンシを抑える。
    """
    global _engine, _SessionFactory, _initialized, _uses_client_credentials
    with _pool_lock:
        if _initialized:
            return
        db_url, pool_size, max_overflow, pool_timeout = _load_config()
        url = make_url(db_url)
        # Pass all query params explicitly as connect_args to avoid driver parsing issues
        connect_args = dict(url.query)

        # If service principal creds are present, fetch an access token via client_credentials
        client_id = connect_args.pop("client_id", None)
        client_secret = connect_args.pop("client_secret", None)
        scope = os.environ.get("DATABRICKS_OAUTH_SCOPE") or "all-apis"
        _uses_client_credentials = bool(client_id and client_secret)
        if _uses_client_credentials:
            token = _get_cached_access_token(url.host, client_id, client_secret, scope)
            connect_args["access_token"] = token
            # Use token-based auth; no interactive OAuth flow
            connect_args.pop("auth_type", None)

        logger.info(
            "initializing SQLAlchemy engine with QueuePool size=%s max_overflow=%s timeout=%s",
            pool_size,
            max_overflow,
            pool_timeout,
        )

        # SQLAlchemy エンジンを作成（最小限のプールで接続を温存）
        _engine = create_engine(
            db_url,
            connect_args=connect_args,
            poolclass=QueuePool,
            pool_size=pool_size if pool_size > 0 else 1,
            max_overflow=max_overflow if max_overflow >= 0 else 0,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,  # 接続の健全性チェック
            echo=False,
        )

        # Add commit()/rollback() compatibility for SQLAlchemy <2 Connection objects used in tests
        _orig_connect = _engine.connect

        def _connect_with_commit(*args, **kwargs):
            conn = _orig_connect(*args, **kwargs)
            if hasattr(conn, "commit"):
                return conn

            class _CommitCompat:
                def __init__(self, inner):
                    self._inner = inner

                def __getattr__(self, name):
                    return getattr(self._inner, name)

                def commit(self):
                    return self._inner.connection.commit()

                def rollback(self):
                    return self._inner.connection.rollback()

                def close(self):
                    return self._inner.close()

                def begin(self):
                    return self._inner.begin()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return self._inner.__exit__(exc_type, exc, tb)

            return _CommitCompat(conn)

        _engine.connect = _connect_with_commit  # type: ignore[attr-defined]
        
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

    # Refresh token-based engine when token is close to expiry
    if _uses_client_credentials and _token_cache:
        now = time.time()
        if _token_cache["expires_at"] - 60 <= now:
            close_pool()
            init_pool()

    # SQLAlchemy の raw connection を取得
    # これは DBAPI connection のラッパーで cursor() メソッドを持つ
    try:
        conn = _engine.raw_connection()
    except DBAPIError:
        # On auth errors, try refreshing the pool once
        if _uses_client_credentials:
            close_pool()
            init_pool()
            conn = _engine.raw_connection()
        else:
            raise
    _configure_raw_connection(conn, timeout=timeout)
    return conn


def release_connection(conn):
    """コネクションをプールに戻す（クローズすることで SQLAlchemy プールに返却）。"""
    try:
        conn.close()
    except Exception as e:
        logger.warning("Error releasing connection: %s", e)


def close_pool():
    """プール内の接続を全てクローズし、初期化状態をリセットする。"""
    global _engine, _SessionFactory, _initialized, _uses_client_credentials
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
            _uses_client_credentials = False
            logger.info("Pool closed successfully")


def _configure_raw_connection(conn, timeout: float | None):
    """Set autocommit/timeout for DBAPI connection when possible (compatibility shim)."""
    try:
        dbapi_conn = getattr(conn, "driver_connection", None) or getattr(conn, "connection", None) or conn
        # Enable autocommit for sqlite to match prior behavior without explicit commit
        if hasattr(dbapi_conn, "isolation_level"):
            try:
                dbapi_conn.isolation_level = None
            except Exception:
                pass
        if timeout is not None and hasattr(dbapi_conn, "settimeout"):
            try:
                dbapi_conn.settimeout(timeout)
            except Exception:
                pass
    except Exception:
        logger.debug("raw connection configuration skipped", exc_info=True)


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
