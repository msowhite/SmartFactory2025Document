"""
# dmMariaDB.py 파일은 데이터베이스 접속 및 작업을 처리하는 함수 모음을 제공합니다.

MariaDB 데이터베이스 접속 및 작업을 처리하는 함수 모음

기본 연결 정보:
  - host: smarticon.kr
  - port: 3306
  - database: database_name

환경 변수 설정 (선택사항):
  - DB_HOST: 데이터베이스 호스트 (기본값: smarticon.kr)
  - DB_PORT: 데이터베이스 포트 (기본값: 3306)
  - DB_NAME: 데이터베이스 이름 (기본값: database_name)
  - DB_USER: 데이터베이스 사용자명
  - DB_PASSWORD: 데이터베이스 비밀번호

설치 방법:
  pip install pymysql
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterable, Optional, Sequence

import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

# 위 구문에서 에러가 발생하는 이유:
# pymysql 패키지가 설치되어 있지 않거나,
# pymysql.cursors 내부에 DictCursor가 없는 경우,
# 혹은 pymysql.connections 내에 Connection이 제대로 정의되어 있지 않은 경우에 ImportError가 발생할 수 있습니다.
# 또한 Python 환경의 경로나 가상환경 문제(패키지가 다른 python 경로에 설치된 경우)도 원인일 수 있습니다.


def get_db_config() -> dict[str, Any]:
    """
    환경 변수에서 데이터베이스 접속 정보를 읽어 설정 딕셔너리를 반환합니다.

    Returns
    -------
    dict[str, Any]:
        pymysql.connect()에 전달할 수 있는 설정 딕셔너리

    Raises
    ------
    RuntimeError:
        사용자명 또는 비밀번호가 설정되지 않은 경우
    """
    # # 환경 변수에서 사용자명과 비밀번호를 읽습니다
    # # 기본값으로 하드코딩된 값 사용 (보안상 환경 변수 사용 권장)
    # user = os.getenv("DB_USER", "root")
    # password = os.getenv("DB_PASSWORD", "gksmfskfk8899")

    user = "root"
    password = "gksmfskfk8899"

    if not user or not password:
        raise RuntimeError("DB_USER and DB_PASSWORD must be set before connecting.")

    return {
        "host": os.getenv("DB_HOST", "smarticon.kr"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": user,
        "password": password,
        # "database": os.getenv("DB_NAME", "theFandDB"),
        "database": "FandB",
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": False,
    }


def get_connection() -> Connection:
    """
    새로운 MariaDB 연결을 생성하고 반환합니다.

    Returns
    -------
    Connection:
        pymysql Connection 객체

    Note
    ----
    연결 사용 후 반드시 close()를 호출해야 합니다.
    컨텍스트 매니저(angel_db_connection) 사용을 권장합니다.
    """
    return pymysql.connect(**get_db_config())


@contextmanager
def db_connection() -> Iterable[Connection]:
    """
    데이터베이스 연결을 관리하는 컨텍스트 매니저입니다.

    사용 예시:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            result = cursor.fetchall()

    Yields
    ------
    Connection:
        사용 가능한 MariaDB 연결 객체

    Note
    ----
    컨텍스트를 벗어나면 자동으로 연결이 닫힙니다.
    """
    conn: Connection = pymysql.connect(**get_db_config())
    try:
        yield conn
    finally:
        conn.close()


# 트랜잭션을 시작하는 기본 함수
def begin_transaction(connection: Connection) -> None:
    """
    트랜잭션을 시작합니다.

    Parameters
    ----------
    connection:
        데이터베이스 연결 객체

    Note
    ----
    autocommit이 False로 설정되어 있으면 자동으로 트랜잭션이 시작됩니다.
    이 함수는 명시적으로 트랜잭션을 시작하고 싶을 때 사용합니다.
    """
    connection.begin()


# 트랜잭션을 커밋하는 기본 함수
def commit_transaction(connection: Connection) -> None:
    """
    트랜잭션을 커밋합니다.

    Parameters
    ----------
    connection:
        데이터베이스 연결 객체
    """
    connection.commit()


# 트랜잭션을 롤백하는 기본 함수
def rollback_transaction(connection: Connection) -> None:
    """
    트랜잭션을 롤백합니다.

    Parameters
    ----------
    connection:
        데이터베이스 연결 객체
    """
    connection.rollback()


# SELECT 쿼리를 실행하고 결과를 반환하는 기본 함수
def execute_query(
    query: str,
    params: Optional[Sequence[Any]] = None,
    *,
    connection: Optional[Connection] = None,
) -> list[dict[str, Any]]:
    """
    SELECT 쿼리를 실행하고 결과를 반환합니다.

    Parameters
    ----------
    query:
        실행할 SQL SELECT 쿼리
    params:
        쿼리의 파라미터 (튜플 또는 리스트)
    connection:
        기존 연결 객체. None이면 새 연결을 생성합니다.

    Returns
    -------
    list[dict[str, Any]]:
        쿼리 결과 행들의 리스트 (각 행은 딕셔너리)

    Example
    -------
        results = execute_query("SELECT * FROM users WHERE id = %s", (1,))
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pymysql.connect(**get_db_config())
        owns_connection = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return list(result)
    finally:
        if owns_connection and conn:
            conn.close()


# INSERT, UPDATE, DELETE 쿼리를 실행하고 영향받은 행의 개수를 반환하는 기본 함수
def execute_update(
    query: str,
    params: Optional[Sequence[Any]] = None,
    *,
    connection: Optional[Connection] = None,
    commit: bool = True,
) -> int:
    """
    INSERT, UPDATE, DELETE 쿼리를 실행합니다.

    Parameters
    ----------
    query:
        실행할 SQL 쿼리 (INSERT, UPDATE, DELETE)
    params:
        쿼리의 파라미터 (튜플 또는 리스트)
    connection:
        기존 연결 객체. None이면 새 연결을 생성합니다.
    commit:
        True이면 자동으로 커밋합니다.

    Returns
    -------
    int:
        영향받은 행의 개수

    Example
    -------
        affected_rows = execute_update(
            "UPDATE users SET name = %s WHERE id = %s",
            ("John", 1)
        )
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pymysql.connect(**get_db_config())
        owns_connection = True

    try:
        with conn.cursor() as cursor:
            affected_rows = cursor.execute(query, params)
        if commit:
            conn.commit()
        return affected_rows
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if owns_connection and conn:
            conn.close()


# 동일한 쿼리를 여러 파라미터로 일괄 실행하는 기본 함수
def execute_many(
    query: str,
    params_list: Sequence[Sequence[Any]],
    *,
    connection: Optional[Connection] = None,
    commit: bool = True,
) -> int:
    """
    동일한 쿼리를 여러 파라미터로 일괄 실행합니다 (배치 처리).

    Parameters
    ----------
    query:
        실행할 SQL 쿼리
    params_list:
        파라미터들의 리스트 (각 요소는 하나의 쿼리 실행에 사용됨)
    connection:
        기존 연결 객체. None이면 새 연결을 생성합니다.
    commit:
        True이면 자동으로 커밋합니다.

    Returns
    -------
    int:
        영향받은 행의 총 개수

    Example
    -------
        params = [("John", 1), ("Jane", 2), ("Bob", 3)]
        affected_rows = execute_many(
            "UPDATE users SET name = %s WHERE id = %s",
            params
        )
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pymysql.connect(**get_db_config())
        owns_connection = True

    try:
        with conn.cursor() as cursor:
            affected_rows = cursor.executemany(query, params_list)
        if commit:
            conn.commit()
        return affected_rows
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if owns_connection and conn:
            conn.close()


# 저장 프로시저를 호출하고 결과를 반환하는 기본 함수
def call_procedure(
    procedure_name: str,
    params: Optional[Sequence[Any]] = None,
    *,
    connection: Optional[Connection] = None,
    commit: bool = True,
) -> list[dict[str, Any]]:
    """
    저장 프로시저를 호출하고 결과를 반환합니다.

    Parameters
    ----------
    procedure_name:
        호출할 저장 프로시저 이름
    params:
        프로시저에 전달할 파라미터 (튜플 또는 리스트)
    connection:
        기존 연결 객체. None이면 새 연결을 생성합니다.
    commit:
        True이면 자동으로 커밋합니다.

    Returns
    -------
    list[dict[str, Any]]:
        프로시저가 반환한 결과 행들의 리스트

    Example
    -------
        result = call_procedure(
            "USP_TBDocument_INSERT",
            (1, "url", "text_pre", "text_post", "N")
        )
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pymysql.connect(**get_db_config())
        owns_connection = True

    try:
        with conn.cursor() as cursor:
            cursor.callproc(procedure_name, params or ())
            result = cursor.fetchall()

        if commit:
            conn.commit()

        return list(result)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if owns_connection and conn:
            conn.close()
