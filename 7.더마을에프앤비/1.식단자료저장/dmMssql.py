"""
# dmMssql.py 파일은 데이터베이스 접속 및 작업을 처리하는 함수 모음을 제공합니다.

MSSQL 데이터베이스 접속 및 작업을 처리하는 함수 모음

기본 연결 정보:
  - host: smarticon.kr
  - port: 1433
  - database: FandB

환경 변수 설정 (선택사항):
  - DB_HOST: 데이터베이스 호스트 (기본값: smarticon.kr)
  - DB_PORT: 데이터베이스 포트 (기본값: 1433)
  - DB_NAME: 데이터베이스 이름 (기본값: FandB)
  - DB_USER: 데이터베이스 사용자명
  - DB_PASSWORD: 데이터베이스 비밀번호
  - DB_DRIVER: ODBC 드라이버 (기본값: ODBC Driver 17 for SQL Server)

설치 방법:
  pip install pyodbc
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterable, Optional, Sequence

import pyodbc
from pyodbc import Connection


def get_connection_string() -> str:
    """
    환경 변수에서 데이터베이스 접속 정보를 읽어 연결 문자열을 반환합니다.

    Returns
    -------
    str:
        pyodbc.connect()에 전달할 수 있는 연결 문자열

    Raises
    ------
    RuntimeError:
        사용자명 또는 비밀번호가 설정되지 않은 경우
    """
    user = "sa"
    password = "gksmfskfk8899"

    if not user or not password:
        raise RuntimeError("DB_USER and DB_PASSWORD must be set before connecting.")

    server = os.getenv("DB_HOST", "smarticon.kr")
    port = os.getenv("DB_PORT", "13000")
    database = os.getenv("DB_NAME", "FandB")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    # 연결 문자열 구성
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )

    return conn_str


def get_connection() -> Connection:
    """
    새로운 MSSQL 연결을 생성하고 반환합니다.

    Returns
    -------
    Connection:
        pyodbc Connection 객체

    Note
    ----
    연결 사용 후 반드시 close()를 호출해야 합니다.
    컨텍스트 매니저(db_connection) 사용을 권장합니다.
    """
    try:
        return pyodbc.connect(get_connection_string(), autocommit=False)
    except pyodbc.Error as e:
        conn_str = get_connection_string()
        # 비밀번호 마스킹
        masked_str = conn_str.split("PWD=")[0] + "PWD=***;"
        print(f"데이터베이스 연결 오류: {e}")
        print(f"연결 문자열: {masked_str}")
        raise


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
        사용 가능한 MSSQL 연결 객체

    Note
    ----
    컨텍스트를 벗어나면 자동으로 연결이 닫힙니다.
    """
    try:
        conn: Connection = pyodbc.connect(get_connection_string(), autocommit=False)
    except pyodbc.Error as e:
        conn_str = get_connection_string()
        # 비밀번호 마스킹
        masked_str = conn_str.split("PWD=")[0] + "PWD=***;"
        print(f"데이터베이스 연결 오류: {e}")
        print(f"연결 문자열: {masked_str}")
        raise
    try:
        yield conn
    finally:
        conn.close()


def _cursor_to_dict(cursor) -> list[dict[str, Any]]:
    """
    cursor의 fetchall() 결과를 딕셔너리 리스트로 변환합니다.

    Parameters
    ----------
    cursor:
        pyodbc cursor 객체

    Returns
    -------
    list[dict[str, Any]]:
        딕셔너리 리스트
    """
    # cursor.description이 None이면 결과셋이 없음
    if cursor.description is None:
        return []

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


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
    # pyodbc는 autocommit=False일 때 자동으로 트랜잭션이 시작됩니다.
    # 명시적으로 BEGIN TRANSACTION을 실행합니다.
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION")
        connection.commit()
    finally:
        cursor.close()


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
        실행할 SQL SELECT 쿼리 (파라미터 플레이스홀더는 ? 사용)
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
        results = execute_query("SELECT * FROM users WHERE id = ?", (1,))
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        owns_connection = True

    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or ())
            result = _cursor_to_dict(cursor)
            return result
        finally:
            cursor.close()
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
        실행할 SQL 쿼리 (INSERT, UPDATE, DELETE) (파라미터 플레이스홀더는 ? 사용)
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
            "UPDATE users SET name = ? WHERE id = ?",
            ("John", 1)
        )
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        owns_connection = True

    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or ())
            affected_rows = cursor.rowcount
            if commit:
                conn.commit()
            return affected_rows
        finally:
            cursor.close()
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
        실행할 SQL 쿼리 (파라미터 플레이스홀더는 ? 사용)
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
            "UPDATE users SET name = ? WHERE id = ?",
            params
        )
    """
    owns_connection = False
    conn = connection
    if conn is None:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        owns_connection = True

    try:
        cursor = conn.cursor()
        try:
            total_affected = 0
            for params in params_list:
                cursor.execute(query, params)
                total_affected += cursor.rowcount
            if commit:
                conn.commit()
            return total_affected
        finally:
            cursor.close()
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
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        owns_connection = True

    try:
        cursor = conn.cursor()
        try:
            # MSSQL 저장 프로시저 호출
            # 파라미터 플레이스홀더 생성
            if params:
                placeholders = ",".join(["?"] * len(params))
                query = f"EXEC {procedure_name} {placeholders}"
                cursor.execute(query, params)
            else:
                query = f"EXEC {procedure_name}"
                cursor.execute(query)

            result = _cursor_to_dict(cursor)

            if commit:
                conn.commit()

            return result
        finally:
            cursor.close()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if owns_connection and conn:
            conn.close()
