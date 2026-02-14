"""
Databricks 연결 테스트 스크립트
==============================
.env 파일의 설정값을 사용하여 Databricks Workspace 연결을 검증합니다.

사용법:
  1. .env 파일에 DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID 입력
  2. python connect_test.py 실행
"""

import os
import sys
from dotenv import load_dotenv

# .env 로드
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")


def check_env():
    """환경 변수 유효성 검사"""
    print("=" * 60)
    print("  Databricks 연결 테스트")
    print("=" * 60)

    errors = []
    if not DATABRICKS_HOST or "your" in DATABRICKS_HOST:
        errors.append("DATABRICKS_HOST 가 설정되지 않았습니다.")
    if not DATABRICKS_TOKEN or "your" in DATABRICKS_TOKEN:
        errors.append("DATABRICKS_TOKEN 을 .env 에 입력해 주세요.")
    if not DATABRICKS_CLUSTER_ID or "your" in DATABRICKS_CLUSTER_ID:
        errors.append("DATABRICKS_CLUSTER_ID 를 .env 에 입력해 주세요.")

    if errors:
        print("\n[ERROR] 환경 변수 미설정:")
        for e in errors:
            print(f"  - {e}")
        print("\n.env 파일을 수정한 뒤 다시 실행하세요.")
        sys.exit(1)

    print(f"\n  Host      : {DATABRICKS_HOST}")
    print(f"  Cluster ID: {DATABRICKS_CLUSTER_ID}")
    print(f"  Token     : {'*' * 8}...{DATABRICKS_TOKEN[-4:]}")


def test_sdk_connection():
    """Databricks SDK를 사용한 연결 테스트"""
    print("\n--- [1/3] Databricks SDK 연결 테스트 ---")
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN,
        )
        me = w.current_user.me()
        print(f"  ✅ 인증 성공! 사용자: {me.user_name}")
        return w
    except Exception as e:
        print(f"  ❌ SDK 연결 실패: {e}")
        return None


def test_cluster_info(w):
    """클러스터 정보 조회"""
    print("\n--- [2/3] 클러스터 정보 조회 ---")
    if w is None:
        print("  ⏭️  SDK 연결 실패로 건너뜁니다.")
        return
    try:
        cluster = w.clusters.get(DATABRICKS_CLUSTER_ID)
        print(f"  ✅ 클러스터: {cluster.cluster_name}")
        print(f"     상태     : {cluster.state}")
        print(f"     Runtime  : {cluster.spark_version}")
        print(f"     Access   : {cluster.data_security_mode}")
        print(f"     Worker   : {cluster.node_type_id}")
    except Exception as e:
        print(f"  ❌ 클러스터 조회 실패: {e}")


def test_dbfs_access(w):
    """DBFS 접근 테스트"""
    print("\n--- [3/3] DBFS 접근 테스트 ---")
    if w is None:
        print("  ⏭️  SDK 연결 실패로 건너뜁니다.")
        return
    try:
        items = list(w.dbfs.list("/"))
        print(f"  ✅ DBFS 루트 항목: {len(items)}개")
        for item in items[:5]:
            print(f"     📁 {item.path}")
        if len(items) > 5:
            print(f"     ... 외 {len(items) - 5}개")
    except Exception as e:
        print(f"  ❌ DBFS 접근 실패: {e}")


if __name__ == "__main__":
    check_env()
    w = test_sdk_connection()
    test_cluster_info(w)
    test_dbfs_access(w)
    print("\n" + "=" * 60)
    print("  테스트 완료!")
    print("=" * 60)
