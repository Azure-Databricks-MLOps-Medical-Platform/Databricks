# Databricks Medical AI Pipeline

Azure Databricks 기반 Medallion Pipeline(Bronze/Silver/Gold) + AI Inference + Judge + MLflow 추적 프로젝트입니다.

## Overview

- 입력 데이터: `data/patient_kimcs/`, `data/ref_vital_signs/`, `data/ref_medical_images/`
- 실행 코드: `model/` (노트북 + Python 실행 스크립트)
- 산출물: `result/` (실행 시 자동 생성, Git 제외)

## Directory

```text
Databricks/
├── data/
│   ├── patient_kimcs/
│   ├── ref_vital_signs/
│   └── ref_medical_images/
├── model/
│   ├── 00_Unity_Catalog_Setup.ipynb
│   ├── 01_config/key_vault_config.ipynb
│   ├── 02_syndata/
│   ├── 03_pipeline/
│   ├── 04_ai_inference/
│   ├── 05_judge/
│   ├── 06_mlops/
│   ├── run_pipeline.py
│   └── run_local.py
├── .env
├── .gitignore
├── connect_test.py
├── requirements.txt
├── README.md
└── TEST_LOG.md
```

## Quick Start

```bash
# 1) 가상환경 생성/활성화
python -m venv .venv
.venv\Scripts\activate

# 2) 패키지 설치
pip install -r requirements.txt

# 3) .env 준비
# .env 파일이 없다면 수동으로 생성 후 아래 키를 채웁니다.
# DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
# AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION

# 4) Databricks 연결 확인
python connect_test.py

# 5) 전체 파이프라인 실행
python model/run_pipeline.py

# 6) 로컬 시뮬레이터 실행(선택)
python model/run_local.py
```

## Execution Guide (Detailed)

### Phase 1. Databricks 연결 점검

1. Azure Portal에서 대상 클러스터를 `Running` 상태로 시작합니다.
2. `python connect_test.py` 실행 후 아래를 확인합니다.
- 현재 사용자 조회 성공
- 클러스터 메타데이터 조회 성공
- DBFS 루트 목록 조회 성공

### Phase 2. 데이터 준비

1. `data/patient_kimcs/`의 CSV/이미지 파일 존재 여부를 확인합니다.
2. 파일 누락 시 파이프라인이 중간에 실패하므로 먼저 보완합니다.

### Phase 3. Medallion Pipeline 실행

`python model/run_pipeline.py` 실행 시 아래 순서로 처리됩니다.

1. Bronze: Raw 데이터 적재
2. Silver: 정제/이상치 제거/Risk score 계산
3. Gold: 환자 요약 집계 생성

결과는 `result/bronze`, `result/silver`, `result/gold`에 저장됩니다.

### Phase 4. AI Inference

동일 실행 안에서 BioMedCLIP 추론을 시도하고 결과를 저장합니다.

- 결과 위치: `result/ai_results/biomedclip_matching.json`
- 필요 의존성: `torch`, `open-clip-torch`, `transformers`, `Pillow`

### Phase 5. MLflow Tracking

동일 실행 안에서 MLflow 실험/런 로그를 기록합니다.

- 결과 위치: `result/mlflow/`
- 리포트: `result/mlflow/experiment_report.json`

### Phase 6. 산출물 확인

1. `result/pipeline_execution.log` 확인
2. `result/ai_results/processing_logs.json` 확인
3. Parquet/JSON 산출물 정상 생성 확인

## Important Notes

- 현재 `model/run_pipeline.py`는 전체 단계를 한 번에 실행합니다.
- `--phase` 같은 부분 실행 CLI 옵션은 현재 구현되어 있지 않습니다.
- 대용량/모델 의존성 설치 환경에 따라 AI 단계만 별도 튜닝이 필요할 수 있습니다.

## Security

- `.env`에는 토큰/키가 포함되므로 Git에 절대 커밋하지 않습니다.
- 키 노출 가능성이 있으면 Databricks PAT/Azure OpenAI 키를 즉시 재발급하세요.
