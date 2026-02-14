# -*- coding: utf-8 -*-
"""
Databricks에 모든 노트북 일괄 업로드
model/ 아래 전체 .ipynb → /Shared/P2T2/
"""
import os
import sys
import base64
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("❌ .env에 DATABRICKS_HOST, DATABRICKS_TOKEN 필요")
    sys.exit(1)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
print(f"✅ Databricks 연결: {DATABRICKS_HOST}")

# 모든 노트북 매핑 (로컬 경로 → Databricks 경로)
MODEL_DIR = os.path.join(os.path.dirname(__file__), "model")

NOTEBOOKS = {
    "00_Unity_Catalog_Setup.ipynb": "/Shared/P2T2/00_Unity_Catalog_Setup",
    "01_config/key_vault_config.ipynb": "/Shared/P2T2/01_config/key_vault_config",
    "02_syndata/01_MCMC_SyntheticData_Generator.ipynb": "/Shared/P2T2/02_syndata/01_MCMC_SyntheticData_Generator",
    "02_syndata/02_NHIS_HIRA_Metadata_Parser.ipynb": "/Shared/P2T2/02_syndata/02_NHIS_HIRA_Metadata_Parser",
    "02_syndata/03_Kaggle_Reference_Loader.ipynb": "/Shared/P2T2/02_syndata/03_Kaggle_Reference_Loader",
    "03_pipeline/10_Bronze_Ingestion.ipynb": "/Shared/P2T2/03_pipeline/10_Bronze_Ingestion",
    "03_pipeline/20_Silver_Refinement.ipynb": "/Shared/P2T2/03_pipeline/20_Silver_Refinement",
    "03_pipeline/30_Gold_Aggregation.ipynb": "/Shared/P2T2/03_pipeline/30_Gold_Aggregation",
    "04_ai_inference/40_OpenAI_Clinical_Inference.ipynb": "/Shared/P2T2/04_ai_inference/40_OpenAI_Clinical_Inference",
    "04_ai_inference/41_GMAI_VL_ImageAnalysis.ipynb": "/Shared/P2T2/04_ai_inference/41_GMAI_VL_ImageAnalysis",
    "04_ai_inference/42_BioMedCLIP_Matching.ipynb": "/Shared/P2T2/04_ai_inference/42_BioMedCLIP_Matching",
    "05_judge/60_LLM_Judge_Pipeline.ipynb": "/Shared/P2T2/05_judge/60_LLM_Judge_Pipeline",
    "05_judge/61_Correction_Summary.ipynb": "/Shared/P2T2/05_judge/61_Correction_Summary",
    "06_mlops/50_MLflow_Experiment_Setup.ipynb": "/Shared/P2T2/06_mlops/50_MLflow_Experiment_Setup",
    "06_mlops/51_MLflow_Model_Registry.ipynb": "/Shared/P2T2/06_mlops/51_MLflow_Model_Registry",
}

# 필요한 폴더 생성
folders = set()
for remote_path in NOTEBOOKS.values():
    parent = "/".join(remote_path.split("/")[:-1])
    folders.add(parent)

for folder in sorted(folders):
    try:
        w.workspace.mkdirs(folder)
    except Exception:
        pass  # 이미 존재

# 업로드
success = 0
fail = 0
for local_rel, remote_path in NOTEBOOKS.items():
    full_local = os.path.join(MODEL_DIR, local_rel)
    
    if not os.path.exists(full_local):
        print(f"❌ 파일 없음: {local_rel}")
        fail += 1
        continue
    
    with open(full_local, "rb") as f:
        content = f.read()
    
    try:
        w.workspace.import_(
            path=remote_path,
            content=base64.b64encode(content).decode("utf-8"),
            format=ImportFormat.JUPYTER,
            overwrite=True,
            language=Language.PYTHON,
        )
        print(f"✅ {remote_path}")
        success += 1
    except Exception as e:
        print(f"⚠️ {remote_path} — {e}")
        fail += 1

print(f"\n{'=' * 60}")
print(f"📦 업로드 완료: {success}/{success+fail} 성공")
print(f"📂 {DATABRICKS_HOST}/#workspace/Shared/P2T2")
print("=" * 60)
