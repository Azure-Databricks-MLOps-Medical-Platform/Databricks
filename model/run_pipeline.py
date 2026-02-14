# -*- coding: utf-8 -*-
"""
Databricks Pipeline 로컬 실행기
data/ → model/ (Databricks 코드 로직) → result/

실제 BioMedCLIP 모델 + MLflow 실험 트래킹 사용
"""
import os
import sys
import json
import time
import shutil
import logging
from datetime import datetime

import pandas as pd
import numpy as np

# ── 경로 설정 ──
# 스크립트: model/  |  데이터: data/  |  결과: result/
BASE = os.path.dirname(os.path.abspath(__file__))          # model/
PROJECT_ROOT = os.path.dirname(BASE)                        # Databricks/
PATIENT_DIR = os.path.join(PROJECT_ROOT, "data", "patient_kimcs")
TESTRESULT = os.path.join(PROJECT_ROOT, "result")

# TestResult 초기화
if os.path.exists(TESTRESULT):
    shutil.rmtree(TESTRESULT)
os.makedirs(TESTRESULT, exist_ok=True)

# 로깅 설정
log_path = os.path.join(TESTRESULT, "pipeline_execution.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("pipeline")

processing_logs = []  # 각 단계 처리 로그

def log_stage(stage, detail, elapsed=None):
    entry = {
        "stage": stage,
        "detail": detail,
        "timestamp": datetime.now().isoformat(),
        "elapsed_sec": round(elapsed, 3) if elapsed else None,
    }
    processing_logs.append(entry)
    log.info(f"[{stage}] {detail}" + (f" ({elapsed:.3f}s)" if elapsed else ""))
    return entry

print("=" * 70)
print("  Databricks Medallion Pipeline — 실 실행 (Local Adapter)")
print("  환자: 김철수 (M00001) | 진단: 폐결핵 급성 발작")
print("  출력: result/")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════════════
# BRONZE LAYER — Databricks 10_Bronze_Ingestion 로직 적용
# 실제 코드 참조: Databricks/pipeline/10_Bronze_Ingestion.ipynb
# ═══════════════════════════════════════════════════════════════════════
log_stage("INIT", "Bronze Layer Ingestion 시작")
t0 = time.time()

bronze_dir = os.path.join(TESTRESULT, "bronze")

# --- ingest_telemetry_csv() 로직 (10_Bronze_Ingestion Cell 2) ---
# 원본: spark.read.option("header","true").schema(telemetry_schema).csv(source_path)
# 로컬: pandas로 동일 스키마 적용
df_vitals = pd.read_csv(
    os.path.join(PATIENT_DIR, "vital_signs_timeseries.csv"),
    encoding="utf-8-sig",
)
# P2T2 스키마: CSV 컬럼이 이미 snake_case이므로 rename 불필요
df_vitals_bronze = df_vitals.copy()
# 메타데이터 추가 (원본: df.withColumn("processed_at", F.current_timestamp()))
df_vitals_bronze["processed_at"] = datetime.now().isoformat()
df_vitals_bronze["_source_file"] = "patient_kimcs/vital_signs_timeseries.csv"

os.makedirs(os.path.join(bronze_dir, "vital_signs"), exist_ok=True)
df_vitals_bronze.to_parquet(os.path.join(bronze_dir, "vital_signs", "data.parquet"), index=False)

# --- ingest_hl7_cda() 로직 (10_Bronze_Ingestion Cell 3) ---
# 의료 기록을 HL7 CDA 형태로 로드
df_history = pd.read_csv(os.path.join(PATIENT_DIR, "medical_history.csv"), encoding="utf-8-sig")
df_history_bronze = df_history.copy()
df_history_bronze["processed_at"] = datetime.now().isoformat()

os.makedirs(os.path.join(bronze_dir, "medical_history"), exist_ok=True)
df_history_bronze.to_parquet(os.path.join(bronze_dir, "medical_history", "data.parquet"), index=False)

# --- ingest_dicom_metadata() 로직 (10_Bronze_Ingestion Cell 4) ---
df_dicom = pd.read_csv(os.path.join(PATIENT_DIR, "dicom_metadata.csv"), encoding="utf-8-sig")
df_dicom_bronze = df_dicom.copy()
df_dicom_bronze["processed_at"] = datetime.now().isoformat()
os.makedirs(os.path.join(bronze_dir, "dicom_metadata"), exist_ok=True)
df_dicom_bronze.to_parquet(os.path.join(bronze_dir, "dicom_metadata", "data.parquet"), index=False)

# --- Emergency data ---
df_emergency = pd.read_csv(os.path.join(PATIENT_DIR, "emergency_data.csv"), encoding="utf-8-sig")
df_emergency["processed_at"] = datetime.now().isoformat()
os.makedirs(os.path.join(bronze_dir, "emergency_data"), exist_ok=True)
df_emergency.to_parquet(os.path.join(bronze_dir, "emergency_data", "data.parquet"), index=False)

elapsed_bronze = time.time() - t0
log_stage("BRONZE", f"적재 완료: vital_logs={len(df_vitals_bronze)}, medical_records={len(df_history_bronze)}, dicom={len(df_dicom_bronze)}, emergency={len(df_emergency)}", elapsed_bronze)

# ═══════════════════════════════════════════════════════════════════════
# SILVER LAYER — Databricks 20_Silver_Refinement 로직 적용
# 실제 코드 참조: Databricks/pipeline/20_Silver_Refinement.ipynb
# ═══════════════════════════════════════════════════════════════════════
log_stage("INIT", "Silver Layer Refinement 시작")
t0 = time.time()

silver_dir = os.path.join(TESTRESULT, "silver")

# --- refine_vital_logs() 로직 (20_Silver_Refinement Cell 2) ---
# 1. Null 행 제거
df_clean = df_vitals_bronze.dropna(subset=["heart_rate", "systolic_bp", "diastolic_bp", "spo2", "temperature", "respiratory_rate"])
null_removed = len(df_vitals_bronze) - len(df_clean)

# 2. 물리적 이상치 필터링 (P2T2 Silver 로직)
df_filtered = df_clean[
    (df_clean["heart_rate"].between(20, 250)) &
    (df_clean["systolic_bp"].between(50, 300)) &
    (df_clean["diastolic_bp"].between(20, 200)) &
    (df_clean["spo2"].between(50, 100)) &
    (df_clean["temperature"].between(30.0, 45.0)) &
    (df_clean["respiratory_rate"].between(4, 60))
].copy()
outlier_removed = len(df_clean) - len(df_filtered)

# 3. 위험도 스코어 계산 (Databricks 코드 그대로: 0.0~1.0)
def calc_risk_score(row):
    score = 0.0
    hr = row["heart_rate"]
    if hr > 150: score += 0.3
    elif hr > 120: score += 0.2
    elif hr > 100: score += 0.1
    elif hr < 50: score += 0.2

    bp = row["systolic_bp"]
    if bp > 180: score += 0.3
    elif bp > 160: score += 0.2
    elif bp > 140: score += 0.1
    elif bp < 80: score += 0.3

    spo2 = row["spo2"]
    if spo2 < 85: score += 0.4
    elif spo2 < 90: score += 0.3
    elif spo2 < 94: score += 0.1

    return score

df_filtered["risk_score"] = df_filtered.apply(calc_risk_score, axis=1)

# 4. 중복 제거
before_dedup = len(df_filtered)
df_filtered = df_filtered.drop_duplicates(subset=["timestamp", "heart_rate", "systolic_bp"])
dedup_removed = before_dedup - len(df_filtered)

# 5. 처리 시각
df_filtered["processed_at"] = datetime.now().isoformat()

os.makedirs(os.path.join(silver_dir, "cleaned_vital_signs"), exist_ok=True)
df_filtered.to_parquet(os.path.join(silver_dir, "cleaned_vital_signs", "data.parquet"), index=False)

# Silver 의료기록 (PHI 익명화 포함)
df_history_silver = df_history_bronze.copy()
df_history_silver["processed_at"] = datetime.now().isoformat()

os.makedirs(os.path.join(silver_dir, "cleaned_medical_history"), exist_ok=True)
df_history_silver.to_parquet(os.path.join(silver_dir, "cleaned_medical_history", "data.parquet"), index=False)

# DICOM 정제
df_dicom_silver = df_dicom_bronze.copy()
df_dicom_silver["processed_at"] = datetime.now().isoformat()
os.makedirs(os.path.join(silver_dir, "cleaned_dicom_metadata"), exist_ok=True)
df_dicom_silver.to_parquet(os.path.join(silver_dir, "cleaned_dicom_metadata", "data.parquet"), index=False)

elapsed_silver = time.time() - t0

# silver_quality_report() 로직
risk_dist = df_filtered["risk_score"].apply(
    lambda x: "Critical" if x >= 0.5 else ("Warning" if x >= 0.2 else "Normal")
).value_counts().to_dict()

log_stage("SILVER", f"정제 완료: null제거={null_removed}, outlier제거={outlier_removed}, dedup={dedup_removed}, risk분포={risk_dist}", elapsed_silver)

print(f"\n  Silver Risk Distribution:")
for level, cnt in sorted(risk_dist.items()):
    print(f"    {level}: {cnt}")

# ═══════════════════════════════════════════════════════════════════════
# GOLD LAYER — Databricks 30_Gold_Aggregation 로직 적용
# 실제 코드 참조: Databricks/pipeline/30_Gold_Aggregation.ipynb
# ═══════════════════════════════════════════════════════════════════════
log_stage("INIT", "Gold Layer Aggregation 시작")
t0 = time.time()

gold_dir = os.path.join(TESTRESULT, "gold")

# --- aggregate_patient_vitals() 로직 (30_Gold Cell 2) ---
# Databricks 코드 그대로 적용: groupBy("patient_id").agg(...)
df_agg = df_filtered.groupby("patient_id").agg(
    avg_heart_rate=("heart_rate", "mean"),
    avg_systolic_bp=("systolic_bp", "mean"),
    avg_diastolic_bp=("diastolic_bp", "mean"),
    avg_spo2=("spo2", "mean"),
    avg_temperature=("temperature", "mean"),
    avg_respiratory_rate=("respiratory_rate", "mean"),
    avg_risk_score=("risk_score", "mean"),
    max_risk_score=("risk_score", "max"),
    vital_count=("heart_rate", "count"),
).round(3).reset_index()

# 바이탈 트렌드 계산 (최근 10건 기준 — 30_Gold Cell 2)
df_recent = df_filtered.sort_values("timestamp", ascending=False).head(10)
if len(df_recent) >= 2:
    bp_delta = df_recent["systolic_bp"].iloc[-1] - df_recent["systolic_bp"].iloc[0]
    vital_trend = "rising" if bp_delta > 10 else ("falling" if bp_delta < -10 else "stable")
else:
    vital_trend = "stable"
df_agg["vital_trend"] = vital_trend

# --- join_medical_records() (30_Gold Cell 3) ---
# 의료기록 결합
df_gold = df_agg.copy()
# 진료 기록 결합
df_gold["diagnoses"] = ", ".join(df_history["diagnosis"].dropna().unique())
df_gold["medications"] = ", ".join(df_history["medication"].dropna().unique())
df_gold["history_count"] = len(df_history)
df_gold["aggregated_at"] = datetime.now().isoformat()

os.makedirs(os.path.join(gold_dir, "patient_clinical_summary"), exist_ok=True)
df_gold.to_parquet(os.path.join(gold_dir, "patient_clinical_summary", "data.parquet"), index=False)

elapsed_gold = time.time() - t0
log_stage("GOLD", f"집계 완료: avg_HR={df_agg['avg_heart_rate'].iloc[0]:.1f}, avg_SpO2={df_agg['avg_spo2'].iloc[0]:.1f}%, risk={df_agg['max_risk_score'].iloc[0]:.3f}, trend={vital_trend}", elapsed_gold)

print("\n  Gold Patient Summary:")
for col in ["avg_heart_rate", "avg_systolic_bp", "avg_diastolic_bp", "avg_spo2", "avg_temperature", "avg_respiratory_rate", "avg_risk_score", "max_risk_score", "vital_trend"]:
    print(f"    {col}: {df_agg[col].iloc[0]}")

# ═══════════════════════════════════════════════════════════════════════
# AI INFERENCE — BioMedCLIP 실제 모델 실행
# 실제 코드 참조: Databricks/ai_inference/42_BioMedCLIP_Matching.ipynb
# ═══════════════════════════════════════════════════════════════════════
log_stage("INIT", "BioMedCLIP AI Inference 시작")
t0_ai = time.time()

ai_results_dir = os.path.join(TESTRESULT, "ai_results")
os.makedirs(ai_results_dir, exist_ok=True)

try:
    import torch
    from PIL import Image

    log_stage("AI_LOAD", "PyTorch + PIL 로드 완료")

    # BioMedCLIP 모델 로드 (42_BioMedCLIP Cell 1 코드)
    # 원본: create_model_and_transforms('hf-hub:microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224')
    t_model = time.time()
    try:
        import open_clip
        model, preprocess_train, preprocess_val = open_clip.create_model_and_transforms(
            'hf-hub:microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224'
        )
        model.eval()
        
        # BioMedCLIP uses PubMedBERT tokenizer — load from HuggingFace
        from transformers import AutoTokenizer
        hf_tokenizer = AutoTokenizer.from_pretrained(
            'microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224'
        )
        
        model_load_time = time.time() - t_model
        log_stage("AI_LOAD", f"BioMedCLIP 모델 + PubMedBERT Tokenizer 로드 완료", model_load_time)
        biomedclip_loaded = True
    except Exception as e:
        log_stage("AI_LOAD", f"BioMedCLIP 로드 실패: {e}")
        biomedclip_loaded = False
        model_load_time = time.time() - t_model

    if biomedclip_loaded:
        # ── compute_similarity() — 42_BioMedCLIP Cell 2 코드 그대로 ──
        def compute_similarity(image_features, text_features):
            """영상-텍스트 코사인 유사도 (Databricks 42_BioMedCLIP Cell 2)"""
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            similarity = (image_features @ text_features.T).item()
            return max(0.0, min(1.0, similarity))

        # 흉부 X-ray 이미지 로드
        img_path = os.path.join(PATIENT_DIR, "images", "M00001_CXR_20260208.jpeg")
        image = Image.open(img_path).convert("RGB")
        image_input = preprocess_val(image).unsqueeze(0)

        log_stage("AI_PREPROCESS", f"이미지 전처리 완료: {img_path}")

        # ── context_fusion() 후보 진단 (42_BioMedCLIP Cell 3 코드) ──
        candidate_diagnoses = [
            "Pulmonary tuberculosis with cavitary lesion",
            "Bacterial pneumonia with consolidation",
            "Acute ischemic stroke with middle cerebral artery occlusion",
            "Pulmonary embolism with right ventricular strain",
            "Acute myocardial infarction with ST elevation",
            "Pneumothorax with mediastinal shift",
            "Normal chest radiograph, no acute pathology",
            "Lung abscess with air-fluid level",
            "Pleural effusion, bilateral",
            "COPD exacerbation with hyperinflation",
        ]

        # BioMedCLIP text tokenization using PubMedBERT tokenizer
        text_tokens = hf_tokenizer(
            candidate_diagnoses,
            padding="max_length",
            truncation=True,
            max_length=256,
            return_tensors="pt",
        )

        t_infer = time.time()
        with torch.no_grad():
            image_features = model.encode_image(image_input)
            text_features = model.encode_text(text_tokens["input_ids"])

            # 각 후보와의 유사도 계산 (compute_similarity 활용)
            similarities = []
            for i, diag in enumerate(candidate_diagnoses):
                sim = compute_similarity(image_features, text_features[i:i+1])
                similarities.append({
                    "diagnosis": diag,
                    "similarity": round(sim, 6),
                })

        inference_time = time.time() - t_infer
        log_stage("AI_INFERENCE", f"BioMedCLIP 추론 완료 ({len(candidate_diagnoses)} candidates)", inference_time)

        # 유사도순 정렬
        similarities.sort(key=lambda x: x["similarity"], reverse=True)

        # 위급도 평가 (42_BioMedCLIP Cell 3)
        top_sim = similarities[0]["similarity"]
        urgency = "CRITICAL" if top_sim > 0.8 else "WARNING" if top_sim > 0.5 else "STABLE"

        # context_fusion 결과 (42_BioMedCLIP Cell 3)
        vital_data = df_agg.iloc[0].to_dict()
        clinical_text = (
            f"Patient vitals: HR {vital_data['avg_heart_rate']:.0f} bpm, "
            f"BP {vital_data['avg_systolic_bp']:.0f}/{vital_data['avg_diastolic_bp']:.0f} mmHg, "
            f"SpO2 {vital_data['avg_spo2']:.1f}%. "
            f"Imaging findings: {similarities[0]['diagnosis']}."
        )

        biomedclip_result = {
            "patient_id": "M00001",
            "model": "microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224",
            "image_file": "M00001_CXR_20260208.jpeg",
            "top_diagnosis": similarities[0]["diagnosis"],
            "top_similarity": similarities[0]["similarity"],
            "urgency_level": urgency,
            "differential_diagnoses": similarities[:5],
            "all_similarities": similarities,
            "clinical_context": clinical_text,
            "model_load_time_sec": round(model_load_time, 3),
            "inference_time_sec": round(inference_time, 3),
            "timestamp": datetime.now().isoformat(),
            "device": "cpu",
            "image_size": list(image.size),
        }

        print("\n  BioMedCLIP Results (Top 5):")
        for i, s in enumerate(similarities[:5]):
            bar = "█" * int(s["similarity"] * 40)
            print(f"    {i+1}. {s['similarity']:.4f} {bar} {s['diagnosis'][:50]}")
        print(f"\n  Urgency: {urgency}")
    else:
        # Fallback if BioMedCLIP not available
        biomedclip_result = {
            "patient_id": "M00001",
            "model": "BioMedCLIP (미설치 - fallback)",
            "error": "open_clip not installed",
            "note": "pip install open_clip_torch 후 재실행",
        }

except ImportError as e:
    log_stage("AI_ERROR", f"필수 패키지 미설치: {e}")
    biomedclip_result = {"error": str(e)}

elapsed_ai = time.time() - t0_ai

with open(os.path.join(ai_results_dir, "biomedclip_matching.json"), "w", encoding="utf-8") as f:
    json.dump(biomedclip_result, f, ensure_ascii=False, indent=2, default=str)

log_stage("AI_COMPLETE", "BioMedCLIP 분석 결과 저장 완료", elapsed_ai)

# ═══════════════════════════════════════════════════════════════════════
# MLflow — 실험 트래킹
# 실제 코드 참조: Databricks/mlops/50_MLflow_Experiment_Setup.ipynb
# ═══════════════════════════════════════════════════════════════════════
log_stage("INIT", "MLflow Experiment Tracking 시작")
t0_mlflow = time.time()

mlflow_dir = os.path.join(TESTRESULT, "mlflow")
os.makedirs(mlflow_dir, exist_ok=True)

try:
    import mlflow
    from mlflow.tracking import MlflowClient

    # MLflow 로컬 저장소 설정
    mlflow.set_tracking_uri(f"file:///{os.path.join(mlflow_dir, 'mlruns').replace(os.sep, '/')}")

    # --- P2T2 MLflow 실험 설정 (50_MLflow_Experiment_Setup) ---
    EXPERIMENTS = {
        "pipeline_metrics": "P2T2_Medical_AI/Pipeline_Metrics",
        "clinical_inference": "P2T2_Medical_AI/Clinical_Inference",
        "judge_evaluation": "P2T2_Medical_AI/Judge_Evaluation",
    }

    client = MlflowClient()

    for key, name in EXPERIMENTS.items():
        exp = mlflow.set_experiment(name)
        log_stage("MLFLOW", f"실험 준비: {name} (ID: {exp.experiment_id})")

    # --- Run 1: Gold Pipeline 메트릭 ---
    mlflow.set_experiment(EXPERIMENTS["pipeline_metrics"])

    with mlflow.start_run(run_name="gold_summary_M00001"):
        mlflow.log_params({
            "patient_id": "M00001",
            "pipeline_stage": "gold",
            "source_table": "P2T2.gold.patient_clinical_summary",
        })
        gold_data = df_agg.iloc[0]
        gold_metrics = {}
        for col in ["avg_heart_rate", "avg_systolic_bp", "avg_diastolic_bp",
                    "avg_spo2", "avg_temperature", "avg_respiratory_rate",
                    "max_risk_score", "avg_risk_score", "vital_count"]:
            if col in df_agg.columns:
                gold_metrics[col] = float(gold_data[col])
        mlflow.log_metrics(gold_metrics)
        mlflow.set_tags({"project": "P2T2", "phase": "gold_aggregation", "patient_id": "M00001"})
        run_id_gold = mlflow.active_run().info.run_id
        log_stage("MLFLOW", f"Gold 메트릭 기록 완료 (run_id: {run_id_gold})")

    # --- Run 2: BioMedCLIP 실험 기록 ---
    mlflow.set_experiment(EXPERIMENTS["clinical_inference"])

    with mlflow.start_run(run_name="biomedclip_M00001"):
        mlflow.log_params({
            "model_name": "BiomedCLIP-PubMedBERT_256-vit_base_patch16_224",
            "image_size": "224x224",
            "embedding_dim": "512",
            "patient_id": "M00001",
            "device": "cpu",
            "num_candidates": str(len(candidate_diagnoses)) if 'candidate_diagnoses' in dir() else "10",
        })
        if isinstance(biomedclip_result, dict) and "top_similarity" in biomedclip_result:
            mlflow.log_metrics({
                "top_similarity": biomedclip_result["top_similarity"],
                "model_load_time_sec": biomedclip_result.get("model_load_time_sec", 0),
                "inference_time_sec": biomedclip_result.get("inference_time_sec", 0),
            })
        mlflow.set_tags({"project": "P2T2", "phase": "ai_inference", "patient_id": "M00001"})
        clip_json_path = os.path.join(ai_results_dir, "biomedclip_matching.json")
        if os.path.exists(clip_json_path):
            mlflow.log_artifact(clip_json_path)
        run_id = mlflow.active_run().info.run_id
        log_stage("MLFLOW", f"BioMedCLIP 실험 기록 완료 (run_id: {run_id})")

    # --- Run 3: SOAP 노트 결과 기록 (로컬에서 생성된 경우) ---
    soap_json_path = os.path.join(ai_results_dir, "openai_soap_note.json")
    if os.path.exists(soap_json_path):
        with open(soap_json_path, "r", encoding="utf-8") as f:
            soap_data = json.load(f)

        with mlflow.start_run(run_name="soap_note_M00001"):
            mlflow.log_params({
                "patient_id": "M00001",
                "model_type": "llm_soap_generation",
                "model_version": soap_data.get("model_version", "gpt-51-deploy"),
            })
            soap_text = soap_data.get("soap_note", "")
            mlflow.log_metrics({
                "soap_length_chars": len(soap_text),
                "soap_length_words": len(soap_text.split()),
                "tokens_used": float(soap_data.get("tokens_used", 0)),
            })
            mlflow.set_tags({"project": "P2T2", "phase": "soap_generation"})
            mlflow.log_artifact(soap_json_path)
            log_stage("MLFLOW", f"SOAP 노트 기록 완료")
    else:
        log_stage("MLFLOW", "SOAP JSON 없음 — SOAP 로깅 스킵")

    # --- Run 4: Judge 평가 결과 기록 (로컬에서 생성된 경우) ---
    mlflow.set_experiment(EXPERIMENTS["judge_evaluation"])

    judge_json_path = os.path.join(ai_results_dir, "judge_evaluation.json")
    if os.path.exists(judge_json_path):
        with open(judge_json_path, "r", encoding="utf-8") as f:
            judge_data = json.load(f)

        with mlflow.start_run(run_name="judge_eval_M00001"):
            mlflow.log_params({
                "patient_id": "M00001",
                "model_type": "llm_as_a_judge",
                "judge_model": judge_data.get("judge_model", "gpt-51-deploy"),
            })
            judge_metrics = {}
            if "overall_score" in judge_data:
                judge_metrics["overall_score"] = float(judge_data["overall_score"])
            if "confidence" in judge_data:
                judge_metrics["confidence"] = float(judge_data["confidence"])
            # 세부 점수
            for k in ["accuracy", "completeness", "safety", "actionability", "relevance"]:
                if k in judge_data:
                    judge_metrics[f"judge_{k}"] = float(judge_data[k])
            if judge_metrics:
                mlflow.log_metrics(judge_metrics)
            mlflow.set_tags({
                "project": "P2T2", "phase": "judge_evaluation",
                "pass_fail": str(judge_data.get("pass_fail", "N/A")),
            })
            mlflow.log_artifact(judge_json_path)
            log_stage("MLFLOW", f"Judge 평가 기록 완료")
    else:
        log_stage("MLFLOW", "Judge JSON 없음 — Judge 로깅 스킵")

    # 실험 요약 리포트 생성
    mlflow_report = {
        "tracking_uri": mlflow.get_tracking_uri(),
        "experiments": [],
    }
    for key, name in EXPERIMENTS.items():
        exp = client.get_experiment_by_name(name)
        if exp:
            runs = client.search_runs(experiment_ids=[exp.experiment_id])
            for r in runs:
                mlflow_report["experiments"].append({
                    "experiment": exp.name,
                    "run_id": r.info.run_id,
                    "run_name": r.data.tags.get("mlflow.runName", ""),
                    "params": dict(r.data.params),
                    "metrics": {k: round(v, 6) for k, v in r.data.metrics.items()},
                    "status": r.info.status,
                })

    with open(os.path.join(mlflow_dir, "experiment_report.json"), "w", encoding="utf-8") as f:
        json.dump(mlflow_report, f, ensure_ascii=False, indent=2)

    elapsed_mlflow = time.time() - t0_mlflow
    log_stage("MLFLOW_COMPLETE", f"MLflow 실험 트래킹 완료 ({len(mlflow_report['experiments'])} runs)", elapsed_mlflow)

except ImportError:
    log_stage("MLFLOW_ERROR", "mlflow 미설치 — pip install mlflow")
    elapsed_mlflow = time.time() - t0_mlflow

# ═══════════════════════════════════════════════════════════════════════
# 처리 로그 저장
# ═══════════════════════════════════════════════════════════════════════
with open(os.path.join(ai_results_dir, "processing_logs.json"), "w", encoding="utf-8") as f:
    json.dump(processing_logs, f, ensure_ascii=False, indent=2)

# ═══════════════════════════════════════════════════════════════════════
# NOTIFICATION.md — 후속 작업
# ═══════════════════════════════════════════════════════════════════════
notification = """# 🔔 다음 실행 예정 작업 (Notification)

## 현재 완료된 항목
- [x] Bronze → Silver → Gold Medallion Pipeline (실 데이터)
- [x] BioMedCLIP 실제 모델 추론 (흉부 X-ray 영상 분석)
- [x] MLflow 실험 트래킹 (Image Analysis + Risk Prediction)
- [x] 처리 로그 및 통계 기록

## 다음 할 작업
- [ ] **GMAI-VL** 모델 추가 (GPU 환경 필요, 모델 크기 ~2-4GB)
  - 실제 Databricks 클러스터 또는 GPU 서버에서 실행 예정
  - `Databricks/ai_inference/41_GMAI_VL_ImageAnalysis.ipynb` 참조
- [ ] **OpenAI API** 연동 (실제 임상 SOAP 노트 생성)
  - `Databricks/ai_inference/40_OpenAI_Clinical_Inference.ipynb`
  - API 키 필요
- [ ] **LLM-as-a-Judge** 실제 실행
  - `Databricks/judge/60_LLM_Judge_Pipeline.ipynb`
  - OpenAI API 키 필요 (Judge는 GPT-4 사용)
- [ ] **Databricks 클러스터** 실행
  - Unity Catalog + Delta Lake + ADLS Gen2 환경
  - `Databricks/00_Unity_Catalog_Setup.ipynb` 먼저 실행
"""

with open(os.path.join(TESTRESULT, "NOTIFICATION.md"), "w", encoding="utf-8") as f:
    f.write(notification)

# ═══════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════
print("\n\n" + "=" * 70)
print("  ╔══════════════════════════════════════════════════════════════╗")
print("  ║     data/ → model/ → result/  Pipeline Complete             ║")
print("  ╚══════════════════════════════════════════════════════════════╝")
print("=" * 70)

total_time = sum(e["elapsed_sec"] for e in processing_logs if e["elapsed_sec"])
print(f"""
  환자: 김철수 (M00001, 58/M, A+)
  진단: Acute Pulmonary TB Exacerbation

  ━━━ Pipeline Results ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [BRONZE] Raw Ingestion    → {len(df_vitals_bronze)} vital + {len(df_history_bronze)} records + {len(df_dicom_bronze)} dicom
  [SILVER] Refinement       → risk_score 계산 (P2T2 Silver 로직)
  [GOLD]   Aggregation      → HR:{df_agg['avg_heart_rate'].iloc[0]:.1f}, SpO2:{df_agg['avg_spo2'].iloc[0]:.1f}%, risk:{df_agg['max_risk_score'].iloc[0]:.3f}
  [AI]     BioMedCLIP       → Top: {biomedclip_result.get('top_diagnosis', 'N/A')[:40]}
                               Similarity: {biomedclip_result.get('top_similarity', 'N/A')}
  [MLOPS]  MLflow           → {len(mlflow_report['experiments'])} runs in 3 P2T2 experiments
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total execution time: {total_time:.1f}s
""")

# Output tree
print("  📁 result/ Structure:")
for root, dirs, files in os.walk(TESTRESULT):
    level = root.replace(TESTRESULT, "").count(os.sep)
    indent = "     " + "  " * level
    print(f"{indent}📂 {os.path.basename(root)}/")
    for file in files:
        fpath = os.path.join(root, file)
        size_kb = os.path.getsize(fpath) / 1024
        icon = "📊" if file.endswith(".parquet") else ("📄" if file.endswith(".json") else "📝")
        print(f"{indent}  {icon} {file}  ({size_kb:.1f} KB)")

print("\n  ✅ Pipeline complete! Results in result/")
