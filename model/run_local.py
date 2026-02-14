# -*- coding: utf-8 -*-
"""
김철수 (Kim Cheolsu) — Databricks Medallion Pipeline 로컬 시뮬레이션
Bronze → Silver → Gold → AI Inference → LLM-as-a-Judge

Pandas 기반 실행 (PySpark 로컬 대체 - Hadoop winutils 미설치 환경)
Databricks에서는 동일 로직이 PySpark/Delta Lake로 실행됩니다.
"""
import os
import sys
import json
import shutil
import pandas as pd
import numpy as np
from datetime import datetime

# ── 경로 설정 ──
BASE = os.path.dirname(os.path.abspath(__file__))          # model/
PROJECT_ROOT = os.path.dirname(BASE)                        # Databricks/
PATIENT_DIR = os.path.join(PROJECT_ROOT, "data", "patient_kimcs")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "result")

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

AGE = 58
GENDER = "Male"
BLOOD_TYPE = "A+"

print("=" * 70)
print("  Databricks Medallion Pipeline — 로컬 시뮬레이션 (Pandas)")
print("  환자: 김철수 (M00001) | 진단: 폐결핵 급성 발작")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════════════
# BRONZE LAYER — Raw Data Ingestion
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("  [BRONZE] Raw Data Ingestion → Parquet 적재")
print("━" * 70)

# 1. Emergency Data
df_emergency = pd.read_csv(os.path.join(PATIENT_DIR, "emergency_data.csv"), encoding="utf-8-sig")
bronze_emergency_path = os.path.join(OUTPUT_DIR, "bronze", "emergency")
os.makedirs(bronze_emergency_path, exist_ok=True)
df_emergency.to_parquet(os.path.join(bronze_emergency_path, "data.parquet"), index=False)
print(f"  ✓ Bronze Emergency: {len(df_emergency)} rows")

# 2. Medical History
df_history = pd.read_csv(os.path.join(PATIENT_DIR, "medical_history.csv"), encoding="utf-8-sig")
bronze_history_path = os.path.join(OUTPUT_DIR, "bronze", "medical_history")
os.makedirs(bronze_history_path, exist_ok=True)
df_history.to_parquet(os.path.join(bronze_history_path, "data.parquet"), index=False)
print(f"  ✓ Bronze Medical History: {len(df_history)} rows")

# 3. Vital Signs
df_vitals = pd.read_csv(os.path.join(PATIENT_DIR, "vital_signs_timeseries.csv"), encoding="utf-8-sig")
bronze_vitals_path = os.path.join(OUTPUT_DIR, "bronze", "vital_logs")
os.makedirs(bronze_vitals_path, exist_ok=True)
df_vitals.to_parquet(os.path.join(bronze_vitals_path, "data.parquet"), index=False)
print(f"  ✓ Bronze Vital Logs: {len(df_vitals)} rows (5분 간격 × 4시간)")

# 4. DICOM Metadata
df_dicom = pd.read_csv(os.path.join(PATIENT_DIR, "dicom_metadata.csv"), encoding="utf-8-sig")
bronze_dicom_path = os.path.join(OUTPUT_DIR, "bronze", "dicom_metadata")
os.makedirs(bronze_dicom_path, exist_ok=True)
df_dicom.to_parquet(os.path.join(bronze_dicom_path, "data.parquet"), index=False)
print(f"  ✓ Bronze DICOM Metadata: {len(df_dicom)} rows")

print("\n  ── Bronze Emergency Record ──")
print(df_emergency[["patient_name", "chief_complaint", "triage_level", "suspected_diagnosis"]].to_string(index=False))

# ═══════════════════════════════════════════════════════════════════════
# SILVER LAYER — Data Refinement
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("  [SILVER] Data Refinement & Quality Checks")
print("━" * 70)

# 1. Outlier Filtering
before_count = len(df_vitals)
df_vitals_filtered = df_vitals[
    (df_vitals["heart_rate"].between(30, 220)) &
    (df_vitals["systolic_bp"].between(60, 250)) &
    (df_vitals["diastolic_bp"].between(30, 150)) &
    (df_vitals["spo2"].between(50, 100)) &
    (df_vitals["temperature"].between(34.0, 42.0)) &
    (df_vitals["respiratory_rate"].between(5, 60))
].copy()
removed = before_count - len(df_vitals_filtered)
print(f"  Outlier filtering: {removed} rows removed ({len(df_vitals_filtered)} retained)")

# 2. Risk Level & Alert Flags
df_vitals_filtered["risk_level"] = pd.cut(
    df_vitals_filtered["risk_score"],
    bins=[-1, 20, 40, 60, 200],
    labels=["LOW", "MODERATE", "HIGH", "CRITICAL"]
)
df_vitals_filtered["alert_flag"] = (
    (df_vitals_filtered["spo2"] < 90) |
    (df_vitals_filtered["heart_rate"] > 120) |
    (df_vitals_filtered["temperature"] > 38.5) |
    (df_vitals_filtered["respiratory_rate"] > 25)
)

silver_vitals_path = os.path.join(OUTPUT_DIR, "silver", "refined_vitals")
os.makedirs(silver_vitals_path, exist_ok=True)
df_vitals_filtered.to_parquet(os.path.join(silver_vitals_path, "data.parquet"), index=False)

alert_count = int(df_vitals_filtered["alert_flag"].sum())
print(f"  ⚠ Alert flags: {alert_count}/{len(df_vitals_filtered)} measurements")

print("\n  Risk Level Distribution:")
risk_dist = df_vitals_filtered["risk_level"].value_counts().sort_index()
for level, cnt in risk_dist.items():
    bar = "█" * int(cnt / len(df_vitals_filtered) * 40)
    print(f"    {level:10s}: {cnt:3d} ({cnt/len(df_vitals_filtered)*100:5.1f}%) {bar}")

# 3. Emergency Enrichment
df_emergency_silver = df_emergency.copy()
df_emergency_silver["response_time_min"] = 13
df_emergency_silver["scene_time_min"] = 10
df_emergency_silver["transport_time_min"] = 10
df_emergency_silver["total_prehospital_min"] = 33

silver_emergency_path = os.path.join(OUTPUT_DIR, "silver", "refined_emergency")
os.makedirs(silver_emergency_path, exist_ok=True)
df_emergency_silver.to_parquet(os.path.join(silver_emergency_path, "data.parquet"), index=False)
print(f"\n  ✓ Silver Emergency: 대응 {13}분 + 현장 {10}분 + 이송 {10}분 = 총 {33}분")

# 4. Medical History Enrichment
df_history_silver = df_history.copy()
df_history_silver["icd_code"] = df_history_silver["diagnosis"].str.extract(r"\(([A-Z]\d+\.?\d*)\)")
df_history_silver["diagnosis_clean"] = df_history_silver["diagnosis"].str.replace(r"\s*\([A-Z]\d+\.?\d*\)", "", regex=True)

silver_history_path = os.path.join(OUTPUT_DIR, "silver", "refined_history")
os.makedirs(silver_history_path, exist_ok=True)
df_history_silver.to_parquet(os.path.join(silver_history_path, "data.parquet"), index=False)

print(f"\n  ── 의료 히스토리 (Silver) ──")
print(df_history_silver[["record_date", "diagnosis_clean", "icd_code", "medication"]].to_string(index=False))

# ═══════════════════════════════════════════════════════════════════════
# GOLD LAYER — Aggregation & Clinical Summary
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("  [GOLD] Patient Clinical Summary Aggregation")
print("━" * 70)

vital_agg = {
    "total_measurements": len(df_vitals_filtered),
    "avg_heart_rate": round(df_vitals_filtered["heart_rate"].mean(), 1),
    "min_heart_rate": round(df_vitals_filtered["heart_rate"].min(), 1),
    "max_heart_rate": round(df_vitals_filtered["heart_rate"].max(), 1),
    "avg_systolic_bp": round(df_vitals_filtered["systolic_bp"].mean(), 1),
    "avg_diastolic_bp": round(df_vitals_filtered["diastolic_bp"].mean(), 1),
    "avg_spo2": round(df_vitals_filtered["spo2"].mean(), 1),
    "min_spo2": round(df_vitals_filtered["spo2"].min(), 1),
    "max_spo2": round(df_vitals_filtered["spo2"].max(), 1),
    "avg_temperature": round(df_vitals_filtered["temperature"].mean(), 2),
    "max_temperature": round(df_vitals_filtered["temperature"].max(), 2),
    "avg_respiratory_rate": round(df_vitals_filtered["respiratory_rate"].mean(), 1),
    "avg_risk_score": round(df_vitals_filtered["risk_score"].mean(), 1),
    "max_risk_score": round(df_vitals_filtered["risk_score"].max(), 1),
    "alert_count": alert_count,
    "critical_count": int((df_vitals_filtered["risk_level"] == "CRITICAL").sum()),
}

gold_vitals_path = os.path.join(OUTPUT_DIR, "gold", "patient_vital_summary")
os.makedirs(gold_vitals_path, exist_ok=True)
pd.DataFrame([vital_agg]).to_parquet(os.path.join(gold_vitals_path, "data.parquet"), index=False)

print("\n  ── 환자 바이탈 집계 (Gold) ──")
print(f"  ┌────────────────────┬───────────────────────────────┐")
print(f"  │ Heart Rate         │ avg {vital_agg['avg_heart_rate']:>6.1f}  "
      f"min {vital_agg['min_heart_rate']:>6.1f}  "
      f"max {vital_agg['max_heart_rate']:>6.1f} │")
print(f"  │ Blood Pressure     │ avg {vital_agg['avg_systolic_bp']:>5.1f} / {vital_agg['avg_diastolic_bp']:>5.1f} mmHg"
      f"             │")
print(f"  │ SpO2               │ avg {vital_agg['avg_spo2']:>6.1f}%  "
      f"min {vital_agg['min_spo2']:>6.1f}%  "
      f"max {vital_agg['max_spo2']:>5.1f}% │")
print(f"  │ Temperature        │ avg {vital_agg['avg_temperature']:>6.2f}°C  "
      f"max {vital_agg['max_temperature']:>6.2f}°C           │")
print(f"  │ Respiratory Rate   │ avg {vital_agg['avg_respiratory_rate']:>6.1f}                        │")
print(f"  │ Risk Score         │ avg {vital_agg['avg_risk_score']:>6.1f}  "
      f"max {vital_agg['max_risk_score']:>6.1f}               │")
print(f"  │ Alerts / Critical  │ {vital_agg['alert_count']:>3d} alerts, "
      f"{vital_agg['critical_count']:>3d} critical episodes    │")
print(f"  └────────────────────┴───────────────────────────────┘")

# Vital trend (first 5, last 5)
print("\n  ── 바이탈 시계열 트렌드 (입원 초기 vs 안정기) ──")
print("  [ 입원 초기 (first 5) ]")
print(df_vitals_filtered[["timestamp", "heart_rate", "systolic_bp", "spo2", "temperature", "risk_score"]].head(5).to_string(index=False))
print("  [ 안정기 (last 5) ]")
print(df_vitals_filtered[["timestamp", "heart_rate", "systolic_bp", "spo2", "temperature", "risk_score"]].tail(5).to_string(index=False))

# Gold clinical profile
gold_profile = {
    "patient_id": "M00001",
    "patient_name": "Kim Cheolsu",
    "age": AGE,
    "gender": GENDER,
    "blood_type": BLOOD_TYPE,
    "admission_type": "Emergency",
    "hospital": str(df_emergency.iloc[0].get("hospital", "N/A")),
    "chief_complaint": str(df_emergency.iloc[0].get("chief_complaint", "N/A")),
    "triage_level": str(df_emergency.iloc[0].get("triage_level", "N/A")),
    "primary_diagnosis": str(df_emergency.iloc[0].get("suspected_diagnosis", "N/A")),
    "prehospital_time_min": 33,
    "medical_history_count": len(df_history_silver),
    "comorbidities": ", ".join(df_history_silver["diagnosis_clean"].tolist()),
    **vital_agg,
}

gold_profile_path = os.path.join(OUTPUT_DIR, "gold", "clinical_profile")
os.makedirs(gold_profile_path, exist_ok=True)
pd.DataFrame([gold_profile]).to_parquet(os.path.join(gold_profile_path, "data.parquet"), index=False)
print("\n  ✓ Gold Clinical Profile saved")

# ═══════════════════════════════════════════════════════════════════════
# AI INFERENCE — Clinical Decision Support (시뮬레이션)
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("  [AI INFERENCE] OpenAI GPT-4 Clinical Report (시뮬레이션)")
print("━" * 70)

ai_report = {
    "patient_id": "M00001",
    "patient_name": "Kim Cheolsu",
    "model": "OpenAI GPT-4 (시뮬레이션)",
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "clinical_assessment": {
        "primary_diagnosis": "Acute Pulmonary Tuberculosis Exacerbation with Respiratory Distress",
        "severity": "CRITICAL → Stabilizing",
        "confidence": 0.92,
        "differential_diagnosis": [
            "Pulmonary TB reactivation (most likely)",
            "Bacterial pneumonia superinfection",
            "Lung abscess",
            "Pulmonary hemorrhage",
        ],
        "key_findings": [
            f"입원 시 SpO2 88% → 치료 후 {vital_agg['max_spo2']:.0f}%로 개선",
            f"초기 빈맥 (HR 118) → {vital_agg['min_heart_rate']:.0f}으로 안정화",
            f"발열 38.7°C, 객혈 동반",
            "흉부 X-ray: 우상엽 공동성 병변 (Cavity) 의심",
            f"총 {alert_count}회 임상 경보 발생 (4시간 모니터링)",
            f"과거 폐결핵 치료력 (2021), COPD 동반",
        ],
    },
    "soap_note": {
        "S": ("환자 김철수(58/M)는 급성 호흡곤란, 객혈, 발열(38.7°C)을 주소로 "
              "119를 통해 서울대학교병원 응급의료센터에 내원하였습니다. "
              "과거력: 2021년 폐결핵(A15.0) 6개월 치료 완료, "
              "2018년부터 고혈압(I10), 2019년부터 2형 당뇨(E11) 관리 중, "
              "2024년 중등도 COPD(J44.1) 진단. "
              "최근 1주간 기침 악화 및 간헐적 객혈 호소."),
        "O": (f"V/S 집계(4h): HR avg {vital_agg['avg_heart_rate']:.0f} "
              f"(range {vital_agg['min_heart_rate']:.0f}-{vital_agg['max_heart_rate']:.0f}), "
              f"BP avg {vital_agg['avg_systolic_bp']:.0f}/{vital_agg['avg_diastolic_bp']:.0f}, "
              f"SpO2 avg {vital_agg['avg_spo2']:.1f}% (min {vital_agg['min_spo2']:.1f}%), "
              f"BT max {vital_agg['max_temperature']:.1f}°C, "
              f"RR avg {vital_agg['avg_respiratory_rate']:.0f}.\n"
              f"Risk Score: avg {vital_agg['avg_risk_score']:.1f}, max {vital_agg['max_risk_score']:.1f}.\n"
              f"Clinical alerts: {alert_count}/{vital_agg['total_measurements']} measurements triggered.\n"
              "CXR (portable PA): Right upper lobe cavitary lesion with perifocal consolidation. "
              "No pleural effusion. Trachea midline."),
        "A": ("1. Pulmonary TB reactivation with cavitary disease (A15.0)\n"
              "2. Acute respiratory failure, improving (initial SpO2 88%)\n"
              "3. Hemoptysis — likely from cavitary erosion into bronchial vessels\n"
              "4. Background: Essential HTN (I10), T2DM (E11), COPD (J44.1)\n"
              "5. Risk: Drug-resistant TB pending culture sensitivity"),
        "P": ("1. Airborne isolation (N95 + negative pressure room)\n"
              "2. Sputum AFB smear/culture × 3, GeneXpert MTB/RIF\n"
              "3. Restart anti-TB: HERZ regimen "
              "(INH 300mg + RIF 600mg + EMB 800mg + PZA 1500mg daily)\n"
              "4. O2 therapy via non-rebreather → maintain SpO2 >94%\n"
              "5. CT chest with contrast — cavitary extent, r/o abscess\n"
              "6. Labs: CBC, CRP, ESR, LFT (baseline for anti-TB), RFT, HbA1c\n"
              "7. ABG if SpO2 deteriorates\n"
              "8. Pulmonology and Infectious Disease consult\n"
              "9. Hemoptysis monitoring — IR consult if >200mL/24h\n"
              "10. Continue amlodipine 5mg, losartan 50mg, metformin 500mg bid\n"
              "11. Tiotropium inhaler continue"),
    },
    "treatment_recommendation": {
        "immediate": [
            "AFB isolation (airborne precautions, negative pressure room)",
            "O2 via non-rebreather mask 10-15L/min, target SpO2 >94%",
            "IV access × 2, NS 500mL bolus",
            "Anti-TB: HERZ restart (INH + RIF + EMB + PZA)",
            "Antipyretics: Acetaminophen 1g IV PRN if BT >38.5°C",
        ],
        "diagnostic": [
            "Sputum AFB smear/culture × 3 (spot, early morning, spot)",
            "GeneXpert MTB/RIF (rapid molecular test)",
            "CT Chest with contrast",
            "CBC, CRP, ESR, Procalcitonin",
            "LFT, RFT, HbA1c, Electrolytes",
            "Blood gas analysis (ABG)",
            "Blood cultures × 2 sets",
        ],
        "monitoring": [
            "Continuous SpO2 + telemetry",
            "Vital signs q15min × 2hr, then q30min",
            "Strict I/O charting",
            "Hemoptysis volume tracking (emesis basin)",
            "Repeat CXR in 48-72 hours",
        ],
    },
}

# Save AI results
ai_results_path = os.path.join(OUTPUT_DIR, "ai_results")
os.makedirs(ai_results_path, exist_ok=True)
with open(os.path.join(ai_results_path, "clinical_report.json"), "w", encoding="utf-8") as f:
    json.dump(ai_report, f, ensure_ascii=False, indent=2)

print("  ✓ AI Clinical Report generated")
print("\n  ── SOAP Note ──")
for section, content in ai_report["soap_note"].items():
    print(f"\n  [{section}]")
    for line in content.split("\n"):
        print(f"    {line}")

# ═══════════════════════════════════════════════════════════════════════
# LLM-as-a-Judge — Quality Evaluation (시뮬레이션)
# ═══════════════════════════════════════════════════════════════════════
print("\n\n" + "━" * 70)
print("  [LLM-as-a-Judge] AI Report Quality Evaluation (시뮬레이션)")
print("━" * 70)

judge_eval = {
    "patient_id": "M00001",
    "model_evaluated": "GPT-4 (sim)",
    "judge_model": "GPT-4 Judge (sim)",
    "evaluation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "criteria": {
        "accuracy": {
            "score": 9, "max": 10,
            "comment": "TB reactivation 정확 진단, 감별진단 4종 적절, confidence 0.92"
        },
        "completeness": {
            "score": 8, "max": 10,
            "comment": "SOAP 전 항목 충실, 약물 용량 명시. LFT baseline monitoring 포함."
        },
        "actionability": {
            "score": 9, "max": 10,
            "comment": "즉각 치료 11개 항목, 우선순위 명확 (isolation → O2 → anti-TB)"
        },
        "safety": {
            "score": 10, "max": 10,
            "comment": "Airborne isolation 최우선, LFT baseline (anti-TB 간독성), 약물 상호작용 고려"
        },
        "relevance": {
            "score": 9, "max": 10,
            "comment": "환자 과거력(TB 치료력, COPD, HTN/DM) 적절 반영, 동반질환 관리 유지"
        },
    },
    "total_score": 45,
    "max_score": 50,
    "percentage": 90.0,
    "verdict": "PASS",
    "group": "A (Original AI Report)",
    "feedback": (
        "전반적으로 높은 품질의 임상 보고서입니다. "
        "개선 권장 사항: (1) Drug-resistant TB 가능성에 대한 경험적 치료 고려, "
        "(2) 접촉자 추적 및 공중보건 신고 절차 추가, "
        "(3) 영양 상태 평가 및 지원 계획 추가."
    ),
}

with open(os.path.join(ai_results_path, "judge_evaluation.json"), "w", encoding="utf-8") as f:
    json.dump(judge_eval, f, ensure_ascii=False, indent=2)

print(f"\n  Judge Verdict: {judge_eval['verdict']}  "
      f"({judge_eval['total_score']}/{judge_eval['max_score']} = {judge_eval['percentage']}%)")
print(f"  ┌{'─'*60}┐")
for k, v in judge_eval["criteria"].items():
    bar = "█" * v["score"] + "░" * (v["max"] - v["score"])
    print(f"  │ {k:15s} {bar} {v['score']:>2d}/{v['max']}  {v['comment'][:30]}│")
print(f"  └{'─'*60}┘")
print(f"\n  Feedback: {judge_eval['feedback']}")

# ═══════════════════════════════════════════════════════════════════════
# CORRECTION (Group B) — Judge 기반 보정
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "━" * 70)
print("  [CORRECTION] Judge 피드백 기반 보정 (Group B용)")
print("━" * 70)

correction = {
    "patient_id": "M00001",
    "original_score": 45,
    "corrections_applied": [
        {
            "category": "completeness",
            "original": "Drug-resistant TB 고려 미흡",
            "corrected": "경험적 MDR-TB 치료 고려: Moxifloxacin 400mg daily 추가 "
                         "pending GeneXpert result",
        },
        {
            "category": "completeness",
            "original": "공중보건 신고 절차 미기재",
            "corrected": "결핵 확진 시 관할 보건소 신고 (감염병예방법 제11조), "
                         "접촉자 추적 검사 의뢰",
        },
        {
            "category": "completeness",
            "original": "영양 평가 미포함",
            "corrected": "BMI 측정, 알부민/프리알부민 검사, 영양 상담 의뢰. "
                         "TB 환자 고칼로리 식이 계획.",
        },
    ],
    "corrected_score": 48,
    "improvement": "+3 points (90% → 96%)",
}

with open(os.path.join(ai_results_path, "correction_summary.json"), "w", encoding="utf-8") as f:
    json.dump(correction, f, ensure_ascii=False, indent=2)

for c in correction["corrections_applied"]:
    print(f"  ✏  [{c['category']}]")
    print(f"     Before: {c['original']}")
    print(f"     After:  {c['corrected']}")

print(f"\n  Score improvement: {correction['original_score']} → {correction['corrected_score']} "
      f"({correction['improvement']})")

# ═══════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════
print("\n\n" + "=" * 70)
print("  ╔══════════════════════════════════════════════════════════════╗")
print("  ║       김철수 (M00001) — End-to-End Pipeline Complete        ║")
print("  ╚══════════════════════════════════════════════════════════════╝")
print("=" * 70)

print(f"""
  환자: 김철수 (Kim Cheolsu, 58/M, A+)
  진단: Acute Pulmonary TB Exacerbation
  입원: Emergency, 서울대학교병원

  ━━━ Pipeline Results ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [BRONZE] Raw Ingestion    → 4 datasets (emergency/history/vitals/imaging)
  [SILVER] Refinement       → Outliers:{removed}, Alerts:{alert_count}/{vital_agg['total_measurements']}
  [GOLD]   Aggregation      → HR avg:{vital_agg['avg_heart_rate']}, SpO2 avg:{vital_agg['avg_spo2']}%
  [AI]     Clinical Report  → SOAP note + Treatment plan generated
  [JUDGE]  Quality Eval     → {judge_eval['total_score']}/{judge_eval['max_score']} ({judge_eval['percentage']}%) {judge_eval['verdict']}
  [CORRECT] Feedback Loop   → {correction['original_score']} → {correction['corrected_score']} ({correction['improvement']})
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# Output folder tree
print("  📁 Output Structure:")
for root, dirs, files in os.walk(OUTPUT_DIR):
    level = root.replace(OUTPUT_DIR, "").count(os.sep)
    indent = "     " + "  " * level
    basename = os.path.basename(root)
    print(f"{indent}📂 {basename}/")
    file_indent = "     " + "  " * (level + 1)
    for file in files:
        size_kb = os.path.getsize(os.path.join(root, file)) / 1024
        icon = "📊" if file.endswith(".parquet") else "📄"
        print(f"{file_indent}{icon} {file}  ({size_kb:.1f} KB)")

print("\n  ✅ Pipeline execution complete!")
