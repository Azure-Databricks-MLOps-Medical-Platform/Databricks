# -*- coding: utf-8 -*-
"""
김철수 (Kim Cheolsu) — 가상 환자 테스트 데이터 생성
시나리오: 58세 남성, 폐결핵 급성 발작으로 119 이송, Emergency 입원
"""
import csv
import json
import os
import shutil
import random
import math
from datetime import datetime, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "patient_kimcs")
os.makedirs(OUT, exist_ok=True)

PATIENT_ID = "M00001"
PATIENT_NAME = "Kim Cheolsu"
AGE = 58
GENDER = "Male"
BLOOD_TYPE = "A+"
CONDITION = "Pulmonary TB Exacerbation"
ADMISSION_DATE = datetime(2026, 2, 8, 9, 15, 0)

# ──────────────────────────────────────────────
# 1) 응급 데이터  (emergency_data.csv)
# ──────────────────────────────────────────────
emergency_rows = [
    {
        "dispatch_id": "D2026020801",
        "patient_id": PATIENT_ID,
        "patient_name": PATIENT_NAME,
        "age": AGE,
        "gender": GENDER,
        "blood_type": BLOOD_TYPE,
        "dispatch_time": "2026-02-08 08:42:00",
        "arrival_scene_time": "2026-02-08 08:55:00",
        "departure_scene_time": "2026-02-08 09:05:00",
        "arrival_hospital_time": "2026-02-08 09:15:00",
        "hospital": "서울대학교병원 응급의료센터",
        "chief_complaint": "급성 호흡곤란, 객혈, 발열 38.7°C",
        "triage_level": "1 - Resuscitation",
        "consciousness": "Alert",
        "gcs_score": 14,
        "initial_hr": 118,
        "initial_sbp": 145,
        "initial_dbp": 92,
        "initial_spo2": 88,
        "initial_temp": 38.7,
        "initial_rr": 28,
        "suspected_diagnosis": "폐결핵 급성 발작 (Acute Pulmonary TB Exacerbation)",
        "treatment_given": "산소 마스크 10L/min, IV 확보, 해열제 투여",
        "paramedic_name": "이민수",
        "ambulance_unit": "119-강남-07",
    }
]

with open(os.path.join(OUT, "emergency_data.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=emergency_rows[0].keys())
    w.writeheader()
    w.writerows(emergency_rows)
print("[1/4] emergency_data.csv  ✓")

# ──────────────────────────────────────────────
# 2) 의료 히스토리  (medical_history.csv)
# ──────────────────────────────────────────────
history_rows = [
    {
        "patient_id": PATIENT_ID,
        "record_date": "2018-03-15",
        "hospital": "강남세브란스병원",
        "department": "내과",
        "diagnosis": "Essential Hypertension (I10)",
        "medication": "Amlodipine 5mg, Losartan 50mg",
        "notes": "혈압 조절 시작, 정기 외래 추적 관찰",
    },
    {
        "patient_id": PATIENT_ID,
        "record_date": "2019-06-22",
        "hospital": "강남세브란스병원",
        "department": "내분비내과",
        "diagnosis": "Type 2 Diabetes Mellitus (E11)",
        "medication": "Metformin 500mg bid",
        "notes": "HbA1c 7.2%, 경구약 시작",
    },
    {
        "patient_id": PATIENT_ID,
        "record_date": "2021-11-10",
        "hospital": "서울대학교병원",
        "department": "호흡기내과",
        "diagnosis": "Pulmonary Tuberculosis (A15.0)",
        "medication": "HERZ regimen (Isoniazid, Rifampin, Ethambutol, Pyrazinamide)",
        "notes": "AFB smear (+), 6개월 치료 완료 (2022-05-10)",
    },
    {
        "patient_id": PATIENT_ID,
        "record_date": "2024-08-20",
        "hospital": "서울병원",
        "department": "호흡기내과",
        "diagnosis": "COPD - Moderate (J44.1)",
        "medication": "Tiotropium inhaler",
        "notes": "FEV1/FVC 62%, 만성기관지염 증상",
    },
    {
        "patient_id": PATIENT_ID,
        "record_date": "2026-02-08",
        "hospital": "서울대학교병원 응급의료센터",
        "department": "응급의학과",
        "diagnosis": "Acute Pulmonary TB Exacerbation (A15.0)",
        "medication": "O2 therapy, IV antibiotics, anti-TB regimen restart",
        "notes": "CXR: 우상엽 공동성 병변 의심, 객혈 동반, SpO2 88%",
    },
]

with open(os.path.join(OUT, "medical_history.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=history_rows[0].keys())
    w.writeheader()
    w.writerows(history_rows)
print("[2/4] medical_history.csv  ✓")

# ──────────────────────────────────────────────
# 3) 바이탈 시계열  (vital_signs_timeseries.csv)
#    5분 간격 × 48 포인트 = 4시간
# ──────────────────────────────────────────────
random.seed(42)

vitals = []
t = ADMISSION_DATE  # 09:15 시작

# 시나리오: 처음 위독(높은HR, 낮은SpO2) → 치료 후 점진 안정화
for i in range(48):
    progress = i / 47  # 0→1
    # HR: 118→92 (안정화)
    hr = 118 - 26 * progress + random.gauss(0, 3)
    # SBP: 145→128
    sbp = 145 - 17 * progress + random.gauss(0, 4)
    # DBP: 92→82
    dbp = 92 - 10 * progress + random.gauss(0, 3)
    # SpO2: 88→96
    spo2 = 88 + 8 * progress + random.gauss(0, 1.2)
    spo2 = min(100, spo2)
    # Temp: 38.7→37.2
    temp = 38.7 - 1.5 * progress + random.gauss(0, 0.15)
    # RR: 28→18
    rr = 28 - 10 * progress + random.gauss(0, 1.5)

    vitals.append({
        "patient_id": PATIENT_ID,
        "timestamp": t.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": round(hr, 1),
        "systolic_bp": round(sbp, 1),
        "diastolic_bp": round(dbp, 1),
        "spo2": round(spo2, 1),
        "temperature": round(temp, 2),
        "respiratory_rate": round(rr, 1),
        "risk_score": round(
            max(0, min(100,
                0.25 * max(0, hr - 100) +
                0.30 * max(0, 95 - spo2) * 5 +
                0.20 * max(0, temp - 37.5) * 10 +
                0.25 * max(0, rr - 20) * 3
            )), 1
        ),
    })
    t += timedelta(minutes=5)

with open(os.path.join(OUT, "vital_signs_timeseries.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=vitals[0].keys())
    w.writeheader()
    w.writerows(vitals)
print("[3/4] vital_signs_timeseries.csv  ✓  (48 data points)")

# ──────────────────────────────────────────────
# 4) 영상 데이터  (DICOM 메타 + 이미지 복사)
# ──────────────────────────────────────────────
img_dir = os.path.join(OUT, "images")
os.makedirs(img_dir, exist_ok=True)

# Chest X-ray PNEUMONIA 이미지 1장 복사
src_img_dir = os.path.join(BASE, "medical_images", "chest_xray", "train", "PNEUMONIA")
src_images = [f for f in os.listdir(src_img_dir) if f.endswith(".jpeg")][:1]
dst_filename = f"{PATIENT_ID}_CXR_20260208.jpeg"
if src_images:
    shutil.copy2(
        os.path.join(src_img_dir, src_images[0]),
        os.path.join(img_dir, dst_filename),
    )

dicom_meta = [
    {
        "patient_id": PATIENT_ID,
        "study_date": "2026-02-08",
        "study_time": "09:32:00",
        "modality": "CR",
        "body_part": "CHEST",
        "view_position": "PA",
        "image_file": dst_filename,
        "institution": "서울대학교병원",
        "referring_physician": "박영수",
        "study_description": "CHEST PA - Suspected Pulmonary TB Exacerbation",
        "finding_labels": "Consolidation, Cavity, Opacity",
        "sop_instance_uid": "1.2.826.0.1.3680043.8.1055.1.20260208093200.1",
    }
]

with open(os.path.join(OUT, "dicom_metadata.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=dicom_meta[0].keys())
    w.writeheader()
    w.writerows(dicom_meta)
print(f"[4/4] dicom_metadata.csv + {dst_filename}  ✓")

# ──────────────────────────────────────────────
# 요약 JSON
# ──────────────────────────────────────────────
summary = {
    "patient_id": PATIENT_ID,
    "patient_name": PATIENT_NAME,
    "age": AGE,
    "gender": GENDER,
    "blood_type": BLOOD_TYPE,
    "primary_diagnosis": CONDITION,
    "admission_date": ADMISSION_DATE.strftime("%Y-%m-%d %H:%M:%S"),
    "admission_type": "Emergency",
    "data_files": {
        "emergency": "emergency_data.csv",
        "medical_history": "medical_history.csv",
        "vital_signs": "vital_signs_timeseries.csv",
        "dicom_metadata": "dicom_metadata.csv",
        "chest_xray_image": f"images/{dst_filename}",
    },
    "vital_stats_summary": {
        "initial": {"HR": 118, "BP": "145/92", "SpO2": 88, "Temp": 38.7, "RR": 28},
        "final":   {"HR": 92,  "BP": "128/82", "SpO2": 96, "Temp": 37.2, "RR": 18},
    },
}
with open(os.path.join(OUT, "patient_summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 60)
print(f"  환자 김철수 (M00001) 테스트 데이터 생성 완료")
print(f"  출력 폴더: {OUT}")
print("=" * 60)
