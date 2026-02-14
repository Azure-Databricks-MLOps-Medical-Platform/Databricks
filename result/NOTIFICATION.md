# 🔔 다음 실행 예정 작업 (Notification)

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
