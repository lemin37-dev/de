FROM python:3.12.12-slim

WORKDIR /app

# pydantic : 데이터 유효성 검사(타입), 구조 검사 - validation
# fastapi  : 백엔드 구성
# uvicorn  : 서비스 로드, fastapi 앞에서 프록시 담당
RUN pip install fastapi uvicorn pydantic

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]