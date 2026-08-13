'''
- 평가를 해야하는 고객 데이터 구조(요청/응답) : [{}, {}, {}, ...]
'''
# 모듈 import
from fastapi import FastAPI     # 앱
from pydantic import BaseModel  # 요청/응답 클래스 구성 시 슈퍼클래스 역할
from typing import List         # 요청/응답 데이터 구성 시 구조 정의를 위해 사용
import random                   # 신용평가 시 활용

# fastapi 객체 생성
app = FastAPI()

# 요청/응답 구조 정의 -> class
class ReqData(BaseModel):
  # column 구성
  user_id:str
  income:int
  loan_amt:int

class ResData(BaseModel):
  # column 구성
  user_id:str
  credit_score:int    # 0 ~ 1000
  grade:str

# routing : url, 처리함수 매핑 정의
@app.get("/")
def home():
  return {"status":"AI 신용평가 서비스 API"}

@app.post("/predict", response_model=List[ResData])
def predict(users:List[ReqData]):
  results = list()
  for user in users:
    '''
    사전반영식 = (소득 // 1000) *10
    credit_score = min(난수(300, 600) + 사전반영식, 990)
    grade = credit_score (~800 : A / ~600 : B / 나머지 : C)
    '''
    사전반영식 = (user.income//1000)*10
    credit_score = min(random.randint(300,600) + 사전반영식, 990)
    grade = "A" if credit_score >= 800 else "B" if credit_score >= 600 else "C"
    results.append({
      "user_id": user.user_id,
      "credit_score": credit_score,
      "grade": grade
    })

  return results
