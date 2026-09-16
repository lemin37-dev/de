# 데이터 생성

# 1. 모듈가져오기
import json
import random
import uuid
from datetime import datetime, timedelta

# 2. 환경변수
RECORD_COUNT = 1000 # 임시 설정, 전처리 해야 하는 대상 데이터 총 개수

# 3. 더미 데이터 생성 함수
def generator_dummy_data_with_noise():
    dummy_data  = list()
    event_types = ['view','click','purchase','error',None]       # 결측 (노이즈 삽입)
    os_types    = ['iOS', 'Android','Windows', 'Mac', 'Unknown'] # 사용자 단말기 OS
    for _ in range( RECORD_COUNT ):
        # 3-1. 기본 베이스 데이터 생성
        record = {
            "event_id"   : str(uuid.uuid4()), # 해시값으로 무작위 세팅
            "user_id"    : f"user_{random.randint(1, 500)}", # 사용자는 중복 이벤트 로그 발생
            "event_type" : random.choice(event_types),   # 이벤트
            "product_id" : random.randint(1000, 2000),   # 상품 번호
            "price"      : random.randint(1000, 100000), # 가격
            "timestamp"  : (datetime.now()
                            - timedelta(hours=random.randint(0, 24))).strftime("%Y-%m-%d %H:%M:%S"),
            "os"         : random.choice(os_types)
        }
        # 3-2. 노이즈 삽입(혹은 교체)
        확률 = random.random()
        if 확률 < 0.05: # 5%
            record['user_id'] = None  # 고객 아이디 결측
        elif 확률 < 0.1: # 5 ~ 10 %
            record['price']   = -50   # 논리적 오류 (실제 1000~100000)
        elif 확률 < 0.15: # 10 ~ 15 %
            record['timestamp']  = "invalid-format" # 형식 깨짐
        # 3-3. 데이터 추가
        dummy_data.append( record )

    return dummy_data
# 4. 함수 호출
if __name__ == '__main__':
    data = generator_dummy_data_with_noise()
    print( len(data) )
    # json 저장
    with open('raw_data.json', 'w', encoding='utf-8') as f:
        # 통째로 저장 x, 라인별로 저장 -> json 형태만 저장
        for log in data:
            # 한줄에 json 1개씩 배치 Newlines Delimited JSON(NDJSON 형식)
            f.write( json.dumps(log) + "\n")
    # s3에 수동저장