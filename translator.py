import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi
from deep_translator import GoogleTranslator

# 1. 환경변수 및 DB 설정
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

try:
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["CTI_DB"]
    collection = db["telegram_logs"]
    print("✅ MongoDB 연결 성공! 번역 작업을 준비합니다...")
except Exception as e:
    print(f"❌ DB 연결 실패: {e}")
    exit()

def main():
    # 2. 번역 대상 찾기 (아직 번역 안 된 것만 쏙 골라내기)
    # text_translated 필드가 null 이거나, 아예 없는 문서만 찾음
    query = {"$or": [{"text_translated": None}, {"text_translated": {"$exists": False}}]}
    
    # 번역 대상 개수 확인
    target_docs = list(collection.find(query))
    total_count = len(target_docs)
    
    print(f"🕵️‍♂️ 번역할 게시글 발견: 총 {total_count}개")
    
    if total_count == 0:
        print("🎉 이미 모든 데이터가 번역되어 있습니다!")
        return

    print("🚀 번역 시작 (러시아어 -> 한국어)")
    print("=" * 50)

    # 3. 하나씩 꺼내서 번역하고 업데이트
    translator = GoogleTranslator(source='auto', target='ko') # 자동 감지 -> 한국어

    for i, doc in enumerate(target_docs, 1):
        original_text = doc.get('text', '')
        msg_id = doc.get('message_id')
        
        if not original_text:
            continue

        try:
            # (1) 번역 실행
            translated_text = translator.translate(original_text)
            
            # (2) 번역된 내용 DB에 채워넣기
            collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'text_translated': translated_text}}
            )
            
            # 진행 상황 출력
            print(f"[{i}/{total_count}] ID:{msg_id} 번역 완료 ✅")
            print(f"   🇷🇺 원문: {original_text[:30]}...")
            print(f"   🇰🇷 번역: {translated_text[:30]}...")
            print("-" * 50)
            
            # (3) 구글 API 차단 방지
            time.sleep(1.0) 

        except Exception as e:
            print(f"❌ 번역 실패 (ID:{msg_id}): {e}")
            time.sleep(2) # 에러 나면 좀 더 쉬기

    print(f"\n🎉 작업 끝! {total_count}개의 게시글을 모두 한국어로 바꿨습니다.")

if __name__ == "__main__":
    main()