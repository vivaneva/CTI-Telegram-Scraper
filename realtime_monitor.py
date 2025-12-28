import asyncio
import requests # 디스코드 전송용
from telethon import TelegramClient, events
from datetime import datetime, timezone, timedelta

# config.py에서 모든 설정을 가져오기
from config import API_ID, API_HASH, TARGET_CHANNEL, DISCORD_WEBHOOK_URL, collection

# 감시할 키워드 리스트 (추가/삭제 가능)
WATCH_KEYWORDS = ['Korea', 'KR', 'Bank', 'Finance', 'Kisa', 'Nuclear', 'DDoS', 'Attack']

# 텔레그램 클라이언트 생성 (세션 이름 다르게)
client = TelegramClient('monitor_session', API_ID, API_HASH)

def send_discord_alert(message, keyword):
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ 디스코드 웹훅 URL이 설정되지 않았습니다.")
        return

    # UTC 시간을 동호주 표준시(AEST)로 변환 (+10시간)
    aest_time = datetime.now(timezone.utc) + timedelta(hours=10)
    formatted_time = aest_time.strftime('%Y-%m-%d %H:%M:%S (AEST)')

    # 디스코드 임베드(Embed) 메시지 꾸미기
    data = {
        "username": "CTI Watchdog",
        "embeds": [{
            "title": f"🚨 위협 키워드 탐지: '{keyword}'",
            "description": message.text[:300] + "...", # 너무 길면 자름
            "color": 15158332, # 빨간색
            "fields": [
                {"name": "채널", "value": TARGET_CHANNEL, "inline": True},
                {"name": "탐지 시간", "value": formatted_time, "inline": True},
                {"name": "바로가기", "value": f"https://t.me/{TARGET_CHANNEL}/{message.id}", "inline": False}
            ],
            "footer": {"text": "CTI Project - Realtime Monitor"}
        }]
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        if response.status_code == 204:
            print(f"🔔 디스코드 알림 전송 성공! ({keyword})")
        else:
            print(f"⚠️ 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ 디스코드 연결 에러: {e}")

@client.on(events.NewMessage(chats=TARGET_CHANNEL))
async def handler(event):
    msg = event.message
    if not msg.text:
        return

    print(f"\n📨 [실시간] 새 메시지 감지 (ID: {msg.id})")
    
    # 1. 텔레그램 전달(Forward) 정보 안전하게 추출
    forward_info = None
    try:
        if msg.fwd_from:
             # 경우1: 채널이나 유저 ID가 있는 경우 (from_id)
             if getattr(msg.fwd_from, 'from_id', None):
                 forward_info = str(msg.fwd_from.from_id) # 전체 정보 문자열로 저장
             # 경우2: 숨겨진 유저 이름만 있는 경우 (from_name)
             elif getattr(msg.fwd_from, 'from_name', None):
                 forward_info = msg.fwd_from.from_name
    except Exception:
        # 여기서 에러 발생하면 멈추지 않고 None으로 둠
         forward_info = "Unknown_Forward"
            
    # 2. DB에 실시간 저장 (Upsert)
    doc = {
            "channel_name": TARGET_CHANNEL,
            "message_id": msg.id,
            "date": msg.date,          # 글 쓴 시간 (UTC)
            "text": msg.text,          # 원문 (러시아어)
            "text_translated": None,       # 번역본 (translator.py)
            "views": msg.views,        # 조회수 (영향력 측정용)
            "is_forwarded": bool(msg.fwd_from), # 공유글 여부 (True/False)
            "forward_from": forward_info,  # 공유 출처
            "url": f"https://t.me/{TARGET_CHANNEL}/{msg.id}",
            "crawled_at": datetime.now(timezone.utc) # 수집된 시점
    }
    
    # 3. 중복 방지 및 DB 저장
    is_new_message = False
    try:
        result = collection.update_one(
            {"message_id": msg.id, "channel_name": TARGET_CHANNEL},
            {"$set": doc},
            upsert=True
        )

        # 신규 등록(upserted_id 존재)일 때만 알림 보내기
        if result.upserted_id:
            print("💾 신규 메시지 저장 완료 (NEW)")
            is_new_message = True
        else:
            print("♻️ 기존 메시지 갱신 완료 (Old)")
            is_new_message = False

    except Exception as e:
        print(f"⚠️ DB 저장 실패: {e}")

    # 4. 키워드 검사 및 알림 (신규 메시지만 알림)

    if is_new_message:
        # 대소문자 구분 없이 검사
        content_lower = msg.text.lower()
        found_keywords = [k for k in WATCH_KEYWORDS if k.lower() in content_lower]
    
        if found_keywords:
            target_kw = found_keywords[0]
            print(f"🚨 심각: '{target_kw}' 키워드 발견! 알림이 전송됩니다.")
            send_discord_alert(msg, target_kw)

if __name__ == "__main__":
    print(f"👀 [{TARGET_CHANNEL}] 실시간 감시 모드 시작... (Ctrl+C로 종료)")
    print(f"🎯 탐지 키워드: {WATCH_KEYWORDS}")

    # 프로그램이 종료되지 않고 계속 돌게 만듦
    client.start()
    client.run_until_disconnected()