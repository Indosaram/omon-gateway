# omo app-server 백엔드 연동 ((a)안) — mass-ulw 실행 계획

Date: 2026-08-28 · Tier: HEAVY (신규 추상화 + 외부 프로토콜 통합 + 데몬 동시성) · Mode: mass-ulw dag

## 목표
Discord 봇의 두뇌를 기존 직접-LLM 호출(LlmClient)에서 `omo app-server` 데몬으로 교체.
`OMON_AGENT_BACKEND=llm|omo` (기본 llm) 선택, 세션 키→omo 세션 결정적 매핑, bot_profiles 페르소나/모델 주입, 트랜스크립트 SQLite 미러 유지.

## 토폴로지 락 (독립 성공/실패 컴포넌트 6개)
1. 프로토콜 스펙 (외부 지식) → research-protocol
2. 게이트웨이 호출 경로 지도 (내부 지식, 2분할: Discord 액터 경로 / 대시보드·설정 경로) → research-actor, research-runtime
3. AgentBackend 시임 (무행동변경 리팩터) → impl-seam
4. OmoBackend 클라이언트 (신규 모듈) → impl-omo-backend
5. 배선 (main/actor/dashboard_runtime) → impl-wiring
6. 검증+실서피스 → verify-surface → review

## 노드/카테고리 (non-quick 사유 포함)
| 노드 | 카테고리 | 사유 | dependsOn |
|---|---|---|---|
| research-actor | unspecified-low | 비동기 흐름 추적 판단 필요 (몇 파일) | — |
| research-runtime | unspecified-low | 상동 | — |
| research-protocol | unspecified-high | 라이브 프로세스 프로토콜 역설계 | — |
| impl-seam | unspecified-high | 3+ 모듈 행동무변경 리팩터 | research-actor, research-runtime |
| impl-omo-backend | unspecified-high | 신규 모듈 + RED-first | research-protocol, impl-seam |
| impl-wiring | unspecified-high | 부팅 선택 + 2 소비자 배선 + e2e | impl-omo-backend |
| verify-surface | unspecified-high | 다중 프로세스 오케스트레이션 + curl 실증 | impl-wiring |
| review | deep | 교차모듈 적대 검증 — 부분 답 무가치 | verify-surface |

- 분할 원칙: 리서치는 증거파일이 분리되어 병렬 안전. 구현 3노드는 쓰기 범위 겹침(actor.rs/main.rs) → 직렬 체인.
- ultrabrain 미사용: 단일 초고난도 추론 노드 없음. quick 미사용 사유: 전 노드 다중 파일/판단 포함.

## 검증 전략
- RED→GREEN 채널: 시임=기존 스위트 pre/post 꼬리 + characterization(페이크 백엔드), OmoBackend=in-process fake app-server 통합테스트 RED 캡처 후 GREEN, 배선=e2e 테스트 RED(배선 부재)→GREEN.
- 실서피스(C5): temp DB + 대체 포트로 부팅, 실제 omo app-server 데몬, `curl -i -X POST /api/sessions/{id}/chat` 센티널 OMOSURFACE-OK → 200 + 본문 센티널 + temp DB messages 행.
- 게이트(C6): cargo build / test / clippy --all-targets -- -D warnings / fmt --check.
- 리뷰어: review 노드가 기준별 APPROVE/BLOCK 판정, criterion-cited 블로커만 수정 → 재제출 ≤2회.

## 증거 경로 (.omo/evidence/)
agent-callpath-actor.md · agent-callpath-runtime.md · omo-appserver-protocol.md · seam-red-green.md · omo-backend-red-green.md · wiring-red-green.md · final-verification.md

## 안전 불변식 (모든 실행 노드에 주입)
- 프로덕션 게이트웨이 pid 3811 / 127.0.0.1:9119 / 저장소 루트 omon_gateway.db 불접촉 (기록·종료·바인딩 금지).
- 노드가 기동한 프로세스는 자신이 종료 + 영수증. `git commit` 금지 (사용자 명시 요청 없음 — 스테이징+초안으로 대체).

## 실패 플레이북
노드 실패 시 retry(동일 노드 재시도) → 프롬프트 결함이면 amend. 조용한 위젯=스톨 아님(슬롯 대기). 노드 출력은 검증 전까지 claim으로만 취급.
