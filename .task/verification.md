# PR 22 Verification Ledger

## Focused (per workstream)
- WS-A: tsc -b ; vitest run packages/redis packages/realtime packages/config packages/contracts apps/scheduler
- WS-B: vitest run apps/realtime-gateway ; docker compose up -d + gateway integration subset
- WS-C: npm run benchmark:websocket

## Final gate (once)
- [ ] npm run format:check
- [ ] npm run lint
- [ ] npm run typecheck
- [ ] npm test            (Postgres+Redis+Docker via docker compose up -d)
- [ ] npm run build
- [ ] npm run benchmark:websocket

## Experiments
- [ ] Redis startup outage — gateway degraded/startup policy
- [ ] Redis runtime outage — WS deterministic, Postgres + outbox untouched
- [ ] Redis recovery — subscription re-establishes
- [ ] Gateway restart — authoritative state survives
- [ ] Client reconnect — re-auth + resubscribe
- [ ] Slow consumer — bounded memory, others unaffected, slow client disconnected
- [ ] Multi-gateway — events reach clients via A and B
