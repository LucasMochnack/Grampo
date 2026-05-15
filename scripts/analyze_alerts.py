"""
Run: railway run --service grampo python scripts/analyze_alerts.py
Analisa alertas dos ultimos 30 dias com as novas regras em 4 niveis.
"""
import os, sys
sys.path.insert(0, ".")

from app.database import SessionLocal
from app.crud import get_agent_mappings, get_client_names
from app.routers.dashboard import (
    _load_period_pipeline, _group_events, _classify_conversation,
    ALERT_IDS, _top_alert, _re,
    _extract_direction, _extract_content_preview, _real_phone,
    BRASILIA, _INTENT_RULES
)
from datetime import datetime, timedelta, timezone
from collections import defaultdict

db = SessionLocal()
canal = "5519997733651"
now_br = datetime.now(BRASILIA)
since_br = (now_br.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=29))
since_utc = since_br.astimezone(timezone.utc)

print("Carregando eventos dos ultimos 30 dias...")
all_events, _cg, _cpl, cam, cnm = _load_period_pipeline(db, canal, since_utc, 100000)
db.close()

groups, phone_learned = _group_events(all_events, cam)
cp = _real_phone(canal)
groups = {k: v for k, v in groups.items() if _real_phone(k) != cp}
groups = {k: v for k, v in groups.items()
          if any(e.received_at and e.received_at.astimezone(BRASILIA) >= since_br for e in v)}

_all_kws = [kw for aid, _, _, kws in _INTENT_RULES if aid in ALERT_IDS for kw in kws]

level_counts   = defaultdict(int)
level_examples = defaultdict(list)
kw_counts      = defaultdict(int)

EPOCH = datetime.min.replace(tzinfo=timezone.utc)

for key, evs in groups.items():
    ph    = _real_phone(key)
    agent = (phone_learned.get(key) or cam.get(ph)
             or phone_learned.get(ph) or "Sem atendente")
    conv_texts = []
    for ev in evs:
        p = ev.raw_payload or {}
        c = _extract_content_preview(p) or ""
        d = _extract_direction(p)
        if c:
            conv_texts.append((d, c))

    intents = _classify_conversation(conv_texts)
    top = _top_alert(intents)
    if not top:
        continue

    level_id = top[0]
    level_counts[level_id] += 1

    matched_kw = ""
    snippet    = ""
    for d, text in conv_texts:
        lower = text.lower()
        for kw in _all_kws:
            hit = (kw in lower) if " " in kw else bool(
                _re.search(r'\b' + _re.escape(kw), lower))
            if hit:
                matched_kw = kw
                snippet    = text[:120]
                kw_counts[kw] += 1
                break
        if matched_kw:
            break

    client_name = cnm.get(ph, ph or key[:20])
    if len(level_examples[level_id]) < 10:
        level_examples[level_id].append((agent, client_name, snippet, matched_kw))

total_alerts = sum(level_counts.values())
print(f"\nTotal conversas analisadas : {len(groups)}")
print(f"Total com algum alerta     : {total_alerts}")
print(f"Cobertura de alertas       : {round(total_alerts/len(groups)*100) if groups else 0}%\n")

order = [
    ("alerta_critico", "CRITICO"),
    ("alerta_alto",    "ALTO RISCO"),
    ("alerta_monit",   "MONITORAMENTO"),
    ("alerta_oper",    "OPERACIONAL"),
]
for lid, label in order:
    cnt = level_counts.get(lid, 0)
    print("=" * 65)
    print(f"  {label}: {cnt} conversas")
    print("=" * 65)
    for agent, client, snippet, kw in level_examples.get(lid, []):
        print(f"  Agente : {agent}")
        print(f"  Cliente: {client}")
        print(f"  Gatilho: [{kw}]")
        print(f"  Trecho : \"{snippet[:100]}\"")
        print()

# Top keywords
print("\n--- TOP 20 KEYWORDS DISPARADAS ---")
for kw, cnt in sorted(kw_counts.items(), key=lambda x: -x[1])[:20]:
    print(f"  {cnt:3d}x  [{kw}]")
