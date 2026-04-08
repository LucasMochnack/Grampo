"""
Grampo Dashboard — server-rendered HTML pages for monitoring Zenvia webhooks.
Brand: Alto Valor Investimentos (Montserrat, navy #0b1120, teal #0fa968).
"""

import csv
import html as html_mod
import io
import secrets
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.crud import get_events, get_agent_mappings, get_client_names, replace_agent_mappings
from app.dependencies import get_db

router = APIRouter(tags=["dashboard"])

BRASILIA = timezone(timedelta(hours=-3))
COMPANY_CHANNEL = "5519997733651"
HOUR_START, HOUR_END = 6, 19

# ── Segment map ──────────────────────────────────────────────────────────────
AGENT_SEGMENT: dict[str, str] = {
    "CAIO HENRIQUE LIMA BATISTA": "Alta Renda",
    "Luis Henrique Gomes Delfini": "Alta Renda",
    "ROSANIA FLOR E SILVA": "Alta Renda",
    "Samuel Menuzzo": "Alta Renda",
    "Eduardo Barbosa": "Externo",
    "Leonardo Teixeira": "Externo",
    "Lucas Mochnack": "Externo",
    "Guilherme Monteiro das Chagas": "On Demand",
    "Ivan Voigt": "On Demand",
    "MAGNO ALENCAR DA SILVA": "On Demand",
    "Paulo José Teixeira Camarotto Manfio": "On Demand",
    "Vinícius Ruas": "On Demand",
}

SEGMENT_COLORS: dict[str, str] = {
    "Alta Renda": "#d4af37",
    "Externo": "#4a9eff",
    "On Demand": "#8b5cf6",
}

# ── Auth helpers ─────────────────────────────────────────────────────────────

def _check_auth(request: Request) -> bool:
    pwd = settings.DASHBOARD_PASSWORD
    if not pwd:
        return True
    return request.cookies.get("grampo_auth") == pwd


def _auth_redirect():
    return RedirectResponse("/dashboard/login", status_code=302)


# ── Payload helpers ──────────────────────────────────────────────────────────

def _extract_client_number(payload: dict) -> str:
    try:
        msg = payload.get("message", {}) or {}
        direction = (msg.get("direction", "") or payload.get("direction", "")).upper()
        # Zenvia Conversations API: for both IN and OUT, "from" is the client phone
        # and "to" is the company channel. Try "from" first, fallback to "to".
        from_num = msg.get("from", "") or payload.get("from", "")
        to_num = msg.get("to", "") or payload.get("to", "")
        # Pick whichever is NOT the company channel
        if from_num and from_num != COMPANY_CHANNEL:
            return from_num
        if to_num and to_num != COMPANY_CHANNEL:
            return to_num
        return ""
    except Exception:
        return ""


def _extract_direction(payload: dict) -> str:
    try:
        msg = payload.get("message", {})
        return (msg.get("direction", "") or payload.get("direction", "")).upper()
    except Exception:
        return ""


def _extract_content_preview(payload: dict) -> str:
    try:
        msg = payload.get("message", {})
        contents = msg.get("contents", [])
        if isinstance(contents, list):
            for c in contents:
                if isinstance(c, str):
                    return c[:300]
                if not isinstance(c, dict):
                    continue
                for key in ("text", "body", "payload"):
                    txt = c.get(key, "")
                    if txt and isinstance(txt, str):
                        return txt[:300]
                    elif txt and isinstance(txt, dict):
                        # Nested: {'text': {'text': '...'}}
                        inner = txt.get("text", "") or txt.get("body", "")
                        if inner:
                            return str(inner)[:300]
                ctype = (c.get("type", "") or "").lower()
                if ctype in ("image", "audio", "video", "file", "document", "sticker"):
                    caption = c.get("caption", "")
                    fname = c.get("fileName", c.get("filename", ""))
                    label = ctype.upper()
                    if caption:
                        return f"[{label}] {caption}"[:300]
                    if fname:
                        return f"[{label}] {fname}"[:300]
                    return f"[{label}]"
        if msg.get("text"):
            return str(msg["text"])[:300]
        # Fallback: try top-level contents
        top_contents = payload.get("contents", [])
        if isinstance(top_contents, list):
            for c in top_contents:
                if isinstance(c, dict):
                    txt = c.get("text", "") or c.get("body", "")
                    if txt and isinstance(txt, str):
                        return txt[:300]
        return ""
    except Exception:
        return ""


def _extract_conversation_id(payload: dict) -> str:
    try:
        msg = payload.get("message", {})
        return msg.get("conversationId", "") or payload.get("conversationId", "")
    except Exception:
        return ""


def _extract_agent_from_payload(payload: dict) -> str:
    try:
        msg = payload.get("message", {})
        agent = msg.get("agent", "") or payload.get("agent", "")
        if agent:
            return agent
        contents = msg.get("contents", [])
        if isinstance(contents, list):
            for c in contents:
                txt = c.get("text", "") or c.get("body", "")
                if txt and txt.startswith("*Name:*"):
                    return txt.split("*Name:*")[1].strip().split("\n")[0].strip()
        return ""
    except Exception:
        return ""


def _get_segment(agent_name: str) -> str:
    if not agent_name or agent_name == "Sem atendente":
        return ""
    for known, seg in AGENT_SEGMENT.items():
        if known.lower() == agent_name.lower():
            return seg
    return ""


def _segment_badge(agent_name: str) -> str:
    seg = _get_segment(agent_name)
    if not seg:
        return ""
    color = SEGMENT_COLORS.get(seg, "#666")
    return f' <span class="seg-badge" style="background:{color}">{seg}</span>'


# ── Shared grouping logic ────────────────────────────────────────────────────

def _group_events(events, client_agent_map):
    conv_to_client: dict[str, str] = {}
    for ev in events:
        p = ev.raw_payload or {}
        conv_id = _extract_conversation_id(p)
        direction = _extract_direction(p)
        client_num = _extract_client_number(p)
        if conv_id and direction == "IN" and client_num:
            conv_to_client[conv_id] = client_num

    # Sort events chronologically for proximity matching
    sorted_evs = sorted(events, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc))

    groups: dict[str, list] = defaultdict(list)
    phone_learned: dict[str, str] = {}

    # Track last known client for OUT messages without from/to
    last_known_client = ""
    for ev in sorted_evs:
        p = ev.raw_payload or {}
        ev_type = (p.get("type", "") or "").upper()
        if ev_type in ("CONVERSATION_STATUS", "MESSAGE_STATUS"):
            continue
        client_num = _extract_client_number(p)
        conv_id = _extract_conversation_id(p)
        if not client_num and conv_id:
            client_num = conv_to_client.get(conv_id, "")
        direction = _extract_direction(p)
        # For OUT without client info, use last known client from nearby messages
        if not client_num and direction == "OUT" and last_known_client:
            client_num = last_known_client
        if not client_num:
            continue
        # Update last known client
        if client_num:
            last_known_client = client_num
        agent = _extract_agent_from_payload(p)
        if direction == "OUT" and agent and client_num:
            phone_learned[client_num] = agent
        groups[client_num].append(ev)
    return groups, phone_learned


# ── CSS — Alto Valor Brand ───────────────────────────────────────────────────

COMMON_CSS = """
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Montserrat', 'Segoe UI', sans-serif; background: #0b1120; color: #e8ecf1; }

  /* Nav */
  nav { background: #0f1629; padding: 14px 28px; display: flex; align-items: center; gap: 8px; border-bottom: 1px solid #1a2540; flex-wrap: wrap; }
  .nav-brand { font-weight: 700; font-size: 15px; letter-spacing: 2px; color: #fff; margin-right: 20px; }
  .nav-brand span { color: #0fa968; }
  nav a { color: #5a6a8a; text-decoration: none; font-weight: 600; font-size: 12px; padding: 7px 16px; border-radius: 6px; transition: .2s; letter-spacing: .5px; text-transform: uppercase; }
  nav a:hover { color: #c0c8d8; background: rgba(255,255,255,.04); }
  nav a.active { color: #fff; background: #0fa968; }

  /* Layout */
  .container { max-width: 1440px; margin: 0 auto; padding: 24px; }
  h2 { color: #fff; margin-bottom: 16px; font-size: 17px; font-weight: 700; letter-spacing: .3px; }

  /* Cards */
  .card { background: #111a2e; border: 1px solid #1a2540; border-radius: 12px; padding: 22px; margin-bottom: 20px; }

  /* KPIs */
  .kpi-row { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 22px; }
  .kpi { background: #111a2e; border: 1px solid #1a2540; border-radius: 12px; padding: 18px 24px; flex: 1; min-width: 150px; }
  .kpi .val { font-size: 30px; font-weight: 700; color: #0fa968; }
  .kpi .label { font-size: 10px; color: #5a6a8a; text-transform: uppercase; letter-spacing: .8px; margin-top: 4px; font-weight: 600; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 10px; color: #5a6a8a; text-transform: uppercase; letter-spacing: .8px; padding: 10px 12px; border-bottom: 1px solid #1a2540; font-weight: 700; }
  td { padding: 10px 12px; border-bottom: 1px solid #141e35; font-size: 13px; }

  /* Direction badges */
  .dir-out { color: #ef6b73; font-weight: 600; }
  .dir-in { color: #0fa968; font-weight: 600; }

  /* Segment badge */
  .seg-badge { color: #fff; font-size: 9px; padding: 2px 8px; border-radius: 10px; margin-left: 8px; font-weight: 700; letter-spacing: .4px; text-transform: uppercase; vertical-align: middle; }

  /* Period buttons */
  .period-btns { display: flex; gap: 6px; }
  .period-btns a { font-size: 11px; padding: 5px 14px; border-radius: 6px; color: #5a6a8a; text-decoration: none; border: 1px solid #1a2540; background: transparent; font-weight: 600; }
  .period-btns a:hover { color: #c0c8d8; border-color: #2a3a5a; background: transparent; }
  .period-btns a.active { background: #0fa968; color: #fff; border-color: #0fa968; }

  /* Upload */
  .upload-section { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  .upload-section input[type=file] { font-size: 12px; color: #5a6a8a; }
  .upload-section button { background: #0fa968; color: #fff; border: none; padding: 8px 20px; border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 700; letter-spacing: .3px; transition: .2s; }
  .upload-section button:hover { background: #0dc575; }

  /* Segment legend */
  .seg-legend { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
  .seg-legend span { font-size: 11px; display: flex; align-items: center; gap: 6px; color: #8a96aa; font-weight: 500; }
  .seg-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }

  /* Fullscreen */
  .fullscreen { position: fixed!important; top: 0; left: 0; width: 100vw!important; height: 100vh!important; z-index: 9999; background: #0b1120; overflow: auto; padding: 24px; border-radius: 0!important; }
  .fs-btn { background: #141e35; color: #5a6a8a; border: 1px solid #1a2540; padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 11px; margin-left: 10px; font-weight: 600; transition: .2s; }
  .fs-btn:hover { background: #1a2540; color: #e8ecf1; }
  .fs-close { position: fixed; top: 16px; right: 24px; z-index: 10000; background: #ef4444; color: #fff; border: none; padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 700; }

  /* Login */
  .login-box { max-width: 360px; margin: 100px auto; text-align: center; }
  .login-box h2 { font-size: 20px; letter-spacing: 3px; margin-bottom: 8px; }
  .login-box .subtitle { color: #5a6a8a; font-size: 11px; letter-spacing: 1px; margin-bottom: 20px; }
  .login-box input { width: 100%; padding: 12px 16px; margin: 10px 0; background: #0b1120; border: 1px solid #1a2540; color: #e8ecf1; border-radius: 8px; font-size: 14px; font-family: 'Montserrat', sans-serif; }
  .login-box input:focus { outline: none; border-color: #0fa968; }
  .login-box button { width: 100%; padding: 12px; background: #0fa968; color: #fff; border: none; border-radius: 8px; font-size: 14px; font-weight: 700; cursor: pointer; font-family: 'Montserrat', sans-serif; letter-spacing: .5px; transition: .2s; }
  .login-box button:hover { background: #0dc575; }

  /* Conversations */
  .conv-row { cursor: pointer; transition: .15s; }
  .conv-row:hover { background: rgba(15,169,104,.06); }
  .chat-box { display: none; background: #0a0f1a; border: 1px solid #1a2540; border-radius: 10px; margin: 6px 12px 16px; padding: 16px; max-height: 450px; overflow-y: auto; }
  .chat-box.open { display: block; }
  .msg-container { display: flex; flex-direction: column; gap: 4px; }
  .msg { padding: 10px 14px; border-radius: 12px; max-width: 70%; font-size: 13px; line-height: 1.5; word-wrap: break-word; }
  .msg-out { background: #0c2e1f; color: #a8e6cf; margin-left: auto; text-align: right; border-bottom-right-radius: 3px; }
  .msg-in { background: #141e35; color: #c8d6e5; margin-right: auto; border-bottom-left-radius: 3px; }
  .msg-time { font-size: 10px; color: #4a5a7a; margin-top: 3px; }
</style>
"""


# ── Debug (temporary) ────────────────────────────────────────────────────────

@router.get("/dashboard/debug-payloads", include_in_schema=False)
def debug_payloads(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return _auth_redirect()
    from app.models import WebhookEvent
    events = db.query(WebhookEvent).order_by(WebhookEvent.received_at.desc()).limit(20).all()
    result = []
    for ev in events:
        p = ev.raw_payload or {}
        msg = p.get("message", {}) or {}
        result.append({
            "type": p.get("type"),
            "direction": _extract_direction(p),
            "msg_from": msg.get("from", ""),
            "msg_to": msg.get("to", ""),
            "top_from": p.get("from", ""),
            "top_to": p.get("to", ""),
            "client_extracted": _extract_client_number(p),
            "content_preview": _extract_content_preview(p),
            "conversationId": msg.get("conversationId", p.get("conversationId", "")),
        })
    return result


# ── Login ────────────────────────────────────────────────────────────────────

@router.get("/dashboard/login", response_class=HTMLResponse, include_in_schema=False)
def login_page():
    if not settings.DASHBOARD_PASSWORD:
        return RedirectResponse("/dashboard", status_code=302)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Login</title>{COMMON_CSS}</head>
    <body><div class="login-box card">
    <h2>ALTO <span style="color:#0fa968">VALOR</span></h2>
    <div class="subtitle">GRAMPO DASHBOARD</div>
    <form method="post"><input type="password" name="password" placeholder="Senha de acesso" autofocus>
    <button type="submit">Entrar</button></form></div></body></html>""")


@router.post("/dashboard/login", response_class=HTMLResponse, include_in_schema=False)
async def login_submit(request: Request):
    form = await request.form()
    pwd = form.get("password", "")
    if pwd == settings.DASHBOARD_PASSWORD:
        resp = RedirectResponse("/dashboard", status_code=302)
        resp.set_cookie("grampo_auth", pwd, httponly=True, max_age=86400 * 7)
        return resp
    return RedirectResponse("/dashboard/login?err=1", status_code=302)


# ── CSV Upload ───────────────────────────────────────────────────────────────

@router.post("/dashboard/upload-csv", include_in_schema=False)
async def upload_csv(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return _auth_redirect()
    form = await request.form()
    csv_file = form.get("csv_file")
    if not csv_file:
        return RedirectResponse("/dashboard/agentes?msg=no_file", status_code=302)
    content = await csv_file.read()
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    mappings: dict[str, dict[str, str]] = {}
    for row in reader:
        phone = (row.get("Telefone") or "").strip()
        agent = (row.get("Agente") or "").strip()
        client = (row.get("Nome completo") or "").strip()
        if phone and agent:
            mappings[phone] = {"agent_name": agent, "client_name": client}
    count = replace_agent_mappings(db, mappings)
    return RedirectResponse(f"/dashboard/agentes?msg=ok&count={count}", status_code=302)


# ── Nav HTML ─────────────────────────────────────────────────────────────────

def _nav_html(active: str, extra: str = "") -> str:
    c_cls = "active" if active == "conversas" else ""
    a_cls = "active" if active == "agentes" else ""
    return f"""<nav>
        <div class="nav-brand">ALTO<span>VALOR</span></div>
        <a href="/dashboard" class="{c_cls}">Conversas</a>
        <a href="/dashboard/agentes" class="{a_cls}">Agentes</a>
        {extra}
    </nav>"""


# ── Conversations Dashboard ─────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard_main(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return _auth_redirect()

    all_events, total = get_events(db, limit=50000, offset=0)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    groups, phone_learned = _group_events(all_events, client_agent_map)

    rows_html = ""
    idx = 0
    for client_num, evs in sorted(groups.items(), key=lambda x: max(e.received_at for e in x[1]), reverse=True):
        evs.sort(key=lambda e: e.received_at)
        agent = client_agent_map.get(client_num) or phone_learned.get(client_num) or "Sem atendente"
        client_name = client_name_map.get(client_num, "")
        badge = _segment_badge(agent)
        last_ev = evs[-1]
        ts = last_ev.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if last_ev.received_at else ""
        out_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "OUT")
        in_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "IN")
        msg_count = out_count + in_count
        chat_id = f"chat_{idx}"
        idx += 1

        client_display = html_mod.escape(client_name) if client_name else '<span style="color:#4a5a7a">Desconhecido</span>'

        rows_html += f"""
        <tr class="conv-row" onclick="toggleChat('{chat_id}')">
            <td style="font-weight:600">{client_display}</td>
            <td style="font-family:monospace;font-size:12px;color:#4a5a7a">{client_num}</td>
            <td>{agent}{badge}</td>
            <td style="text-align:center"><span class="dir-out">&uarr;{out_count}</span> &nbsp;<span class="dir-in">&darr;{in_count}</span></td>
            <td style="color:#5a6a8a">{ts}</td>
        </tr>
        <tr><td colspan="5" style="padding:0;border:none">
            <div id="{chat_id}" class="chat-box"><div class="msg-container">"""

        for ev in evs:
            p = ev.raw_payload or {}
            ev_type = (p.get("type", "") or "").upper()
            if ev_type in ("MESSAGE_STATUS", "CONVERSATION_STATUS"):
                continue
            direction = _extract_direction(p)
            content = html_mod.escape(_extract_content_preview(p) or "")
            if not content:
                continue
            msg_ts = ev.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if ev.received_at else ""
            if direction == "OUT":
                rows_html += f'<div class="msg msg-out">{content}<div class="msg-time">{msg_ts} &uarr;</div></div>'
            else:
                rows_html += f'<div class="msg msg-in">{content}<div class="msg-time">{msg_ts} &darr;</div></div>'

        rows_html += """</div></div></td></tr>"""

    nav = _nav_html("conversas")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Conversas</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        <div class="kpi-row">
            <div class="kpi"><div class="val">{total}</div><div class="label">Eventos totais</div></div>
            <div class="kpi"><div class="val">{len(groups)}</div><div class="label">Clientes</div></div>
            <div class="kpi"><div class="val">{len(set(client_agent_map.values()))}</div><div class="label">Agentes mapeados</div></div>
        </div>
        <div class="card">
            <table>
                <thead><tr><th>Cliente</th><th>Telefone</th><th>Agente</th><th style="text-align:center">Msgs</th><th>Ultima</th></tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>
    <script>
    function toggleChat(id) {{
        var el = document.getElementById(id);
        el.classList.toggle('open');
        if (el.classList.contains('open')) {{
            el.scrollTop = el.scrollHeight;
        }}
    }}
    </script>
    </body></html>""")


# ── Agents Dashboard ─────────────────────────────────────────────────────────

@router.get("/dashboard/agentes", response_class=HTMLResponse, include_in_schema=False)
def dashboard_agentes(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return _auth_redirect()

    periodo = request.query_params.get("periodo", "total")
    msg = request.query_params.get("msg", "")
    count = request.query_params.get("count", "")

    now_br = datetime.now(BRASILIA)
    cutoff = None
    if periodo == "hoje":
        cutoff = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "semana":
        cutoff = now_br - timedelta(days=now_br.weekday())
        cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "7dias":
        cutoff = now_br - timedelta(days=7)

    all_events, _ = get_events(db, limit=50000, offset=0)
    client_agent_map = get_agent_mappings(db)

    if cutoff:
        events = [ev for ev in all_events if ev.received_at and ev.received_at.astimezone(BRASILIA) >= cutoff]
    else:
        events = list(all_events)

    groups, phone_learned = _group_events(events, client_agent_map)

    agent_stats: dict[str, dict] = defaultdict(lambda: {"out": 0, "in": 0, "clients": set(), "days_out": defaultdict(int), "days_in": defaultdict(int)})
    hourly_msgs: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for client_num, evs in groups.items():
        agent = client_agent_map.get(client_num) or phone_learned.get(client_num) or "Sem atendente"
        for ev in evs:
            p = ev.raw_payload or {}
            direction = _extract_direction(p)
            ts = ev.received_at.astimezone(BRASILIA) if ev.received_at else None
            if not ts:
                continue
            day_key = ts.strftime("%d/%m")
            hour = ts.hour
            if direction == "OUT":
                agent_stats[agent]["out"] += 1
                agent_stats[agent]["days_out"][day_key] += 1
                if HOUR_START <= hour <= HOUR_END:
                    hourly_msgs[agent][hour] += 1
            elif direction == "IN":
                agent_stats[agent]["in"] += 1
                agent_stats[agent]["days_in"][day_key] += 1
                if HOUR_START <= hour <= HOUR_END:
                    hourly_msgs[agent][hour] += 1
        agent_stats[agent]["clients"].add(client_num)

    sorted_agents = sorted(agent_stats.items(), key=lambda x: x[1]["out"] + x[1]["in"], reverse=True)

    # Ranking
    ranking_html = ""
    for rank, (agent, stats) in enumerate(sorted_agents, 1):
        seg = _get_segment(agent)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        badge = _segment_badge(agent)
        total_msgs = stats["out"] + stats["in"]
        ranking_html += f"""<tr>
            <td style="text-align:center;color:#4a5a7a;font-weight:700">{rank}</td>
            <td style="border-left:3px solid {seg_color};padding-left:14px">{agent}{badge}</td>
            <td style="text-align:center">{len(stats['clients'])}</td>
            <td style="text-align:center"><span class="dir-out">&uarr;{stats['out']}</span></td>
            <td style="text-align:center"><span class="dir-in">&darr;{stats['in']}</span></td>
            <td style="text-align:center;font-weight:700;color:#fff">{total_msgs}</td>
        </tr>"""

    # Daily charts
    all_days = set()
    for stats in agent_stats.values():
        all_days.update(stats["days_out"].keys())
        all_days.update(stats["days_in"].keys())
    sorted_days = sorted(all_days, key=lambda d: datetime.strptime(d, "%d/%m"))

    daily_charts_html = ""
    if sorted_days:
        for agent, stats in sorted_agents[:12]:
            cid = f"chart_{secrets.token_hex(4)}"
            out_vals = [stats["days_out"].get(d, 0) for d in sorted_days]
            in_vals = [stats["days_in"].get(d, 0) for d in sorted_days]
            seg = _get_segment(agent)
            seg_label = f' ({seg})' if seg else ''
            daily_charts_html += f"""
            <div style="display:inline-block;width:320px;margin:8px;vertical-align:top">
                <div style="font-size:12px;font-weight:600;margin-bottom:4px;color:#e8ecf1">{agent}{seg_label}</div>
                <canvas id="{cid}" width="310" height="130"></canvas>
                <script>
                (function(){{
                    var c=document.getElementById('{cid}'),ctx=c.getContext('2d');
                    var days={sorted_days},out={out_vals},inv={in_vals};
                    var max=Math.max(...out,...inv,1),bw=Math.floor(280/days.length/2.5),gap=2;
                    ctx.fillStyle='#0b1120';ctx.fillRect(0,0,310,130);
                    for(var i=0;i<days.length;i++){{
                        var x=25+i*(bw*2+gap*3);
                        var ho=out[i]/max*90,hi=inv[i]/max*90;
                        ctx.fillStyle='#ef6b73';ctx.fillRect(x,100-ho,bw,ho);
                        ctx.fillStyle='#0fa968';ctx.fillRect(x+bw+gap,100-hi,bw,hi);
                        ctx.fillStyle='#5a6a8a';ctx.font='9px Montserrat,sans-serif';ctx.save();
                        ctx.translate(x+bw,118);ctx.rotate(-0.5);ctx.fillText(days[i],0,0);ctx.restore();
                    }}
                }})();
                </script>
            </div>"""

    # Hourly heatmap
    hours = list(range(HOUR_START, HOUR_END + 1))
    hour_headers = "".join(f'<th style="text-align:center;min-width:44px;font-size:10px;padding:6px 2px">{h:02d}h</th>' for h in hours)
    hour_headers += '<th style="text-align:center;min-width:50px;font-size:10px;padding:6px 4px;color:#0fa968">TOTAL</th>'

    hourly_rows = ""
    max_val = 1
    for agent in hourly_msgs:
        for h in hours:
            v = hourly_msgs[agent].get(h, 0)
            if v > max_val:
                max_val = v

    for agent, stats in sorted_agents:
        if agent == "Sem atendente":
            continue
        seg = _get_segment(agent)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        badge = _segment_badge(agent)
        cells = ""
        row_total = 0
        for h in hours:
            v = hourly_msgs[agent].get(h, 0)
            row_total += v
            if v == 0:
                bg = "#0b1120"
                txt_color = "#1a2540"
                display = ""
            else:
                intensity = min(v / max(max_val, 1), 1.0)
                r = int(10 + intensity * 5)
                g = int(40 + intensity * 129)
                b = int(20 + intensity * 84)
                a = 0.3 + intensity * 0.7
                bg = f"rgba({r},{g},{b},{a:.2f})"
                txt_color = "#fff" if intensity > 0.35 else "#7dcea0"
                display = str(v)
            cells += f'<td style="text-align:center;background:{bg};color:{txt_color};font-weight:700;font-size:15px;padding:8px 2px;border:1px solid #0f1629">{display}</td>'
        cells += f'<td style="text-align:center;font-weight:800;font-size:15px;color:#0fa968;background:#0f1629;padding:8px 4px;border:1px solid #1a2540">{row_total}</td>'
        hourly_rows += f'<tr><td style="border-left:3px solid {seg_color};padding-left:10px;white-space:nowrap;font-size:12px;font-weight:600;background:#0b1120">{agent}{badge}</td>{cells}</tr>'

    # Period buttons
    periods = [("hoje", "Hoje"), ("semana", "Semana"), ("7dias", "7 dias"), ("total", "Total")]
    period_html = '<div style="margin-left:auto" class="period-btns">'
    for val, label in periods:
        active = "active" if periodo == val else ""
        period_html += f'<a href="/dashboard/agentes?periodo={val}" class="{active}">{label}</a>'
    period_html += '</div>'

    # Upload feedback
    msg_html = ""
    if msg == "ok":
        msg_html = f'<div style="background:#0fa968;color:#fff;padding:10px 18px;border-radius:8px;margin-bottom:16px;font-size:13px;font-weight:600">Gabarito atualizado com {count} mapeamentos.</div>'
    elif msg == "no_file":
        msg_html = '<div style="background:#ef4444;color:#fff;padding:10px 18px;border-radius:8px;margin-bottom:16px;font-size:13px;font-weight:600">Nenhum arquivo selecionado.</div>'

    # Segment legend
    seg_legend = '<div class="seg-legend">'
    for seg_name, seg_col in SEGMENT_COLORS.items():
        seg_legend += f'<span><span class="seg-dot" style="background:{seg_col}"></span> {seg_name}</span>'
    seg_legend += '</div>'

    nav = _nav_html("agentes", period_html)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Agentes</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        {msg_html}

        <div class="card" style="padding:14px 22px">
            <form action="/dashboard/upload-csv" method="post" enctype="multipart/form-data" class="upload-section">
                <input type="file" name="csv_file" accept=".csv">
                <button type="submit">Atualizar Gabarito</button>
            </form>
            <div style="margin-top:8px;font-size:11px">
                <a href="https://app.zenvia.com/sales_contacts" target="_blank" style="color:#0fa968;text-decoration:none;font-weight:500">Baixar gabarito em app.zenvia.com/sales_contacts</a>
            </div>
        </div>

        {seg_legend}

        <div class="kpi-row">
            <div class="kpi"><div class="val">{len(sorted_agents)}</div><div class="label">Agentes ativos</div></div>
            <div class="kpi"><div class="val">{sum(len(s['clients']) for _,s in sorted_agents)}</div><div class="label">Clientes no periodo</div></div>
            <div class="kpi"><div class="val">{sum(s['out'] for _,s in sorted_agents)}</div><div class="label">Msgs enviadas</div></div>
            <div class="kpi"><div class="val">{sum(s['in'] for _,s in sorted_agents)}</div><div class="label">Msgs recebidas</div></div>
        </div>

        <div class="card">
            <h2>Ranking de Agentes</h2>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:12px;font-weight:500">
                Clientes = telefones unicos atendidos &bull; OUT = msgs enviadas &bull; IN = msgs recebidas
            </p>
            <table>
                <thead><tr><th style="width:40px">#</th><th>Agente</th><th style="text-align:center">Clientes</th><th style="text-align:center">OUT</th><th style="text-align:center">IN</th><th style="text-align:center">Total</th></tr></thead>
                <tbody>{ranking_html}</tbody>
            </table>
        </div>

        <div class="card">
            <h2>Mensagens por dia</h2>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500">
                <span style="color:#ef6b73">&block;</span> OUT &nbsp; <span style="color:#0fa968">&block;</span> IN
            </p>
            <div style="overflow-x:auto">{daily_charts_html}</div>
        </div>

        <div class="card" id="hourly-section">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
                <h2 style="margin:0">Mapa de calor &mdash; Mensagens por hora (06h &mdash; 19h)</h2>
                <button class="fs-btn" onclick="toggleFullscreen('hourly-section')">&#x26F6; Tela cheia</button>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500">
                Total de mensagens (IN + OUT) por agente em cada faixa horaria.
                Quanto mais <strong style="color:#0fa968">verde</strong>, maior o volume.
            </p>
            <div style="overflow-x:auto">
                <table style="min-width:700px">
                    <thead><tr><th style="min-width:180px">Agente</th>{hour_headers}</tr></thead>
                    <tbody>{hourly_rows}</tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
    function toggleFullscreen(id) {{
        var el = document.getElementById(id);
        var isFs = el.classList.toggle('fullscreen');
        var existing = el.querySelector('.fs-close');
        if (isFs) {{
            var btn = document.createElement('button');
            btn.className = 'fs-close';
            btn.textContent = 'Fechar (Esc)';
            btn.onclick = function() {{ toggleFullscreen(id); }};
            el.appendChild(btn);
        }} else if (existing) {{
            existing.remove();
        }}
    }}
    document.addEventListener('keydown', function(e) {{
        if (e.key === 'Escape') {{
            var fs = document.querySelector('.fullscreen');
            if (fs) {{ fs.classList.remove('fullscreen'); var c = fs.querySelector('.fs-close'); if(c) c.remove(); }}
        }}
    }});
    </script>
    </body></html>""")
