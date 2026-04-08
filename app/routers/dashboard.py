"""
Grampo Dashboard — server-rendered HTML pages for monitoring Zenvia webhooks.
Includes: Conversations view, Agent analytics with segments, CSV upload, period filters.
"""

import csv
import io
import secrets
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.crud import get_events, get_agent_mappings, replace_agent_mappings
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
        msg = payload.get("message", {})
        direction = (msg.get("direction", "") or payload.get("direction", "")).upper()
        if direction == "OUT":
            num = msg.get("to", "") or payload.get("to", "")
        else:
            num = msg.get("from", "") or payload.get("from", "")
        return "" if num == COMPANY_CHANNEL else num
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
        for key in ("text", "body"):
            c = msg.get("contents", [{}])
            if isinstance(c, list) and c:
                txt = c[0].get(key, "")
                if txt:
                    return txt[:120]
        return msg.get("text", "")[:120] if msg.get("text") else ""
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
    return f' <span style="background:{color};color:#fff;font-size:10px;padding:1px 6px;border-radius:8px;margin-left:6px;font-weight:600;letter-spacing:.3px">{seg}</span>'


# ── CSS ──────────────────────────────────────────────────────────────────────

COMMON_CSS = """
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0d1117; color: #e6edf3; }
  nav { background: #161b22; padding: 12px 24px; display: flex; align-items: center; gap: 18px; border-bottom: 1px solid #30363d; flex-wrap: wrap; }
  nav a { color: #8b949e; text-decoration: none; font-weight: 600; font-size: 13px; padding: 6px 14px; border-radius: 6px; transition: .15s; }
  nav a:hover, nav a.active { color: #e6edf3; background: #21262d; }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
  h2 { color: #e6edf3; margin-bottom: 16px; font-size: 18px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
  .kpi-row { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 20px; }
  .kpi { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 16px 22px; flex: 1; min-width: 140px; }
  .kpi .val { font-size: 28px; font-weight: 700; color: #58a6ff; }
  .kpi .label { font-size: 11px; color: #8b949e; text-transform: uppercase; margin-top: 2px; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 11px; color: #8b949e; text-transform: uppercase; padding: 8px 10px; border-bottom: 1px solid #30363d; }
  td { padding: 8px 10px; border-bottom: 1px solid #21262d; font-size: 13px; }
  tr:hover { background: #1c2128; }
  .dir-out { color: #f97583; } .dir-in { color: #56d364; }
  .period-btns { display: flex; gap: 6px; }
  .period-btns a { font-size: 12px; padding: 4px 12px; border-radius: 4px; color: #8b949e; text-decoration: none; border: 1px solid #30363d; }
  .period-btns a.active { background: #58a6ff; color: #fff; border-color: #58a6ff; }
  .upload-section { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .upload-section input[type=file] { font-size: 12px; color: #8b949e; }
  .upload-section button { background: #238636; color: #fff; border: none; padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; }
  .upload-section button:hover { background: #2ea043; }
  .seg-legend { display: flex; gap: 14px; margin-bottom: 14px; flex-wrap: wrap; }
  .seg-legend span { font-size: 11px; display: flex; align-items: center; gap: 5px; }
  .seg-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .fullscreen { position: fixed!important; top: 0; left: 0; width: 100vw!important; height: 100vh!important; z-index: 9999; background: #0d1117; overflow: auto; padding: 20px; border-radius: 0!important; }
  .fs-btn { background: #21262d; color: #8b949e; border: 1px solid #30363d; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 11px; margin-left: 10px; }
  .fs-btn:hover { background: #30363d; color: #e6edf3; }
  .fs-close { position: fixed; top: 14px; right: 20px; z-index: 10000; background: #da3633; color: #fff; border: none; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
  .login-box { max-width: 340px; margin: 80px auto; text-align: center; }
  .login-box input { width: 100%; padding: 10px; margin: 10px 0; background: #0d1117; border: 1px solid #30363d; color: #e6edf3; border-radius: 6px; font-size: 14px; }
  .login-box button { width: 100%; padding: 10px; background: #238636; color: #fff; border: none; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; }
</style>
"""


# ── Login ────────────────────────────────────────────────────────────────────

@router.get("/dashboard/login", response_class=HTMLResponse, include_in_schema=False)
def login_page():
    if not settings.DASHBOARD_PASSWORD:
        return RedirectResponse("/dashboard", status_code=302)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo Login</title>{COMMON_CSS}</head>
    <body><div class="login-box card"><h2>Grampo</h2>
    <form method="post"><input type="password" name="password" placeholder="Senha" autofocus>
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


# ── Conversations Dashboard ─────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard_main(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return _auth_redirect()

    all_events, total = get_events(db, limit=2000, offset=0)
    client_agent_map = get_agent_mappings(db)

    # Two-pass grouping: first map conversation IDs to client phones
    conv_to_client: dict[str, str] = {}
    for ev in all_events:
        p = ev.raw_payload or {}
        conv_id = _extract_conversation_id(p)
        direction = _extract_direction(p)
        client_num = _extract_client_number(p)
        if conv_id and direction == "IN" and client_num:
            conv_to_client[conv_id] = client_num

    # Group events by client phone
    groups: dict[str, list] = defaultdict(list)
    phone_learned: dict[str, str] = {}
    for ev in all_events:
        p = ev.raw_payload or {}
        client_num = _extract_client_number(p)
        conv_id = _extract_conversation_id(p)
        if not client_num and conv_id:
            client_num = conv_to_client.get(conv_id, "")
        if not client_num:
            continue
        direction = _extract_direction(p)
        agent = _extract_agent_from_payload(p)
        if direction == "OUT" and agent and client_num:
            phone_learned[client_num] = agent
        groups[client_num].append(ev)

    # Build conversation rows
    conversations = []
    for client_num, evs in sorted(groups.items(), key=lambda x: max(e.received_at for e in x[1]), reverse=True):
        evs.sort(key=lambda e: e.received_at, reverse=True)
        last = evs[0]
        out_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "OUT")
        in_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "IN")
        agent = client_agent_map.get(client_num) or phone_learned.get(client_num) or "Sem atendente"
        ts = last.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if last.received_at else ""
        preview = _extract_content_preview(last.raw_payload or {})
        conversations.append((client_num, agent, out_count, in_count, ts, preview))

    rows_html = ""
    for phone, agent, out_c, in_c, ts, preview in conversations:
        badge = _segment_badge(agent)
        rows_html += f"""<tr>
            <td style="font-family:monospace;font-size:12px">{phone}</td>
            <td>{agent}{badge}</td>
            <td><span class="dir-out">&uarr;{out_c}</span> &nbsp; <span class="dir-in">&darr;{in_c}</span></td>
            <td>{ts}</td>
            <td style="color:#8b949e;font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{preview}</td>
        </tr>"""

    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo</title>{COMMON_CSS}</head><body>
    <nav>
        <a href="/dashboard" class="active">CONVERSAS</a>
        <a href="/dashboard/agentes">AGENTES</a>
    </nav>
    <div class="container">
        <div class="kpi-row">
            <div class="kpi"><div class="val">{total}</div><div class="label">Eventos totais</div></div>
            <div class="kpi"><div class="val">{len(groups)}</div><div class="label">Clientes</div></div>
            <div class="kpi"><div class="val">{len(set(client_agent_map.values()))}</div><div class="label">Agentes mapeados</div></div>
        </div>
        <div class="card">
            <table>
                <thead><tr><th>Telefone</th><th>Agente</th><th>Msgs</th><th>Ultima</th><th>Preview</th></tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div></body></html>""")


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

    all_events, _ = get_events(db, limit=5000, offset=0)
    client_agent_map = get_agent_mappings(db)

    if cutoff:
        events = [ev for ev in all_events if ev.received_at and ev.received_at.astimezone(BRASILIA) >= cutoff]
    else:
        events = list(all_events)

    # Two-pass grouping
    conv_to_client: dict[str, str] = {}
    for ev in events:
        p = ev.raw_payload or {}
        conv_id = _extract_conversation_id(p)
        direction = _extract_direction(p)
        client_num = _extract_client_number(p)
        if conv_id and direction == "IN" and client_num:
            conv_to_client[conv_id] = client_num

    groups: dict[str, list] = defaultdict(list)
    phone_learned: dict[str, str] = {}
    for ev in events:
        p = ev.raw_payload or {}
        client_num = _extract_client_number(p)
        conv_id = _extract_conversation_id(p)
        if not client_num and conv_id:
            client_num = conv_to_client.get(conv_id, "")
        if not client_num:
            continue
        direction = _extract_direction(p)
        agent = _extract_agent_from_payload(p)
        if direction == "OUT" and agent and client_num:
            phone_learned[client_num] = agent
        groups[client_num].append(ev)

    # Build agent stats
    agent_stats: dict[str, dict] = defaultdict(lambda: {"out": 0, "in": 0, "clients": set(), "days_out": defaultdict(int), "days_in": defaultdict(int)})

    # Hourly message count tracking (06h-19h) — total msgs (IN+OUT) per agent per hour
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

    # Sort agents by total messages desc
    sorted_agents = sorted(agent_stats.items(), key=lambda x: x[1]["out"] + x[1]["in"], reverse=True)

    # ── Ranking table ────────────────────────────────────────────────────
    ranking_html = ""
    for rank, (agent, stats) in enumerate(sorted_agents, 1):
        seg = _get_segment(agent)
        seg_color = SEGMENT_COLORS.get(seg, "#30363d")
        badge = _segment_badge(agent)
        total_msgs = stats["out"] + stats["in"]
        ranking_html += f"""<tr>
            <td style="text-align:center;color:#8b949e;font-weight:700">{rank}</td>
            <td style="border-left:3px solid {seg_color};padding-left:12px">{agent}{badge}</td>
            <td style="text-align:center">{len(stats['clients'])}</td>
            <td style="text-align:center"><span class="dir-out">&uarr;{stats['out']}</span></td>
            <td style="text-align:center"><span class="dir-in">&darr;{stats['in']}</span></td>
            <td style="text-align:center;font-weight:600">{total_msgs}</td>
        </tr>"""

    # ── Daily bar charts (canvas) ────────────────────────────────────────
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
                <div style="font-size:12px;font-weight:600;margin-bottom:4px;color:#e6edf3">{agent}{seg_label}</div>
                <canvas id="{cid}" width="310" height="130"></canvas>
                <script>
                (function(){{
                    var c=document.getElementById('{cid}'),ctx=c.getContext('2d');
                    var days={sorted_days},out={out_vals},inv={in_vals};
                    var max=Math.max(...out,...inv,1),bw=Math.floor(280/days.length/2.5),gap=2;
                    ctx.fillStyle='#0d1117';ctx.fillRect(0,0,310,130);
                    for(var i=0;i<days.length;i++){{
                        var x=25+i*(bw*2+gap*3);
                        var ho=out[i]/max*90,hi=inv[i]/max*90;
                        ctx.fillStyle='#f97583';ctx.fillRect(x,100-ho,bw,ho);
                        ctx.fillStyle='#56d364';ctx.fillRect(x+bw+gap,100-hi,bw,hi);
                        ctx.fillStyle='#8b949e';ctx.font='9px sans-serif';ctx.save();
                        ctx.translate(x+bw,118);ctx.rotate(-0.5);ctx.fillText(days[i],0,0);ctx.restore();
                    }}
                }})();
                </script>
            </div>"""

    # ── Hourly heatmap table ─────────────────────────────────────────────
    hours = list(range(HOUR_START, HOUR_END + 1))
    hour_headers = "".join(f'<th style="text-align:center;min-width:44px;font-size:11px;padding:6px 2px">{h:02d}h</th>' for h in hours)
    hour_headers += '<th style="text-align:center;min-width:50px;font-size:11px;padding:6px 4px">TOTAL</th>'

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
        seg_color = SEGMENT_COLORS.get(seg, "#30363d")
        badge = _segment_badge(agent)
        cells = ""
        row_total = 0
        for h in hours:
            v = hourly_msgs[agent].get(h, 0)
            row_total += v
            if v == 0:
                bg = "#0d1117"
                txt_color = "#21262d"
                display = ""
            else:
                intensity = min(v / max(max_val, 1), 1.0)
                # Green heatmap gradient: dark -> bright green
                r = int(13 + intensity * 10)
                g = int(50 + intensity * 166)
                b = int(27 + intensity * 40)
                a = 0.25 + intensity * 0.75
                bg = f"rgba({r},{g},{b},{a:.2f})"
                txt_color = "#ffffff" if intensity > 0.4 else "#a5d6a7"
                display = str(v)
            cells += f'<td style="text-align:center;background:{bg};color:{txt_color};font-weight:700;font-size:15px;padding:8px 2px;border:1px solid #161b22">{display}</td>'
        # Total column
        cells += f'<td style="text-align:center;font-weight:800;font-size:15px;color:#e6edf3;background:#161b22;padding:8px 4px;border:1px solid #21262d">{row_total}</td>'
        hourly_rows += f'<tr><td style="border-left:3px solid {seg_color};padding-left:10px;white-space:nowrap;font-size:12px;font-weight:600;background:#0d1117">{agent}{badge}</td>{cells}</tr>'

    # ── Period filter buttons ────────────────────────────────────────────
    periods = [("hoje", "Hoje"), ("semana", "Semana"), ("7dias", "7 dias"), ("total", "Total")]
    period_html = ""
    for val, label in periods:
        active = "active" if periodo == val else ""
        period_html += f'<a href="/dashboard/agentes?periodo={val}" class="{active}">{label}</a>'

    # ── Upload feedback ──────────────────────────────────────────────────
    msg_html = ""
    if msg == "ok":
        msg_html = f'<div style="background:#238636;color:#fff;padding:8px 16px;border-radius:6px;margin-bottom:14px;font-size:13px">Gabarito atualizado com {count} mapeamentos.</div>'
    elif msg == "no_file":
        msg_html = '<div style="background:#da3633;color:#fff;padding:8px 16px;border-radius:6px;margin-bottom:14px;font-size:13px">Nenhum arquivo selecionado.</div>'

    # ── Segment legend ───────────────────────────────────────────────────
    seg_legend = '<div class="seg-legend">'
    for seg_name, seg_col in SEGMENT_COLORS.items():
        seg_legend += f'<span><span class="seg-dot" style="background:{seg_col}"></span> {seg_name}</span>'
    seg_legend += '</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Agentes</title>{COMMON_CSS}</head><body>
    <nav>
        <a href="/dashboard">CONVERSAS</a>
        <a href="/dashboard/agentes" class="active">AGENTES</a>
        <div style="margin-left:auto" class="period-btns">{period_html}</div>
    </nav>
    <div class="container">
        {msg_html}

        <!-- Upload + Zenvia link -->
        <div class="card" style="padding:14px 20px">
            <form action="/dashboard/upload-csv" method="post" enctype="multipart/form-data" class="upload-section">
                <input type="file" name="csv_file" accept=".csv">
                <button type="submit">Atualizar Gabarito</button>
            </form>
            <div style="margin-top:6px;font-size:11px">
                <a href="https://app.zenvia.com/sales_contacts" target="_blank" style="color:#58a6ff">Baixar gabarito em app.zenvia.com/sales_contacts</a>
            </div>
        </div>

        {seg_legend}

        <!-- KPIs -->
        <div class="kpi-row">
            <div class="kpi"><div class="val">{len(sorted_agents)}</div><div class="label">Agentes ativos</div></div>
            <div class="kpi"><div class="val">{sum(len(s['clients']) for _,s in sorted_agents)}</div><div class="label">Clientes no periodo</div></div>
            <div class="kpi"><div class="val">{sum(s['out'] for _,s in sorted_agents)}</div><div class="label">Msgs enviadas (OUT)</div></div>
            <div class="kpi"><div class="val">{sum(s['in'] for _,s in sorted_agents)}</div><div class="label">Msgs recebidas (IN)</div></div>
        </div>

        <!-- Ranking -->
        <div class="card">
            <h2>Ranking de Agentes</h2>
            <p style="font-size:11px;color:#8b949e;margin-bottom:12px">
                Clientes = telefones unicos atendidos &bull; OUT = msgs enviadas &bull; IN = msgs recebidas
            </p>
            <table>
                <thead><tr><th style="width:40px">#</th><th>Agente</th><th style="text-align:center">Clientes</th><th style="text-align:center">OUT</th><th style="text-align:center">IN</th><th style="text-align:center">Total</th></tr></thead>
                <tbody>{ranking_html}</tbody>
            </table>
        </div>

        <!-- Daily charts -->
        <div class="card">
            <h2>Mensagens por dia</h2>
            <p style="font-size:11px;color:#8b949e;margin-bottom:10px">
                <span style="color:#f97583">&block;</span> OUT &nbsp; <span style="color:#56d364">&block;</span> IN &mdash; barras por dia de atividade de cada agente
            </p>
            <div style="overflow-x:auto">{daily_charts_html}</div>
        </div>

        <!-- Hourly heatmap -->
        <div class="card" id="hourly-section">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
                <h2 style="margin:0">Mapa de calor &mdash; Mensagens por hora (06h &mdash; 19h)</h2>
                <button class="fs-btn" onclick="toggleFullscreen('hourly-section')">&#x26F6; Tela cheia</button>
            </div>
            <p style="font-size:11px;color:#8b949e;margin-bottom:10px">
                Total de mensagens (IN + OUT) por agente em cada faixa horaria.
                Quanto mais <strong style="color:#4caf50">verde</strong>, maior o volume.
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
