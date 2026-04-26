"""
Grampo Dashboard — server-rendered HTML pages for monitoring Zenvia webhooks.
Brand: Alto Valor Investimentos (Montserrat, navy #0b1120, teal #0fa968).
"""

import csv
import hashlib
import hmac
import html as html_mod
import io
import json
import secrets
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import quote as _url_quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.crud import get_events, get_events_only, get_agent_mappings, get_client_names, replace_agent_mappings, get_setting, set_setting
from app.dependencies import get_db

router = APIRouter(tags=["dashboard"])

BRASILIA = timezone(timedelta(hours=-3))
# Company WhatsApp channels: number → label
COMPANY_CHANNELS_MAP: dict[str, str] = {
    "5519997733651": "Principal",
    "551920425800": "Suporte Interno",
    "5519971572548": "Mesa RV",
    "5519996789754": "Mesa Cambio",
}
COMPANY_CHANNELS = set(COMPANY_CHANNELS_MAP.keys())
HOUR_START, HOUR_END = 6, 19

# ── Intent classification ───────────────────────────────────────────────────
import re as _re

_INTENT_RULES: list[tuple[str, str, str, list[str]]] = [
    # (id, label, color, keywords)
    ("alerta", "⚠ ALERTA", "#ef4444", [
        "absurdo", "vergonha", "ridiculo", "ridículo", "palhaçada", "palhacada",
        "incompetente", "incompetência", "incompetencia", "lixo", "péssimo", "pessimo",
        "horrível", "horrivel", "nojo", "nojento", "merda", "porra", "caralho",
        "puta", "fdp", "vai se foder", "vai tomar", "filho da puta", "otário",
        "otario", "idiota", "imbecil", "babaca", "cuzão", "cuzao",
        "reclamação", "reclamacao", "reclamo", "reclamar", "procon",
        "advogado", "ouvidoria", "denúncia", "denuncia",
        "fraude", "roubando", "roubo", "enganado", "enganando",
        "desrespeit", "falta de respeito", "abuso", "descaso",
    ]),
    ("reuniao", "Reunião", "#3b82f6", [
        "reunião", "reuniao", "meet", "agenda", "agendar", "agendado",
        "horário", "horario", "disponível", "disponivel", "disponibilidade",
        "call", "ligação", "ligacao", "ligar", "videoconferência",
    ]),
    ("resgate", "Movimentação", "#f59e0b", [
        "resgate", "resgatar", "resgatando", "retirada", "retirar", "sacar", "saque",
        "transferir", "transferência", "transferencia", "aplicar", "aplicação", "aplicacao",
        "investir", "aportar", "aporte", "movimentar", "movimentação", "movimentacao",
        "vencimento", "liquidez", "previdência", "previdencia", "portabilidade",
    ]),
    ("duvida", "Dúvida", "#8b5cf6", [
        "como funciona", "o que é", "o que e", "quanto rende", "rendimento",
        "qual a taxa", "qual o prazo", "pode me explicar", "não entendi",
        "nao entendi", "dúvida", "duvida", "me explica", "o que significa",
    ]),
    ("followup", "Follow-up", "#06b6d4", [
        "conseguiu verificar", "alguma novidade", "tem retorno", "tem novidade",
        "atualização", "atualizacao", "como ficou", "e aí", "e ai",
        "tem previsão", "tem previsao", "posição", "posicao",
    ]),
]


def _classify_conversation(messages_texts: list[tuple[str, str]]) -> list[tuple[str, str, str]]:
    """Classify conversation intent from list of (direction, text).
    Returns list of (id, label, color) for matched intents."""
    found: dict[str, tuple[str, str, str]] = {}
    for direction, text in messages_texts:
        lower = text.lower()
        for intent_id, label, color, keywords in _INTENT_RULES:
            if intent_id in found:
                continue
            for kw in keywords:
                # Use word-boundary matching to avoid false positives
                # (e.g., "puta" inside "disputa", "reputação")
                if " " in kw:
                    # multi-word phrases: substring match is fine
                    if kw in lower:
                        found[intent_id] = (intent_id, label, color)
                        break
                else:
                    # single word: require word boundary at start
                    if _re.search(r'\b' + _re.escape(kw), lower):
                        found[intent_id] = (intent_id, label, color)
                        break
    # Sort: alerta always first
    result = []
    if "alerta" in found:
        result.append(found.pop("alerta"))
    result.extend(found.values())
    return result


def _intent_badges(intents: list[tuple[str, str, str]]) -> str:
    if not intents:
        return ""
    badges = ""
    for _, label, color in intents:
        badges += f' <span style="background:{color};color:#fff;font-size:9px;padding:2px 7px;border-radius:4px;font-weight:700;letter-spacing:.3px;vertical-align:middle">{label}</span>'
    return badges

# ── Segment map ──────────────────────────────────────────────────────────────
AGENT_SEGMENT: dict[str, str] = {
    "CAIO HENRIQUE LIMA BATISTA": "Alta Renda",
    "Luis Henrique Gomes Delfini": "Alta Renda",
    "REINALDO MATHIAS FERREIRA": "Alta Renda",
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


# ── Topic / Theme rules ──────────────────────────────────────────────────────
# (id, label, color, keywords)
TOPIC_RULES: list[tuple[str, str, str, list[str]]] = [
    ("consorcio",   "Consórcio",          "#f59e0b", ["consórcio","consorcio","consorciado","consorciada"]),
    ("seguro_vida", "Seguro de Vida",      "#ef4444", ["seguro de vida","seguro vida","proteção familiar","seguro","apólice"]),
    ("previdencia", "Previdência",         "#8b5cf6", ["previdência","previdencia","pgbl","vgbl","aposentadoria","previdenciário"]),
    ("renda_fixa",  "Renda Fixa",          "#06b6d4", ["renda fixa","cdb","lci","lca","tesouro direto","tesouro","debênture","debenture","cri","cra","letras"]),
    ("acoes",       "Ações / BDR",         "#0fa968", ["ações","acoes","bdr","bolsa","b3","bovespa","ação","stock"]),
    ("fundos",      "Fundos",              "#3b82f6", ["fundo","fundos","multimercado","fundo de ações","fundo cambial"]),
    ("fii",         "Fundos Imobiliários", "#10b981", ["fii","fundo imobiliário","fundo imobiliario","tijolo","papel","imóvel","imovel"]),
    ("coe",         "COE",                 "#d4af37", ["coe","certificado de operações estruturadas","operações estruturadas"]),
    ("offshore",    "Offshore / Int'l",    "#7c3aed", ["offshore","exterior","internacional","dólar","dolares","global","investimento no exterior"]),
    ("carteira",    "Revisão de Carteira", "#64748b", ["carteira","revisão","revisar","alocação","alocacao","diversificação","diversificacao","rebalanceamento","portfólio","portfolio"]),
    ("resgate",     "Resgate / Saque",     "#f97316", ["resgate","resgatar","saque","sacar","retirada","retirar"]),
    ("reuniao",     "Reunião / Call",      "#0ea5e9", ["reunião","reuniao","call","ligação","ligacao","videoconferência","videoconferencia","agendar","agendamento"]),
    ("credito",     "Crédito / Empréstimo","#fb7185", ["crédito","credito","empréstimo","emprestimo","financiamento","home equity","ccb"]),
    ("cambio",      "Câmbio",              "#a78bfa", ["câmbio","cambio","remessa","dólar","euro","moeda estrangeira"]),
]


def _short_agent_name(name: str) -> str:
    """Return 'Primeiro Último' with Title Case. E.g. 'CAIO HENRIQUE LIMA BATISTA' → 'Caio Batista'."""
    if not name or name in ("Sem atendente", ""):
        return name
    parts = name.strip().split()
    if len(parts) <= 2:
        return " ".join(p.capitalize() for p in parts)
    return f"{parts[0].capitalize()} {parts[-1].capitalize()}"


# ── Auth helpers ─────────────────────────────────────────────────────────────

AUTH_COOKIE = "grampo_auth"
_MASTER_TOKEN = "master"


_ACCESSES_SETTING_KEY = "dashboard_accesses"


def _parse_accesses_json(raw: str) -> list[dict]:
    if not raw or not raw.strip():
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        pw = item.get("password")
        if not pw or not isinstance(pw, str):
            continue
        role = item.get("role") if item.get("role") in ("admin", "viewer") else "viewer"
        agents = item.get("agents") or []
        if not isinstance(agents, list):
            agents = []
        agents = [str(a).strip() for a in agents if str(a).strip()]
        out.append({"password": pw, "role": role, "agents": agents})
    return out


def _load_accesses(db: Session | None = None) -> list[dict]:
    """Load accesses. Prefers DB setting; falls back to DASHBOARD_ACCESSES env var."""
    if db is not None:
        try:
            raw = get_setting(db, _ACCESSES_SETTING_KEY)
            if raw:
                return _parse_accesses_json(raw)
        except Exception:
            pass
    return _parse_accesses_json(settings.DASHBOARD_ACCESSES or "")


def _save_accesses(db: Session, accesses: list[dict]) -> None:
    cleaned = []
    for a in accesses:
        pw = (a.get("password") or "").strip()
        if not pw:
            continue
        role = a.get("role") if a.get("role") in ("admin", "viewer") else "viewer"
        agents = a.get("agents") or []
        if not isinstance(agents, list):
            agents = []
        agents = [str(x).strip() for x in agents if str(x).strip()]
        cleaned.append({"password": pw, "role": role, "agents": agents})
    set_setting(db, _ACCESSES_SETTING_KEY, json.dumps(cleaned, ensure_ascii=False))


def _session_secret() -> bytes:
    s = settings.SESSION_SECRET or settings.DASHBOARD_PASSWORD or "grampo-default-secret"
    return s.encode("utf-8")


def _sign_token(token: str) -> str:
    sig = hmac.new(_session_secret(), token.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{token}.{sig}"


def _verify_token(signed: str) -> str | None:
    if not signed or "." not in signed:
        return None
    token, sig = signed.rsplit(".", 1)
    expected = hmac.new(_session_secret(), token.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    if hmac.compare_digest(sig, expected):
        return token
    return None


def _get_access(request: Request, db: Session | None = None) -> dict | None:
    """Return the active access dict {role, agents} or None if not authenticated.
    Admin (master password) → {"role":"admin","agents":[]}.
    """
    master_pwd = settings.DASHBOARD_PASSWORD
    if not master_pwd:
        # auth disabled
        return {"role": "admin", "agents": []}
    cookie = request.cookies.get(AUTH_COOKIE) or ""

    # Legacy cookie: raw master password (backward compat with old deployments)
    if cookie == master_pwd:
        return {"role": "admin", "agents": []}

    token = _verify_token(cookie)
    if not token:
        return None
    if token == _MASTER_TOKEN:
        return {"role": "admin", "agents": []}
    if token.startswith("idx:"):
        try:
            idx = int(token[4:])
        except ValueError:
            return None
        accesses = _load_accesses(db)
        if 0 <= idx < len(accesses):
            a = accesses[idx]
            return {"role": a["role"], "agents": list(a["agents"])}
    return None


def _check_auth(request: Request) -> bool:
    return _get_access(request) is not None


def _user_sees(access: dict | None, agent_name: str) -> bool:
    """Return True if the access is allowed to view the given agent."""
    if not access:
        return False
    if access.get("role") == "admin":
        return True
    allowed = access.get("agents") or []
    if not allowed:
        return False
    name = (agent_name or "").strip()
    return any(name == a for a in allowed)


def _auth_redirect():
    return RedirectResponse("/dashboard/login", status_code=302)


# ── Payload helpers ──────────────────────────────────────────────────────────

def _extract_client_number(payload: dict) -> str:
    try:
        msg = payload.get("message", {}) or {}
        direction = (msg.get("direction", "") or payload.get("direction", "")).upper()
        from_num = msg.get("from", "") or payload.get("from", "")
        to_num = msg.get("to", "") or payload.get("to", "")
        if not from_num and not to_num:
            return ""

        # Primary strategy: use direction. Zenvia Conversations API:
        # - IN  (client → company): from=client, to=company → client is `from`
        # - OUT (company → client): from=company, to=client → client is `to`
        if direction == "IN" and from_num:
            return from_num
        if direction == "OUT" and to_num:
            return to_num

        # Fallback when direction is missing/unknown: use channel registry.
        # Only skip if BOTH sides are recognized company channels (internal
        # chat) or NEITHER is recognized (likely personal/test traffic).
        if from_num and to_num:
            from_is_co = from_num in COMPANY_CHANNELS
            to_is_co = to_num in COMPANY_CHANNELS
            if from_is_co and to_is_co:
                return ""  # company-to-company internal — ignore
            if not from_is_co and not to_is_co:
                # Neither side is a known company number. To avoid losing
                # legitimate traffic on a NEW Zenvia number not yet listed
                # in COMPANY_CHANNELS_MAP, return `from` as best effort.
                return from_num
            # One side is a company channel — pick the other
            return to_num if from_is_co else from_num
        # Single-sided: return whatever exists
        return from_num or to_num
    except Exception:
        return ""


def _extract_channel(payload: dict) -> str:
    """Return the company channel number involved in this message, or ''."""
    try:
        msg = payload.get("message", {}) or {}
        from_num = msg.get("from", "") or payload.get("from", "")
        to_num = msg.get("to", "") or payload.get("to", "")
        if from_num in COMPANY_CHANNELS:
            return from_num
        if to_num in COMPANY_CHANNELS:
            return to_num
        return ""
    except Exception:
        return ""


def _filter_events_by_channel(events, canal: str):
    """Filter events to only those on a specific company channel."""
    if not canal:
        return events
    return [ev for ev in events if _extract_channel(ev.raw_payload or {}) == canal]


def _extract_addressed_name(text: str) -> str:
    """Extract the first name being addressed in an OUT message.
    E.g. 'Bom dia, Sr. Josimar!' → 'josimar'
         'Giselle, temos um bate papo...' → 'giselle'
         '*Luis Delfini:* Bom dia, Sr. Josimar!' → 'josimar'
    Returns lowercase first name, or '' if not detected."""
    import re
    t = text.strip()
    # Remove agent prefix *Name:*
    t = re.sub(r'^\*[^*]+\*:?\s*', '', t)

    # Pattern 1: greeting + name — "Bom dia, Giselle" / "Olá, Sr. Josimar"
    m = re.search(
        r'(?:Ol[aá]|Oi|Bom dia|Boa tarde|Boa noite|Prezad[oa]|Car[oa]|Fala)\s*[,!]?\s*'
        r'(?:Sr\.?\s*|Sra\.?\s*)?'
        r'([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ][a-záéíóúãõâêîôûç]{2,})',
        t, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Pattern 2: name at start of message followed by comma — "Giselle, temos..."
    m = re.match(
        r'^([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ][a-záéíóúãõâêîôûç]{2,})\s*[,!]',
        t)
    if m:
        candidate = m.group(1).lower()
        # Exclude common words that aren't names
        if candidate not in {'tudo', 'bom', 'boa', 'hoje', 'para', 'aqui', 'como', 'certo', 'combinado', 'perfeito'}:
            return candidate

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
                # Handle Zenvia Conversations API nested payload.json (templates, files)
                inner_payload = c.get("payload")
                if isinstance(inner_payload, dict):
                    inner_json = inner_payload.get("json", {})
                    if isinstance(inner_json, dict):
                        # Template: payload.json.text
                        tmpl_text = inner_json.get("text", "")
                        if tmpl_text:
                            return str(tmpl_text)[:300]
                        # File: payload.json.fileCaption / fileName
                        fcaption = inner_json.get("fileCaption", "")
                        fname_inner = inner_json.get("fileName", "")
                        fmime = inner_json.get("fileMimeType", "")
                        if fcaption:
                            return f"[ARQUIVO] {fcaption}"[:300]
                        if fname_inner:
                            return f"[ARQUIVO] {fname_inner}"[:300]
                ctype = (c.get("type", "") or "").lower()
                if "template" in ctype:
                    return "[TEMPLATE]"
                if "file" in ctype or "image" in ctype or "audio" in ctype or "video" in ctype:
                    return f"[{ctype.split('/')[-1].upper()}]"
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
    import re
    try:
        msg = payload.get("message", {})
        agent = msg.get("agent", "") or payload.get("agent", "")
        if agent:
            return agent
        contents = msg.get("contents", [])
        if isinstance(contents, list):
            for c in contents:
                # Get text from Conversations API format
                txt = ""
                inner = c.get("payload")
                if isinstance(inner, dict):
                    txt = inner.get("text", "")
                    # Zenvia Conversations template format: payload.json.text
                    if not txt:
                        inner_json = inner.get("json")
                        if isinstance(inner_json, dict):
                            txt = inner_json.get("text", "")
                if not txt:
                    txt = c.get("text", "") or c.get("body", "")
                if not txt:
                    continue
                if txt.startswith("*Name:*"):
                    return txt.split("*Name:*")[1].strip().split("\n")[0].strip()
                # Match *Agent Name* or *Agent Name:* at start of message (Zenvia Conversations format)
                m = re.match(r'^\*([^*]+)\*', txt)
                if m:
                    candidate = m.group(1).strip().rstrip(":").strip()
                    # Verify it's a known agent name (exact match, case-insensitive)
                    for known in AGENT_SEGMENT:
                        if known.lower() == candidate.lower():
                            return known
                    # Partial match: if candidate is part of a known agent name
                    # e.g. "Caio Batista" matches "CAIO HENRIQUE LIMA BATISTA"
                    candidate_parts = candidate.lower().split()
                    if len(candidate_parts) >= 2:
                        for known in AGENT_SEGMENT:
                            known_parts = known.lower().split()
                            if all(cp in known_parts for cp in candidate_parts):
                                return known

        # Fallback: scan full message body for "Sou [Nome]" / "me chamo [Nome]"
        # matching any known agent name (full or partial)
        full_text = ""
        if isinstance(contents, list):
            for c in contents:
                if not isinstance(c, dict):
                    continue
                inner = c.get("payload")
                if isinstance(inner, dict):
                    full_text += " " + (inner.get("text", "") or "")
                    # Zenvia Conversations template format: payload.json.text
                    inner_json = inner.get("json")
                    if isinstance(inner_json, dict):
                        full_text += " " + (inner_json.get("text", "") or "")
                full_text += " " + (c.get("text", "") or c.get("body", "") or "")

        if full_text:
            full_lower = full_text.lower()
            # Look for "Sou X" / "me chamo X" / "meu nome é X" patterns
            for pat in [r'sou\s+([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+(?:\s+[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+){0,3})',
                        r'me chamo\s+([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+(?:\s+[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+){0,3})',
                        r'meu nome [eé]\s+([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+(?:\s+[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+){0,3})']:
                for m in re.finditer(pat, full_text, flags=re.IGNORECASE):
                    candidate = m.group(1).strip().rstrip(",.!?;:").strip()
                    if not candidate:
                        continue
                    # Exact match
                    for known in AGENT_SEGMENT:
                        if known.lower() == candidate.lower():
                            return known
                    # Partial match: all candidate words found in known agent name
                    candidate_parts = [p for p in candidate.lower().split() if len(p) >= 3]
                    if candidate_parts:
                        for known in AGENT_SEGMENT:
                            known_parts = known.lower().split()
                            if all(cp in known_parts for cp in candidate_parts):
                                return known

            # Also try matching any known agent full/first name anywhere in text
            for known in AGENT_SEGMENT:
                first = known.split()[0]
                if len(first) >= 4 and re.search(r'\bsou\s+\w*\s*' + re.escape(first), full_lower):
                    return known

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


# ── Agent-prefix helpers ─────────────────────────────────────────────────────

_AGENT_PREFIX_RE = _re.compile(r'^\*([^*]{3,60})\*\s*:?', _re.UNICODE)

def _extract_agent_prefix(text: str) -> str:
    """Return the agent name from a *Name:* prefix at the start of a message body.
    Returns a canonical AGENT_SEGMENT key when possible, else the raw name."""
    if not text:
        return ""
    m = _AGENT_PREFIX_RE.match(text.strip())
    if not m:
        return ""
    candidate = m.group(1).strip().rstrip(":").strip()
    if not candidate or not any(c.isalpha() for c in candidate):
        return ""
    # Exact match against known agents
    for known in AGENT_SEGMENT:
        if known.lower() == candidate.lower():
            return known
    # Partial match (e.g. "Caio Batista" → "CAIO HENRIQUE LIMA BATISTA")
    cparts = candidate.lower().split()
    if len(cparts) >= 2:
        for known in AGENT_SEGMENT:
            kparts = known.lower().split()
            if all(cp in kparts for cp in cparts):
                return known
    # Accept raw if it looks like ≥2-word proper name
    words = candidate.split()
    if len(words) >= 2 and words[0][0].isupper():
        return candidate
    return ""


def _real_phone(key: str) -> str:
    """Strip any ##agent## suffix added during conversation splitting."""
    return key.split("##agent##")[0] if "##agent##" in key else key


# ── Shared grouping logic ────────────────────────────────────────────────────

def _group_events(events, client_agent_map):
    # First pass: build conversationId → client phone mapping from ANY message with from/to
    conv_to_client: dict[str, str] = {}
    for ev in events:
        p = ev.raw_payload or {}
        conv_id = _extract_conversation_id(p)
        client_num = _extract_client_number(p)
        if conv_id and client_num:
            conv_to_client[conv_id] = client_num

    # Sort events chronologically
    sorted_evs = sorted(events, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc))

    groups: dict[str, list] = defaultdict(list)
    phone_learned: dict[str, str] = {}
    # Track the "addressed name" per group to detect cross-talk
    group_addressed_names: dict[str, set] = defaultdict(set)

    # Proximity with time limit (max 10 min gap) for rare orphan events
    # without from/to/conversationId. The cap exists to avoid mixing two
    # different conversations into one if a long pause occurs.
    PROXIMITY_MAX_SECONDS = 600
    last_known_client = ""
    last_known_time = None

    for ev in sorted_evs:
        p = ev.raw_payload or {}
        ev_type = (p.get("type", "") or "").upper()
        if ev_type in ("CONVERSATION_STATUS", "MESSAGE_STATUS"):
            continue
        client_num = _extract_client_number(p)
        conv_id = _extract_conversation_id(p)
        direction = _extract_direction(p)

        # Track whether this client_num came from explicit from/to vs inferred
        has_explicit_fromto = bool(client_num)

        # If no from/to, try conversationId mapping
        if not client_num and conv_id:
            client_num = conv_to_client.get(conv_id, "")

        # If still no client, use time-limited proximity (only within 3 min of last known)
        if not client_num and direction == "OUT" and last_known_client and last_known_time and ev.received_at:
            gap = abs((ev.received_at - last_known_time).total_seconds())
            if gap <= PROXIMITY_MAX_SECONDS:
                client_num = last_known_client

        # If still no client, use conversationId as group key
        if not client_num and conv_id:
            client_num = f"conv:{conv_id}"
        if not client_num:
            continue

        # ── Name conflict check ──────────────────────────────────────────────
        # For inferred assignments (no explicit from/to), check if the addressed
        # name in this OUT message conflicts with the group's established names.
        if not has_explicit_fromto and direction == "OUT":
            content = _extract_content_preview(p) or ""
            addressed = _extract_addressed_name(content)
            if addressed:
                known_names = group_addressed_names.get(client_num, set())
                if known_names and addressed not in known_names:
                    # Name mismatch — this message belongs to a different conversation
                    # Assign to an isolated conv: group to avoid polluting the current group
                    iso_key = f"conv:iso:{conv_id or addressed}"
                    groups[iso_key].append(ev)
                    if addressed:
                        group_addressed_names[iso_key].add(addressed)
                    continue
                else:
                    group_addressed_names[client_num].add(addressed)
        elif direction == "OUT":
            # Explicit from/to — register the addressed name for this group
            content = _extract_content_preview(p) or ""
            addressed = _extract_addressed_name(content)
            if addressed:
                group_addressed_names[client_num].add(addressed)
        # ────────────────────────────────────────────────────────────────────

        # Update last known client and time
        if client_num and not client_num.startswith("conv:"):
            last_known_client = client_num
            last_known_time = ev.received_at

        agent = _extract_agent_from_payload(p)
        if direction == "OUT" and agent:
            phone_learned[client_num] = agent
            if client_num.startswith("conv:") and conv_id:
                real_phone = conv_to_client.get(conv_id, "")
                if real_phone:
                    phone_learned[real_phone] = agent
        groups[client_num].append(ev)

    # Second pass: merge conv: groups into real phone groups when possible
    merged_groups: dict[str, list] = defaultdict(list)
    for key, evs in groups.items():
        if key.startswith("conv:"):
            conv_id = key[5:]
            real_phone = conv_to_client.get(conv_id, "")
            if real_phone:
                merged_groups[real_phone].extend(evs)
                continue
        merged_groups[key].extend(evs)

    # Third pass: split groups where multiple *Agent:* prefixes coexist
    return _split_by_agent_prefix(dict(merged_groups), phone_learned)


def _split_by_agent_prefix(
    groups: dict[str, list], phone_learned: dict[str, str]
) -> tuple[dict[str, list], dict[str, str]]:
    """Split conversation groups when OUT messages carry different *Agent:* prefixes."""
    new_groups: dict[str, list] = defaultdict(list)
    new_phone_learned = dict(phone_learned)

    for key, evs in groups.items():
        sorted_evs = sorted(evs, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc))

        # Count unique agent prefixes in OUT messages
        prefix_counts: dict[str, int] = defaultdict(int)
        for ev in sorted_evs:
            p = ev.raw_payload or {}
            if _extract_direction(p) == "OUT":
                pfx = _extract_agent_prefix(_extract_content_preview(p) or "")
                if pfx:
                    prefix_counts[pfx] += 1

        if len(prefix_counts) <= 1:
            # Single or no prefix — no split needed
            new_groups[key].extend(sorted_evs)
            continue

        # Multiple agents detected — split sequentially, tracking active agent
        primary = max(prefix_counts, key=prefix_counts.get)
        current_prefix = primary

        for ev in sorted_evs:
            p = ev.raw_payload or {}
            direction = _extract_direction(p)
            if direction == "OUT":
                pfx = _extract_agent_prefix(_extract_content_preview(p) or "")
                if pfx:
                    current_prefix = pfx

            sub_key = key if current_prefix == primary else f"{key}##agent##{current_prefix}"
            new_groups[sub_key].append(ev)

        # Register agents for sub-group keys
        for pfx in prefix_counts:
            sub_key = key if pfx == primary else f"{key}##agent##{pfx}"
            new_phone_learned[sub_key] = pfx

    return dict(new_groups), new_phone_learned


# ── CSS — Alto Valor Brand ───────────────────────────────────────────────────

COMMON_CSS = """
<meta http-equiv="refresh" content="600">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Montserrat', 'Segoe UI', sans-serif; background: #0b1120; color: #e8ecf1; padding-left: 220px; padding-top: 60px; min-height: 100vh; }

  /* ── Sidebar ──────────────────────────────────────────────────────────── */
  .gp-sidebar {
    position: fixed; left: 0; top: 0; width: 220px; height: 100vh;
    background: #0b1120; border-right: 1px solid #1a2540;
    display: flex; flex-direction: column; padding: 20px 14px;
    z-index: 100; overflow-y: auto;
  }
  .sidebar-brand { padding: 0 6px; margin-bottom: 28px; }
  .nav-group { display: flex; flex-direction: column; gap: 2px; margin-bottom: 24px; }
  .nav-group-label { font-size: 9px; letter-spacing: 1.5px; color: #5a6a8a; padding: 0 12px; margin-bottom: 6px; font-weight: 700; text-transform: uppercase; }
  .nav-item { display: flex; align-items: center; justify-content: space-between; padding: 9px 14px; border-radius: 8px; background: transparent; color: #8a96aa; font-size: 12px; font-weight: 600; cursor: pointer; letter-spacing: 0.5px; text-transform: uppercase; text-decoration: none; transition: .15s; }
  .nav-item:hover { background: rgba(255,255,255,.04); color: #c0c8d8; }
  .nav-item.active { background: #0fa968; color: #fff; }
  .nav-badge { background: #ef4444; color: #fff; font-size: 10px; font-weight: 700; padding: 1px 7px; border-radius: 10px; min-width: 18px; text-align: center; }
  .nav-item.active .nav-badge { background: #fff; color: #0fa968; }
  .nav-badge-muted { background: #1a2540; color: #5a6a8a; font-size: 10px; font-weight: 700; padding: 1px 7px; border-radius: 10px; min-width: 18px; text-align: center; }
  .sidebar-footer { margin-top: auto; padding: 10px 12px; border-top: 1px solid #1a2540; display: flex; align-items: center; gap: 10px; }

  /* ── Topbar ───────────────────────────────────────────────────────────── */
  .gp-topbar {
    position: fixed; left: 220px; top: 0; right: 0; height: 60px;
    background: #0b1120; border-bottom: 1px solid #1a2540;
    display: flex; align-items: center; padding: 0 28px; z-index: 99; gap: 12px;
  }
  .topbar-title { font-weight: 700; font-size: 18px; letter-spacing: -.3px; color: #fff; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .topbar-right { display: flex; align-items: center; gap: 10px; flex-shrink: 0; flex-wrap: wrap; }

  /* ── Layout ───────────────────────────────────────────────────────────── */
  .container { max-width: 1440px; margin: 0 auto; padding: 24px; }
  h2 { color: #fff; margin-bottom: 16px; font-size: 17px; font-weight: 700; letter-spacing: .3px; }

  /* Cards */
  .card { background: #111a2e; border: 1px solid #1a2540; border-radius: 12px; padding: 22px; margin-bottom: 20px; }

  /* KPIs */
  .kpi-row { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 22px; }
  .kpi { background: #111a2e; border: 1px solid #1a2540; border-radius: 12px; padding: 18px 24px; flex: 1; min-width: 150px; }
  .kpi .val { font-size: 30px; font-weight: 700; color: #0fa968; font-family: 'JetBrains Mono', monospace; }
  .kpi .label { font-size: 10px; color: #5a6a8a; text-transform: uppercase; letter-spacing: .8px; margin-top: 4px; font-weight: 600; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 10px; color: #5a6a8a; text-transform: uppercase; letter-spacing: .8px; padding: 10px 12px; border-bottom: 1px solid #1a2540; font-weight: 700; }
  td { padding: 10px 12px; border-bottom: 1px solid #141e35; font-size: 13px; }

  /* Direction badges */
  .dir-out { color: #ef6b73; font-weight: 600; font-family: 'JetBrains Mono', monospace; }
  .dir-in { color: #0fa968; font-weight: 600; font-family: 'JetBrains Mono', monospace; }

  /* Segment badge */
  .seg-badge { color: #fff; font-size: 9px; padding: 2px 8px; border-radius: 10px; margin-left: 8px; font-weight: 700; letter-spacing: .4px; text-transform: uppercase; vertical-align: middle; }

  /* Period buttons */
  .period-btns { display: flex; gap: 6px; flex-wrap: wrap; }
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

  /* Login (no sidebar) */
  .login-page { padding-left: 0 !important; padding-top: 0 !important; }
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
  .msg-time { font-size: 10px; color: #4a5a7a; margin-top: 3px; font-family: 'JetBrains Mono', monospace; }
  td .mono { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #4a5a7a; }
</style>
"""



# ── Login ────────────────────────────────────────────────────────────────────

@router.get("/dashboard/login", response_class=HTMLResponse, include_in_schema=False)
def login_page():
    if not settings.DASHBOARD_PASSWORD:
        return RedirectResponse("/dashboard", status_code=302)
    err_html = '<div style="background:rgba(239,68,68,.15);border:1px solid #ef4444;color:#fca5a5;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:12px;font-weight:600">Senha incorreta. Tente novamente.</div>' if "err=1" in str(request.url) else ""
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Login</title>{COMMON_CSS}</head>
    <body class="login-page" style="background:radial-gradient(ellipse at top, #111a2e 0%, #0b1120 60%, #070b15 100%);display:flex;align-items:center;justify-content:center;min-height:100vh;padding:40px">
    <div style="display:flex;flex-direction:column;align-items:center;gap:36px;width:100%;max-width:380px">
      <div style="display:flex;flex-direction:column;align-items:center;gap:14px">
        <div style="width:56px;height:56px;border-radius:14px;background:#0fa968;box-shadow:0 8px 32px rgba(15,169,104,.35);display:flex;align-items:center;justify-content:center;font-weight:800;color:#0b1120;font-size:24px;letter-spacing:-1px">AV</div>
        <div style="text-align:center">
          <div style="font-size:18px;letter-spacing:4px;font-weight:700;color:#fff">ALTO<span style="color:#0fa968">VALOR</span></div>
          <div style="font-size:10px;letter-spacing:2.5px;color:#5a6a8a;margin-top:6px;font-weight:600">GRAMPO DASHBOARD</div>
        </div>
      </div>
      <div style="width:100%;background:#111a2e;border:1px solid #1a2540;border-radius:14px;padding:28px;box-shadow:0 20px 60px rgba(0,0,0,.4)">
        <div style="text-align:center;margin-bottom:20px">
          <h2 style="font-size:16px;font-weight:700;color:#fff;margin-bottom:4px">Entrar no painel</h2>
          <p style="font-size:11px;color:#8a96aa">Acesso restrito · Equipe Alto Valor</p>
        </div>
        {err_html}
        <form method="post" style="display:flex;flex-direction:column;gap:14px">
          <div>
            <label style="font-size:10px;color:#5a6a8a;letter-spacing:1px;font-weight:700;display:block;margin-bottom:6px">SENHA</label>
            <input type="password" name="password" placeholder="••••••••••" autofocus style="width:100%;padding:12px 14px;background:#0b1120;border:1px solid #1a2540;color:#e8ecf1;border-radius:8px;font-size:14px;font-family:'Montserrat',sans-serif;letter-spacing:2px;outline:none">
          </div>
          <button type="submit" style="background:#0fa968;color:#fff;border:none;padding:12px;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;font-family:'Montserrat',sans-serif;letter-spacing:.5px;margin-top:4px">Entrar</button>
        </form>
        <div style="font-size:10.5px;color:#5a6a8a;text-align:center;margin-top:16px">Sessão válida por 7 dias</div>
      </div>
      <div style="font-size:10px;color:#3a4a6a;letter-spacing:1px;font-weight:600">ALTO VALOR INVESTIMENTOS</div>
    </div>
    </body></html>""")


@router.post("/dashboard/login", response_class=HTMLResponse, include_in_schema=False)
async def login_submit(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    pwd = form.get("password", "")
    master = settings.DASHBOARD_PASSWORD

    token: str | None = None
    if master and pwd == master:
        token = _MASTER_TOKEN
    else:
        for i, a in enumerate(_load_accesses(db)):
            if hmac.compare_digest(a["password"], pwd):
                token = f"idx:{i}"
                break

    if token is None:
        return RedirectResponse("/dashboard/login?err=1", status_code=302)

    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie(AUTH_COOKIE, _sign_token(token), httponly=True, max_age=86400 * 7)
    return resp


@router.get("/dashboard/logout", include_in_schema=False)
def logout():
    resp = RedirectResponse("/dashboard/login", status_code=302)
    resp.delete_cookie(AUTH_COOKIE)
    return resp


# ── Acessos (admin-only) ─────────────────────────────────────────────────────

def _all_known_agents(db: Session) -> list[str]:
    """Return a sorted list of known agent names (from AGENT_SEGMENT + mappings)."""
    known = set(AGENT_SEGMENT.keys())
    try:
        mappings = get_agent_mappings(db)
        for ag in mappings.values():
            if ag and ag != "Sem atendente":
                known.add(ag)
    except Exception:
        pass
    return sorted(known)


@router.get("/dashboard/acessos", response_class=HTMLResponse, include_in_schema=False)
def dashboard_acessos(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()
    if access.get("role") != "admin":
        return HTMLResponse("<h3 style='color:#e8ecf1;font-family:sans-serif;padding:40px'>Apenas administradores podem gerenciar acessos.</h3>", status_code=403)

    accesses = _load_accesses(db)
    # Never expose the master password in the UI. If it somehow got saved
    # as a viewer entry, auto-clean it from the DB so it stops appearing.
    master = settings.DASHBOARD_PASSWORD or ""
    if master:
        filtered = [a for a in accesses if a.get("password") != master]
        if len(filtered) != len(accesses):
            _save_accesses(db, filtered)
            accesses = filtered
    agents = _all_known_agents(db)
    msg = request.query_params.get("msg", "")

    # Build a data-json blob for the JS editor
    data_json = json.dumps(accesses, ensure_ascii=False)
    agents_json = json.dumps(agents, ensure_ascii=False)

    msg_html = ""
    if msg == "saved":
        msg_html = '<div style="background:rgba(15,169,104,.15);border:1px solid #0fa968;color:#0fa968;padding:10px 14px;border-radius:8px;margin-bottom:16px;font-size:12px;font-weight:600">Acessos salvos com sucesso.</div>'
    elif msg == "error":
        msg_html = '<div style="background:rgba(239,68,68,.15);border:1px solid #ef4444;color:#fca5a5;padding:10px 14px;border-radius:8px;margin-bottom:16px;font-size:12px;font-weight:600">Erro ao salvar. Verifique se todas as senhas estão preenchidas.</div>'

    nav = _nav_html("acessos", is_admin=True, title="Acessos")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Acessos</title>{COMMON_CSS}
    <style>
    .acc-row {{ background:#0a0f1a; border:1px solid #1a2540; border-radius:10px; padding:14px 16px; margin-bottom:12px; }}
    .acc-row label {{ font-size:10px; color:#5a6a8a; font-weight:700; letter-spacing:.3px; display:block; margin-bottom:4px; text-transform:uppercase }}
    .acc-row input[type=text], .acc-row select {{ background:#0f1629; color:#e8ecf1; border:1px solid #1a2540; padding:8px 10px; border-radius:6px; font-family:inherit; font-size:13px; width:100%; box-sizing:border-box }}
    .agents-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(200px,1fr)); gap:6px 12px; max-height:260px; overflow-y:auto; padding:10px; background:#0f1629; border:1px solid #1a2540; border-radius:6px }}
    .agents-grid label {{ display:flex; align-items:center; gap:6px; font-size:12px; color:#e8ecf1; font-weight:500; text-transform:none; letter-spacing:0; margin:0; cursor:pointer }}
    .agents-grid input {{ margin:0; accent-color:#0fa968 }}
    .btn-add {{ background:#0fa968; color:#fff; border:none; border-radius:6px; padding:8px 16px; font-size:12px; font-weight:700; cursor:pointer }}
    .btn-del {{ background:transparent; color:#ef4444; border:1px solid #ef4444; border-radius:6px; padding:4px 12px; font-size:11px; font-weight:600; cursor:pointer }}
    .btn-save {{ background:#0fa968; color:#fff; border:none; border-radius:8px; padding:12px 24px; font-size:13px; font-weight:700; cursor:pointer }}
    .role-badge-admin {{ background:rgba(168,85,247,.2); color:#c4b5fd; padding:2px 8px; border-radius:4px; font-size:10px; font-weight:700 }}
    .role-badge-viewer {{ background:rgba(15,169,104,.2); color:#6ee7b7; padding:2px 8px; border-radius:4px; font-size:10px; font-weight:700 }}
    </style>
    </head><body>
    {nav}
    <div class="container">
        <div class="kpi-row">
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{len(accesses)}</div><div class="label">Acessos configurados</div></div>
            <div class="kpi" style="border-top:3px solid #8b5cf6"><div class="val" style="color:#8b5cf6">{sum(1 for a in accesses if a.get('role')=='admin')}</div><div class="label">Administradores</div></div>
            <div class="kpi" style="border-top:3px solid #4a9eff"><div class="val" style="color:#4a9eff">{sum(1 for a in accesses if a.get('role')!='admin')}</div><div class="label">Viewers</div></div>
        </div>
        <div class="card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Gerenciar Acessos</h2>
            </div>
            <p style="font-size:12px;color:#5a6a8a;margin-bottom:20px;font-weight:500">
                Cada senha configurada abaixo libera um conjunto específico de agentes.<br>
                A senha master (do Railway) continua funcionando e vê <strong>todos</strong> os agentes.
            </p>
            {msg_html}
            <form method="post" id="acc-form">
                <input type="hidden" name="data" id="acc-data">
                <div id="acc-list"></div>
                <div style="margin-top:12px;display:flex;gap:12px;align-items:center">
                    <button type="button" class="btn-add" onclick="addRow()">+ Adicionar acesso</button>
                    <button type="submit" class="btn-save" onclick="prepareSubmit()">Salvar</button>
                </div>
            </form>
        </div>
    </div>
    <script>
    var KNOWN_AGENTS = {agents_json};
    var accesses = {data_json};

    function render() {{
        var list = document.getElementById('acc-list');
        list.innerHTML = '';
        if (accesses.length === 0) {{
            list.innerHTML = '<p style="color:#4a5a7a;font-size:12px;padding:20px;text-align:center">Nenhum acesso cadastrado. Clique em "+ Adicionar acesso" para criar.</p>';
            return;
        }}
        accesses.forEach(function(a, i) {{
            var row = document.createElement('div');
            row.className = 'acc-row';
            var agentsHtml = KNOWN_AGENTS.map(function(name) {{
                var checked = a.agents && a.agents.indexOf(name) !== -1 ? 'checked' : '';
                var escName = name.replace(/"/g, '&quot;');
                return '<label><input type="checkbox" data-idx="' + i + '" data-name="' + escName + '" ' + checked + ' onchange="toggleAgent(' + i + ', this.dataset.name, this.checked)">' + escName + '</label>';
            }}).join('');
            var role = a.role || 'viewer';
            var roleDisabled = role === 'admin' ? 'style="opacity:.5;pointer-events:none"' : '';
            var pwId = 'pw-' + i;
            row.innerHTML =
                '<div style="display:grid;grid-template-columns:1fr 160px 100px;gap:14px;margin-bottom:12px">' +
                  '<div><label>Senha</label>' +
                    '<div style="position:relative">' +
                      '<input id="' + pwId + '" type="password" value="' + (a.password || '').replace(/"/g,'&quot;') + '" oninput="accesses[' + i + '].password=this.value" style="padding-right:70px">' +
                      '<button type="button" onclick="togglePw(\\\'' + pwId + '\\\', this)" style="position:absolute;right:6px;top:50%;transform:translateY(-50%);background:transparent;color:#5a6a8a;border:1px solid #1a2540;border-radius:4px;padding:3px 8px;font-size:10px;font-weight:700;cursor:pointer;font-family:inherit">MOSTRAR</button>' +
                    '</div>' +
                  '</div>' +
                  '<div><label>Papel</label><select onchange="accesses[' + i + '].role=this.value;render()">' +
                    '<option value="viewer"' + (role==='viewer'?' selected':'') + '>Viewer</option>' +
                    '<option value="admin"' + (role==='admin'?' selected':'') + '>Admin</option>' +
                  '</select></div>' +
                  '<div style="display:flex;align-items:flex-end;justify-content:flex-end"><button type="button" class="btn-del" onclick="delRow(' + i + ')">Remover</button></div>' +
                '</div>' +
                (role === 'admin'
                    ? '<p style="font-size:11px;color:#c4b5fd;margin:0">Admin vê <strong>todos</strong> os agentes e pode gerenciar acessos.</p>'
                    : '<div><label>Agentes visíveis <span style="color:#4a5a7a;text-transform:none;letter-spacing:0;font-weight:500">(' + (a.agents ? a.agents.length : 0) + ' selecionados)</span></label><div class="agents-grid">' + agentsHtml + '</div></div>');
            list.appendChild(row);
        }});
    }}

    function addRow() {{
        accesses.push({{password: '', role: 'viewer', agents: []}});
        render();
    }}
    function delRow(i) {{
        if (!confirm('Remover este acesso?')) return;
        accesses.splice(i, 1);
        render();
    }}
    function toggleAgent(i, name, checked) {{
        if (!accesses[i].agents) accesses[i].agents = [];
        var idx = accesses[i].agents.indexOf(name);
        if (checked && idx === -1) accesses[i].agents.push(name);
        if (!checked && idx !== -1) accesses[i].agents.splice(idx, 1);
    }}
    function prepareSubmit() {{
        document.getElementById('acc-data').value = JSON.stringify(accesses);
    }}
    function togglePw(id, btn) {{
        var el = document.getElementById(id);
        if (!el) return;
        if (el.type === 'password') {{ el.type = 'text'; btn.textContent = 'OCULTAR'; }}
        else {{ el.type = 'password'; btn.textContent = 'MOSTRAR'; }}
    }}
    render();
    </script>
    </body></html>""")


@router.post("/dashboard/acessos", include_in_schema=False)
async def dashboard_acessos_save(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()
    if access.get("role") != "admin":
        return HTMLResponse("Forbidden", status_code=403)
    form = await request.form()
    raw = form.get("data", "")
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("not a list")
        master = settings.DASHBOARD_PASSWORD or ""
        # Validate: every entry needs a non-empty password
        # Silently drop any entry whose password matches the master password
        cleaned = []
        for item in data:
            if not isinstance(item, dict):
                return RedirectResponse("/dashboard/acessos?msg=error", status_code=302)
            pw = (item.get("password") or "").strip()
            if not pw:
                return RedirectResponse("/dashboard/acessos?msg=error", status_code=302)
            if master and pw == master:
                # Refuse to save the master password as a viewer entry
                continue
            cleaned.append(item)
        _save_accesses(db, cleaned)
    except Exception:
        return RedirectResponse("/dashboard/acessos?msg=error", status_code=302)
    return RedirectResponse("/dashboard/acessos?msg=saved", status_code=302)


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
    set_setting(db, "gabarito_updated_at", datetime.now(BRASILIA).isoformat())
    return RedirectResponse(f"/dashboard/agentes?msg=ok&count={count}", status_code=302)


# ── Acked-alerts helpers ─────────────────────────────────────────────────────

def _get_acked_alerts(db) -> dict[str, dict]:
    raw = get_setting(db, "acked_alerts")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _save_acked_alerts(db, acked: dict) -> None:
    set_setting(db, "acked_alerts", json.dumps(acked, ensure_ascii=False))


# ── Nav HTML ─────────────────────────────────────────────────────────────────

_PAGE_TITLES: dict[str, str] = {
    "conversas":  "Conversas",
    "agentes":    "Agentes",
    "mensagens":  "Mensagens Iniciais",
    "alertas":    "Alertas",
    "temas":      "Temas",
    "evolucao":   "Evolução",
    "acessos":    "Acessos",
}


def _nav_html(active: str, extra: str = "", canal: str = "", unacked_alerts: int = 0, acked_alerts: int = 0, is_admin: bool = False, title: str = "") -> str:
    canal_qs = f"?canal={canal}"

    def _ni(page_id: str, label: str, href: str, badge: str = "") -> str:
        cls = "active" if active == page_id else ""
        b = f'<span class="nav-badge">{badge}</span>' if badge else ""
        return f'<a href="{href}" class="nav-item {cls}">{label}{b}</a>'

    unacked_b = str(unacked_alerts) if unacked_alerts > 0 else ""
    acked_b_html = (f'<span class="nav-badge-muted">{acked_alerts}</span>' if acked_alerts > 0 else "")

    admin_group = ""
    if is_admin:
        admin_group = f"""<div class="nav-group">
      <div class="nav-group-label">ADMIN</div>
      {_ni("acessos", "Acessos", "/dashboard/acessos")}
    </div>"""

    ch_options = '<option value="">Todos os canais</option>'
    for ch_num, ch_label in sorted(COMPANY_CHANNELS_MAP.items(), key=lambda x: x[1]):
        sel = "selected" if canal == ch_num else ""
        ch_options += f'<option value="{ch_num}" {sel}>{ch_label} ({ch_num[-4:]})</option>'

    page_title = title or _PAGE_TITLES.get(active, "Dashboard")

    # extra goes in topbar right (period filter for agentes, etc.)
    topbar_extra = f'<div style="display:flex;align-items:center;gap:8px">{extra}</div>' if extra else ""

    return f"""<aside class="gp-sidebar">
  <div class="sidebar-brand">
    <div style="display:flex;align-items:center;gap:10px">
      <div style="width:28px;height:28px;border-radius:6px;background:#0fa968;display:flex;align-items:center;justify-content:center;font-weight:800;color:#0b1120;font-size:14px;letter-spacing:-.5px;flex-shrink:0">AV</div>
      <div style="line-height:1.2">
        <div style="font-size:11px;letter-spacing:2px;font-weight:700;color:#fff">ALTO<span style="color:#0fa968">VALOR</span></div>
        <div style="font-size:8px;letter-spacing:1.5px;color:#5a6a8a;margin-top:2px;font-weight:600">GRAMPO</div>
      </div>
    </div>
  </div>
  <div class="nav-group">
    <div class="nav-group-label">MONITORAMENTO</div>
    {_ni("conversas", "Conversas", f"/dashboard{canal_qs}", unacked_b)}
    <a href="/dashboard/alertas{canal_qs}" class="nav-item {'active' if active == 'alertas' else ''}">Alertas{acked_b_html}</a>
    {_ni("agentes", "Agentes", f"/dashboard/agentes{canal_qs}")}
  </div>
  <div class="nav-group">
    <div class="nav-group-label">ANÁLISE</div>
    {_ni("temas", "Temas", f"/dashboard/temas{canal_qs}")}
    {_ni("evolucao", "Evolução", f"/dashboard/evolucao{canal_qs}")}
    {_ni("mensagens", "Mensagens iniciais", f"/dashboard/mensagens{canal_qs}")}
  </div>
  {admin_group}
  <div class="sidebar-footer">
    <div style="width:28px;height:28px;border-radius:50%;background:#0fa968;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800;color:#0b1120;flex-shrink:0">AV</div>
    <div style="flex:1;min-width:0;line-height:1.3">
      <div style="font-size:11px;color:#e8ecf1;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Gestor</div>
      <a href="/dashboard/logout" style="font-size:10px;color:#5a6a8a;text-decoration:none;font-weight:500">Sair</a>
    </div>
  </div>
</aside>
<div class="gp-topbar">
  <span class="topbar-title">{page_title}</span>
  <div class="topbar-right">
    {topbar_extra}
    <select id="canal-select" onchange="switchCanal(this.value)" style="background:#111a2e;color:#e8ecf1;border:1px solid #1a2540;padding:6px 12px;border-radius:8px;font-family:'Montserrat',sans-serif;font-size:11px;font-weight:600;cursor:pointer">
      {ch_options}
    </select>
    <span id="refresh-timer" style="font-size:11px;color:#5a6a8a;font-family:'JetBrains Mono',monospace;white-space:nowrap"></span>
    <button onclick="location.reload()" style="background:#111a2e;border:1px solid #1a2540;color:#c0c8d8;padding:6px 12px;border-radius:8px;font-size:11px;cursor:pointer;font-weight:600;font-family:'Montserrat',sans-serif">&#x21bb;</button>
  </div>
</div>
<script>
function switchCanal(val) {{
    var url = new URL(window.location.href);
    url.searchParams.set('canal', val);
    window.location.href = url.toString();
}}
(function(){{
    var loadTime = new Date();
    var REFRESH_SEC = 600;
    function pad(n){{ return n < 10 ? '0'+n : n; }}
    function updateTimer(){{
        var now = new Date();
        var elapsed = Math.floor((now - loadTime) / 1000);
        var remaining = Math.max(REFRESH_SEC - elapsed, 0);
        var h = pad(loadTime.getHours()), m = pad(loadTime.getMinutes());
        var el = document.getElementById('refresh-timer');
        if (el) el.textContent = 'Atualizado ' + h + ':' + m + ' · ' + Math.floor(remaining/60) + ':' + pad(remaining%60);
    }}
    updateTimer();
    setInterval(updateTimer, 1000);
}})();
</script>"""


# ── Conversations Dashboard ─────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard_main(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")
    all_events, total = get_events(db, limit=50000, offset=0)
    filtered_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    acked_alerts = _get_acked_alerts(db)   # {phone_key: {agent, snippet, display_name, acked_at}}
    db.close()  # release connection before heavy processing
    groups, phone_learned = _group_events(filtered_events, client_agent_map)

    rows_html = ""
    alerts_html = ""
    unacked_alert_count = 0
    intent_counts: dict[str, int] = defaultdict(int)
    intent_by_agent: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    convs_with_any_intent = 0
    idx = 0
    for client_num, evs in sorted(groups.items(), key=lambda x: max(e.received_at for e in x[1]), reverse=True):
        evs.sort(key=lambda e: e.received_at)
        phone = _real_phone(client_num)  # strip ##agent## suffix if present
        agent = phone_learned.get(client_num) or client_agent_map.get(phone) or phone_learned.get(phone) or "Sem atendente"
        if not _user_sees(access, agent):
            continue
        client_name = client_name_map.get(phone, "")
        badge = _segment_badge(agent)
        last_ev = evs[-1]
        ts = last_ev.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if last_ev.received_at else ""
        out_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "OUT")
        in_count = sum(1 for e in evs if _extract_direction(e.raw_payload or {}) == "IN")
        msg_count = out_count + in_count
        chat_id = f"chat_{idx}"
        idx += 1

        # Collect texts for intent classification
        conv_texts = []
        for ev in evs:
            p = ev.raw_payload or {}
            direction = _extract_direction(p)
            content = _extract_content_preview(p) or ""
            if content:
                conv_texts.append((direction, content))
        intents = _classify_conversation(conv_texts)
        intent_tags = _intent_badges(intents)
        if intents:
            convs_with_any_intent += 1
        for iid, ilabel, icolor in intents:
            intent_counts[iid] += 1
            intent_by_agent[agent][iid] += 1

        # Build alert if flagged
        has_alert = any(i[0] == "alerta" for i in intents)
        alert_snippet = ""
        if has_alert:
            display_name_raw = client_name if client_name else phone
            display_name = html_mod.escape(display_name_raw)
            # Find the offending message snippet
            _alert_kws = ["absurdo", "vergonha", "ridiculo", "ridículo", "palhaçada",
                "incompetente", "lixo", "péssimo", "pessimo", "horrível", "horrivel", "merda", "porra",
                "caralho", "puta", "fdp", "vai se foder", "idiota", "imbecil", "babaca",
                "reclamação", "reclamacao", "reclamo", "procon", "advogado",
                "fraude", "roubando", "desrespeit", "falta de respeito", "abuso", "descaso"]
            for _dir, text in conv_texts:
                lower = text.lower()
                matched = False
                for kw in _alert_kws:
                    if " " in kw:
                        if kw in lower:
                            matched = True
                            break
                    else:
                        if _re.search(r'\b' + _re.escape(kw), lower):
                            matched = True
                            break
                if matched:
                    alert_snippet = text[:120]
                    break

            # Skip if already acked — it belongs in the Alertas tab now
            if phone in acked_alerts:
                continue

            unacked_alert_count += 1
            snippet_html = f' — <em style="font-weight:400;opacity:.85">"{html_mod.escape(alert_snippet)}..."</em>' if alert_snippet else ""
            alerts_html += f'<div onclick="openAlert(\'{chat_id}\')" style="cursor:pointer;background:rgba(239,68,68,.15);border:1px solid #ef4444;color:#fca5a5;padding:10px 16px;border-radius:8px;margin-bottom:8px;font-size:12px;font-weight:600;display:flex;align-items:center;gap:8px;transition:.2s" onmouseover="this.style.background=\'rgba(239,68,68,.25)\'" onmouseout="this.style.background=\'rgba(239,68,68,.15)\'"><span style="font-size:16px">⚠</span> <span><strong>{display_name}</strong> ({agent}){snippet_html}</span></div>'

        client_display = html_mod.escape(client_name) if client_name else '<span style="color:#4a5a7a">Desconhecido</span>'

        segment = _get_segment(agent)
        safe_agent = html_mod.escape(agent)
        safe_segment = html_mod.escape(segment) if segment else ""
        safe_phone = html_mod.escape(phone)
        safe_snippet = html_mod.escape(alert_snippet).replace("'", "&#39;")
        safe_display_name = html_mod.escape(client_name if client_name else phone).replace("'", "&#39;")

        # OK button — only on alert rows, stops row toggle click
        ok_btn = ""
        if has_alert:
            ok_btn = f'''<button onclick="event.stopPropagation();ackAlert('{safe_phone}','{html_mod.escape(agent).replace("'","&#39;")}','{safe_snippet}','{safe_display_name}','{chat_id}')" style="background:#0fa968;color:#fff;border:none;border-radius:6px;padding:4px 10px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">✓ OK</button>'''

        rows_html += f"""
        <tr id="conv-row-{chat_id}" class="conv-row" data-agent="{safe_agent}" data-segment="{safe_segment}" onclick="toggleChat('{chat_id}')" {"style='background:rgba(239,68,68,.08)'" if has_alert else ""}>
            <td style="font-weight:600">{client_display}</td>
            <td style="font-family:monospace;font-size:12px;color:#4a5a7a">{safe_phone}</td>
            <td>{_short_agent_name(agent)}{badge}{intent_tags}</td>
            <td style="text-align:center"><span class="dir-out">&uarr;{out_count}</span> &nbsp;<span class="dir-in">&darr;{in_count}</span></td>
            <td style="color:#5a6a8a">{ts}</td>
            <td style="text-align:center;width:70px">{ok_btn}</td>
        </tr>
        <tr id="chat-row-{chat_id}" class="chat-row" data-agent="{safe_agent}" data-segment="{safe_segment}"><td colspan="6" style="padding:0;border:none">
            <div id="{chat_id}" class="chat-box"><div class="msg-container">"""

        for ev in evs:
            p = ev.raw_payload or {}
            ev_type = (p.get("type", "") or "").upper()
            if ev_type in ("MESSAGE_STATUS", "CONVERSATION_STATUS"):
                continue
            direction = _extract_direction(p)
            raw_content = _extract_content_preview(p) or ""
            content = html_mod.escape(raw_content) if raw_content else '<em style="opacity:.5">[mensagem sem conteúdo legível]</em>'
            msg_ts = ev.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if ev.received_at else ""
            if direction == "OUT":
                rows_html += f'<div class="msg msg-out">{content}<div class="msg-time">{msg_ts} &uarr;</div></div>'
            else:
                rows_html += f'<div class="msg msg-in">{content}<div class="msg-time">{msg_ts} &darr;</div></div>'

        rows_html += """</div></div></td></tr>"""

    # Collect unique agents and segments for filters
    all_agents_set = set()
    all_segments_set = set()
    for client_num, evs in groups.items():
        _ph = _real_phone(client_num)
        ag = phone_learned.get(client_num) or client_agent_map.get(_ph) or phone_learned.get(_ph) or "Sem atendente"
        all_agents_set.add(ag)
        seg = _get_segment(ag)
        if seg:
            all_segments_set.add(seg)
    agent_options = "".join(f'<option value="{html_mod.escape(a)}">{html_mod.escape(a)}</option>' for a in sorted(all_agents_set))
    segment_options = "".join(f'<option value="{html_mod.escape(s)}">{html_mod.escape(s)}</option>' for s in sorted(all_segments_set))

    filter_html = f"""
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
        <select id="filter-segment" onchange="applyFilters()" style="background:#0f1629;color:#e8ecf1;border:1px solid #1a2540;padding:8px 14px;border-radius:8px;font-family:'Montserrat',sans-serif;font-size:12px;font-weight:600;cursor:pointer">
            <option value="">Todos os segmentos</option>
            {segment_options}
        </select>
        <select id="filter-agent" onchange="applyFilters()" style="background:#0f1629;color:#e8ecf1;border:1px solid #1a2540;padding:8px 14px;border-radius:8px;font-family:'Montserrat',sans-serif;font-size:12px;font-weight:600;cursor:pointer">
            <option value="">Todos os agentes</option>
            {agent_options}
        </select>
        <span id="filter-count" style="font-size:11px;color:#5a6a8a;font-weight:500;margin-left:4px"></span>
    </div>"""

    # Build intent analysis panel
    intent_meta = {
        "alerta": ("⚠ Alertas", "#ef4444"),
        "reuniao": ("Reunião", "#3b82f6"),
        "resgate": ("Movimentação", "#f59e0b"),
        "duvida": ("Dúvida", "#8b5cf6"),
        "followup": ("Follow-up", "#06b6d4"),
        "geral": ("Geral", "#5a6a8a"),
    }
    # Count conversations with no intent as "geral"
    intent_counts["geral"] = max(len(groups) - convs_with_any_intent, 0)

    # KPI cards for intents
    intent_kpis = ""
    for iid, (ilabel, icolor) in intent_meta.items():
        cnt = intent_counts.get(iid, 0)
        intent_kpis += f'<div class="kpi" style="border-top:3px solid {icolor}"><div class="val" style="color:{icolor}">{cnt}</div><div class="label">{ilabel}</div></div>'

    # Breakdown by agent table
    # Get agents that have at least one intent
    agents_with_intents = sorted(intent_by_agent.keys(), key=lambda a: sum(intent_by_agent[a].values()), reverse=True)
    agent_breakdown_rows = ""
    for ag in agents_with_intents:
        if ag == "Sem atendente":
            continue
        cells = ""
        row_total = 0
        for iid, (ilabel, icolor) in intent_meta.items():
            v = intent_by_agent[ag].get(iid, 0)
            row_total += v
            if v == 0:
                cells += f'<td style="text-align:center;color:#1a2540">-</td>'
            elif iid == "alerta":
                cells += f'<td style="text-align:center;color:{icolor};font-weight:700">{v}</td>'
            else:
                cells += f'<td style="text-align:center;color:{icolor};font-weight:600">{v}</td>'
        seg = _get_segment(ag)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{seg_color};margin-right:6px;vertical-align:middle"></span>' if seg else ''
        agent_breakdown_rows += f'<tr><td style="font-size:12px;font-weight:600">{dot}{_short_agent_name(ag)}</td>{cells}<td style="text-align:center;font-weight:700;color:#e8ecf1">{row_total}</td></tr>'

    # Sem atendente row
    if "Sem atendente" in intent_by_agent:
        cells = ""
        row_total = 0
        for iid, (ilabel, icolor) in intent_meta.items():
            v = intent_by_agent["Sem atendente"].get(iid, 0)
            row_total += v
            if v == 0:
                cells += f'<td style="text-align:center;color:#1a2540">-</td>'
            else:
                cells += f'<td style="text-align:center;color:{icolor};font-weight:600">{v}</td>'
        agent_breakdown_rows += f'<tr><td style="font-size:12px;font-weight:600;color:#5a6a8a">Sem atendente</td>{cells}<td style="text-align:center;font-weight:700;color:#e8ecf1">{row_total}</td></tr>'

    intent_headers = "".join(f'<th style="text-align:center;font-size:10px;color:{icolor}">{ilabel}</th>' for iid, (ilabel, icolor) in intent_meta.items())

    analysis_html = f"""
        <div class="card" style="margin-bottom:16px">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
                <h2 style="margin:0">Analise por tipo de conversa</h2>
            </div>
            <div class="kpi-row" style="margin-bottom:16px">{intent_kpis}</div>
            <table>
                <thead><tr><th style="min-width:180px">Agente</th>{intent_headers}<th style="text-align:center">Total</th></tr></thead>
                <tbody>{agent_breakdown_rows}</tbody>
            </table>
        </div>"""

    # Totals for KPIs
    _total_out = sum(1 for ev in filtered_events if _extract_direction(ev.raw_payload or {}) == "OUT")
    _total_in  = sum(1 for ev in filtered_events if _extract_direction(ev.raw_payload or {}) == "IN")
    _active_agents = len({
        (phone_learned.get(cn) or client_agent_map.get(_real_phone(cn)) or phone_learned.get(_real_phone(cn)) or "Sem atendente")
        for cn in groups
    } - {"Sem atendente"})

    nav = _nav_html("conversas", canal=canal, unacked_alerts=unacked_alert_count, acked_alerts=len(acked_alerts), is_admin=(access or {}).get('role')=='admin', title="Conversas")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Conversas</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        <div class="kpi-row">
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{len(groups)}</div><div class="label">Conversas</div></div>
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val" style="font-size:20px">&uarr;{_total_out}&nbsp;<span style="color:#5a6a8a;font-size:14px">/</span>&nbsp;&darr;{_total_in}</div><div class="label">Msgs enviadas / recebidas</div></div>
            <div class="kpi" style="border-top:3px solid {'#ef4444' if unacked_alert_count > 0 else '#1a2540'}"><div class="val" style="color:{'#ef4444' if unacked_alert_count > 0 else '#5a6a8a'}">{unacked_alert_count}</div><div class="label">Alertas ativos</div></div>
            <div class="kpi" style="border-top:3px solid #4a9eff"><div class="val" style="color:#4a9eff">{_active_agents}</div><div class="label">Agentes com conv.</div></div>
        </div>
        {analysis_html}
        {filter_html}
        {alerts_html}
        <div class="card">
            <table>
                <thead><tr><th>Cliente</th><th>Telefone</th><th>Agente</th><th style="text-align:center">Msgs</th><th>Ultima</th><th style="width:70px"></th></tr></thead>
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
    function openAlert(chatId) {{
        document.getElementById('filter-segment').value = '';
        document.getElementById('filter-agent').value = '';
        applyFilters();
        var el = document.getElementById(chatId);
        if (!el.classList.contains('open')) {{ el.classList.add('open'); }}
        el.scrollTop = el.scrollHeight;
        el.closest('tr').previousElementSibling.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
    }}
    async function ackAlert(phoneKey, agent, snippet, displayName, chatId) {{
        try {{
            var resp = await fetch('/dashboard/ack-alert', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{phone_key: phoneKey, agent: agent, snippet: snippet, display_name: displayName}})
            }});
            if (resp.ok) {{
                var convRow = document.getElementById('conv-row-' + chatId);
                var chatRow = document.getElementById('chat-row-' + chatId);
                if (convRow) convRow.remove();
                if (chatRow) chatRow.remove();
                // Update badge
                var badge = document.getElementById('alert-badge');
                if (badge) {{
                    var n = Math.max(0, parseInt(badge.textContent || '0') - 1);
                    if (n > 0) {{ badge.textContent = n; badge.style.display = ''; }}
                    else {{ badge.style.display = 'none'; }}
                }}
                document.getElementById('filter-count').textContent = document.querySelectorAll('tr.conv-row:not([style*="display: none"])').length + ' conversas';
            }}
        }} catch(e) {{ console.error(e); }}
    }}
    function applyFilters() {{
        var seg = document.getElementById('filter-segment').value;
        var agent = document.getElementById('filter-agent').value;
        var rows = document.querySelectorAll('tr.conv-row');
        var chatRows = document.querySelectorAll('tr.chat-row');
        var visible = 0;
        rows.forEach(function(row, i) {{
            var rSeg = row.getAttribute('data-segment') || '';
            var rAgent = row.getAttribute('data-agent') || '';
            var show = true;
            if (seg && rSeg !== seg) show = false;
            if (agent && rAgent !== agent) show = false;
            row.style.display = show ? '' : 'none';
            if (chatRows[i]) {{
                chatRows[i].style.display = show ? '' : 'none';
                if (!show) {{
                    var chatBox = chatRows[i].querySelector('.chat-box');
                    if (chatBox) chatBox.classList.remove('open');
                }}
            }}
            if (show) visible++;
        }});
        document.getElementById('filter-count').textContent = visible + ' conversas';
    }}
    applyFilters();
    </script>
    </body></html>""")


# ── Alert triage endpoints ───────────────────────────────────────────────────

@router.post("/dashboard/ack-alert", include_in_schema=False)
async def ack_alert_endpoint(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return JSONResponse({"error": "unauth"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad json"}, status_code=400)
    phone_key = (body.get("phone_key") or "").strip()
    if not phone_key:
        return JSONResponse({"error": "missing phone_key"}, status_code=400)
    acked = _get_acked_alerts(db)
    acked[phone_key] = {
        "agent": body.get("agent", ""),
        "snippet": body.get("snippet", ""),
        "display_name": body.get("display_name", phone_key),
        "acked_at": datetime.now(BRASILIA).isoformat(),
    }
    _save_acked_alerts(db, acked)
    return JSONResponse({"ok": True})


@router.post("/dashboard/unack-alert", include_in_schema=False)
async def unack_alert_endpoint(request: Request, db: Session = Depends(get_db)):
    if not _check_auth(request):
        return JSONResponse({"error": "unauth"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad json"}, status_code=400)
    phone_key = (body.get("phone_key") or "").strip()
    acked = _get_acked_alerts(db)
    acked.pop(phone_key, None)
    _save_acked_alerts(db, acked)
    return JSONResponse({"ok": True})


# ── Agente Detalhe ───────────────────────────────────────────────────────────

@router.get("/dashboard/agente-detalhe", response_class=HTMLResponse, include_in_schema=False)
def agente_detalhe(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")
    agent_name = request.query_params.get("agent", "")
    _dias_raw = request.query_params.get("dias", "1")
    dias = int(_dias_raw) if _dias_raw.isdigit() and int(_dias_raw) in (1, 7, 15, 30) else 1
    if not agent_name:
        return RedirectResponse(f"/dashboard/agentes?canal={canal}", status_code=302)
    if not _user_sees(access, agent_name):
        return HTMLResponse("<h3 style='color:#e8ecf1;font-family:sans-serif;padding:40px'>Sem permissão para visualizar este agente.</h3>", status_code=403)

    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    db.close()  # release connection before heavy processing

    now_br = datetime.now(BRASILIA)
    if dias == 1:
        period_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        period_start = (now_br - timedelta(days=dias - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    period_events = [ev for ev in all_events if ev.received_at and ev.received_at.astimezone(BRASILIA) >= period_start]

    # Single pass: group all events for attribution, then filter to period
    all_groups, phone_learned = _group_events(all_events, client_agent_map)
    # For display, only keep groups that have at least one event in the period
    period_phone_set = set()
    for ev in period_events:
        p = ev.raw_payload or {}
        cn = _extract_client_number(p)
        if cn:
            period_phone_set.add(cn)
    groups = {k: v for k, v in all_groups.items()
              if any(ev.received_at and ev.received_at.astimezone(BRASILIA) >= period_start
                     for ev in v)}

    rows_html = ""
    idx = 0
    client_count = 0

    for client_num, evs in sorted(groups.items(), key=lambda x: max(e.received_at for e in x[1] if e.received_at), reverse=True):
        ph = _real_phone(client_num)
        ag = phone_learned.get(client_num) or client_agent_map.get(ph) or phone_learned.get(ph) or "Sem atendente"
        if ag.lower() != agent_name.lower():
            continue
        out_msgs = [ev for ev in evs if _extract_direction(ev.raw_payload or {}) == "OUT"]
        if not out_msgs:
            continue

        client_name = client_name_map.get(ph, "")
        client_display = html_mod.escape(client_name) if client_name else '<span style="color:#4a5a7a">Desconhecido</span>'
        in_count = len(evs) - len(out_msgs)
        last_ts = max(ev.received_at for ev in evs if ev.received_at)
        _ts_fmt = "%d/%m %H:%M" if dias > 1 else "%H:%M"
        ts = last_ts.astimezone(BRASILIA).strftime(_ts_fmt)
        chat_id = f"det_{idx}"
        idx += 1
        client_count += 1

        evs_sorted = sorted(evs, key=lambda e: e.received_at)
        msgs_html = ""
        for ev in evs_sorted:
            p = ev.raw_payload or {}
            ev_type = (p.get("type", "") or "").upper()
            if ev_type in ("MESSAGE_STATUS", "CONVERSATION_STATUS"):
                continue
            direction = _extract_direction(p)
            raw_content = _extract_content_preview(p) or ""
            content = html_mod.escape(raw_content) if raw_content else '<em style="opacity:.5">[mensagem sem conteúdo legível]</em>'
            msg_ts = ev.received_at.astimezone(BRASILIA).strftime("%d/%m %H:%M") if ev.received_at else ""
            if direction == "OUT":
                msgs_html += f'<div class="msg msg-out">{content}<div class="msg-time">{msg_ts} &uarr;</div></div>'
            else:
                msgs_html += f'<div class="msg msg-in">{content}<div class="msg-time">{msg_ts} &darr;</div></div>'

        rows_html += f"""
        <tr class="conv-row" onclick="toggleChat('{chat_id}')">
            <td style="font-weight:600">{client_display}</td>
            <td style="font-family:monospace;font-size:12px;color:#4a5a7a">{html_mod.escape(ph)}</td>
            <td style="text-align:center"><span class="dir-out">&uarr;{len(out_msgs)}</span> &nbsp;<span class="dir-in">&darr;{in_count}</span></td>
            <td style="color:#5a6a8a">{ts}</td>
        </tr>
        <tr><td colspan="4" style="padding:0;border:none">
            <div id="{chat_id}" class="chat-box"><div class="msg-container">{msgs_html}</div></div>
        </td></tr>"""

    _period_label = "Hoje" if dias == 1 else f"Últimos {dias} dias"
    if not rows_html:
        rows_html = f'<tr><td colspan="4" style="text-align:center;color:#4a5a7a;padding:32px;font-size:13px">Nenhum cliente contatado ({_period_label.lower()}) por {html_mod.escape(_short_agent_name(agent_name))}.</td></tr>'

    today_str = now_br.strftime("%d/%m/%Y")
    short_name = _short_agent_name(agent_name)
    seg = _get_segment(agent_name)
    seg_color = SEGMENT_COLORS.get(seg, "#5a6a8a")
    seg_label = f'<span style="background:{seg_color};color:#fff;font-size:10px;padding:2px 10px;border-radius:10px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;margin-left:10px;vertical-align:middle">{seg}</span>' if seg else ""

    loaded_at = now_br.strftime("%H:%M")
    _agent_qs = f"agent={_url_quote(agent_name)}&canal={canal}"
    _auto_refresh_secs = 60 if dias == 1 else 300  # só auto-refresh agressivo no modo "hoje"
    _d1_cls = "active" if dias == 1 else ""
    _d7_cls = "active" if dias == 7 else ""
    _d15_cls = "active" if dias == 15 else ""
    _d30_cls = "active" if dias == 30 else ""

    page = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>{short_name} — {_period_label}</title>{COMMON_CSS}
<script>
function toggleChat(id) {{
    var el = document.getElementById(id);
    el.classList.toggle('open');
}}
(function(){{
    var loadTime = new Date();
    var REFRESH = {_auto_refresh_secs};
    function tick(){{
        var elapsed = Math.floor((new Date() - loadTime) / 1000);
        var remaining = Math.max(REFRESH - elapsed, 0);
        var el = document.getElementById('det-timer');
        if(el) el.textContent = 'Atualizado {loaded_at} · próximo em ' + remaining + 's';
        if(elapsed >= REFRESH) location.reload();
    }}
    tick();
    setInterval(tick, 1000);
}})();
</script>
</head><body>
{_nav_html("", canal=canal, is_admin=(access or {}).get('role')=='admin', title=html_mod.escape(short_name))}
<div class="container">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap">
    <a href="/dashboard/agentes?canal={canal}" style="color:#5a6a8a;text-decoration:none;font-size:12px;font-weight:600">&larr; Voltar</a>
    <h2 style="margin:0">{html_mod.escape(short_name)}{seg_label}</h2>
    <div class="period-btns">
      <a href="?{_agent_qs}&dias=1" class="{_d1_cls}">Hoje</a>
      <a href="?{_agent_qs}&dias=7" class="{_d7_cls}">7 dias</a>
      <a href="?{_agent_qs}&dias=15" class="{_d15_cls}">15 dias</a>
      <a href="?{_agent_qs}&dias=30" class="{_d30_cls}">30 dias</a>
    </div>
    <span style="background:#0f1629;color:#0fa968;border:1px solid #1a2540;border-radius:8px;padding:4px 14px;font-size:12px;font-weight:700">{client_count} cliente{"s" if client_count != 1 else ""}</span>
    <span id="det-timer" style="color:#3a4a6a;font-size:10px;margin-left:auto"></span>
    <span style="color:#0fa968;cursor:pointer;font-size:11px;font-weight:600" onclick="location.reload()">&#x21bb; Atualizar</span>
  </div>
  <div class="card">
    <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Cliente</th><th>Telefone</th><th style="text-align:center">Msgs</th><th>Último contato</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>
</body></html>"""

    return HTMLResponse(page)


# ── Temas Dashboard ──────────────────────────────────────────────────────────

@router.get("/dashboard/temas", response_class=HTMLResponse, include_in_schema=False)
def dashboard_temas(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")
    _dias_raw = request.query_params.get("dias", "7")
    dias = int(_dias_raw) if _dias_raw.isdigit() and int(_dias_raw) in (1, 7, 15, 30) else 7

    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    db.close()  # release connection before heavy processing

    now_br = datetime.now(BRASILIA)
    if dias == 1:
        period_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        period_start = (now_br - timedelta(days=dias - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    period_events = [ev for ev in all_events if ev.received_at and ev.received_at.astimezone(BRASILIA) >= period_start]
    groups, phone_learned = _group_events(period_events, client_agent_map)

    # ── Build topic × agent matrix ───────────────────────────────────────────
    # topic_data[topic_id][agent] = set of client phones that mentioned the topic
    topic_data: dict[str, dict[str, set]] = {tid: defaultdict(set) for tid, *_ in TOPIC_RULES}
    # Also store sample clients for drill-down: topic_clients[topic_id] = [(phone, name, agent, snippet)]
    topic_clients: dict[str, list] = {tid: [] for tid, *_ in TOPIC_RULES}
    _seen_topic_client: dict[str, set] = {tid: set() for tid, *_ in TOPIC_RULES}

    for client_num, evs in groups.items():
        ph = _real_phone(client_num)
        agent = phone_learned.get(client_num) or client_agent_map.get(ph) or phone_learned.get(ph) or "Sem atendente"
        if not _user_sees(access, agent):
            continue
        # Collect all message texts for this conversation
        full_text = " ".join(
            (_extract_content_preview(ev.raw_payload or {}) or "")
            for ev in evs
        ).lower()
        if not full_text.strip():
            continue
        for tid, tlabel, tcolor, keywords in TOPIC_RULES:
            matched_kw = None
            for kw in keywords:
                if kw in full_text:
                    matched_kw = kw
                    break
            if matched_kw and ph not in _seen_topic_client[tid]:
                topic_data[tid][agent].add(ph)
                _seen_topic_client[tid].add(ph)
                name = client_name_map.get(ph, "")
                topic_clients[tid].append({
                    "phone": ph, "name": name, "agent": agent, "kw": matched_kw
                })

    # ── Known agents list ────────────────────────────────────────────────────
    known_agents = [a for a in AGENT_SEGMENT]
    # Also include any agents found via phone_learned not in AGENT_SEGMENT
    extra_agents = set()
    for tid in topic_data:
        for ag in topic_data[tid]:
            if ag not in AGENT_SEGMENT and ag != "Sem atendente":
                extra_agents.add(ag)
    all_agents = known_agents + sorted(extra_agents)

    # Filter out agents with zero activity across all topics
    active_agents = [ag for ag in all_agents if any(topic_data[tid].get(ag) for tid in topic_data)]

    # ── Build HTML table (rows = agentes, colunas = temas) ───────────────────
    active_topics = [(tid, tlabel, tcolor, kws) for tid, tlabel, tcolor, kws in TOPIC_RULES if _seen_topic_client[tid]]

    # Global max for color intensity
    _mx = max((len(_seen_topic_client[tid]) for tid, *_ in active_topics), default=1)

    topic_headers = "".join(
        f'<th style="text-align:center;min-width:90px;font-size:10px;padding:6px 4px;white-space:nowrap">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{tcolor};margin-right:4px;vertical-align:middle"></span>'
        f'{tlabel}</th>'
        for tid, tlabel, tcolor, _ in active_topics
    )
    topic_headers += '<th style="text-align:center;min-width:60px;font-size:10px;padding:6px 4px;color:#0fa968">TOTAL</th>'

    rows_html = ""
    for ag in active_agents:
        seg = _get_segment(ag)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{seg_color};margin-right:6px;vertical-align:middle"></span>'
        cells = ""
        row_total = 0
        for tid, tlabel, tcolor, keywords in active_topics:
            v = len(topic_data[tid].get(ag, set()))
            row_total += v
            if v == 0:
                cells += '<td style="text-align:center;color:#1a2540;font-size:13px;border:1px solid #0f1629">—</td>'
            else:
                intensity = min(v / max(_mx, 1), 1.0)
                _r = int(10 + intensity * 5)
                _g = int(40 + intensity * 129)
                _b = int(20 + intensity * 84)
                _a = 0.25 + intensity * 0.65
                cell_bg = f"rgba({_r},{_g},{_b},{_a:.2f})"
                # Build drill-down clients for this agent+topic
                clients_here = [c for c in topic_clients[tid] if c["agent"].lower() == ag.lower()]
                tip = ", ".join(c["name"] or c["phone"] for c in clients_here[:8])
                cells += f'<td style="text-align:center;background:{cell_bg};font-size:13px;font-weight:700;color:#fff;border:1px solid #0f1629;cursor:default" title="{html_mod.escape(tip)}">{v}</td>'
        cells += f'<td style="text-align:center;font-weight:800;font-size:14px;color:#0fa968;background:#0f1629;border:1px solid #1a2540">{row_total}</td>'

        drill_id = f"drill_ag_{ag.replace(' ','_').replace('/','_')}"
        rows_html += f"""
        <tr style="cursor:pointer" onclick="toggleDrill('{drill_id}')">
            <td style="border-left:3px solid {seg_color};padding-left:10px;white-space:nowrap;font-size:12px;font-weight:600;background:#0b1120">{dot}{_short_agent_name(ag)}</td>
            {cells}
        </tr>
        <tr id="{drill_id}" style="display:none">
            <td colspan="{1 + len(active_topics) + 1}" style="padding:0;border:none;background:#0a0f1a">
                <div style="padding:12px 20px">
                    <div style="font-size:11px;color:#5a6a8a;margin-bottom:10px;font-weight:600">TEMAS — {html_mod.escape(_short_agent_name(ag)).upper()}</div>
                    <div style="display:flex;flex-wrap:wrap;gap:10px">
                        {"".join(
                            f'<div style="background:#111a2e;border:1px solid #1a2540;border-radius:8px;padding:8px 14px;min-width:140px">'
                            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px">'
                            f'<span style="width:8px;height:8px;border-radius:50%;background:{tcolor};display:inline-block"></span>'
                            f'<span style="font-size:11px;font-weight:700;color:#e8ecf1">{tlabel}</span>'
                            f'</div>'
                            f'<div style="display:flex;flex-direction:column;gap:2px">'
                            + "".join(
                                f'<span style="font-size:11px;color:#0fa968">{html_mod.escape(c["name"] or c["phone"])}'
                                f'<span style="color:#3a4a6a;font-size:9px"> · {html_mod.escape(c["kw"])}</span></span>'
                                for c in topic_clients[tid] if c["agent"].lower() == ag.lower()
                            ) +
                            f'</div></div>'
                            for tid, tlabel, tcolor, _ in active_topics
                            if any(c["agent"].lower() == ag.lower() for c in topic_clients[tid])
                        )}
                    </div>
                </div>
            </td>
        </tr>"""

    if not rows_html:
        rows_html = '<tr><td colspan="20" style="text-align:center;color:#4a5a7a;padding:40px">Nenhum tema identificado no período.</td></tr>'
    else:
        # ── Total row (sum per topic column) ──────────────────────────────────
        total_cells = ""
        grand_total = 0
        for tid, tlabel, tcolor, _ in active_topics:
            v = len(_seen_topic_client[tid])
            grand_total += v
            intensity = min(v / max(_mx, 1), 1.0)
            _r = int(10 + intensity * 5)
            _g = int(40 + intensity * 129)
            _b = int(20 + intensity * 84)
            _a = 0.3 + intensity * 0.7
            cell_bg = f"rgba({_r},{_g},{_b},{_a:.2f})"
            total_cells += f'<td style="text-align:center;background:{cell_bg};font-size:14px;font-weight:800;color:#fff;border:1px solid #0f1629">{v}</td>'
        total_cells += f'<td style="text-align:center;font-weight:800;font-size:15px;color:#0fa968;background:#0f1629;border:1px solid #1a2540">{grand_total}</td>'
        rows_html += f"""
        <tr style="border-top:2px solid #1a2540">
            <td style="padding:10px 12px;font-size:12px;font-weight:800;color:#e8ecf1;background:#0b1120;letter-spacing:.5px;text-transform:uppercase">TOTAL</td>
            {total_cells}
        </tr>"""

    # Period buttons
    _base_qs = f"canal={canal}"
    _d1_cls = "active" if dias == 1 else ""
    _d7_cls = "active" if dias == 7 else ""
    _d15_cls = "active" if dias == 15 else ""
    _d30_cls = "active" if dias == 30 else ""
    _period_label = "Hoje" if dias == 1 else f"Últimos {dias} dias"

    # ── Chart data (sorted by count desc) ────────────────────────────────────
    chart_data = sorted(
        [(tlabel, len(_seen_topic_client[tid]), tcolor) for tid, tlabel, tcolor, _ in TOPIC_RULES if _seen_topic_client[tid]],
        key=lambda x: x[1], reverse=True
    )
    _chart_labels = json.dumps([d[0] for d in chart_data])
    _chart_values = json.dumps([d[1] for d in chart_data])
    _chart_colors = json.dumps([d[2] for d in chart_data])
    _chart_h = max(180, len(chart_data) * 36)

    page = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Temas — Alto Valor</title>{COMMON_CSS}
<style>
  .temas-scroll {{ overflow-x: auto; }}
  .temas-scroll::-webkit-scrollbar {{ height: 4px; }}
  .temas-scroll::-webkit-scrollbar-track {{ background: #0b1120; border-radius: 2px; }}
  .temas-scroll::-webkit-scrollbar-thumb {{ background: #1a2540; border-radius: 2px; }}
  .temas-scroll::-webkit-scrollbar-thumb:hover {{ background: #0fa968; }}
</style>
<script>
function toggleDrill(id) {{
    var el = document.getElementById(id);
    el.style.display = (el.style.display === 'none' || el.style.display === '') ? 'table-row' : 'none';
}}
</script>
</head><body>
{_nav_html("temas", canal=canal, is_admin=(access or {}).get('role')=='admin', title="Temas")}
<div class="container">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
    <div class="period-btns">
      <a href="?{_base_qs}&dias=1" class="{_d1_cls}">Hoje</a>
      <a href="?{_base_qs}&dias=7" class="{_d7_cls}">7 dias</a>
      <a href="?{_base_qs}&dias=15" class="{_d15_cls}">15 dias</a>
      <a href="?{_base_qs}&dias=30" class="{_d30_cls}">30 dias</a>
    </div>
    <span style="color:#4a5a7a;font-size:12px">{_period_label}</span>
  </div>

  <div class="kpi-row">
    <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{sum(d[1] for d in chart_data)}</div><div class="label">Ocorrências totais</div></div>
    <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{len(chart_data)}</div><div class="label">Temas com dados</div></div>
    <div class="kpi" style="border-top:3px solid {'#d4af37' if chart_data else '#1a2540'}"><div class="val" style="font-size:14px;color:#d4af37">{chart_data[0][0] if chart_data else '—'}</div><div class="label">Tema mais frequente</div></div>
  </div>

  <!-- Bar chart -->
  <div class="card" style="margin-bottom:20px">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
      <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
      <h2 style="margin:0;font-size:15px">Ocorrências por tema</h2>
      <span style="font-size:11px;color:#5a6a8a">clientes únicos · {_period_label.lower()}</span>
    </div>
    <canvas id="temas-chart" height="{_chart_h}" style="width:100%;display:block"></canvas>
    <script>
    (function(){{
      var labels = {_chart_labels};
      var values = {_chart_values};
      var colors = {_chart_colors};
      var canvas = document.getElementById('temas-chart');
      var W = canvas.parentElement.offsetWidth - 32;
      canvas.width = W;
      var H = canvas.height;
      var ctx = canvas.getContext('2d');
      var padL = 180, padR = 50, padT = 10, padB = 10;
      var plotW = W - padL - padR;
      var n = labels.length;
      var barH = Math.floor((H - padT - padB) / n);
      var gap = Math.max(4, Math.floor(barH * 0.25));
      var bH = barH - gap;
      var maxVal = Math.max.apply(null, values) || 1;
      ctx.clearRect(0, 0, W, H);
      // Grid lines
      var steps = Math.min(maxVal, 6);
      ctx.strokeStyle = '#1a2540';
      ctx.lineWidth = 1;
      for (var s = 0; s <= steps; s++) {{
        var gx = padL + Math.round(s / steps * plotW);
        ctx.beginPath(); ctx.moveTo(gx, padT); ctx.lineTo(gx, H - padB); ctx.stroke();
        if (s > 0) {{
          ctx.fillStyle = '#3a4a6a';
          ctx.font = '10px Montserrat,sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText(Math.round(s / steps * maxVal), gx, padT + 9);
        }}
      }}
      for (var i = 0; i < n; i++) {{
        var y = padT + i * barH + gap / 2;
        var bw = Math.round(values[i] / maxVal * plotW);
        // Bar
        ctx.fillStyle = colors[i];
        ctx.globalAlpha = 0.85;
        ctx.beginPath();
        var radius = 4;
        ctx.moveTo(padL + radius, y);
        ctx.lineTo(padL + bw - radius, y);
        ctx.quadraticCurveTo(padL + bw, y, padL + bw, y + radius);
        ctx.lineTo(padL + bw, y + bH - radius);
        ctx.quadraticCurveTo(padL + bw, y + bH, padL + bw - radius, y + bH);
        ctx.lineTo(padL + radius, y + bH);
        ctx.quadraticCurveTo(padL, y + bH, padL, y + bH - radius);
        ctx.lineTo(padL, y + radius);
        ctx.quadraticCurveTo(padL, y, padL + radius, y);
        ctx.closePath();
        ctx.fill();
        ctx.globalAlpha = 1.0;
        // Label (left)
        ctx.fillStyle = '#c0c8d8';
        ctx.font = '11px Montserrat,sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText(labels[i], padL - 8, y + bH / 2 + 4);
        // Value (inside or right of bar)
        if (values[i] > 0) {{
          ctx.fillStyle = bw > 30 ? '#fff' : '#c0c8d8';
          ctx.font = 'bold 12px Montserrat,sans-serif';
          ctx.textAlign = bw > 30 ? 'right' : 'left';
          ctx.fillText(values[i], bw > 30 ? padL + bw - 8 : padL + bw + 6, y + bH / 2 + 4);
        }}
      }}
    }})();
    </script>
  </div>

  <!-- Table -->
  <div class="card">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
      <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
      <h2 style="margin:0;font-size:15px">Heatmap — Temas × Agentes</h2>
      <span style="font-size:11px;color:#5a6a8a">clientes únicos por tema</span>
    </div>
    <p style="font-size:11px;color:#4a5a7a;margin-bottom:16px">
      Clique em uma linha para ver os clientes por tema.
    </p>
    <div class="temas-scroll">
      <table>
        <thead>
          <tr>
            <th style="min-width:180px">Agente</th>
            {topic_headers}
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>
</body></html>"""

    return HTMLResponse(page)


# ── Alertas Dashboard ─────────────────────────────────────────────────────────

@router.get("/dashboard/debug/agent-clients", include_in_schema=False)
def debug_agent_clients(request: Request, db: Session = Depends(get_db)):
    """Temporary debug: show which client phones are counted per agent today."""
    if not _check_auth(request):
        return JSONResponse({"error": "unauth"}, status_code=401)
    import re as _re2
    canal = request.query_params.get("canal", "5519997733651")
    agent_filter = request.query_params.get("agent", "").lower()
    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    now_br = datetime.now(BRASILIA)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    today_events = [ev for ev in all_events if ev.received_at and ev.received_at.astimezone(BRASILIA) >= today_start]
    groups, phone_learned = _group_events(today_events, client_agent_map)
    result = {}
    for client_num, evs in groups.items():
        ph = _real_phone(client_num)
        ag = phone_learned.get(client_num) or client_agent_map.get(ph) or phone_learned.get(ph) or "Sem atendente"
        if agent_filter and agent_filter not in ag.lower():
            continue
        out_msgs = [ev for ev in evs if _extract_direction(ev.raw_payload or {}) == "OUT"]
        if not out_msgs:
            continue
        name = client_name_map.get(ph, "")
        if ag not in result:
            result[ag] = []
        result[ag].append({"phone": ph, "name": name, "out": len(out_msgs), "in": len(evs) - len(out_msgs)})
    # Sort agents and clients
    out = {ag: sorted(cs, key=lambda x: x["phone"]) for ag, cs in sorted(result.items())}
    counts = {ag: len(cs) for ag, cs in out.items()}
    return JSONResponse({"date": today_start.strftime("%d/%m/%Y"), "counts": counts, "clients": out})


@router.get("/dashboard/alertas", response_class=HTMLResponse, include_in_schema=False)
def dashboard_alertas(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")
    acked = _get_acked_alerts(db)
    client_name_map = get_client_names(db)

    # Filter out alerts for agents the user can't see
    acked = {k: v for k, v in acked.items() if _user_sees(access, v.get("agent", ""))}

    rows_html = ""
    if not acked:
        rows_html = '<tr><td colspan="5" style="text-align:center;color:#4a5a7a;padding:32px;font-size:13px">Nenhum alerta revisado ainda. Clique em <strong>✓ OK</strong> em uma conversa para movê-la aqui.</td></tr>'
    else:
        for phone_key, info in sorted(acked.items(), key=lambda x: x[1].get("acked_at", ""), reverse=True):
            agent = html_mod.escape(info.get("agent", ""))
            snippet = html_mod.escape(info.get("snippet", ""))
            display_raw = info.get("display_name") or client_name_map.get(phone_key, phone_key)
            display = html_mod.escape(display_raw)
            acked_at_raw = info.get("acked_at", "")
            try:
                acked_dt = datetime.fromisoformat(acked_at_raw)
                acked_str = acked_dt.strftime("%d/%m %H:%M")
            except Exception:
                acked_str = acked_at_raw[:16]
            badge = _segment_badge(agent)
            safe_phone = html_mod.escape(phone_key)
            rows_html += f"""<tr>
                <td style="font-weight:600">{display}</td>
                <td style="font-family:monospace;font-size:12px;color:#4a5a7a">{safe_phone}</td>
                <td>{agent}{badge}</td>
                <td style="color:#5a6a8a;max-width:340px;font-size:11px;font-style:italic">"{snippet}..."</td>
                <td style="color:#5a6a8a;font-size:11px">{acked_str}</td>
                <td style="text-align:center">
                    <button onclick="unackAlert('{safe_phone}', this)" style="background:transparent;color:#ef4444;border:1px solid #ef4444;border-radius:6px;padding:3px 10px;font-size:11px;font-weight:600;cursor:pointer">Reabrir</button>
                </td>
            </tr>"""

    nav = _nav_html("alertas", canal=canal, acked_alerts=len(acked), is_admin=(access or {}).get('role')=='admin', title="Alertas")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Alertas</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        <div class="kpi-row">
            <div class="kpi" style="border-top:3px solid #ef4444"><div class="val" style="color:#ef4444">0</div><div class="label">Ativos · aguardando triagem</div></div>
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{len(acked)}</div><div class="label">Revisados · marcados OK</div></div>
            <div class="kpi" style="border-top:3px solid #1a2540"><div class="val" style="color:#5a6a8a">{len(acked)}</div><div class="label">Total disparado</div></div>
        </div>
        <div class="card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Revisados</h2>
                <span style="font-size:12px;color:#5a6a8a;font-weight:500">{len(acked)} conversas marcadas como OK</span>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:16px;font-weight:500">
                Clique em <strong style="color:#ef4444">Reabrir</strong> para devolver à aba Conversas.
            </p>
            <table>
                <thead><tr><th>Cliente</th><th>Telefone</th><th>Agente</th><th>Trecho do alerta</th><th>Revisado em</th><th style="width:80px"></th></tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>
    <script>
    async function unackAlert(phoneKey, btn) {{
        try {{
            var resp = await fetch('/dashboard/unack-alert', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{phone_key: phoneKey}})
            }});
            if (resp.ok) {{
                var row = btn.closest('tr');
                if (row) row.remove();
            }}
        }} catch(e) {{ console.error(e); }}
    }}
    </script>
    </body></html>""")


# ── Agents Dashboard ─────────────────────────────────────────────────────────

@router.get("/dashboard/agentes", response_class=HTMLResponse, include_in_schema=False)
def dashboard_agentes(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    periodo = request.query_params.get("periodo", "hoje")
    if periodo not in ("hoje", "7dias", "custom"):
        periodo = "hoje"
    inicio_raw = request.query_params.get("inicio", "")
    fim_raw = request.query_params.get("fim", "")
    segmento = request.query_params.get("segmento", "")
    canal = request.query_params.get("canal", "5519997733651")
    msg = request.query_params.get("msg", "")
    count = request.query_params.get("count", "")

    now_br = datetime.now(BRASILIA)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)

    if periodo == "hoje":
        cutoff = today_start
        cutoff_end = today_start + timedelta(days=1)
    elif periodo == "7dias":
        cutoff = today_start - timedelta(days=6)
        cutoff_end = today_start + timedelta(days=1)
    else:  # custom
        try:
            cutoff = datetime.strptime(inicio_raw, "%Y-%m-%d").replace(tzinfo=BRASILIA)
            _end = datetime.strptime(fim_raw, "%Y-%m-%d").replace(tzinfo=BRASILIA)
            cutoff_end = _end + timedelta(days=1)
            if cutoff_end <= cutoff:
                raise ValueError("invalid range")
        except ValueError:
            periodo = "hoje"
            cutoff = today_start
            cutoff_end = today_start + timedelta(days=1)
            inicio_raw = ""
            fim_raw = ""

    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    gabarito_ts_raw = get_setting(db, "gabarito_updated_at")  # fetch before closing session
    db.close()  # release connection before heavy processing

    # Daily heatmap data — one column per day in the active period
    _date_key_fmt = "%d/%m"
    _n_days = (cutoff_end.date() - cutoff.date()).days
    period_dates = [(cutoff.date() + timedelta(days=i)) for i in range(_n_days)]
    period_date_set = {d.strftime(_date_key_fmt) for d in period_dates}

    events = [ev for ev in all_events if ev.received_at and cutoff <= ev.received_at.astimezone(BRASILIA) < cutoff_end]

    groups, phone_learned = _group_events(events, client_agent_map)

    # ── Daily heatmap data (built here so phone_learned is available) ──────────
    # Combined agent map: gabarito + learned from main-period grouping
    _combined_agent_map = dict(client_agent_map)
    for _k, _v in phone_learned.items():
        _rk = _real_phone(_k)
        if _rk and _rk not in _combined_agent_map:
            _combined_agent_map[_rk] = _v
    date_clients_period: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for _ev in events:
        if not _ev.received_at:
            continue
        _ts_br = _ev.received_at.astimezone(BRASILIA)
        _dk = _ts_br.strftime(_date_key_fmt)
        if _dk not in period_date_set:
            continue
        _p = _ev.raw_payload or {}
        _cn = _extract_client_number(_p)
        if not _cn:
            continue
        if _extract_direction(_p) != "OUT":
            continue
        _ag = _combined_agent_map.get(_cn, "") or _extract_agent_from_payload(_p) or "Sem atendente"
        if _ag == "Sem atendente":
            continue
        if not _user_sees(access, _ag):
            continue
        date_clients_period[_ag][_dk].add(_cn)
    # ──────────────────────────────────────────────────────────────────────────

    agent_stats: dict[str, dict] = defaultdict(lambda: {"out": 0, "in": 0, "clients": set(), "days_out": defaultdict(int), "days_in": defaultdict(int), "days_clients_out": defaultdict(set), "days_clients_in": defaultdict(set)})
    hourly_msgs: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    hourly_clients: dict[str, dict[int, set]] = defaultdict(lambda: defaultdict(set))
    # Track first OUT contact hour per (agent, client) so each client counts only once
    _first_out_hour: dict[str, int] = {}  # key: "agent|phone"

    for client_num, evs in groups.items():
        _ph = _real_phone(client_num)
        agent = phone_learned.get(client_num) or client_agent_map.get(_ph) or phone_learned.get(_ph) or "Sem atendente"
        if not _user_sees(access, agent):
            continue
        for ev in sorted(evs, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc)):
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
                agent_stats[agent]["days_clients_out"][day_key].add(_ph)
                if HOUR_START <= hour <= HOUR_END:
                    hourly_msgs[agent][hour] += 1
                    # Only add to hourly_clients on first OUT contact for this client
                    _fk = f"{agent}|{_ph}"
                    if _fk not in _first_out_hour:
                        _first_out_hour[_fk] = hour
                        hourly_clients[agent][hour].add(_ph)
            elif direction == "IN":
                agent_stats[agent]["in"] += 1
                agent_stats[agent]["days_in"][day_key] += 1
                agent_stats[agent]["days_clients_in"][day_key].add(_ph)
                if HOUR_START <= hour <= HOUR_END:
                    hourly_msgs[agent][hour] += 1
        agent_stats[agent]["clients"].add(_ph)

    sorted_agents = sorted(agent_stats.items(), key=lambda x: x[1]["out"] + x[1]["in"], reverse=True)

    # Filter by segment if selected
    if segmento:
        sorted_agents = [(a, s) for a, s in sorted_agents if _get_segment(a) == segmento]

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
            out_vals = [len(stats["days_clients_out"].get(d, set())) for d in sorted_days]
            in_vals = [len(stats["days_clients_in"].get(d, set())) for d in sorted_days]
            seg = _get_segment(agent)
            seg_label = f' ({seg})' if seg else ''
            total_out = sum(out_vals)
            total_in = sum(in_vals)
            seg_color_chart = SEGMENT_COLORS.get(seg, "#1a2540")
            daily_charts_html += f"""
            <div style="display:inline-block;width:340px;margin:8px;vertical-align:top;background:#0a0f1a;border:1px solid #1a2540;border-left:3px solid {seg_color_chart};border-radius:8px;padding:12px 14px">
                <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px">
                    <div style="font-size:12px;font-weight:600;color:#e8ecf1">{agent}</div>
                    <div style="font-size:10px;color:#5a6a8a;font-weight:500">{seg}</div>
                </div>
                <div style="display:flex;gap:10px;font-size:10px;margin-bottom:4px">
                    <span style="color:#ef6b73;font-weight:700">&uarr; {total_out}</span>
                    <span style="color:#0fa968;font-weight:700">&darr; {total_in}</span>
                </div>
                <canvas id="{cid}" width="310" height="150"></canvas>
                <script>
                (function(){{
                    var c=document.getElementById('{cid}'),ctx=c.getContext('2d');
                    var days={sorted_days},out={out_vals},inv={in_vals};
                    var max=Math.max(...out,...inv,1);
                    var W=310, H=150, padL=22, padR=8, padT=18, padB=22;
                    var plotW=W-padL-padR, plotH=H-padT-padB;
                    var groupW=plotW/days.length, bw=Math.max(Math.floor(groupW/2.6), 6), gap=3;
                    ctx.clearRect(0,0,W,H);
                    // Y-axis gridlines + labels (0, max/2, max)
                    ctx.strokeStyle='#141e35'; ctx.lineWidth=1;
                    ctx.fillStyle='#3a4a6a'; ctx.font='9px Montserrat,sans-serif'; ctx.textAlign='right';
                    for(var yi=0; yi<=2; yi++) {{
                        var yv = Math.round(max * (1 - yi/2));
                        var yp = padT + (plotH * yi/2);
                        ctx.beginPath(); ctx.moveTo(padL, yp); ctx.lineTo(W-padR, yp); ctx.stroke();
                        ctx.fillText(yv, padL-3, yp+3);
                    }}
                    // Bars with data labels
                    for(var i=0;i<days.length;i++){{
                        var gx = padL + i*groupW + (groupW - (bw*2+gap))/2;
                        var ho=out[i]/max*plotH, hi=inv[i]/max*plotH;
                        // OUT bar
                        ctx.fillStyle='#ef6b73'; ctx.fillRect(gx, padT+plotH-ho, bw, ho);
                        // IN bar
                        ctx.fillStyle='#0fa968'; ctx.fillRect(gx+bw+gap, padT+plotH-hi, bw, hi);
                        // Data labels on top of bars (only if value > 0)
                        ctx.font='bold 9px Montserrat,sans-serif'; ctx.textAlign='center';
                        if(out[i]>0) {{
                            ctx.fillStyle='#fca5a5';
                            ctx.fillText(out[i], gx+bw/2, padT+plotH-ho-3);
                        }}
                        if(inv[i]>0) {{
                            ctx.fillStyle='#6ee7b7';
                            ctx.fillText(inv[i], gx+bw+gap+bw/2, padT+plotH-hi-3);
                        }}
                        // Day label
                        ctx.fillStyle='#5a6a8a'; ctx.font='9px Montserrat,sans-serif'; ctx.textAlign='center';
                        ctx.fillText(days[i], gx+bw+gap/2, H-6);
                    }}
                }})();
                </script>
            </div>"""

    # Snapshot active agents (with messages) for accurate KPI counts.
    # `sorted_agents` is then expanded below to include zero-stat agents
    # so the heatmap shows a complete grid — but KPIs use this snapshot.
    active_agents_stats = list(sorted_agents)

    # Hourly heatmap
    hours = list(range(HOUR_START, HOUR_END + 1))
    hour_headers = "".join(f'<th style="text-align:center;min-width:44px;font-size:10px;padding:6px 2px">{h:02d}h</th>' for h in hours)
    hour_headers += '<th style="text-align:center;min-width:50px;font-size:10px;padding:6px 4px;color:#0fa968">TOTAL</th>'

    # Build rows for ALL known agents (from AGENT_SEGMENT), filtered by segment
    all_known_agents = [a for a in AGENT_SEGMENT if not segmento or AGENT_SEGMENT[a] == segmento]
    agents_in_stats = {a for a, _ in sorted_agents}
    for known_agent in all_known_agents:
        if known_agent not in agents_in_stats:
            sorted_agents.append((known_agent, {"out": 0, "in": 0, "clients": set(), "days_out": defaultdict(int), "days_in": defaultdict(int)}))

    def _build_heatmap_rows(data_source, client_mode=False):
        """Build heatmap rows. data_source: hourly_msgs or hourly_clients"""
        rows = ""
        mx = 1
        for agent_name in data_source:
            for h in hours:
                val = data_source[agent_name].get(h, 0) if not client_mode else len(data_source[agent_name].get(h, set()))
                if val > mx:
                    mx = val
        for agent_name, stats in sorted_agents:
            if agent_name == "Sem atendente":
                continue
            seg = _get_segment(agent_name)
            seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
            dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{seg_color};margin-right:6px;vertical-align:middle"></span>' if seg else ''
            cells = ""
            row_total = 0
            _union_set: set = set()
            for h in hours:
                if client_mode:
                    _hs = data_source[agent_name].get(h, set())
                    v = len(_hs)
                    _union_set |= _hs
                else:
                    v = data_source[agent_name].get(h, 0)
                    row_total += v
                if v == 0:
                    bg = "#0b1120"
                    txt_color = "#1a2540"
                    display = ""
                else:
                    intensity = min(v / max(mx, 1), 1.0)
                    r = int(10 + intensity * 5)
                    g = int(40 + intensity * 129)
                    b = int(20 + intensity * 84)
                    a = 0.3 + intensity * 0.7
                    bg = f"rgba({r},{g},{b},{a:.2f})"
                    txt_color = "#fff" if intensity > 0.35 else "#7dcea0"
                    display = str(v)
                cells += f'<td style="text-align:center;background:{bg};color:{txt_color};font-weight:700;font-size:15px;padding:8px 2px;border:1px solid #0f1629">{display}</td>'
            if client_mode:
                row_total = len(_union_set)
            cells += f'<td style="text-align:center;font-weight:800;font-size:15px;color:#0fa968;background:#0f1629;padding:8px 4px;border:1px solid #1a2540">{row_total}</td>'
            _det_url = f"/dashboard/agente-detalhe?agent={_url_quote(agent_name)}&canal={canal}"
            rows += f'<tr><td style="border-left:3px solid {seg_color};padding-left:10px;white-space:nowrap;font-size:12px;font-weight:600;background:#0b1120">{dot}<a href="{_det_url}" target="_blank" style="color:inherit;text-decoration:none;border-bottom:1px dotted #3a4a6a" onmouseover="this.style.color=\'#0fa968\'" onmouseout="this.style.color=\'inherit\'">{_short_agent_name(agent_name)}</a></td>{cells}</tr>'
        return rows

    hourly_rows_msgs = _build_heatmap_rows(hourly_msgs, client_mode=False)
    hourly_rows_clients = _build_heatmap_rows(hourly_clients, client_mode=True)

    # ── Daily heatmap rows (one column per specific date) ────────────────────
    _WDAY_NAMES = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    _today_date = now_br.date()

    # Global max for color intensity
    _dh_mx = 1
    for _ag_d in date_clients_period.values():
        for _dset in _ag_d.values():
            if len(_dset) > _dh_mx:
                _dh_mx = len(_dset)

    def _dh_cell(v: int, mx: int, is_today: bool = False) -> str:
        border_extra = "border-bottom:2px solid #0fa968;" if is_today else ""
        if v == 0:
            return f'<td style="text-align:center;background:#0b1120;color:#1a2540;font-weight:700;font-size:13px;padding:8px 2px;border:1px solid #0f1629;{border_extra}"></td>'
        intensity = min(v / max(mx, 1), 1.0)
        r = int(10 + intensity * 5)
        g = int(40 + intensity * 129)
        b = int(20 + intensity * 84)
        a = 0.3 + intensity * 0.7
        bg = f"rgba({r},{g},{b},{a:.2f})"
        txt = "#fff" if intensity > 0.35 else "#7dcea0"
        return f'<td style="text-align:center;background:{bg};color:{txt};font-weight:700;font-size:13px;padding:8px 2px;border:1px solid #0f1629;{border_extra}">{v}</td>'

    # Agent list: known agents filtered by segment, sorted by total
    _dh_agents = list(AGENT_SEGMENT.keys())
    if segmento:
        _dh_agents = [a for a in _dh_agents if AGENT_SEGMENT.get(a) == segmento]
    for _ag in date_clients_period:
        if _ag not in _dh_agents and (not segmento or _get_segment(_ag) == segmento):
            _dh_agents.append(_ag)
    _dh_agents.sort(key=lambda a: -sum(len(s) for s in date_clients_period.get(a, {}).values()))

    weekday_heatmap_rows = ""
    for _ag in _dh_agents:
        seg = _get_segment(_ag)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{seg_color};margin-right:6px;vertical-align:middle"></span>' if seg else ''
        cells = ""
        row_total = 0
        for _d in period_dates:
            _dk = _d.strftime(_date_key_fmt)
            v = len(date_clients_period.get(_ag, {}).get(_dk, set()))
            row_total += v
            cells += _dh_cell(v, _dh_mx, is_today=(_d == _today_date))
        cells += f'<td style="text-align:center;font-weight:800;font-size:13px;color:#0fa968;background:#0f1629;padding:8px 4px;border:1px solid #1a2540">{row_total}</td>'
        _det_url_w = f"/dashboard/agente-detalhe?agent={_url_quote(_ag)}&canal={canal}"
        weekday_heatmap_rows += f'<tr><td style="border-left:3px solid {seg_color};padding-left:10px;white-space:nowrap;font-size:12px;font-weight:600;background:#0b1120">{dot}<a href="{_det_url_w}" target="_blank" style="color:inherit;text-decoration:none;border-bottom:1px dotted #3a4a6a" onmouseover="this.style.color=\'#0fa968\'" onmouseout="this.style.color=\'inherit\'">{_short_agent_name(_ag)}</a></td>{cells}</tr>'

    # Column headers: one per specific date  (day name + date, today highlighted)
    _wday_headers = ""
    for _d in period_dates:
        _dk_lbl = _d.strftime(_date_key_fmt)   # "20/04"
        _wd_lbl = _WDAY_NAMES[_d.weekday()]     # "Seg"
        _is_today = (_d == _today_date)
        _is_weekend = _d.weekday() >= 5
        if _is_today:
            _th_style = 'text-align:center;min-width:46px;font-size:10px;padding:5px 2px;background:#0d2d1e;border-bottom:2px solid #0fa968;color:#0fa968;font-weight:800'
        elif _is_weekend:
            _th_style = 'text-align:center;min-width:46px;font-size:10px;padding:5px 2px;color:#4a5a7a'
        else:
            _th_style = 'text-align:center;min-width:46px;font-size:10px;padding:5px 2px'
        _date_small = f'<br><span style="font-size:9px;color:#e8ecf1;font-weight:400">{_dk_lbl}</span>'
        _wday_headers += f'<th style="{_th_style}">{_wd_lbl}{_date_small}</th>'
    _wday_headers += '<th style="text-align:center;min-width:50px;font-size:10px;padding:6px 4px;color:#0fa968">TOTAL</th>'

    # ── Unified period filter (Hoje / 7 dias / Personalizado) ────────────────
    canal_qs = f"&canal={canal}"  # always include so explicit "todos" (canal="") is preserved
    seg_qs = f"&segmento={segmento}" if segmento else ""

    def _periodo_link(p: str) -> str:
        return f"/dashboard/agentes?periodo={p}{canal_qs}{seg_qs}"

    _hoje_active = "active" if periodo == "hoje" else ""
    _7d_active = "active" if periodo == "7dias" else ""
    _custom_active = "active" if periodo == "custom" else ""
    _custom_inicio_val = inicio_raw if inicio_raw else (today_start - timedelta(days=6)).strftime("%Y-%m-%d")
    _custom_fim_val = fim_raw if fim_raw else today_start.strftime("%Y-%m-%d")
    _custom_panel_display = "flex" if periodo == "custom" else "none"

    period_html = f"""<div class="period-btns">
            <a href="{_periodo_link('hoje')}" class="{_hoje_active}">Hoje</a>
            <a href="{_periodo_link('7dias')}" class="{_7d_active}">7 dias</a>
            <a href="#" onclick="togglePeriodoCustom();return false" id="periodo-custom-btn" class="{_custom_active}">Personalizado</a>
        </div>
        <div id="periodo-custom-panel" style="display:{_custom_panel_display};align-items:center;gap:6px;background:#0f1629;border:1px solid #1a2540;border-radius:6px;padding:4px 8px">
            <span style="font-size:10px;color:#5a6a8a;font-weight:700">DE</span>
            <input type="date" id="periodo-inicio" value="{_custom_inicio_val}" style="background:#0b1120;color:#e8ecf1;border:1px solid #1a2540;padding:4px 8px;border-radius:4px;font-family:'Montserrat',sans-serif;font-size:11px;font-weight:600;cursor:pointer">
            <span style="font-size:10px;color:#5a6a8a;font-weight:700">ATÉ</span>
            <input type="date" id="periodo-fim" value="{_custom_fim_val}" style="background:#0b1120;color:#e8ecf1;border:1px solid #1a2540;padding:4px 8px;border-radius:4px;font-family:'Montserrat',sans-serif;font-size:11px;font-weight:600;cursor:pointer">
            <button onclick="aplicarPeriodoCustom()" style="background:#0fa968;color:#fff;border:none;border-radius:4px;padding:5px 12px;font-size:10px;font-weight:700;cursor:pointer;font-family:'Montserrat',sans-serif">APLICAR</button>
        </div>
        <script>
        function togglePeriodoCustom() {{
            var panel = document.getElementById('periodo-custom-panel');
            panel.style.display = (panel.style.display === 'none' || !panel.style.display) ? 'flex' : 'none';
        }}
        function aplicarPeriodoCustom() {{
            var i = document.getElementById('periodo-inicio').value;
            var f = document.getElementById('periodo-fim').value;
            if (!i || !f) {{ alert('Selecione data inicial e final.'); return; }}
            if (f < i) {{ alert('Data final deve ser maior ou igual à inicial.'); return; }}
            var url = new URL(window.location.href);
            url.searchParams.set('periodo', 'custom');
            url.searchParams.set('inicio', i);
            url.searchParams.set('fim', f);
            window.location.href = url.toString();
        }}
        </script>"""

    # Segment filter buttons (preserves periodo + canal)
    _periodo_qs = f"&periodo={periodo}"
    if periodo == "custom" and inicio_raw and fim_raw:
        _periodo_qs += f"&inicio={inicio_raw}&fim={fim_raw}"
    segs = [("", "Todos"), ("Alta Renda", "Alta Renda"), ("Externo", "Externo"), ("On Demand", "On Demand")]
    seg_filter_html = '<div class="period-btns" style="margin-bottom:16px;flex-wrap:wrap">'
    for val, label in segs:
        active = "active" if segmento == val else ""
        seg_link = f"/dashboard/agentes?{_periodo_qs.lstrip('&')}{canal_qs}"
        if val:
            seg_link += f"&segmento={val}"
        color_style = ""
        if val and val in SEGMENT_COLORS and segmento != val:
            color_style = f'style="border-left:3px solid {SEGMENT_COLORS[val]}"'
        seg_filter_html += f'<a href="{seg_link}" class="{active}" {color_style}>{label}</a>'
    seg_filter_html += '</div>'

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

    # Gabarito last update (fetched earlier before db.close())
    gabarito_info = ""
    gabarito_alert = ""
    if gabarito_ts_raw:
        try:
            gabarito_dt = datetime.fromisoformat(gabarito_ts_raw)
            gabarito_str = gabarito_dt.strftime("%d/%m/%Y %H:%M")
            hours_ago = (now_br - gabarito_dt).total_seconds() / 3600
            if hours_ago > 3:
                hours_int = int(hours_ago)
                gabarito_alert = f'<div style="background:#ef4444;color:#fff;padding:10px 18px;border-radius:8px;margin-bottom:16px;font-size:13px;font-weight:600;display:flex;align-items:center;gap:8px">&#9888; Gabarito desatualizado! Ultima atualizacao: {gabarito_str} ({hours_int}h atras). Atualize o gabarito.</div>'
            gabarito_info = f'<span style="color:#5a6a8a;font-size:11px;margin-left:12px">Atualizado em {gabarito_str}</span>'
        except Exception:
            pass
    else:
        gabarito_alert = '<div style="background:#ef4444;color:#fff;padding:10px 18px;border-radius:8px;margin-bottom:16px;font-size:13px;font-weight:600;display:flex;align-items:center;gap:8px">&#9888; Gabarito nunca foi enviado! Faca upload do CSV.</div>'

    _ag_active  = sum(1 for _, s in active_agents_stats if (s['out'] + s['in']) > 0)
    _ag_clients = len(set().union(*(s['clients'] for _, s in active_agents_stats)) if active_agents_stats else set())
    _ag_out     = sum(s['out'] for _, s in active_agents_stats)
    _ag_in      = sum(s['in']  for _, s in active_agents_stats)

    nav = _nav_html("agentes", period_html, canal=canal, is_admin=(access or {}).get('role')=='admin', title="Agentes")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Agentes</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        {msg_html}
        {gabarito_alert}

        <!-- KPI strip -->
        <div class="kpi-row">
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{_ag_active}</div><div class="label">Agentes ativos</div></div>
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{_ag_clients}</div><div class="label">Clientes únicos</div></div>
            <div class="kpi" style="border-top:3px solid #ef6b73"><div class="val" style="color:#ef6b73">{_ag_out}</div><div class="label">Msgs enviadas ↑</div></div>
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{_ag_in}</div><div class="label">Msgs recebidas ↓</div></div>
        </div>

        <!-- Gabarito upload (collapsible) -->
        <div class="card" style="padding:14px 22px">
            <form action="/dashboard/upload-csv" method="post" enctype="multipart/form-data" class="upload-section">
                <input type="file" name="csv_file" accept=".csv">
                <button type="submit">Atualizar Gabarito</button>
                {gabarito_info}
            </form>
            <div style="margin-top:8px;font-size:11px">
                <a href="https://app.zenvia.com/sales_contacts" target="_blank" style="color:#0fa968;text-decoration:none;font-weight:500">Baixar gabarito em app.zenvia.com/sales_contacts</a>
            </div>
        </div>

        {seg_filter_html}

        <!-- Heatmap por hora -->
        <div class="card" id="hourly-section">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Mapa de calor — Atividade por hora (06h&ndash;19h)</h2>
                <div class="period-btns" style="margin-left:0">
                    <a href="#" onclick="toggleHeatmap('msgs');return false" id="hm-btn-msgs">Mensagens</a>
                    <a href="#" class="active" onclick="toggleHeatmap('clients');return false" id="hm-btn-clients">Clientes únicos</a>
                </div>
                <button class="fs-btn" onclick="toggleFullscreen('hourly-section')" style="margin-left:auto">&#x26F6; Tela cheia</button>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500;display:none" id="hm-desc-msgs">
                Total de mensagens (IN + OUT) por agente em cada faixa horária.
            </p>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500" id="hm-desc-clients">
                Clientes únicos (telefones distintos) por agente em cada faixa horária.
            </p>
            <div style="overflow-x:auto;display:none" id="hm-table-msgs">
                <table style="min-width:700px;border-collapse:separate;border-spacing:2px">
                    <thead><tr><th style="min-width:180px;font-family:'Montserrat',sans-serif">Agente</th>{hour_headers}</tr></thead>
                    <tbody>{hourly_rows_msgs}</tbody>
                </table>
            </div>
            <div style="overflow-x:auto" id="hm-table-clients">
                <table style="min-width:700px;border-collapse:separate;border-spacing:2px">
                    <thead><tr><th style="min-width:180px;font-family:'Montserrat',sans-serif">Agente</th>{hour_headers}</tr></thead>
                    <tbody>{hourly_rows_clients}</tbody>
                </table>
            </div>
            <div style="display:flex;align-items:center;gap:8px;margin-top:14px;font-size:10px;color:#5a6a8a;font-weight:600;letter-spacing:.5px">
                <span>MENOS</span>
                <span style="width:16px;height:10px;border-radius:2px;background:#141e35;display:inline-block"></span>
                <span style="width:16px;height:10px;border-radius:2px;background:rgba(15,169,104,0.35);display:inline-block"></span>
                <span style="width:16px;height:10px;border-radius:2px;background:rgba(15,169,104,0.65);display:inline-block"></span>
                <span style="width:16px;height:10px;border-radius:2px;background:rgba(15,169,104,0.9);display:inline-block"></span>
                <span>MAIS</span>
                <span style="margin-left:16px">{seg_legend}</span>
            </div>
        </div>

        <!-- Heatmap por dia -->
        <div class="card" id="wday-section">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;flex-wrap:wrap">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Clientes únicos por dia do período</h2>
                <button class="fs-btn" onclick="toggleFullscreen('wday-section')" style="margin-left:auto">&#x26F6; Tela cheia</button>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500">
                Clientes únicos contatados (OUT) por agente em cada dia do período.
            </p>
            <div style="overflow-x:auto">
                <table style="min-width:500px;border-collapse:separate;border-spacing:2px">
                    <thead><tr><th style="min-width:180px">Agente</th>{_wday_headers}</tr></thead>
                    <tbody>{weekday_heatmap_rows}</tbody>
                </table>
            </div>
            <p style="font-size:10px;color:#3a4a6a;margin-top:10px;margin-bottom:0;font-style:italic">
                💡 Cada cliente conta <strong>1 vez por dia</strong>. Mesmo cliente em dias diferentes: aparece em cada dia, mas o total da linha deduplica.
            </p>
        </div>

        <!-- Ranking -->
        <div class="card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Ranking de Agentes</h2>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:12px;font-weight:500">
                Clientes = telefones únicos &bull; OUT = msgs enviadas &bull; IN = msgs recebidas
            </p>
            <table>
                <thead><tr><th style="width:40px">#</th><th>Agente</th><th style="text-align:center">Clientes</th><th style="text-align:center">OUT ↑</th><th style="text-align:center">IN ↓</th><th style="text-align:center">Total</th></tr></thead>
                <tbody>{ranking_html}</tbody>
            </table>
        </div>

        <!-- Daily bar charts -->
        <div class="card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Clientes únicos por dia</h2>
            </div>
            <p style="font-size:11px;color:#4a5a7a;margin-bottom:10px;font-weight:500">
                <span style="color:#ef6b73">&block;</span> Contatados (OUT) &nbsp; <span style="color:#0fa968">&block;</span> Responderam (IN)
            </p>
            <div style="overflow-x:auto">{daily_charts_html}</div>
        </div>
    </div>

    <script>
    function toggleHeatmap(mode) {{
        var show = mode === 'msgs' ? 'msgs' : 'clients';
        var hide = mode === 'msgs' ? 'clients' : 'msgs';
        document.getElementById('hm-table-' + show).style.display = '';
        document.getElementById('hm-table-' + hide).style.display = 'none';
        document.getElementById('hm-desc-' + show).style.display = '';
        document.getElementById('hm-desc-' + hide).style.display = 'none';
        document.getElementById('hm-btn-' + show).className = 'active';
        document.getElementById('hm-btn-' + hide).className = '';
    }}
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


# ── Initial Messages Dashboard ──────────────────────────────────────────────

def _normalize_template(text: str) -> str:
    """Normalize a message to identify template patterns.
    Aggressively replaces variable parts (names, products, values, dates, numbers)
    so that messages with the same structure but different fill-ins are grouped together."""
    import re
    t = text.strip()
    # Remove agent prefix *Name:* or *Name*
    t = re.sub(r'^\*[^*]+\*:?\s*', '', t)
    # Remove [TEMPLATE] / [ARQUIVO] tags
    t = re.sub(r'^\[(TEMPLATE|ARQUIVO)\]\s*', '', t)
    # Replace monetary values (R$ 1.234,56 or 1234.56)
    t = re.sub(r'R\$\s*[\d.,]+', '{valor}', t)
    t = re.sub(r'\b\d{1,3}(?:\.\d{3})*,\d{2}\b', '{valor}', t)
    # Replace dates (dd/mm, dd/mm/yyyy, dd-mm-yyyy)
    t = re.sub(r'\d{1,2}[/\-]\d{1,2}(?:[/\-]\d{2,4})?', '{data}', t)
    # Replace phone numbers
    t = re.sub(r'\(?\d{2}\)?\s*\d{4,5}[\-\s]?\d{4}', '{tel}', t)
    t = re.sub(r'\b\d{10,13}\b', '{tel}', t)
    # Replace percentages
    t = re.sub(r'\d+[,.]?\d*\s*%', '{pct}', t)
    # Replace standalone numbers (amounts, IDs, etc.)
    t = re.sub(r'\b\d{3,}\b', '{num}', t)

    # --- Name replacement ---
    # Portuguese stop words that start with uppercase at sentence start but are NOT names
    _STOP = {
        'tudo', 'bem', 'bom', 'boa', 'como', 'aqui', 'para', 'por', 'com',
        'que', 'uma', 'um', 'seu', 'sua', 'meu', 'minha', 'isso', 'este',
        'esta', 'esse', 'essa', 'mais', 'muito', 'sobre', 'entre', 'ainda',
        'conforme', 'conversado', 'anteriormente', 'entrando', 'contato',
        'entender', 'melhor', 'seus', 'objetivos', 'financeiros', 'estrategia',
        'investimentos', 'investimento', 'assessor', 'obrigado', 'obrigada',
        'perfeito', 'certo', 'combinado', 'seguem', 'segue', 'acima',
        'entro', 'poderia', 'falar', 'pouco', 'gostaria', 'preciso',
        'estou', 'estamos', 'somos', 'vamos', 'quando', 'onde', 'quem',
        'hoje', 'ontem', 'agora', 'depois', 'antes', 'tambem', 'também',
        'nosso', 'nossa', 'pode', 'favor', 'obrigado', 'obrigada',
    }

    # Known agent names → {assessor}
    for known in AGENT_SEGMENT:
        t = re.sub(re.escape(known), '{assessor}', t, flags=re.IGNORECASE)
        first = known.split()[0]
        if len(first) >= 4:
            t = re.sub(r'\b' + re.escape(first) + r'\b', '{assessor}', t, flags=re.IGNORECASE)

    # Helper: check if word looks like a proper name (not a stop word)
    def _is_name(word):
        return word[0].isupper() and word.lower() not in _STOP and len(word) >= 2

    # Greeting + name: "Olá, Robert Allan!" / "Fala Bruno!"
    _GREET_PAT = re.compile(
        r'((?:Ol[aá]|Oi|Bom dia|Boa tarde|Boa noite|Prezado|Prezada|Caro|Cara|Fala|Sr\.|Sra\.)'
        r'\s*[,!]?\s*)'
        r'((?:[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+\s*){1,4})'
        r'([!.,;]?\s*)',
        re.IGNORECASE)

    def _greet_repl(m):
        greeting = m.group(1)
        name_part = m.group(2).strip()
        punct = m.group(3)
        # Check if the captured words are actually names
        words = name_part.split()
        name_words = []
        rest_words = []
        for w in words:
            if _is_name(w) and not rest_words:
                name_words.append(w)
            else:
                rest_words.append(w)
        if name_words:
            result = greeting + '{cliente} '
            if rest_words:
                result += ' '.join(rest_words) + ' '
            result += punct
            return result
        return m.group(0)  # no change

    t = _GREET_PAT.sub(_greet_repl, t)

    # "Sou X" / "me chamo X" / "meu nome é X" → {assessor}
    def _sou_repl(m):
        prefix = m.group(1)
        name_part = m.group(2).strip()
        words = name_part.split()
        if any(_is_name(w) for w in words):
            return prefix + '{assessor}'
        return m.group(0)
    t = re.sub(
        r'((?:Sou|Me chamo|Meu nome [eé])\s+)'
        r'((?:[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇa-záéíóúãõâêîôûç]+\s*){1,4})',
        _sou_repl, t, flags=re.IGNORECASE)

    # "X da XP aqui" — name before context
    t = re.sub(
        r'\b([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ][a-záéíóúãõâêîôûç]{2,})(\s+(?:da XP|aqui|da empresa))',
        lambda m: '{assessor}' + m.group(2) if _is_name(m.group(1)) else m.group(0), t)

    # Any remaining 2+ capitalized name-words together
    def _multi_name_repl(m):
        words = m.group(0).split()
        if all(_is_name(w) for w in words):
            return '{nome}'
        return m.group(0)
    t = re.sub(
        r'\b[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ][a-záéíóúãõâêîôûç]{2,}(?:\s+[A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ][a-záéíóúãõâêîôûç]{2,})+\b',
        _multi_name_repl, t)

    # Collapse consecutive/adjacent placeholders
    for ph in ['{cliente}', '{assessor}', '{nome}', '{valor}', '{num}']:
        t = re.sub(r'(' + re.escape(ph) + r'\s*)+', ph + ' ', t)

    # Collapse multiple spaces / newlines
    t = re.sub(r'\s+', ' ', t).strip()
    # Truncate for grouping key
    return t[:250]


def _fuzzy_group_templates(template_data: dict) -> dict:
    """Merge template keys that are very similar (>80% character overlap).
    Returns merged dict with same structure."""
    keys = list(template_data.keys())
    if len(keys) <= 1:
        return template_data

    # Build groups using simple ratio comparison
    merged: dict[str, list[str]] = {}  # canonical_key -> [keys]
    used = set()

    for i, k1 in enumerate(keys):
        if k1 in used:
            continue
        group = [k1]
        used.add(k1)
        for j in range(i + 1, len(keys)):
            k2 = keys[j]
            if k2 in used:
                continue
            if _similarity(k1, k2) > 0.75:
                group.append(k2)
                used.add(k2)
        merged[k1] = group

    # Merge the data
    result: dict[str, dict[str, dict]] = {}
    for canonical, group_keys in merged.items():
        combined: dict[str, dict] = defaultdict(lambda: {"sent": 0, "replied": 0, "example": ""})
        for gk in group_keys:
            for agent, data in template_data[gk].items():
                combined[agent]["sent"] += data["sent"]
                combined[agent]["replied"] += data["replied"]
                if not combined[agent]["example"] and data["example"]:
                    combined[agent]["example"] = data["example"]
        result[canonical] = dict(combined)
    return result


def _similarity(a: str, b: str) -> float:
    """Simple character-level similarity ratio between two strings."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    # Use set of character bigrams for fast comparison
    def bigrams(s):
        return set(s[i:i+2] for i in range(len(s) - 1)) if len(s) > 1 else {s}
    ba, bb = bigrams(a.lower()), bigrams(b.lower())
    if not ba or not bb:
        return 0.0
    intersection = len(ba & bb)
    return 2.0 * intersection / (len(ba) + len(bb))


@router.get("/dashboard/mensagens", response_class=HTMLResponse, include_in_schema=False)
def dashboard_mensagens(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")
    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    client_name_map = get_client_names(db)
    db.close()  # release connection before heavy processing
    groups, phone_learned = _group_events(all_events, client_agent_map)

    # For each conversation, find the first OUT message and check if client replied
    # template_key -> { agent -> { "sent": int, "replied": int, "example": str } }
    template_data: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {"sent": 0, "replied": 0, "example": ""}))
    # Also track per-agent totals
    agent_totals: dict[str, dict] = defaultdict(lambda: {"sent": 0, "replied": 0})

    for client_num, evs in groups.items():
        evs_sorted = sorted(evs, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc))
        agent = client_agent_map.get(client_num) or phone_learned.get(client_num) or ""
        if not agent or agent == "Sem atendente":
            continue
        if not _user_sees(access, agent):
            continue

        # Find first OUT message
        first_out_text = ""
        found_first_out = False
        has_client_reply = False

        for ev in evs_sorted:
            p = ev.raw_payload or {}
            ev_type = (p.get("type", "") or "").upper()
            if ev_type in ("MESSAGE_STATUS", "CONVERSATION_STATUS"):
                continue
            direction = _extract_direction(p)
            content = _extract_content_preview(p) or ""
            if not content:
                continue

            if not found_first_out and direction == "OUT":
                first_out_text = content
                found_first_out = True
            elif found_first_out and direction == "IN":
                has_client_reply = True
                break

        if not first_out_text:
            continue

        # Normalize and group
        tpl_key = _normalize_template(first_out_text)
        template_data[tpl_key][agent]["sent"] += 1
        if has_client_reply:
            template_data[tpl_key][agent]["replied"] += 1
        if not template_data[tpl_key][agent]["example"]:
            template_data[tpl_key][agent]["example"] = first_out_text[:300]

        agent_totals[agent]["sent"] += 1
        if has_client_reply:
            agent_totals[agent]["replied"] += 1

    # Merge similar templates via fuzzy grouping
    template_data = _fuzzy_group_templates(template_data)

    # Build agent summary table
    agent_summary_rows = ""
    for ag in sorted(agent_totals.keys()):
        s = agent_totals[ag]["sent"]
        r = agent_totals[ag]["replied"]
        rate = round(r / s * 100, 1) if s > 0 else 0
        seg = _get_segment(ag)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{seg_color};margin-right:6px;vertical-align:middle"></span>' if seg else ''
        # Color the rate
        if rate >= 50:
            rate_color = "#0fa968"
        elif rate >= 25:
            rate_color = "#f59e0b"
        else:
            rate_color = "#ef4444"
        agent_summary_rows += f'<tr><td style="font-weight:600">{dot}{html_mod.escape(ag)}</td><td style="text-align:center">{s}</td><td style="text-align:center">{r}</td><td style="text-align:center;font-weight:700;color:{rate_color}">{rate}%</td></tr>'

    # Build template detail table — sorted by total sent desc
    # Only include templates with at least 10 occurrences
    MIN_OCCURRENCES = 10
    tpl_items = []
    for tpl_key, agents_dict in template_data.items():
        total_sent = sum(d["sent"] for d in agents_dict.values())
        total_replied = sum(d["replied"] for d in agents_dict.values())
        if total_sent < MIN_OCCURRENCES:
            continue
        # Pick example from the agent who sent the most
        top_agent = max(agents_dict.keys(), key=lambda a: agents_dict[a]["sent"])
        example = agents_dict[top_agent]["example"]
        tpl_items.append((tpl_key, agents_dict, total_sent, total_replied, example))
    tpl_items.sort(key=lambda x: x[2], reverse=True)

    template_rows = ""
    for idx, (tpl_key, agents_dict, total_sent, total_replied, example) in enumerate(tpl_items):
        rate = round(total_replied / total_sent * 100, 1) if total_sent > 0 else 0
        if rate >= 50:
            rate_color = "#0fa968"
        elif rate >= 25:
            rate_color = "#f59e0b"
        else:
            rate_color = "#ef4444"

        # Agents who use this template
        agents_list = sorted(agents_dict.keys(), key=lambda a: agents_dict[a]["sent"], reverse=True)
        agents_html = ""
        for ag in agents_list:
            d = agents_dict[ag]
            ag_rate = round(d["replied"] / d["sent"] * 100, 1) if d["sent"] > 0 else 0
            seg = _get_segment(ag)
            seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
            dot = f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{seg_color};margin-right:4px;vertical-align:middle"></span>'
            agents_html += f'<div style="font-size:11px;padding:2px 0">{dot}<strong>{html_mod.escape(ag)}</strong>: {d["sent"]} envios, {ag_rate}% retorno</div>'

        # Show normalized template with highlighted placeholders
        tpl_display = html_mod.escape(tpl_key[:250])
        # Highlight placeholders with colored tags
        for ph, ph_color, ph_label in [
            ("{cliente}", "#0fa968", "CLIENTE"),
            ("{assessor}", "#4a9eff", "ASSESSOR"),
            ("{nome}", "#d4af37", "NOME"),
            ("{valor}", "#f59e0b", "VALOR"),
            ("{data}", "#8b5cf6", "DATA"),
            ("{tel}", "#06b6d4", "TEL"),
            ("{pct}", "#ec4899", "PCT"),
            ("{num}", "#6b7280", "NUM"),
        ]:
            esc_ph = html_mod.escape(ph)
            badge = f'<span style="background:{ph_color};color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;font-weight:700">{ph_label}</span>'
            tpl_display = tpl_display.replace(esc_ph, badge)

        detail_id = f"tpl_{idx}"
        template_rows += f"""
        <tr class="conv-row" onclick="document.getElementById('{detail_id}').classList.toggle('open')" style="cursor:pointer">
            <td style="max-width:500px"><div style="font-size:12px;line-height:1.6;color:#c0c8d8;word-break:break-word">{tpl_display}</div></td>
            <td style="text-align:center;font-weight:600">{total_sent}</td>
            <td style="text-align:center;font-weight:600">{total_replied}</td>
            <td style="text-align:center;font-weight:700;color:{rate_color}">{rate}%</td>
            <td style="text-align:center;font-size:12px;color:#5a6a8a">{len(agents_list)}</td>
        </tr>
        <tr><td colspan="5" style="padding:0;border:none">
            <div id="{detail_id}" class="chat-box" style="padding:12px 16px">
                {agents_html}
            </div>
        </td></tr>"""

    # KPIs
    total_convs_with_first_out = sum(agent_totals[a]["sent"] for a in agent_totals)
    total_replies = sum(agent_totals[a]["replied"] for a in agent_totals)
    overall_rate = round(total_replies / total_convs_with_first_out * 100, 1) if total_convs_with_first_out > 0 else 0
    unique_templates = len(tpl_items)

    nav = _nav_html("mensagens", canal=canal, is_admin=(access or {}).get('role')=='admin', title="Mensagens Iniciais")
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Grampo — Mensagens Iniciais</title>{COMMON_CSS}</head><body>
    {nav}
    <div class="container">
        <div class="kpi-row">
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{total_convs_with_first_out}</div><div class="label">Conversas c/ msg inicial</div></div>
            <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{total_replies}</div><div class="label">Clientes responderam</div></div>
            <div class="kpi" style="border-top:3px solid {'#0fa968' if overall_rate >= 50 else '#f59e0b' if overall_rate >= 25 else '#ef4444'}"><div class="val" style="color:{'#0fa968' if overall_rate >= 50 else '#f59e0b' if overall_rate >= 25 else '#ef4444'}">{overall_rate}%</div><div class="label">Taxa de retorno geral</div></div>
            <div class="kpi" style="border-top:3px solid #4a9eff"><div class="val" style="color:#4a9eff">{unique_templates}</div><div class="label">Templates únicos</div></div>
        </div>

        <div class="card" style="margin-bottom:20px">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Taxa de Retorno por Assessor</h2>
            </div>
            <p style="color:#5a6a8a;font-size:11px;margin-bottom:14px">Primeira mensagem enviada pelo assessor vs. cliente respondeu</p>
            <table>
                <thead><tr><th>Assessor</th><th style="text-align:center">Enviadas</th><th style="text-align:center">Respondidas</th><th style="text-align:center">Taxa de Retorno</th></tr></thead>
                <tbody>{agent_summary_rows}</tbody>
            </table>
        </div>

        <div class="card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
                <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
                <h2 style="margin:0;font-size:15px">Templates de Abertura</h2>
            </div>
            <p style="color:#5a6a8a;font-size:11px;margin-bottom:14px">Apenas templates com 10+ envios. Clique para ver quais assessores usam cada mensagem. Mensagens similares são agrupadas.</p>
            <table>
                <thead><tr><th>Mensagem</th><th style="text-align:center">Envios</th><th style="text-align:center">Respostas</th><th style="text-align:center">Retorno</th><th style="text-align:center">Assessores</th></tr></thead>
                <tbody>{template_rows}</tbody>
            </table>
        </div>
    </div>
    </body></html>""")


# ── Evolução Dashboard ────────────────────────────────────────────────────────

@router.get("/dashboard/evolucao", response_class=HTMLResponse, include_in_schema=False)
def dashboard_evolucao(request: Request, db: Session = Depends(get_db)):
    access = _get_access(request, db)
    if access is None:
        return _auth_redirect()

    canal = request.query_params.get("canal", "5519997733651")

    all_events = get_events_only(db, limit=50000)
    all_events = _filter_events_by_channel(all_events, canal)
    client_agent_map = get_agent_mappings(db)
    db.close()  # release connection before heavy processing

    now_br = datetime.now(BRASILIA)
    # Look at last ~90 days (~13 weeks)
    cutoff = now_br - timedelta(days=91)
    recent_events = [ev for ev in all_events
                     if ev.received_at and ev.received_at.astimezone(BRASILIA) >= cutoff]

    # Single _group_events call on recent events only
    groups, phone_learned = _group_events(recent_events, client_agent_map)

    # ── Aggregate by ISO week ────────────────────────────────────────────────
    # weekly_clients[week_key][agent] = set of phones (unique clients)
    # weekly_topics[week_key][topic_id] = set of phones
    weekly_clients: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    weekly_topics: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))

    for client_num, evs in groups.items():
        ph = _real_phone(client_num)
        agent = (phone_learned.get(client_num)
                 or client_agent_map.get(ph)
                 or phone_learned.get(ph)
                 or "Sem atendente")
        if agent == "Sem atendente":
            continue
        if not _user_sees(access, agent):
            continue

        # Use the date of the first OUT message for weekly attribution
        out_evs = sorted(
            [ev for ev in evs if _extract_direction(ev.raw_payload or {}) == "OUT"],
            key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc)
        )
        if not out_evs:
            continue
        ev_dt = out_evs[0].received_at.astimezone(BRASILIA)
        iso_year, iso_week, _ = ev_dt.isocalendar()
        week_key = f"{iso_year}-{iso_week:02d}"

        weekly_clients[week_key][agent].add(ph)

        # Topic detection across all messages in conversation
        full_text = " ".join(
            (_extract_content_preview(ev.raw_payload or {}) or "") for ev in evs
        ).lower()
        for tid, _tlabel, _tcolor, keywords in TOPIC_RULES:
            for kw in keywords:
                if kw in full_text:
                    weekly_topics[week_key][tid].add(ph)
                    break

    # Sorted list of ISO week keys (last 12 max)
    all_weeks = sorted(weekly_clients.keys())
    if len(all_weeks) > 12:
        all_weeks = all_weeks[-12:]

    def _week_label(wk: str) -> str:
        year, week = wk.split("-")
        return f"S{int(week):02d}/{year[2:]}"

    week_labels = [_week_label(wk) for wk in all_weeks]

    # Active agents: known agents that appear in at least one week
    known_agents = list(AGENT_SEGMENT.keys())
    active_agents = [ag for ag in known_agents
                     if any(ag in weekly_clients.get(wk, {}) for wk in all_weeks)]

    # ── Section 1: heatmap table (agents × weeks) ────────────────────────────
    _mx1 = max(
        (len(weekly_clients[wk].get(ag, set()))
         for wk in all_weeks for ag in active_agents),
        default=1
    ) or 1

    wk_headers = "".join(
        f'<th style="text-align:center;min-width:70px;font-size:10px;padding:8px 4px">{_week_label(wk)}</th>'
        for wk in all_weeks
    )
    wk_headers += '<th style="text-align:center;min-width:65px;font-size:10px;padding:8px 4px;color:#0fa968">TOTAL</th>'

    s1_rows = ""
    for ag in active_agents:
        seg = _get_segment(ag)
        seg_color = SEGMENT_COLORS.get(seg, "#1a2540")
        dot = (f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
               f'background:{seg_color};margin-right:6px;vertical-align:middle"></span>')
        cells = ""
        union_set: set = set()
        for wk in all_weeks:
            s = weekly_clients.get(wk, {}).get(ag, set())
            union_set |= s
            v = len(s)
            if v == 0:
                cells += '<td style="text-align:center;color:#1a2540;font-size:13px">—</td>'
            else:
                intensity = min(v / _mx1, 1.0)
                _r = int(10 + intensity * 5)
                _g = int(40 + intensity * 129)
                _b = int(20 + intensity * 84)
                _a = 0.25 + intensity * 0.65
                cells += (f'<td style="text-align:center;background:rgba({_r},{_g},{_b},{_a:.2f});'
                          f'font-size:13px;font-weight:700;color:#fff">{v}</td>')
        row_total = len(union_set)
        cells += (f'<td style="text-align:center;font-weight:800;color:#0fa968;'
                  f'font-size:14px;background:#0f1629">{row_total}</td>')
        s1_rows += (f'<tr>'
                    f'<td style="border-left:3px solid {seg_color};padding-left:10px;'
                    f'white-space:nowrap;font-size:12px;font-weight:600;background:#0b1120">'
                    f'{dot}{_short_agent_name(ag)}</td>{cells}</tr>')

    # Total row
    if s1_rows:
        total_cells = ""
        grand_union: set = set()
        for wk in all_weeks:
            wk_union: set = set()
            for ag in active_agents:
                wk_union |= weekly_clients.get(wk, {}).get(ag, set())
            grand_union |= wk_union
            v = len(wk_union)
            if v == 0:
                total_cells += '<td style="text-align:center;color:#1a2540">—</td>'
            else:
                intensity = min(v / _mx1, 1.0)
                _r = int(10 + intensity * 5)
                _g = int(40 + intensity * 129)
                _b = int(20 + intensity * 84)
                _a = 0.3 + intensity * 0.7
                total_cells += (f'<td style="text-align:center;background:rgba({_r},{_g},{_b},{_a:.2f});'
                                f'font-size:14px;font-weight:800;color:#fff">{v}</td>')
        total_cells += (f'<td style="text-align:center;font-weight:800;color:#0fa968;'
                        f'font-size:15px;background:#0f1629">{len(grand_union)}</td>')
        s1_rows += (f'<tr style="border-top:2px solid #1a2540">'
                    f'<td style="font-size:12px;font-weight:800;color:#e8ecf1;background:#0b1120;'
                    f'text-transform:uppercase;letter-spacing:.5px">TOTAL</td>{total_cells}</tr>')
    elif not s1_rows:
        s1_rows = '<tr><td colspan="20" style="text-align:center;color:#4a5a7a;padding:40px">Sem dados no período.</td></tr>'

    # ── Section 2: Chart data ─────────────────────────────────────────────────
    # Per-agent weekly counts (for line chart)
    agent_chart: dict[str, list] = {}
    agent_colors: dict[str, str] = {}
    for ag in active_agents:
        agent_chart[ag] = [len(weekly_clients.get(wk, {}).get(ag, set())) for wk in all_weeks]
        seg = _get_segment(ag)
        agent_colors[ag] = SEGMENT_COLORS.get(seg, "#5a6a8a")

    # Team total per week (union of all agents)
    team_totals = []
    for wk in all_weeks:
        wk_union: set = set()
        for ag in active_agents:
            wk_union |= weekly_clients.get(wk, {}).get(ag, set())
        team_totals.append(len(wk_union))

    # Active topics with data
    active_topic_info = [
        (tid, tlabel, tcolor)
        for tid, tlabel, tcolor, _ in TOPIC_RULES
        if any(weekly_topics.get(wk, {}).get(tid) for wk in all_weeks)
    ]
    topic_week_data = {
        tid: [len(weekly_topics.get(wk, {}).get(tid, set())) for wk in all_weeks]
        for tid, *_ in active_topic_info
    }

    # JSON payloads
    _wk_labels_js   = json.dumps(week_labels)
    _team_totals_js  = json.dumps(team_totals)
    _agents_data_js  = json.dumps({ag: agent_chart[ag] for ag in active_agents})
    _agents_names_js = json.dumps({ag: _short_agent_name(ag) for ag in active_agents})
    _agents_colors_js= json.dumps({ag: agent_colors[ag] for ag in active_agents})
    _topics_data_js  = json.dumps({tid: topic_week_data[tid] for tid, *_ in active_topic_info})
    _topics_labels_js= json.dumps([tlabel for _, tlabel, _ in active_topic_info])
    _topics_colors_js= json.dumps([tcolor for _, _, tcolor in active_topic_info])

    # KPIs
    _total_weeks = len(all_weeks)
    _max_week_total = max(team_totals) if team_totals else 0
    _avg_week_total = round(sum(team_totals) / len(team_totals), 1) if team_totals else 0
    _peak_week = week_labels[team_totals.index(_max_week_total)] if team_totals else "—"

    seg_legend = '<div class="seg-legend">'
    for seg_name, seg_col in SEGMENT_COLORS.items():
        seg_legend += f'<span><span class="seg-dot" style="background:{seg_col}"></span> {seg_name}</span>'
    seg_legend += '</div>'

    nav = _nav_html("evolucao", canal=canal, is_admin=(access or {}).get('role')=='admin', title="Evolução")
    page = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Evolução — Alto Valor</title>{COMMON_CSS}
<style>
  .evol-scroll {{ overflow-x: auto; }}
  .evol-scroll::-webkit-scrollbar {{ height: 4px; }}
  .evol-scroll::-webkit-scrollbar-track {{ background: #0b1120; border-radius: 2px; }}
  .evol-scroll::-webkit-scrollbar-thumb {{ background: #1a2540; border-radius: 2px; }}
  .evol-scroll::-webkit-scrollbar-thumb:hover {{ background: #0fa968; }}
</style>
</head><body>
{nav}
<div class="container">

  <!-- KPIs -->
  <div class="kpi-row">
    <div class="kpi" style="border-top:3px solid #4a9eff"><div class="val" style="color:#4a9eff">{_total_weeks}</div><div class="label">Semanas analisadas</div></div>
    <div class="kpi" style="border-top:3px solid #d4af37"><div class="val" style="color:#d4af37">{_max_week_total}</div><div class="label">Pico semanal</div></div>
    <div class="kpi" style="border-top:3px solid #0fa968"><div class="val">{_avg_week_total}</div><div class="label">Média semanal</div></div>
    <div class="kpi" style="border-top:3px solid #d4af37"><div class="val" style="font-size:16px;color:#d4af37">{_peak_week}</div><div class="label">Semana de pico</div></div>
  </div>

  <!-- SECTION 1: Heatmap agents × weeks -->
  <div class="card" style="margin-bottom:20px">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
      <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
      <h2 style="margin:0;font-size:15px">Clientes únicos por semana × agente</h2>
    </div>
    <p style="font-size:11px;color:#4a5a7a;margin-bottom:16px">
      Clientes únicos contactados (OUT) por agente em cada semana. Total = clientes únicos no período inteiro.
    </p>
    {seg_legend}
    <div class="evol-scroll">
      <table style="min-width:500px">
        <thead><tr><th style="min-width:160px">Agente</th>{wk_headers}</tr></thead>
        <tbody>{s1_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- SECTION 2: Team volume line chart -->
  <div class="card" style="margin-bottom:20px">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
      <span style="width:8px;height:8px;border-radius:50%;background:#0fa968;display:inline-block"></span>
      <h2 style="margin:0;font-size:15px">Volume geral do time por semana</h2>
    </div>
    <p style="font-size:11px;color:#4a5a7a;margin-bottom:16px">
      Clientes únicos totais (linha branca tracejada) e por agente por semana.
    </p>
    <canvas id="vol-chart" style="width:100%;display:block" height="260"></canvas>
    <script>
    (function(){{
      var weeks   = {_wk_labels_js};
      var totals  = {_team_totals_js};
      var agData  = {_agents_data_js};
      var agNames = {_agents_names_js};
      var agColors= {_agents_colors_js};
      var agents  = Object.keys(agData);

      var canvas = document.getElementById('vol-chart');
      var W = canvas.parentElement.offsetWidth - 32;
      canvas.width = W;
      var H = canvas.height;
      var ctx = canvas.getContext('2d');
      var padL = 40, padR = 160, padT = 20, padB = 40;
      var plotW = W - padL - padR;
      var plotH = H - padT - padB;
      var n = weeks.length;
      if (n < 2) return;

      var allVals = totals.concat.apply(totals, agents.map(function(a){{return agData[a];}}));
      var maxVal = Math.max.apply(null, allVals) || 1;

      ctx.clearRect(0, 0, W, H);

      // Grid
      var gridSteps = 5;
      ctx.strokeStyle = '#1a2540'; ctx.lineWidth = 1;
      for (var s = 0; s <= gridSteps; s++) {{
        var gy = padT + plotH - Math.round(s / gridSteps * plotH);
        ctx.beginPath(); ctx.moveTo(padL, gy); ctx.lineTo(padL + plotW, gy); ctx.stroke();
        ctx.fillStyle = '#3a4a6a'; ctx.font = '9px Montserrat,sans-serif'; ctx.textAlign = 'right';
        ctx.fillText(Math.round(s / gridSteps * maxVal), padL - 4, gy + 3);
      }}

      // X labels
      ctx.fillStyle = '#5a6a8a'; ctx.font = '9px Montserrat,sans-serif'; ctx.textAlign = 'center';
      for (var i = 0; i < n; i++) {{
        var gx = padL + Math.round(i / (n-1) * plotW);
        ctx.fillText(weeks[i], gx, H - padB + 14);
      }}

      // Draw per-agent lines (faded)
      agents.forEach(function(ag) {{
        var vals = agData[ag];
        var col = agColors[ag];
        ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.globalAlpha = 0.6;
        ctx.setLineDash([]);
        ctx.beginPath();
        for (var i = 0; i < n; i++) {{
          var gx = padL + Math.round(i / (n-1) * plotW);
          var gy = padT + plotH - Math.round(vals[i] / maxVal * plotH);
          if (i === 0) ctx.moveTo(gx, gy); else ctx.lineTo(gx, gy);
        }}
        ctx.stroke();
        // dot on last point
        var lastX = padL + plotW;
        var lastY = padT + plotH - Math.round(vals[n-1] / maxVal * plotH);
        ctx.globalAlpha = 1.0;
        ctx.fillStyle = col;
        ctx.beginPath(); ctx.arc(lastX, lastY, 3, 0, 2*Math.PI); ctx.fill();
      }});

      // Draw TOTAL line (white dashed, thick)
      ctx.strokeStyle = '#ffffff'; ctx.lineWidth = 2.5; ctx.globalAlpha = 1.0;
      ctx.setLineDash([6, 4]);
      ctx.beginPath();
      for (var i = 0; i < n; i++) {{
        var gx = padL + Math.round(i / (n-1) * plotW);
        var gy = padT + plotH - Math.round(totals[i] / maxVal * plotH);
        if (i === 0) ctx.moveTo(gx, gy); else ctx.lineTo(gx, gy);
      }}
      ctx.stroke();
      ctx.setLineDash([]);

      // Legend (right side)
      var ly = padT;
      var lineH = 18;
      // Total first
      ctx.strokeStyle = '#ffffff'; ctx.lineWidth = 2; ctx.setLineDash([5,3]);
      ctx.beginPath(); ctx.moveTo(W - padR + 8, ly + 6); ctx.lineTo(W - padR + 26, ly + 6); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#e8ecf1'; ctx.font = 'bold 10px Montserrat,sans-serif'; ctx.textAlign = 'left';
      ctx.fillText('TOTAL', W - padR + 30, ly + 10);
      ly += lineH;

      agents.forEach(function(ag) {{
        var col = agColors[ag];
        ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.globalAlpha = 0.9;
        ctx.beginPath(); ctx.moveTo(W - padR + 8, ly + 6); ctx.lineTo(W - padR + 26, ly + 6); ctx.stroke();
        ctx.globalAlpha = 1.0;
        ctx.fillStyle = '#c0c8d8'; ctx.font = '10px Montserrat,sans-serif'; ctx.textAlign = 'left';
        ctx.fillText(agNames[ag], W - padR + 30, ly + 10);
        ly += lineH;
      }});
    }})();
    </script>
  </div>

  <!-- SECTION 3: Topics by week stacked bar -->
  <div class="card">
    <h2 style="margin-bottom:6px">Temas abordados por semana</h2>
    <p style="font-size:11px;color:#4a5a7a;margin-bottom:16px">
      Clientes únicos por tema a cada semana. Barras empilhadas — alturas representam volume relativo.
    </p>
    <canvas id="topic-chart" style="width:100%;display:block" height="280"></canvas>
    <div id="topic-legend" style="display:flex;flex-wrap:wrap;gap:10px;margin-top:14px"></div>
    <script>
    (function(){{
      var weeks      = {_wk_labels_js};
      var topicData  = {_topics_data_js};
      var topicLabels= {_topics_labels_js};
      var topicColors= {_topics_colors_js};
      var tids       = Object.keys(topicData);
      var n          = weeks.length;

      // Build legend
      var leg = document.getElementById('topic-legend');
      tids.forEach(function(tid, i) {{
        var d = document.createElement('span');
        d.style.cssText = 'font-size:10px;display:flex;align-items:center;gap:5px;color:#8a96aa';
        d.innerHTML = '<span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:'+topicColors[i]+'"></span>' + topicLabels[i];
        leg.appendChild(d);
      }});

      if (n < 1 || tids.length < 1) return;

      var canvas = document.getElementById('topic-chart');
      var W = canvas.parentElement.offsetWidth - 32;
      canvas.width = W;
      var H = canvas.height;
      var ctx = canvas.getContext('2d');
      var padL = 40, padR = 20, padT = 20, padB = 40;
      var plotW = W - padL - padR;
      var plotH = H - padT - padB;

      // Max stack height per week
      var stackMaxes = weeks.map(function(_, wi) {{
        return tids.reduce(function(sum, tid) {{ return sum + (topicData[tid][wi] || 0); }}, 0);
      }});
      var maxStack = Math.max.apply(null, stackMaxes) || 1;

      ctx.clearRect(0, 0, W, H);

      // Grid
      ctx.strokeStyle = '#1a2540'; ctx.lineWidth = 1;
      for (var s = 0; s <= 5; s++) {{
        var gy = padT + plotH - Math.round(s / 5 * plotH);
        ctx.beginPath(); ctx.moveTo(padL, gy); ctx.lineTo(padL + plotW, gy); ctx.stroke();
        ctx.fillStyle = '#3a4a6a'; ctx.font = '9px Montserrat,sans-serif'; ctx.textAlign = 'right';
        ctx.fillText(Math.round(s / 5 * maxStack), padL - 4, gy + 3);
      }}

      var barW = Math.max(8, Math.floor(plotW / n * 0.6));
      var stepW = plotW / n;

      for (var wi = 0; wi < n; wi++) {{
        var gx = padL + Math.round(wi * stepW + stepW / 2 - barW / 2);
        var baseY = padT + plotH;
        tids.forEach(function(tid, ti) {{
          var v = topicData[tid][wi] || 0;
          if (v === 0) return;
          var bh = Math.round(v / maxStack * plotH);
          ctx.fillStyle = topicColors[ti];
          ctx.globalAlpha = 0.82;
          ctx.fillRect(gx, baseY - bh, barW, bh);
          baseY -= bh;
        }});
        ctx.globalAlpha = 1.0;
        // X label
        ctx.fillStyle = '#5a6a8a'; ctx.font = '9px Montserrat,sans-serif'; ctx.textAlign = 'center';
        ctx.fillText(weeks[wi], padL + Math.round(wi * stepW + stepW / 2), H - padB + 14);
      }}
    }})();
    </script>
  </div>

</div>
</body></html>"""

    return HTMLResponse(page)
