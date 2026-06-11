"""
"Sem Resposta" classifier — uses Claude Sonnet 4.6 to decide whether a
conversation ended naturally or is still waiting for an advisor reply.

The result is cached in `conversation_analyses` keyed by
(phone, last_event_id). Whenever a new message arrives, the last_event_id
changes and the conversation gets re-analyzed.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy.orm import Session

from app.config import settings
from app.models import ConversationAnalysis

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v2"
MAX_MESSAGES_IN_PROMPT = 30  # last N messages of the conversation
MAX_CHARS_PER_MESSAGE = 500


# ── Prompt ───────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Você é um analista de compliance da Alto Valor, uma assessoria \
de investimentos. Sua tarefa é classificar conversas de WhatsApp entre \
assessores e clientes para detectar atendimentos em que o ASSESSOR deixou o \
CLIENTE sem resposta.

Você deve responder SEMPRE em formato JSON estrito, sem texto adicional, sem \
markdown, sem comentários. O JSON deve ter exatamente esses campos:

{
  "status": "encerrada" | "pendente",
  "confidence": número de 0.0 a 1.0,
  "reason": string em português, máx 150 caracteres,
  "priority": "alta" | "media" | "baixa"
}

A última mensagem da conversa é SEMPRE do CLIENTE (o assessor não respondeu
depois dela). Sua tarefa é decidir se essa última mensagem do cliente DEMANDA
uma resposta do assessor ou não.

Regras de classificação:

- "encerrada": a última mensagem do cliente NÃO precisa de resposta. Casos típicos:
  • Fechamento social / agradecimento: "ok", "obrigado", "valeu", "perfeito", \
"tá bom", "blz", "show", "👍", "🙏", "combinado", "fechado", "entendi".
  • Confirmação simples de algo que o assessor já resolveu ("pode ser", "isso mesmo").
  • Despedida ("até mais", "abraço", "bom fim de semana").
  • Comentário social sem pergunta nem pedido.
  Nesses casos o assessor PODE até responder por cortesia, mas NÃO há atendimento \
pendente — classifique como "encerrada".

- "pendente": a última mensagem do cliente é uma PERGUNTA, PEDIDO, RECLAMAÇÃO ou \
sinal claro de que ele espera ação/retorno do assessor. Inclui: dúvidas sobre \
produtos, pedidos de cotação, solicitações de resgate/aporte/transferência, \
pedidos de informação ou documento, reclamações, sinais de insatisfação, ou uma \
pergunta direta que ficou no ar. Em caso de dúvida entre as duas, se houver \
QUALQUER pergunta ou pedido não respondido, classifique como "pendente".

IMPORTANTE: avalie apenas a ÚLTIMA mensagem do cliente no contexto da conversa. \
Se o assessor já tinha respondido a dúvida ANTES e o cliente só agradeceu, é \
"encerrada". Se o cliente trouxe um assunto novo ou repetiu um pedido não \
atendido, é "pendente".

Quando "status" = "pendente", classifique a prioridade:
- "alta": envolve decisão financeira concreta, valor monetário específico, \
prazo iminente, pedido de resgate/movimentação, ou cliente demonstra urgência \
("preciso", "urgente", "rápido").
- "media": pedido normal de informação, dúvida sobre produto, agendamento.
- "baixa": comentário ambíguo, follow-up genérico ("alguma novidade?").

O campo "reason" deve descrever em UMA frase curta (≤150 chars) o que o \
cliente está esperando. Exemplo: "Cliente pediu cotação de CDB e não houve \
resposta" ou "Cliente questionou rendimento abaixo do esperado".
"""


def _build_user_prompt(messages: list[tuple[str, str, datetime]]) -> str:
    """Format the conversation as a chronological transcript for the LLM."""
    # Trim to the last MAX_MESSAGES_IN_PROMPT events
    msgs = messages[-MAX_MESSAGES_IN_PROMPT:]

    lines: list[str] = []
    for direction, text, ts in msgs:
        who = "CLIENTE" if direction.upper() == "IN" else "ASSESSOR"
        ts_str = ts.strftime("%d/%m %H:%M") if ts else "??/??"
        # Trim very long messages so a single huge audio transcript doesn't
        # blow the context window.
        body = (text or "").strip().replace("\n", " ")
        if len(body) > MAX_CHARS_PER_MESSAGE:
            body = body[:MAX_CHARS_PER_MESSAGE] + "…"
        lines.append(f"[{ts_str}] {who}: {body}")

    transcript = "\n".join(lines)
    return (
        "Classifique a conversa abaixo. Responda APENAS com o JSON, sem texto adicional.\n\n"
        "=== CONVERSA ===\n"
        f"{transcript}\n"
        "=== FIM ===\n"
    )


# ── JSON parsing (tolerant) ──────────────────────────────────────────────────

_JSON_OBJ_RE = re.compile(r"\{[\s\S]*\}")


def _parse_response(raw: str) -> dict | None:
    """Tolerant JSON extraction — Claude usually returns clean JSON but in
    rare cases adds preamble; this peels off any non-JSON wrapping."""
    if not raw:
        return None
    raw = raw.strip()
    # Try direct parse first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Try to find the first { … } block
    m = _JSON_OBJ_RE.search(raw)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _normalize(data: dict) -> dict:
    """Coerce fields into the expected types/values."""
    status = (data.get("status") or "").strip().lower()
    if status not in ("encerrada", "pendente"):
        status = "encerrada"

    try:
        conf = float(data.get("confidence", 0.0))
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))

    reason = (data.get("reason") or "")[:200]

    priority = (data.get("priority") or "").strip().lower()
    if priority not in ("alta", "media", "baixa"):
        priority = "media" if status == "pendente" else None

    return {
        "status": status,
        "confidence": int(round(conf * 100)),
        "reason": reason,
        "priority": priority,
    }


# ── Public API ───────────────────────────────────────────────────────────────


def get_cached(db: Session, phone: str, last_event_id: str) -> ConversationAnalysis | None:
    """Return cached analysis if it exists for this (phone, last_event_id)."""
    return db.get(ConversationAnalysis, (phone, last_event_id))


def analyze_conversation(
    db: Session,
    phone: str,
    last_event_id: str,
    messages: list[tuple[str, str, datetime]],
) -> ConversationAnalysis | None:
    """Classify a single conversation. Cached: if (phone, last_event_id)
    already exists, returns the existing row without calling the LLM.

    `messages` is the chronological list of (direction, text, received_at).
    Returns None on hard failure (missing API key, network, parse error).
    """
    # Cache hit
    existing = get_cached(db, phone, last_event_id)
    if existing and existing.prompt_version == PROMPT_VERSION:
        return existing

    if not settings.ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — Sem Resposta analysis unavailable")
        return None

    if not messages:
        return None

    # Lazy import so the dependency only kicks in when the feature is used
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic SDK not installed")
        return None

    user_prompt = _build_user_prompt(messages)

    try:
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=settings.ANTHROPIC_MODEL_BULK,
            max_tokens=400,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as exc:
        logger.error("Claude call failed for phone %s: %s", phone, exc)
        return None

    # Concatenate text blocks from the response
    raw = ""
    try:
        for block in (resp.content or []):
            if getattr(block, "type", "") == "text":
                raw += block.text or ""
    except Exception:
        raw = ""

    parsed = _parse_response(raw)
    if not parsed:
        logger.warning("Could not parse Claude response for %s: %r", phone, raw[:200])
        return None

    normalized = _normalize(parsed)

    # Upsert in cache
    try:
        if existing:
            existing.status = normalized["status"]
            existing.confidence = normalized["confidence"]
            existing.reason = normalized["reason"]
            existing.priority = normalized["priority"]
            existing.prompt_version = PROMPT_VERSION
            existing.analyzed_at = datetime.now(timezone.utc)
            row = existing
        else:
            row = ConversationAnalysis(
                phone=phone,
                last_event_id=last_event_id,
                status=normalized["status"],
                confidence=normalized["confidence"],
                reason=normalized["reason"],
                priority=normalized["priority"],
                prompt_version=PROMPT_VERSION,
            )
            db.add(row)
        db.commit()
        db.refresh(row)
        return row
    except Exception as exc:
        logger.warning("Failed to cache analysis for %s: %s", phone, exc)
        db.rollback()
        return None


def analyze_many(
    db: Session,
    candidates: list[tuple[str, str, list[tuple[str, str, datetime]]]],
    max_new: int = 15,
) -> dict[str, dict]:
    """Analyze up to `max_new` candidates and return a dict of {phone: result_dict}.

    Returns plain dicts (not ORM instances) so callers can safely use the data
    after the SQLAlchemy session is closed without DetachedInstanceError.

    Dict shape: {"status", "confidence", "reason", "priority"}
    """
    def _to_dict(row: ConversationAnalysis) -> dict:
        return {
            "status":     row.status,
            "confidence": row.confidence,
            "reason":     row.reason or "",
            "priority":   row.priority or "",
        }

    out: dict[str, dict] = {}
    new_attempts = 0  # cap on LLM ATTEMPTS (not successes) to keep page load fast
    for phone, last_event_id, messages in candidates:
        existing = get_cached(db, phone, last_event_id)
        if existing and existing.prompt_version == PROMPT_VERSION:
            out[phone] = _to_dict(existing)
            continue
        if new_attempts >= max_new:
            break
        new_attempts += 1
        result = analyze_conversation(db, phone, last_event_id, messages)
        if result:
            out[phone] = _to_dict(result)
    return out


# ── Reply suggestion ─────────────────────────────────────────────────────────

_REPLY_SYSTEM = """Você é um assessor de investimentos sênior da Alto Valor, \
parceira XP Investimentos. Você deve sugerir UMA resposta curta e profissional \
para retomar a conversa com o cliente que ficou sem resposta.

Regras:
- Escreva como se fosse o próprio assessor (1ª pessoa, tom informal-profissional)
- Máximo 3 frases curtas
- NÃO inclua saudações genéricas como "Bom dia" ou "Tudo bem?"
  a menos que o contexto seja claramente uma retomada após longa ausência
- Se o cliente fez uma pergunta específica, responda/aborde ela diretamente
- Se o cliente pediu uma cotação ou informação, ofereça buscar
- Não invente números, taxas ou produtos — diga que vai verificar se não sabe
- Use a linguagem natural do assessor na conversa (informal mas respeitoso)
- Responda APENAS com o texto da mensagem, sem aspas, sem introdução, sem nada mais
"""


def suggest_reply(
    messages: list[tuple[str, str, datetime]],
    reason: str = "",
) -> str:
    """Generate a suggested advisor reply for a pending conversation.

    Returns the suggested text, or an error string starting with '[Erro'.
    No caching — one-shot call triggered by the user.
    """
    if not settings.ANTHROPIC_API_KEY:
        return "[Erro: ANTHROPIC_API_KEY não configurada]"

    try:
        import anthropic
    except ImportError:
        return "[Erro: SDK anthropic não instalado]"

    msgs = messages[-MAX_MESSAGES_IN_PROMPT:]
    lines: list[str] = []
    for direction, text, ts in msgs:
        who = "CLIENTE" if direction.upper() == "IN" else "ASSESSOR"
        ts_str = ts.strftime("%d/%m %H:%M") if ts else "??/??"
        body = (text or "").strip().replace("\n", " ")
        if len(body) > MAX_CHARS_PER_MESSAGE:
            body = body[:MAX_CHARS_PER_MESSAGE] + "…"
        lines.append(f"[{ts_str}] {who}: {body}")

    transcript = "\n".join(lines)
    context = f"\n\nContexto: {reason}" if reason else ""
    user_prompt = (
        f"Conversa abaixo. Sugira uma resposta para o ASSESSOR retomar o atendimento.{context}\n\n"
        f"=== CONVERSA ===\n{transcript}\n=== FIM ===\n\n"
        "Escreva APENAS a mensagem sugerida, sem mais nada:"
    )

    try:
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=settings.ANTHROPIC_MODEL,
            max_tokens=300,
            system=_REPLY_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = ""
        for block in (resp.content or []):
            if getattr(block, "type", "") == "text":
                raw += block.text or ""
        return raw.strip() or "[Sem sugestão gerada]"
    except Exception as exc:
        logger.error("suggest_reply failed: %s", exc)
        return f"[Erro ao gerar sugestão: {exc}]"
