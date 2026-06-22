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

    # Teto diário de chamadas à IA (proteção de custo).
    from app.services import llm_budget as _llm
    if not _llm.try_consume(db, feature="sem-resposta"):
        logger.warning("Teto diário de IA atingido — Sem Resposta não analisado")
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
parceira XP Investimentos. Sugira UMA mensagem curta para o assessor mandar \
AGORA no WhatsApp do cliente.

A mensagem TEM que parecer escrita por uma PESSOA REAL no WhatsApp. NUNCA pode \
ter "cara de IA" ou de texto automático.

QUEM É O CLIENTE (importante): é um cliente que JÁ tem conta ativa na XP e JÁ é \
atendido por este escritório. NÃO é lead novo nem onboarding. Logo:
- NUNCA pergunte se ele "tem conta na XP", se "quer abrir conta", "qual plataforma/corretora você usa", nem peça dados de cadastro. Isso já existe.
- O papel aqui é entender o objetivo e ENCAMINHAR o investimento/alocação, não resolver cadastro nem operação de transferência.
- Engaje com o que o cliente ACABOU de dizer e leve para o próximo passo concreto. Se ele disse que quer aportar e já informou um valor, confirme de forma natural e avance (combinar onde aplicar / dizer que vai montar a sugestão). NUNCA mude de assunto para uma pergunta operacional que não vem ao caso.

Conteúdo:
- 1ª pessoa, como o próprio assessor. No máximo 2-3 frases curtas (é WhatsApp, não e-mail).
- Se o cliente fez uma pergunta, aborde direto.
- Se pediu cotação/informação que você não tem, diga de forma natural que vai verificar e retornar. NUNCA invente número, taxa, produto, rentabilidade ou prazo.
- NÃO afirme um valor/quantia específico (ex.: "R$10.000") a menos que o cliente tenha dito esse valor na mensagem ATUAL. Se o número veio de uma mensagem antiga ou você não tem certeza de que ainda vale, escreva "esse valor" ou pergunte quanto é. O mesmo vale para status/fatos que você só inferiu (conta já ativa, transferência feita, etc.): na dúvida, PERGUNTE em vez de afirmar como certo.
- Se a última mensagem (ou as recentes) do cliente for um ARQUIVO / IMAGEM / ÁUDIO / FIGURINHA — você só vê um marcador tipo "[ARQUIVO]", "[IMAGEM]", "[ÁUDIO]", SEM o conteúdo — NÃO invente nem reaja ao que tem nele. Reconheça de forma natural que recebeu (ex.: "recebi sua figurinha 😄", "vi seu arquivo, já te retorno") ou puxe o assunto; NUNCA finja que viu a imagem ou ouviu o áudio.
- Espelhe o tom e o vocabulário que o ASSESSOR já usou na conversa (se ele é informal, seja informal; se trata por "você" ou "senhor", mantenha o mesmo).
- Se vier um bloco "CONTEXTO DO CLIENTE", use como pano de fundo para deixar a resposta mais relevante (assunto pendente, produto já oferecido, oportunidade) — mas NUNCA repita nota, tag, jargão ou nome de produto interno desse bloco; soe como se você já conhecesse o cliente naturalmente.

Para soar humano, EVITE os erros que mais entregam que é IA:
- PROIBIDO travessão (o caractere "—" ou "–"). Use ponto ou vírgula, ou reescreva a frase.
- PROIBIDO oferecer um "cardápio" de opções (ex.: "liquidez, rendimento de curto prazo ou longo prazo?"). Se for perguntar, faça UMA pergunta simples e específica.
- PROIBIDO entusiasmo genérico de vendedor: "Que ótimo!", "Vamos aproveitar!", "Perfeito!", "te indico a melhor opção".
- PROIBIDO clichê corporativo: "fico à disposição", "não hesite em entrar em contato", "entendo perfeitamente", "espero que esteja tudo bem", "é um prazer", "qualquer dúvida estou à disposição".
- Não repita nem parafraseie o que o cliente disse ("vi que você quer...", "entendi que você...").
- Emoji só se o assessor já usa, no máximo um. Sem exclamação em excesso.
- Sem saudação robótica repetida; muitas vezes é continuação de conversa e nem precisa.
- Vá direto ao ponto, como uma pessoa de verdade mandando um zap rápido, NÃO como um texto de vendas.

EXEMPLO do que NÃO fazer (puro vício de IA):
"Bom dia, Lucas! Que ótimo, vamos aproveitar esse aporte. Me fala o que você busca: liquidez, rendimento de curto prazo, ou algo de longo prazo? Assim te indico a melhor opção."
(erros: entusiasmo de vendedor, cardápio de 3 opções, fechamento genérico)

EXEMPLO do que NÃO fazer (tratou cliente da casa como lead novo):
Cliente: "Recebi meu pagamento e gostaria de aportar" / "8000"
Resposta ruim: "Boa tarde, Lucas! Qual plataforma você usa pra transferir, já tem conta ativa na XP?"
(erros: o cliente JÁ tem conta XP; pergunta de onboarding/operação que não vem ao caso; ignorou o aporte que ele pediu; duas perguntas)

EXEMPLO bom (humano e direto):
"Bom dia, Lucas! Esse valor você prefere deixar mais à mão ou pode deixar rendendo um tempo?"
(uma pergunta só, simples, sem lista, sem encheção)

EXEMPLO bom (avança o aporte de um cliente da casa):
Cliente: "gostaria de aportar" / "8000"
Resposta boa: "Boa tarde, Lucas! Fechado. Esses R$8 mil você vai precisar nos próximos meses ou pode deixar rendendo um tempo?"
(confirma o aporte, usa o valor que ELE disse, uma pergunta só, avança pra alocação)

Formato: responda APENAS com o texto da mensagem (sem aspas, sem "Sugestão:", sem explicação).
"""


def suggest_reply(
    messages: list[tuple[str, str, datetime]],
    reason: str = "",
    client_context: str = "",
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
    _ctx = f"\n\nContexto (motivo): {reason}" if reason else ""
    _bg = ""
    if client_context:
        _bg = ("\n\n=== CONTEXTO DO CLIENTE (uso interno — NÃO repita nada disto ao "
               "cliente; use só para deixar a resposta mais relevante e pessoal) ===\n"
               + client_context + "\n=== FIM CONTEXTO ===")
    user_prompt = (
        f"Conversa abaixo. Sugira uma resposta para o ASSESSOR retomar o atendimento.{_ctx}{_bg}\n\n"
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
        raw = raw.strip()
        for _dash in (" — ", " – ", "—", "–"):   # trava determinística: sem travessão (vício de IA)
            raw = raw.replace(_dash, ", ")
        raw = raw.replace(",  ", ", ").replace(" ,", ",")
        return raw or "[Sem sugestão gerada]"
    except Exception as exc:
        logger.error("suggest_reply failed: %s", exc)
        return f"[Erro ao gerar sugestão: {exc}]"
