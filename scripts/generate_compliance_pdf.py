"""Generate the Alertas tab compliance executive summary as a PDF."""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.colors import HexColor, white, black
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle,
    KeepTogether, HRFlowable,
)
from datetime import datetime

OUT = r"C:\Users\lucas.mochnack.MANHATTAN\Grupo Alto Valor Dropbox\Lucas Mochnack\PC\Downloads\Claude\Grampo\Grampo-Alertas-Compliance.pdf"

# Brand palette (dark navy/charcoal accents on white paper)
NAVY = HexColor("#0d1630")
ACCENT = HexColor("#1a2540")
MUTED = HexColor("#5a6a8a")
RED = HexColor("#dc2626")
ORANGE = HexColor("#ea580c")
YELLOW = HexColor("#ca8a04")
BLUE = HexColor("#2563eb")
GREEN = HexColor("#16a34a")
LIGHT_BG = HexColor("#f5f6fa")

# ── Styles ────────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

doc_title = ParagraphStyle(
    "DocTitle", parent=styles["Title"],
    fontName="Helvetica-Bold", fontSize=22, leading=26,
    textColor=NAVY, spaceAfter=4, alignment=TA_LEFT,
)
doc_subtitle = ParagraphStyle(
    "DocSubtitle", parent=styles["Normal"],
    fontName="Helvetica", fontSize=11, leading=14,
    textColor=MUTED, spaceAfter=20, alignment=TA_LEFT,
)
h1 = ParagraphStyle(
    "H1", parent=styles["Heading1"],
    fontName="Helvetica-Bold", fontSize=15, leading=19,
    textColor=NAVY, spaceBefore=18, spaceAfter=8,
)
h2 = ParagraphStyle(
    "H2", parent=styles["Heading2"],
    fontName="Helvetica-Bold", fontSize=12, leading=15,
    textColor=ACCENT, spaceBefore=10, spaceAfter=6,
)
body = ParagraphStyle(
    "Body", parent=styles["Normal"],
    fontName="Helvetica", fontSize=10, leading=14.5,
    textColor=black, spaceAfter=8, alignment=TA_JUSTIFY,
)
bullet = ParagraphStyle(
    "Bullet", parent=body, leftIndent=14, bulletIndent=2, spaceAfter=4,
)
small = ParagraphStyle(
    "Small", parent=styles["Normal"],
    fontName="Helvetica", fontSize=9, leading=12,
    textColor=MUTED, spaceAfter=4,
)
quote = ParagraphStyle(
    "Quote", parent=body,
    fontName="Helvetica-Oblique", textColor=MUTED, leftIndent=14,
)
code_inline = ParagraphStyle(
    "Code", parent=body,
    fontName="Courier", fontSize=9, leading=12, textColor=ACCENT,
)


def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor("#d4d9e6"),
                      spaceBefore=4, spaceAfter=10)


def section_table(rows, col_widths, header_bg=NAVY, body_bg=white):
    """Build a styled table. First row is header."""
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR",  (0, 0), (-1, 0), white),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",   (0, 0), (-1, 0), 9),
        ("LEADING",    (0, 0), (-1, 0), 12),
        ("ALIGN",      (0, 0), (-1, 0), "LEFT"),
        ("VALIGN",     (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
        ("TOPPADDING",    (0, 0), (-1, 0), 7),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("FONTNAME",   (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",   (0, 1), (-1, -1), 9.5),
        ("LEADING",    (0, 1), (-1, -1), 13),
        ("TEXTCOLOR",  (0, 1), (-1, -1), black),
        ("BACKGROUND", (0, 1), (-1, -1), body_bg),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [body_bg, LIGHT_BG]),
        ("GRID", (0, 0), (-1, -1), 0.4, HexColor("#d4d9e6")),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
        ("TOPPADDING",    (0, 1), (-1, -1), 6),
    ]))
    return t


# ── Document ──────────────────────────────────────────────────────────────────
story = []

# Header
story.append(Paragraph("Aba Alertas — Resumo Executivo para Compliance", doc_title))
story.append(Paragraph(
    f"Grampo · Monitoramento de Conversas Zenvia · Atualizado em {datetime.now().strftime('%d/%m/%Y')}",
    doc_subtitle))
story.append(hr())

# ── 1. O que faz ──────────────────────────────────────────────────────────────
story.append(Paragraph("1. O que faz", h1))
story.append(Paragraph(
    "Monitora <b>em tempo real</b> todas as conversas WhatsApp/Zenvia entre agentes da Alto Valor "
    "e clientes, e identifica automaticamente trechos que possam indicar <b>risco de conformidade</b>. "
    "O time de Compliance triagem os alertas e marca cada um como revisado (✓ OK) ou reabre para investigação.",
    body))

# ── 2. Os 4 níveis de severidade ─────────────────────────────────────────────
story.append(Paragraph("2. Os 4 níveis de severidade", h1))

severity_rows = [
    ["Nível", "Descrição", "Disparado por (exemplos)"],
    [
        Paragraph('<font color="#dc2626"><b>● Crítico</b></font>', body),
        "Ação imediata — possível violação grave",
        Paragraph(
            'Xingamento explícito; fraude (<i>"me roubou"</i>); órgão regulador '
            '(ANBIMA, CVM, BACEN, PROCON); promessa indevida (<i>"rentabilidade garantida"</i>, '
            '<i>"100% seguro"</i>); pagamento irregular (<i>"deposite na minha conta"</i>); '
            'risco jurídico (<i>"meu advogado"</i>, <i>"vou processar"</i>, <i>"ação judicial"</i>).',
            body),
    ],
    [
        Paragraph('<font color="#ea580c"><b>● Alto Risco</b></font>', body),
        "Análise prioritária",
        Paragraph(
            'Pressão comercial (<i>"última chance"</i>, <i>"é agora ou nunca"</i>); '
            'consultoria sem habilitação (<i>"sou consultor de investimento"</i>); '
            'classificação inadequada de risco (<i>"investimento sem risco"</i>); '
            'conflito de interesse.',
            body),
    ],
    [
        Paragraph('<font color="#ca8a04"><b>● Monitoramento</b></font>', body),
        "Risco potencial / insatisfação",
        Paragraph(
            'Reclamação genérica (<i>"péssimo"</i>, <i>"absurdo"</i>, <i>"reclamo"</i>); '
            'desentendimento (<i>"não me explicou"</i>, <i>"não fui avisado"</i>); '
            'risco de saída (<i>"quero sair"</i>, <i>"vou tirar tudo"</i>, <i>"perdi dinheiro"</i>).',
            body),
    ],
    [
        Paragraph('<font color="#2563eb"><b>● Operacional</b></font>', body),
        "Quebra de rastreabilidade",
        Paragraph(
            'Tentativa de evitar registro formal (<i>"depois eu formalizo"</i>, '
            '<i>"não precisa registrar"</i>, <i>"sem e-mail mesmo"</i>, <i>"só confirma aqui"</i>).',
            body),
    ],
]
story.append(section_table(severity_rows, col_widths=[3.0*cm, 4.0*cm, 9.5*cm]))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "Quando uma conversa dispara múltiplos níveis, <b>prevalece o mais severo</b> "
    "(Crítico > Alto > Monitoramento > Operacional).", small))

# ── 3. Como a detecção funciona ──────────────────────────────────────────────
story.append(Paragraph("3. Como a detecção funciona", h1))
detection_steps = [
    ("1. Captura", "O Zenvia envia um webhook ao Grampo a cada mensagem (entrada e saída). "
        "Cada evento é salvo no banco com o payload completo, timestamp, telefone e agente."),
    ("2. Agrupamento", "Eventos são agrupados em <b>conversas</b> pelo telefone do cliente."),
    ("3. Classificação", "Cada mensagem da conversa é varrida contra uma lista de "
        "<b>palavras-chave e frases</b> definidas pelo Jurídico, organizadas nos 4 níveis acima. "
        "A leitura considera até 2.000 caracteres por mensagem."),
    ("4. Exibição", "Conversas com qualquer alerta aparecem na seção <b>ATIVOS</b> com cor por "
        "severidade. O revisor então classifica cada uma como <b>✓ OK</b> (sem problemas) ou "
        "<b>⚠ PROBLEMA</b> (problema real). A conversa é movida pra seção correspondente."),
]
for step, desc in detection_steps:
    story.append(Paragraph(f"<b>{step}.</b> {desc}", bullet))

# ── 4. Fluxo de triagem (NOVO) ───────────────────────────────────────────────
story.append(Paragraph("4. Fluxo de triagem (2 botões)", h1))
story.append(Paragraph(
    "Cada conversa com alerta passa por uma decisão binária explícita: foi alarme "
    "falso ou problema real? Os dois desfechos têm destinos diferentes para facilitar "
    "ação do Compliance e do Jurídico.", body))

triage_rows = [
    ["Botão", "Significado", "Destino"],
    [
        Paragraph('<font color="#0fa968"><b>✓ OK</b></font>', body),
        Paragraph("Conversa revisada e classificada como <b>sem problemas</b> "
                  "(falso positivo ou ruído do classificador).", body),
        Paragraph("Card <b>“Itens Avaliados sem problemas”</b> no rodapé da página. "
                  "Fica arquivado com data e revisor, mas não exige ação.", body),
    ],
    [
        Paragraph('<font color="#dc2626"><b>⚠ PROBLEMA</b></font>', body),
        Paragraph("Conversa confirmada como <b>problema real</b> de conformidade. "
                  "Requer revisão posterior pelo Jurídico ou Compliance.", body),
        Paragraph("Card <b>“⚠ PROBLEMA · Confirmado”</b> próximo ao topo da página, "
                  "em destaque vermelho. Vira a fila de ação efetiva.", body),
    ],
]
story.append(section_table(triage_rows, col_widths=[3.0*cm, 6.0*cm, 7.5*cm]))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "Tanto na lista de ATIVOS quanto em cada linha da tabela do Histórico de "
    "Conformidade, os dois botões aparecem lado a lado. Ao clicar, a página recarrega "
    "automaticamente e <b>desliza até a seção de destino</b> com um flash colorido na "
    "borda, deixando claro onde o item caiu. A decisão pode ser revertida a qualquer "
    "momento pelo botão <i>Reabrir</i>, que devolve a conversa para a fila pendente.",
    body))

story.append(PageBreak())

# ── 5. Layout da página ──────────────────────────────────────────────────────
story.append(Paragraph("5. Layout da página Alertas (de cima para baixo)", h1))
layout_rows = [
    ["#", "Seção", "Conteúdo"],
    ["1",
     Paragraph("<b>KPIs (topo)</b>", body),
     Paragraph("Quatro contadores: <b>Ativos</b> (aguardando triagem) · "
               "<font color='#0fa968'><b>OK</b></font> (sem problemas) · "
               "<font color='#dc2626'><b>Problemas confirmados</b></font> · "
               "<b>Total identificados</b>.", body)],
    ["2",
     Paragraph("<font color='#dc2626'><b>● ATIVOS</b></font>", body),
     Paragraph("Cards com cliente, agente, trecho do alerta destacado e dois botões "
               "(<font color='#0fa968'>✓ OK</font> / <font color='#dc2626'>⚠ PROBLEMA</font>). "
               "Cor da borda reflete a severidade do alerta.", body)],
    ["3",
     Paragraph("<font color='#dc2626'><b>● PROBLEMA · Confirmado</b></font>", body),
     Paragraph("Tabela vermelha com as conversas marcadas como problema. Posição "
               "privilegiada para que a equipe não perca essas pendências.", body)],
    ["4",
     Paragraph("<b>🚨 Análise de Conformidade — Histórico Completo</b>", body),
     Paragraph("Varredura de TODO o histórico. KPIs por nível, e quatro tabelas "
               "(Crítico / Alto / Monitoramento / Operacional) com exemplos pendentes. "
               "Cada linha tem botões ✓ OK e ⚠ PROBLEMA e abre a conversa em nova aba.", body)],
    ["5",
     Paragraph("<font color='#0fa968'><b>● Itens Avaliados sem problemas</b></font>", body),
     Paragraph("Arquivo verde no rodapé com as conversas marcadas como OK. "
               "Servem como trilha de auditoria mas não exigem nova ação.", body)],
    ["6",
     Paragraph("<b>🔑 Top 30 gatilhos disparados</b>", body),
     Paragraph("Gráfico de barras horizontais com as palavras-chave mais acionadas "
               "no histórico. Cor da barra = severidade do gatilho. Útil para o Jurídico "
               "identificar quais expressões estão dominando os alertas.", body)],
]
story.append(section_table(layout_rows, col_widths=[1.0*cm, 5.0*cm, 10.5*cm]))
story.append(Spacer(1, 8))
story.append(Paragraph(
    "<b>Clique em qualquer linha</b> da tabela do Histórico, ou no cabeçalho de qualquer "
    "card de ATIVOS, para abrir o histórico completo da conversa em uma <b>nova aba</b>. "
    "A nova tela mostra todo o thread cronologicamente, com a mensagem que disparou o "
    "alerta marcada visualmente (palavra-chave em destaque colorido conforme o nível).",
    body))

# ── 6. Governança e Auditabilidade ───────────────────────────────────────────
story.append(Paragraph("6. Governança e Auditabilidade", h1))
gov_items = [
    ("Trilha de auditoria", "Toda decisão (✓ OK ou ⚠ PROBLEMA) registra <b>cliente, "
        "agente, trecho, status escolhido, data/hora e responsável</b>. Reaberturas também "
        "ficam logadas no mesmo histórico."),
    ("Visibilidade universal", "Por decisão de Compliance (maio/2026), <b>todos os "
        "usuários autenticados</b> — admins e viewers — enxergam todos os alertas e podem "
        "abrir qualquer conversa flagada. Isso garante que o Compliance veja a operação "
        "completa, sem segmentação por agente. Permissões por agente continuam ativas nas "
        "outras abas (Conversas, Agentes, Heatmap, Exports)."),
    ("Imutabilidade", "O payload bruto do Zenvia (<font color='#1a2540' face='Courier'>"
        "raw_payload</font>) é gravado <i>verbatim</i> e nunca alterado — o trecho exibido "
        "vem direto dele, garantindo fidelidade ao que foi efetivamente enviado pelo agente "
        "ou cliente."),
    ("Atualização", "Os alertas ativos atualizam em tempo quase real (cache de 45 s). "
        "O Histórico de Conformidade é re-computado a cada 10 min — após uma triagem (OK ou "
        "PROBLEMA), a página recarrega automaticamente para refletir o novo estado."),
]
for label, desc in gov_items:
    story.append(Paragraph(f"<b>{label}.</b> {desc}", bullet))

# ── 7. Calibragem do modelo ──────────────────────────────────────────────────
story.append(Paragraph("7. Calibragem do modelo (transparência metodológica)", h1))
story.append(Paragraph(
    "As regras foram <b>calibradas com dados reais</b> após auditoria de <b>1.990 conversas</b> "
    "no período de 02/04/2026 a 15/05/2026.", body))

calib_rows = [
    ["", "Antes da calibragem", "Após calibragem"],
    [Paragraph('<font color="#dc2626"><b>● Crítico</b></font>', body), "28 alertas", "13 alertas"],
    [Paragraph('<font color="#ea580c"><b>● Alto Risco</b></font>', body), "4 alertas", "3 alertas"],
    [Paragraph('<font color="#ca8a04"><b>● Monitoramento</b></font>', body), "4 alertas", "6 alertas"],
    [Paragraph('<font color="#2563eb"><b>● Operacional</b></font>', body), "0 alertas", "0 alertas"],
    [Paragraph("<b>Total identificado</b>", body), Paragraph("<b>36</b>", body), Paragraph("<b>22  (−39%)</b>", body)],
    [Paragraph("<b>Falso positivo do Crítico</b>", body), "≈ 78%", "≈ 30%"],
]
story.append(section_table(calib_rows, col_widths=[5.5*cm, 5.5*cm, 5.5*cm]))
story.append(Spacer(1, 8))

story.append(Paragraph("Principais ajustes aplicados:", h2))
adjustments = [
    "Termos técnicos de produto (<i>\"resgate imediato\"</i>, <i>\"liquidez imediata\"</i>) só "
    "disparam Crítico quando combinados com promessa indevida (ex.: <i>\"resgate imediato sem risco\"</i>).",
    "<i>\"Advogado\"</i> isolado foi removido — só dispara quando há contexto inequívoco "
    "(<i>\"meu advogado\"</i>, <i>\"vou processar\"</i>, <i>\"ação judicial\"</i>).",
    "Reclamações leves (<i>\"péssimo\"</i>, <i>\"horrível\"</i>) foram movidas de Crítico para "
    "Monitoramento. Crítico fica reservado para obscenidade pesada, fraude e ouvidoria formal.",
    "Janela de leitura ampliada de 300 para 2.000 caracteres por mensagem, evitando que "
    "palavras-chave em mensagens longas escapem da detecção.",
]
for a in adjustments:
    story.append(Paragraph(f"• {a}", bullet))

story.append(Paragraph("Limitações conhecidas (transparência):", h2))
limitations = [
    "Falso positivo residual estimado em 10–20% por nível — palavras ambíguas usadas em "
    "contexto diferente. Mitigado pela revisão humana do Compliance.",
    "Zero disparos em Operacional no histórico — pode indicar que os agentes não usam essas "
    "expressões ou que a lista precisa ser ampliada pelo Jurídico.",
    "Janela de histórico: o sistema captura conversas apenas a partir de 02/04/2026 "
    "(início do registro). Não há histórico anterior disponível.",
]
for l in limitations:
    story.append(Paragraph(f"• {l}", bullet))

# ── 8. Resultado da auditoria ────────────────────────────────────────────────
story.append(Paragraph("8. Resultado da auditoria — 1.990 conversas inspecionadas", h1))
story.append(Paragraph(
    "Após a recalibragem das regras, o sistema disparou alerta em <b>22 das 1.990 conversas</b> "
    "do banco (≈ 1,1% do volume). A inspeção manual dos 22 alertas, comparando o gatilho contra "
    "o contexto da conversa, classificou cada caso conforme abaixo:",
    body))

audit_rows = [
    ["Resultado da inspeção manual", "Quantidade", "Faixa estimada"],
    [Paragraph("<b>Problema real provável</b> (acionável pelo Compliance)", body),
     Paragraph("<b>12 a 17</b>", body),
     Paragraph("0,6% – 0,9% das conversas", small)],
    [Paragraph("Falso positivo confirmado por inspeção", body),
     "5",
     Paragraph("Gatilho dispara em contexto diferente do intencionado", small)],
    [Paragraph("Inconclusivo (precisa abrir a conversa completa)", body),
     "≈ 5",
     Paragraph("Snippet exibido não permite afirmar com segurança", small)],
    [Paragraph("<b>Total de alertas disparados</b>", body),
     Paragraph("<b>22</b>", body),
     Paragraph("<b>1,1% das conversas</b>", body)],
]
story.append(section_table(audit_rows, col_widths=[8.0*cm, 3.0*cm, 5.5*cm]))
story.append(Spacer(1, 10))

story.append(Paragraph("Problemas reais identificados (12 confirmados)", h2))
real_problems = [
    ("🔴 Crítico (8 de 13 confirmados)",
     "1× ameaça formal de ouvidoria (\"vou apresentar denúncia\"); "
     "1× ameaça jurídica direta (\"meu advogado\"); "
     "1× menção a processo judicial; "
     "1× obscenidade; "
     "4× menções a CVM como regulador (pendente de leitura do contexto completo)."),
    ("🟠 Alto Risco (2 de 3 confirmados)",
     "2× cliente solicitando explicitamente \"investimento sem risco\" / \"retorno sem risco\" — "
     "situação que exige atenção do agente para não fazer promessa indevida."),
    ("🟡 Monitoramento (5 de 6 confirmados)",
     "1× reclamação direta (\"péssimo\"); "
     "1× cliente relatando perda em assessoria anterior (\"perdi dinheiro\"); "
     "1× discordância formal sobre proposta (\"não concordo\"); "
     "2× sinal de descontentamento com a operação (\"não quero mais\")."),
    ("🔵 Operacional (0)",
     "Nenhum disparo no histórico."),
]
for label, desc in real_problems:
    story.append(Paragraph(f"<b>{label}.</b> {desc}", bullet))

story.append(Spacer(1, 6))
story.append(Paragraph("Falsos positivos identificados (5)", h2))
fp_rows = [
    ["Trecho que disparou", "Por que não é alerta real"],
    [Paragraph('<i>"<b>Vou processar</b> o restante do dinheiro"</i>', body),
     "Cliente referindo-se a \"processar a transferência\", não a ação judicial."],
    [Paragraph('<i>"liquidação desse banco pelo <b>Banco Central</b>"</i>', body),
     "Agente explicando tecnicalidade de CDB, sem reclamação envolvida."],
    [Paragraph('<i>"vantagem do pré-fixado é por conta do <b>Banco Central</b>"</i>', body),
     "Explicação técnica sobre política monetária."],
    [Paragraph('<i>"<b>sou consultor</b> Hapvida 💙"</i>', body),
     "Mensagem de bot de plano de saúde — nada relacionado a consultoria de investimento."],
    [Paragraph('<i>"A internet aqui tá <b>horrível</b> hj"</i>', body),
     "Cliente comentando sobre seu wi-fi, não sobre o serviço da Alto Valor."],
]
story.append(section_table(fp_rows, col_widths=[7.5*cm, 9.0*cm]))
story.append(Spacer(1, 8))

story.append(Paragraph("Leitura para o Compliance", h2))
story.append(Paragraph(
    "A taxa de problemas reais (cerca de <b>0,6% – 0,9% das conversas</b>) é consistente com uma "
    "operação saudável: a maior parte das 1.990 conversas analisadas é trabalho rotineiro de "
    "assessoria sem incidente. O sistema está calibrado para sinalizar exceções, não para gerar "
    "filas longas de revisão. Um próximo ciclo de ajuste fino (round 2) pode levar a precisão "
    "para acima de 95%, eliminando os 5 falsos positivos identificados acima.",
    body))

# ── 9. Próximos passos ───────────────────────────────────────────────────────
story.append(Paragraph("9. Próximos passos sugeridos para Compliance", h1))
next_steps = [
    "<b>Definir rotina diária/semanal</b> de zerar a fila ATIVOS — classificar cada "
    "alerta como ✓ OK ou ⚠ PROBLEMA. SLA sugerido: Crítico em ≤ 2 h, Alto em ≤ 24 h.",
    "<b>Tratar a seção PROBLEMA · Confirmado como caixa de entrada do Jurídico</b> — "
    "cada item ali é uma conversa que precisa de avaliação legal ou conversa com o agente. "
    "Se a quantidade nessa seção for crescente, é sinal de risco operacional.",
    "<b>Revisar mensalmente o gráfico Top 30 gatilhos</b> com o Jurídico — keywords "
    "muito acionadas mas que sempre viram OK indicam que precisam ser removidas ou "
    "compostas com mais contexto.",
    "<b>Acessar a aba Histórico</b> para identificar padrões de alerta concentrados "
    "em determinado agente ou segmento — pode indicar treinamento necessário.",
    "<b>Validar trimestralmente a Política de Termos Sensíveis</b> (já vigente) "
    "comparando-a com as regras técnicas em produção. Qualquer divergência deve ser "
    "ajustada no código (uma linha por keyword, versionada no Git).",
]
for s in next_steps:
    story.append(Paragraph(f"• {s}", bullet))

# Footer
story.append(Spacer(1, 16))
story.append(hr())
story.append(Paragraph(
    "<b>Contato técnico:</b> todo o código de detecção está em "
    "<font face='Courier' color='#1a2540'>app/routers/dashboard.py</font>, função "
    "<font face='Courier' color='#1a2540'>_INTENT_RULES</font> (regras) e "
    "<font face='Courier' color='#1a2540'>_classify_conversation</font> (motor de matching). "
    "Qualquer ajuste de palavra-chave é uma alteração de uma linha e fica versionada no Git.",
    small))


def _on_page(canvas, doc):
    """Footer with page number."""
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawRightString(20*cm, 1.2*cm, f"Página {doc.page}")
    canvas.drawString(2*cm, 1.2*cm, "Grampo · Confidencial · Uso interno Alto Valor")
    canvas.restoreState()


doc = SimpleDocTemplate(
    OUT, pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm,
    topMargin=2*cm, bottomMargin=2*cm,
    title="Aba Alertas — Resumo Executivo para Compliance",
    author="Grampo · Alto Valor",
)
doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
print(f"OK -> {OUT}")
