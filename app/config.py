from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./grampo_dev.db"
    WEBHOOK_SECRET_TOKEN: str = ""
    DASHBOARD_PASSWORD: str = ""
    # JSON list of extra accesses:
    # [{"password":"abc","role":"viewer","agents":["Nome1","Nome2"]}, ...]
    # role "admin" ignores the agents list and sees everything.
    DASHBOARD_ACCESSES: str = ""
    # Secret used to sign the auth cookie. If empty, derived from DASHBOARD_PASSWORD.
    SESSION_SECRET: str = ""
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"
    # Audio transcription (Whisper). Either key works — Groq is free and is
    # tried first; OpenAI is the paid fallback. Both speak the same API.
    GROQ_API_KEY: str = ""          # Free Whisper via Groq (console.groq.com)
    OPENAI_API_KEY: str = ""        # Paid Whisper via OpenAI
    ANTHROPIC_API_KEY: str = ""     # Required for Claude-based "Sem Resposta" analysis
    # Token da API da Zenvia (Customer Cloud) p/ ENVIAR mensagens pela aba
    # Copiloto. Gerar em Configurações → Tokens e Webhooks. Vazio = envio desativado.
    ZENVIA_API_TOKEN: str = ""
    # Model id for interactive/quality-sensitive calls (sugestão de resposta).
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"
    # Model id for high-volume batch analysis (notas, oportunidades, Sem Resposta).
    # Same as interactive by choice (quality > cost). To cut spend ~3x, set
    # ANTHROPIC_MODEL_BULK=claude-haiku-4-5 in the environment — no deploy needed.
    ANTHROPIC_MODEL_BULK: str = "claude-sonnet-4-6"
    # Weekly auto-score: runs the agent-evaluation scan inside the app, every
    # Sunday at AUTO_SCORE_HOUR (Brasília). Recovers until Monday 06:59 if the
    # app was down. Disable with AUTO_SCORE_ENABLED=0 (no deploy needed).
    AUTO_SCORE_ENABLED: str = "1"
    AUTO_SCORE_HOUR: int = 22                 # domingo, 22h Brasília
    AUTO_SCORE_DAYS: int = 9                  # janela com folga; cache pula o resto
    AUTO_SCORE_CANAL: str = "5519997733651"   # canal principal
    # Endpoint HTTP /dashboard/cron/score-daily DESATIVADO por padrão: a
    # avaliação roda pelo agendador interno de domingo (AUTO_SCORE_*). Um cron
    # externo batendo nesse endpoint todo dia (days=30) era o que estourava o
    # custo. Reative com CRON_SCORE_ENABLED=1 se algum dia precisar.
    CRON_SCORE_ENABLED: str = "0"
    # Teto rígido de chamadas à API da Anthropic por dia (horário de Brasília),
    # somando avaliação, oportunidades, sem-resposta e sugestão. Ao atingir, as
    # features param de chamar a IA até a virada do dia (degradam pro cache).
    # Protege contra qualquer disparo acidental. 0 = sem limite.
    # 500 é conservador: cobre o uso normal (~150-250/dia em dias movimentados)
    # com folga, mas corta cedo um disparo descontrolado (o pico foi 1.761).
    LLM_DAILY_CAP: int = 500

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
