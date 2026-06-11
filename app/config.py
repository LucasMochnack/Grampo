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
    # Model id for interactive/quality-sensitive calls (suggestion de resposta).
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"
    # Model id for high-volume batch analysis (notas, oportunidades, Sem Resposta).
    # Haiku 4.5 is ~3x cheaper than Sonnet ($1 vs $3 per 1M input tokens) and
    # holds up well on classification/extraction. Override via env to revert.
    ANTHROPIC_MODEL_BULK: str = "claude-haiku-4-5"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
