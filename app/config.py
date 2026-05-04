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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
