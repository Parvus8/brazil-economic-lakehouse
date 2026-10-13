FROM python:3.12-slim-bookworm

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER app
# A extensão vem embutida na imagem, para o contêiner não precisar de internet em execução.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY --chown=app:app . .

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=4)"
CMD ["streamlit", "run", "app.py"]
