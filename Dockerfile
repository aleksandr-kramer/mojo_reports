# Базовый образ: Python 3.12 (slim)
FROM python:3.12-slim

# Не буферизовать вывод Python + не проверять обновления pip
ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Europe/Podgorica

# Обновим apt и поставим tzdata (время в контейнере) + системные сертификаты
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Рабочая директория приложения
WORKDIR /app

# Сначала внесём только requirements для кеша слоёв
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Затем весь код проекта (кроме того, что отфильтрует .dockerignore)
COPY src/ /app/src/
COPY sql/ /app/sql/
COPY config/ /app/config/
COPY README.md /app/README.md

# Создадим непривилегированного пользователя
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# По умолчанию контейнер ничего не запускает.
# Все команды исполняем через `docker compose run --rm app ...`
CMD ["python", "--version"]
