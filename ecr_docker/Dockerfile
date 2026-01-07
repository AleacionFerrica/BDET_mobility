# Usamos una imagen ligera de Python
FROM python:3.12-slim

# Instalamos dependencias del sistema (necesarias para compilar algunas libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Preparamos el directorio de trabajo
WORKDIR /app

# Copiamos los requisitos e instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el script ejecutor
COPY executor.py .

# Definimos el punto de entrada
ENTRYPOINT ["python", "executor.py"]