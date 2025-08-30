# Usa imagen oficial de Python
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de dependencias
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el proyecto al contenedor
COPY . .

# Comando por defecto para ejecutar el ETL con parámetro
CMD ["python", "src/etl_job.py", "--since", "2025-08-01"]