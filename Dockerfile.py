FROM python:3.11-slim

WORKDIR /app

# Copia requirements e instala no build (fica permanente na imagem)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Mantém o container aberto
CMD ["bash"]