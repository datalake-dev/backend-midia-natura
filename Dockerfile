# Use a imagem base oficial do Python
FROM python:3.12-slim

# Defina o diretório de trabalho no contêiner
WORKDIR /app

# Copie o arquivo de dependências para o diretório de trabalho
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie o resto do código da aplicação para o diretório de trabalho
COPY . .

# Exponha a porta em que o aplicativo será executado (o padrão do Cloud Run é 8080)
EXPOSE 8080

# Comando para executar a aplicação
CMD ["functions-framework", "--target=gemini_chat", "--port=8080"]