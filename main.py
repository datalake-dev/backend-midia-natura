# Adicionamos 'traceback' para podermos imprimir o erro detalhado
import os
import re
import functions_framework
import google.generativeai as genai
from flask import make_response, jsonify
from google.cloud import bigquery
import pandas as pd
import traceback # IMPORTAÇÃO ADICIONAL

# --- O CÓDIGO COMEÇA A SER ENVOLVIDO PELO 'TRY' AQUI ---
try:
    # --- CONFIGURAÇÕES DO PROJETO (PREENCHIDO) ---
    PROJECT_ID = "africa-br"
    DATASET_ID = "NaturaProcessData"
    TABLE_ID = "PreClique"

    TABLE_SCHEMA_DESCRIPTION = """
    (
        Submarca STRING, Campanha STRING, ObjetivodeComunicacao STRING, Plataforma STRING, FreeText1 STRING, ObjetivoDeMidia STRING, TipoAtivacao STRING, Estrategia STRING, Segmentacao STRING, FaixaEtaria STRING, Genero STRING, Praca STRING, Freetext2 STRING, CategoriaAd STRING, SubmarcaAd STRING, AdFront STRING, Formato STRING, Dimensao STRING, Segundagem STRING, Skippable STRING, Pilar STRING, AcaoComInfluenciador STRING, Influenciador STRING, RT1 STRING, LinhaCriativa STRING, CTA STRING, TipoDeCompra STRING, Canal STRING, PrecisionMkt STRING, ChaveGa STRING, Cost FLOAT64, Revenue FLOAT64, Impressions INT64, Clicks INT64, VideoViews INT64, ThreeSecondsVideoViews INT64, TwoSecondsVideoViews INT64, SixSecondsVideoViews INT64, VideoViews25 INT64, VideoViews50 FLOAT64, VideoViews75 INT64, VideoViews100 INT64, Comments INT64, Likes INT64, Saves INT64, Shares INT64, Follows INT64
    )
    """
    # --------------------------------------------------------------------

    # Inicializa os clientes do Google
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    genai.configure(api_key=GEMINI_API_KEY)
    bigquery_client = bigquery.Client(project=PROJECT_ID)

    # --- PERSONAS E PROMPTS ---
    AGENT_PERSONA = """Você é um especialista sênior em análise de mídia paga para a Natura. Sua missão é analisar dados e responder perguntas de forma clara, acionável e visualmente organizada.

    **Regra de Formatação:** Sempre formate suas respostas usando Markdown para melhor clareza. Use negrito (`**texto**`) para destacar métricas e organize dados comparativos em tabelas simples. Responda sempre em português do Brasil."""

    SQL_GENERATOR_PROMPT = f"""Você é um expert em GoogleSQL (SQL do BigQuery). Sua única função é converter uma pergunta em linguagem natural em uma consulta SQL otimizada para uma tabela chamada `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`.

    O esquema da tabela é:
    {TABLE_SCHEMA_DESCRIPTION}

    **Regra de Ouro:** Use **EXCLUSIVAMENTE** os nomes de colunas fornecidos no esquema acima. Não infira ou utilize qualquer outro nome de coluna ou pseudo-coluna (como `_PARTITIONTIME`). Todos os filtros e seleções devem se referir a colunas existentes no esquema.

    **Métricas Calculadas (Fórmulas):**
    - **CPM:** `(SUM(Cost) / SUM(Impressions)) * 1000`
    - **CTR:** `(SUM(Clicks) / SUM(Impressions)) * 100`
    - **CPC:** `(SUM(Cost) / SUM(Clicks)`
    - **CPView 100% ou CPV:** `(SUM(Cost) / SUM(VideoViews100))`
    - **VTR 100%:** `(SUM(VideoViews100) / SUM(Impressions))`

    **Regras de Geração:**
    1.  **Análise para Otimização:** Se a pergunta for sobre otimização ou comparação, retorne a métrica principal AGRUPADA pelas dimensões mais relevantes.
    2.  **Análise de Tendência:** Se a pergunta envolver "tendência" ou "evolução", agrupe a métrica por `DATE_TRUNC(data, MONTH)` (para mês) ou `data` (para dia), e ordene pela data. A coluna de data se chama `data`.
    3.  Responda APENAS com o código SQL.
    4.  Use a cláusula `WHERE` sempre que possível.
    5.  **Filtros Flexíveis e Sinônimos:** Aplique filtros flexíveis (`LOWER(coluna) LIKE '%termo%'`) para: `Campanha`, `Plataforma`, `Segmentacao` (sinônimos: audiência), `Canal`, `ObjetivoDeMidia`, `ObjetivodeComunicacao`, `PrecisionMkt` (sinônimos: precision, pm).
    6.  A
