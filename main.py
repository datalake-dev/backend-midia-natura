import os
import functions_framework
import google.generativeai as genai
from flask import make_response

# Pega a chave da API das variáveis de ambiente no Google Cloud
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Configura o modelo do Gemini
genai.configure(api_key=GEMINI_API_KEY)

# --- Persona Especialista em Mídia Paga para Natura ---
AGENT_PERSONA = (
    "Você é um especialista sênior em análise de mídia paga, trabalhando como consultor para a marca Natura. Sua missão é interpretar dados de performance de campanhas (gráficos e tabelas) e traduzi-los em insights claros, objetivos e acionáveis.

    **Suas diretrizes são:**
    1.  **Foco em Métricas (KPIs):** Sempre que analisar os dados, priorize métricas de performance como CPM, CPView 100% (custo por visualização completa), CTR (Taxa de Cliques), CPSessão e CPS20s ou CPQV (custo por sessão de 20s ou visita qualificada, a marca considera os dois nomes para KPIs de tráfego).
    2.  **Linguagem de Negócios:** Seja direto, conciso e use uma linguagem de negócios. Forneça a conclusão principal primeiro e depois os dados que a suportam.
    3.  **Contexto Natura:** Lembre-se que o objetivo não é apenas a venda direta, mas construir um crescimento sustentável e fortalecer Awareness e Consideração em topo e meio de funil. Conecte os dados a esses objetivos estratégicos.
    4.  **Sugira Ações e Otimizações:** Ao identificar uma tendência, seja uma oportunidade ou um risco, sugira ativamente um próximo passo. Por exemplo: 'A campanha de Dia das Mães mostra um CTR alto mas baixa conversão, sugerindo que devemos investigar e otimizar a experiência na página de destino do produto X ou explorar audiência Y'
    5.  **Idioma:** Responda sempre em português do Brasil."
)

# Criamos o modelo com a instrução de sistema (a persona) e o modelo Flash
model = genai.GenerativeModel(
    'gemini-2.5-flash',
    system_instruction=AGENT_PERSONA
)

@functions_framework.http
def gemini_chat(request):
    """Função HTTP que é acionada por uma requisição."""
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    if request.method == 'OPTIONS':
        return ('', 204, headers)

    if request.method != 'POST':
        return make_response(('Método não permitido', 405, headers))

    request_json = request.get_json(silent=True)

    if not request_json or 'prompt' not in request_json:
        return make_response(('Corpo da requisição inválido ou "prompt" ausente.', 400, headers))
    
    user_prompt = request_json['prompt']

    try:
        response = model.generate_content(user_prompt)
        api_response = make_response({'text': response.text}, 200, headers)
        return api_response
    except Exception as e:
        error_response = make_response(({'error': f'Erro ao chamar a API do Gemini: {e}'}), 500, headers)
        return error_response