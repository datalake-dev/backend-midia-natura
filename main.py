import functions_framework
from flask import jsonify
import datetime

# Um código super simples para testar o ambiente
@functions_framework.http
def gemini_chat(request):
    """Uma função de teste que sempre retorna sucesso."""
    
    # Imprime uma mensagem simples no log a cada chamada para vermos se o log está funcionando
    log_message = f"FUNÇÃO DE TESTE ACIONADA EM: {datetime.datetime.now()}"
    print(log_message)
    
    # Retorna uma resposta JSON simples e fixa
    # O frontend espera os campos 'text' e 'suggestions'
    return jsonify({
        'text': 'Olá! O serviço de teste está funcionando perfeitamente. ✅',
        'suggestions': ['Teste 1', 'Teste 2']
    })
