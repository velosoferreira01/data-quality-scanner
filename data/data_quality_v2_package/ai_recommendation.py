
def generate_recommendation(input_data):
    if input_data['null_pct'] > 0.2:
        return "Alta quantidade de nulos. Recomenda-se validação obrigatória na origem."
    if input_data['duplicate_pct'] > 0.05:
        return "Duplicidade detectada. Implementar chave única."
    return "Sem problemas críticos."
