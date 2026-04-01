
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Blueprint reutilizável do motor de recomendações.
Pode ser usado isoladamente no pipeline ou acoplado ao dashboard.
"""

from typing import Dict, List


def calculate_priority_score(
    criticality: int,
    business_impact: int,
    technical_severity: int,
    trend_penalty: int = 0,
    quick_win_bonus: int = 0,
) -> int:
    return criticality + business_impact + technical_severity + trend_penalty + quick_win_bonus


def classify_priority(priority_score: int) -> str:
    if priority_score >= 15:
        return "Imediata"
    if priority_score >= 10:
        return "Alta"
    if priority_score >= 6:
        return "Média"
    return "Baixa"


def recommend_by_dimension(lowest_dimension: str) -> Dict[str, str]:
    mapping = {
        "Completude": {
            "root_cause": "Campos obrigatórios sem preenchimento consistente na origem.",
            "recommendation": "Aplicar obrigatoriedade, preenchimento default controlado e monitoração de nulos."
        },
        "Consistência": {
            "root_cause": "Regras cruzadas entre colunas não estão sendo validadas.",
            "recommendation": "Criar validações de coerência na ingestão e na camada analítica."
        },
        "Unicidade": {
            "root_cause": "Não existe chave única confiável ou deduplicação por negócio.",
            "recommendation": "Implementar chave de negócio e deduplicação antes da persistência final."
        },
        "Validade": {
            "root_cause": "Formato, domínio ou tipagem chegam inválidos.",
            "recommendation": "Padronizar regex, domínio permitido e casting controlado."
        },
        "Atualidade": {
            "root_cause": "SLA de atualização não está sendo cumprido.",
            "recommendation": "Revisar agenda, carga incremental e monitoramento do atraso."
        },
        "Integridade": {
            "root_cause": "Existem chaves órfãs ou quebra de referência entre entidades.",
            "recommendation": "Aplicar validação referencial e reconciliação automática."
        },
    }
    return mapping.get(lowest_dimension, {
        "root_cause": "Causa raiz ainda não classificada.",
        "recommendation": "Revisar regras e padrão de origem."
    })
