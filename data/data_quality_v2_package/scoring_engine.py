
def calculate_dimension_score(rules):
    total_weight = sum(r['weight'] for r in rules)
    score = sum(r['score'] * r['weight'] for r in rules) / total_weight
    return score

def calculate_final_score(dimensions, weights):
    return sum(dimensions[d] * weights[d] for d in dimensions) / sum(weights.values())
