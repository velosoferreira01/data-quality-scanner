# -*- coding: utf-8 -*-

from pathlib import Path
import re

TARGET = Path(r"C:\Users\Daniel\Downloads\Data_Quality\data\data_quality_v2_package\export_data_quality_report_v2_integrated.py")

new_function = r'''
def export_radar_html(path, run_id, dim_df):
    import plotly.graph_objects as go

    if dim_df is None or dim_df.empty:
        html = f"<html><body><h1>Sem dados para radar</h1></body></html>"
        path.write_text(html, encoding="utf-8")
        return

    work = dim_df.copy()

    if "dataset_label" not in work.columns:
        work["dataset_label"] = work["table_name"]

    dim_map = [
        ("dim_consistencia", "Consistência"),
        ("dim_completude", "Completude"),
        ("dim_integridade_ref", "Integridade"),
        ("dim_freshness", "Atualidade"),
        ("dim_validade", "Validade"),
        ("dim_unicidade", "Unicidade"),
    ]

    categories = [label for _, label in dim_map if _ in work.columns]
    fig = go.Figure()

    for _, row in work.iterrows():
        values = []
        for col, _label in dim_map:
            if col in work.columns:
                try:
                    values.append(float(row.get(col, 0)))
                except:
                    values.append(0)

        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories,
                fill="toself",
                name=str(row["dataset_label"])
            )
        )

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
        showlegend=True
    )

    html = fig.to_html(include_plotlyjs="cdn")
    path.write_text(html, encoding="utf-8")
'''

def main():
    if not TARGET.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {TARGET}")

    text = TARGET.read_text(encoding="utf-8")

    pattern = r"def export_radar_html\(.*?\):.*?def export_history_html"
    replacement = new_function + "\n\ndef export_history_html"

    new_text = re.sub(pattern, replacement, text, flags=re.S)

    TARGET.write_text(new_text, encoding="utf-8")

    print("[OK] Patch aplicado com sucesso!")

if __name__ == "__main__":
    main()