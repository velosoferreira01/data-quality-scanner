# -*- coding: utf-8 -*-
"""
MJV Data Quality - Radar Executivo (Nível Diretoria)

Função:
    save_radar_chart_executive(dimensions_df, output_path, previous_df=None)

Requisitos:
    pip install plotly kaleido
"""

import plotly.graph_objects as go
import plotly.io as pio
from pathlib import Path


def save_radar_chart_executive(dimensions_df, output_path, previous_df=None):
    """
    dimensions_df: DataFrame com colunas [dimension, score]
    previous_df: opcional (mesma estrutura)
    """

    categories = dimensions_df["dimension"].tolist()
    values = dimensions_df["score"].tolist()

    # Fechamento do radar
    categories += [categories[0]]
    values += [values[0]]

    # Cor dinâmica baseada na média
    avg_score = sum(values[:-1]) / len(values[:-1])

    if avg_score >= 8:
        color = "green"
        status = "Excelente"
    elif avg_score >= 6:
        color = "orange"
        status = "Atenção"
    else:
        color = "red"
        status = "Crítico"

    fig = go.Figure()

    # Radar atual
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='Atual',
        line=dict(color=color)
    ))

    # Radar anterior (se existir)
    if previous_df is not None:
        prev_values = previous_df["score"].tolist()
        prev_values += [prev_values[0]]

        fig.add_trace(go.Scatterpolar(
            r=prev_values,
            theta=categories,
            fill='toself',
            name='Anterior',
            line=dict(color='gray', dash='dash')
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )
        ),
        showlegend=True,
        title=dict(
            text="Qualidade por Dimensão",
            x=0.5,
            font=dict(size=20)
        )
    )

    plot_div = pio.to_html(fig, full_html=False)

    html_content = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Radar Executivo</title>
        <style>
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background-color: #ffffff;
            }}

            .header {{
                background-color: #1f4e79;
                color: white;
                padding: 25px 40px;
            }}

            .header-title {{
                font-size: 12px;
                letter-spacing: 2px;
                opacity: 0.8;
            }}

            .header-main {{
                font-size: 30px;
                font-weight: bold;
                margin-top: 5px;
            }}

            .header-sub {{
                font-size: 14px;
                margin-top: 5px;
                opacity: 0.9;
            }}

            .container {{
                padding: 40px;
            }}

            .insight-box {{
                background-color: #f5f7fa;
                padding: 15px;
                border-left: 5px solid #1f4e79;
                margin-bottom: 20px;
                font-size: 14px;
            }}

            .footer {{
                text-align: center;
                font-size: 12px;
                color: #666;
                padding: 20px;
                border-top: 1px solid #ddd;
            }}
        </style>
    </head>

    <body>

        <div class="header">
            <div class="header-title">MJV DATA QUALITY</div>
            <div class="header-main">Validação de Qualidade por Dimensão</div>
            <div class="header-sub">
                Avaliação consolidada das dimensões críticas de qualidade de dados
            </div>
        </div>

        <div class="container">

            <div class="insight-box">
                Score médio: <b>{round(avg_score,2)}</b> |
                Classificação: <b>{status}</b>
            </div>

            {plot_div}

        </div>

        <div class="footer">
            Qualidade por dimensão MJV - 2026
        </div>

    </body>
    </html>
    """

    Path(output_path).write_text(html_content, encoding="utf-8")

    # Export PNG
    png_path = str(output_path).replace(".html", ".png")

    try:
        fig.write_image(png_path, width=900, height=700)
        print(f"[OK] PNG: {png_path}")
    except Exception as e:
        print("[WARN] PNG não gerado (instalar kaleido):", e)

    print(f"[OK] Radar Executivo: {output_path}")
