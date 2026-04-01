PASSO A PASSO

1) Copie o arquivo export_data_quality_report_v2_integrated.py para a raiz do projeto:
   C:\Users\Daniel\Downloads\Data_Quality\

2) Opcionalmente copie streamlit_app_mjv.py para a mesma raiz.

3) Instale as dependências:
   python -m pip install -r data\data_quality_v2_package\requirements.txt

4) Rode o relatório:
   python export_data_quality_report_v2_integrated.py

5) Rode o Streamlit:
   python -m streamlit run streamlit_app_mjv.py

Se preferir manter o script dentro de data\data_quality_v2_package, o import do legado agora também procura automaticamente a raiz do projeto.
