"""Dashboard reactivo (Streamlit) sobre la base SQLite.

    streamlit run dashboard/app.py

Patrón visto en clase: el consumer alimenta el ESTADO/base; el dashboard visualiza ese estado
(no consume Kafka directamente). El fragmento se refresca solo cada N segundos.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.express as px
import streamlit as st

from src.viz_data import error_by_year, kpis, load_predictions

st.set_page_config(page_title="World Happiness | Predicciones", layout="wide")
st.title("World Happiness · Predicciones del modelo")
st.caption("Fuente: SQLite (`happiness.db`). El consumer (`python -m src.stream_predict`) escribe; "
           "este dashboard solo lee.")

source = st.sidebar.radio("Fuente de datos", ["stream", "test"],
                          format_func=lambda s: "Streaming (Kafka → modelo)" if s == "stream" else "Conjunto de test")
refresh = st.sidebar.slider("Refrescar cada (s)", 2, 30, 5)


@st.fragment(run_every=refresh)
def live():
    df = load_predictions(source)
    if df.empty:
        st.warning("Aún no hay filas. Ejecuta `python run_pipeline.py` y/o `python -m src.stream_predict`.")
        return
    k = kpis(df)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Filas", k["filas"])
    c2.metric("Rechazados", k["rechazados"])
    c3.metric("MAE", "—" if k["MAE"] is None else f"{k['MAE']:.3f}")
    c4.metric("RMSE", "—" if k["RMSE"] is None else f"{k['RMSE']:.3f}")

    left, right = st.columns(2)
    both = df.dropna(subset=["actual_score", "predicted_score"])
    if not both.empty:
        fig = px.scatter(both, x="actual_score", y="predicted_score", color="year", hover_data=["country"],
                         title="Real vs predicho")
        lo, hi = float(both[["actual_score", "predicted_score"]].min().min()), float(both[["actual_score", "predicted_score"]].max().max())
        fig.add_shape(type="line", x0=lo, y0=lo, x1=hi, y1=hi, line=dict(dash="dash"))
        left.plotly_chart(fig, use_container_width=True)
        right.plotly_chart(px.bar(error_by_year(df), x="year", y="MAE", title="Error absoluto medio por año"),
                           use_container_width=True)
    st.subheader("Últimas filas")
    st.dataframe(df.tail(20), use_container_width=True)


live()
