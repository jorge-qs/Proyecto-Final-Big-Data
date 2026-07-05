"""
Dashboard analítico — Yelp Big Data Project (Philadelphia, PA)
Arquitectura: MongoDB + Cassandra + Neo4j | Orquestado con Apache Airflow
"""
from __future__ import annotations
import math
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="Yelp Big Data — UTEC",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card {
    background: #1e1e2e; border-radius: 10px;
    padding: 1rem 1.2rem; margin-bottom: 0.5rem;
    border-left: 4px solid #E63946;
}
.metric-card h3 { color: #aaa; font-size: 0.8rem; margin: 0; text-transform: uppercase; }
.metric-card p  { color: #fff; font-size: 1.8rem; font-weight: 700; margin: 0; }
.metric-card small { color: #888; font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)


# ── Conexiones cacheadas ───────────────────────────────────────────────────

@st.cache_resource
def get_mongo():
    from pymongo import MongoClient
    from src.common.config import MONGO_URI
    return MongoClient(MONGO_URI)["yelp"]

@st.cache_resource
def get_cassandra():
    from cassandra.cluster import Cluster
    from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE
    return Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT).connect(CASSANDRA_KEYSPACE)

@st.cache_resource
def get_neo4j():
    from neo4j import GraphDatabase
    from src.common.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


@st.cache_data(ttl=300)
def get_last_kpi_date() -> str:
    try:
        # Consultar solo la partición reviews_per_day y filtrar fechas con datos reales
        rows = list(get_cassandra().execute(
            "SELECT kpi_date, value FROM kpi_results WHERE kpi_name='reviews_per_day'"
        ))
        dates = [str(r.kpi_date) for r in rows if r.kpi_date and (r.value or 0) > 0]
        return max(dates) if dates else ""
    except Exception:
        return ""


# ── Sidebar ────────────────────────────────────────────────────────────────

st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/a/ad/Yelp_Logo.svg", width=120)
st.sidebar.title("Yelp Big Data")
st.sidebar.caption("Philadelphia, PA · UTEC 2026")
st.sidebar.divider()
st.sidebar.subheader("Filtros")
ciudad_filtro = st.sidebar.text_input("Ciudad", "Philadelphia")
min_stars = st.sidebar.slider("Rating mínimo", 1.0, 5.0, 1.0, 0.5)
top_n = st.sidebar.slider("Top N resultados", 5, 25, 10)
st.sidebar.divider()
st.sidebar.caption("Fuente: Yelp Open Dataset")
st.sidebar.caption("Pipeline: Airflow @daily")
_last_upd = get_last_kpi_date()
if _last_upd:
    st.sidebar.caption(f"Última actualización KPIs: {_last_upd}")


# ── Header ────────────────────────────────────────────────────────────────

st.title("🍽️ Yelp Open Dataset — Dashboard Analítico")
st.caption("Arquitectura multimodelo: MongoDB · Cassandra · Neo4j | Orquestado con Apache Airflow")
st.divider()

# Métricas globales
with st.spinner("Cargando métricas globales..."):
    db = get_mongo()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏪 Negocios",  f"{db.businesses.count_documents({}):,}")
    c2.metric("⭐ Reseñas",   f"{db.reviews.count_documents({}):,}")
    c3.metric("👤 Usuarios",  f"{db.users.count_documents({}):,}")
    c4.metric("💡 Tips",      f"{db.tips.count_documents({}):,}")
    try:
        n_fr = get_neo4j().session().run("MATCH ()-[r:FRIEND]->() RETURN count(r) AS n").single()["n"]
        c5.metric("🤝 Amistades", f"{n_fr:,}")
    except Exception:
        c5.metric("🤝 Amistades", "N/A")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────

tab_mongo, tab_cassandra, tab_neo4j, tab_kpis = st.tabs([
    "🗂️  MongoDB — Documentos",
    "📈  Cassandra — Tendencias",
    "🕸️  Neo4j — Red social",
    "📊  KPIs del pipeline",
])


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — MongoDB
# ═══════════════════════════════════════════════════════════════════════════

with tab_mongo:
    st.header("Documentos originales — MongoDB")

    # Top negocios por review_count
    st.subheader(f"Top {top_n} negocios con más reseñas (rating ≥ {min_stars}★)")

    @st.cache_data(ttl=120)
    def get_top_businesses(min_s: float, ciudad: str, n: int):
        f = {"stars": {"$gte": min_s}}
        if ciudad:
            f["city"] = {"$regex": ciudad, "$options": "i"}
        return list(get_mongo().businesses.find(
            f, {"name": 1, "stars": 1, "review_count": 1, "categories": 1}
        ).sort("review_count", -1).limit(n))

    with st.spinner("Consultando negocios..."):
        top_biz = get_top_businesses(min_stars, ciudad_filtro, top_n)
    if top_biz:
        df_biz = pd.DataFrame(top_biz)
        df_biz["categoría"] = df_biz["categories"].apply(
            lambda x: x[0] if isinstance(x, list) and x else "Otra"
        )
        fig1 = px.bar(
            df_biz, x="name", y="review_count", color="stars",
            color_continuous_scale="RdYlGn", range_color=[1, 5],
            hover_data=["stars", "categoría"],
            labels={"name": "Negocio", "review_count": "N° reseñas", "stars": "Rating"},
            title=f"Top {top_n} negocios — {ciudad_filtro}",
        )
        fig1.update_layout(xaxis_tickangle=-35, coloraxis_colorbar_title="Stars")
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("Sin negocios con esos filtros.")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Distribución de estrellas en reseñas")
        @st.cache_data(ttl=300)
        def get_stars_dist():
            pipeline = [{"$group": {"_id": "$stars", "total": {"$sum": 1}}},
                        {"$sort": {"_id": 1}}]
            return list(get_mongo().reviews.aggregate(pipeline))
        with st.spinner("Calculando distribución..."):
            dist = get_stars_dist()
        if dist:
            df_dist = pd.DataFrame(dist).rename(columns={"_id": "estrellas", "total": "reseñas"})
            df_dist["estrellas_str"] = df_dist["estrellas"].astype(str)
            fig2 = px.bar(
                df_dist, x="estrellas_str", y="reseñas",
                color="estrellas_str",
                color_discrete_map={"1.0": "#e74c3c", "2.0": "#e67e22", "3.0": "#f1c40f", "4.0": "#2ecc71", "5.0": "#27ae60"},
                title="Distribución de calificaciones — curva bimodal de Yelp",
                labels={"estrellas_str": "Estrellas", "reseñas": "N° reseñas"},
            )
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)
            _tot = df_dist["reseñas"].sum()
            _ext = df_dist[df_dist["estrellas"].isin([1.0, 5.0])]["reseñas"].sum()
            if _tot > 0:
                st.caption(
                    f"Distribución bimodal: el {_ext/_tot*100:.0f}% de las reseñas son 1★ o 5★. "
                    f"Los usuarios de Yelp tienden a escribir solo cuando la experiencia es extrema."
                )
        else:
            st.info("Sin reseñas en MongoDB. Ejecuta el DAG o bulk_ingest.py")

    with col_r:
        st.subheader("Top categorías por número de negocios")
        @st.cache_data(ttl=300)
        def get_top_categories():
            pipeline = [
                {"$unwind": "$categories"},
                {"$group": {"_id": "$categories", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 15},
            ]
            return list(get_mongo().businesses.aggregate(pipeline))
        with st.spinner("Calculando categorías..."):
            cats = get_top_categories()
        if cats:
            df_cats = pd.DataFrame(cats).rename(columns={"_id": "categoría", "count": "negocios"})
            fig3 = px.bar(df_cats, x="negocios", y="categoría", orientation="h",
                          color="negocios", color_continuous_scale="Reds",
                          title="Top 15 categorías")
            fig3.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig3, use_container_width=True)

    # Hidden Gems
    st.subheader("💎 Hidden Gems — Alta calidad, bajo volumen")
    @st.cache_data(ttl=300)
    def get_hidden_gems(ciudad: str, n: int):
        f = {"stars": {"$gte": 4.5}, "review_count": {"$gte": 5, "$lt": 50}}
        if ciudad:
            f["city"] = {"$regex": ciudad, "$options": "i"}
        return list(get_mongo().businesses.find(
            f, {"name": 1, "stars": 1, "review_count": 1, "categories": 1}
        ).sort("stars", -1).limit(n))

    with st.spinner("Buscando hidden gems..."):
        gems = get_hidden_gems(ciudad_filtro, top_n)
    if gems:
        df_gems = pd.DataFrame(gems)
        df_gems["categoría"] = df_gems["categories"].apply(
            lambda x: x[0] if isinstance(x, list) and x else "Otra"
        )
        fig_gems = px.scatter(
            df_gems, x="review_count", y="stars",
            hover_name="name", color="categoría", size="stars",
            labels={"review_count": "N° de reseñas", "stars": "Rating"},
            title=f"Negocios ≥ 4.5★ con menos de 50 reseñas — {ciudad_filtro}",
        )
        fig_gems.add_vline(x=25, line_dash="dash", line_color="gray", opacity=0.4,
                           annotation_text="25 reseñas")
        st.plotly_chart(fig_gems, use_container_width=True)
        st.caption(
            f"{len(gems)} negocios encontrados — alta calificación pero poco descubiertos. "
            f"Potenciales recomendaciones de nicho en Philadelphia."
        )
    else:
        st.info("Sin hidden gems con los filtros actuales.")

    # Mapa geográfico
    st.subheader("🗺️ Mapa de negocios")
    @st.cache_data(ttl=300)
    def get_biz_geo(min_s: float, ciudad: str):
        f = {
            "stars": {"$gte": min_s},
            "latitude":  {"$exists": True, "$ne": None},
            "longitude": {"$exists": True, "$ne": None},
        }
        if ciudad:
            f["city"] = {"$regex": ciudad, "$options": "i"}
        return list(get_mongo().businesses.find(
            f, {"name": 1, "stars": 1, "latitude": 1, "longitude": 1, "categories": 1, "review_count": 1}
        ).limit(800))

    @st.cache_data(ttl=300)
    def get_geo_counts(ciudad: str):
        f_base = {"city": {"$regex": ciudad, "$options": "i"}} if ciudad else {}
        total = get_mongo().businesses.count_documents(f_base)
        f_geo = {**f_base, "latitude": {"$exists": True, "$ne": None}, "longitude": {"$exists": True, "$ne": None}}
        with_geo = get_mongo().businesses.count_documents(f_geo)
        return total, with_geo

    with st.spinner("Cargando mapa..."):
        geo_data = get_biz_geo(min_stars, ciudad_filtro)
        _total_biz, _with_geo = get_geo_counts(ciudad_filtro)
    _pct_geo = int(_with_geo * 100 / _total_biz) if _total_biz > 0 else 0
    st.caption(f"📍 {_with_geo:,} de {_total_biz:,} negocios en {ciudad_filtro} tienen coordenadas válidas ({_pct_geo}%)")
    if geo_data:
        df_geo = pd.DataFrame(geo_data)
        df_geo = df_geo.dropna(subset=["latitude", "longitude"])
        df_geo["categoría"] = df_geo["categories"].apply(
            lambda x: x[0] if isinstance(x, list) and x else "Otra"
        )
        _star_min = float(df_geo["stars"].min())
        _star_max = float(df_geo["stars"].max())
        fig_map = px.scatter_mapbox(
            df_geo, lat="latitude", lon="longitude",
            color="stars", hover_name="name",
            hover_data={"latitude": False, "longitude": False, "review_count": True, "categoría": True},
            color_continuous_scale="RdYlGn", range_color=[_star_min, _star_max],
            size="review_count", size_max=15,
            zoom=11, mapbox_style="open-street-map",
            title=f"Negocios en {ciudad_filtro} — rating {_star_min:.1f}–{_star_max:.1f}★",
        )
        fig_map.update_layout(height=500, margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Sin negocios con coordenadas para mostrar en el mapa.")

    # Reseñas recientes
    st.subheader("📝 Reseñas recientes")
    @st.cache_data(ttl=60)
    def get_recent_reviews(n: int):
        return list(get_mongo().reviews.find(
            {}, {"_id": 0, "text": 1, "stars": 1, "date": 1, "business_id": 1, "useful": 1}
        ).sort("date", -1).limit(n))

    with st.spinner("Cargando reseñas recientes..."):
        recent = get_recent_reviews(5)
    if recent:
        for rev in recent:
            stars_str = "★" * int(rev.get("stars", 0)) + "☆" * (5 - int(rev.get("stars", 0)))
            st.markdown(
                f"**{stars_str}** &nbsp; `{rev.get('date', '')}` &nbsp; "
                f"*útil: {rev.get('useful', 0)}*"
            )
            st.caption(rev.get("text", "")[:300] + ("..." if len(rev.get("text", "")) > 300 else ""))
            st.divider()
    else:
        st.info("Sin reseñas aún. Ejecuta el DAG o `python scripts/bulk_ingest.py`")

    # Búsqueda por nombre
    st.subheader("Explorar documentos — Búsqueda por nombre")
    busqueda = st.text_input("Nombre del negocio", placeholder="ej. Pizza...")
    if busqueda:
        resultados = list(db.businesses.find(
            {"name": {"$regex": busqueda, "$options": "i"}},
            {"_id": 0, "name": 1, "city": 1, "stars": 1, "review_count": 1, "categories": 1}
        ).limit(10))
        if resultados:
            st.dataframe(pd.DataFrame(resultados), use_container_width=True)
        else:
            st.warning("Sin resultados.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — Cassandra
# ═══════════════════════════════════════════════════════════════════════════

with tab_cassandra:
    st.header("Series temporales y agregados — Cassandra")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("📅 Reseñas procesadas por día")
        @st.cache_data(ttl=60)
        def get_daily():
            rows = get_cassandra().execute(
                "SELECT review_date, total FROM daily_review_counts"
            )
            # Excluir fechas sin reviews reales (DAG corrió pero dataset no llega a esa fecha)
            return pd.DataFrame([
                {"fecha": str(r.review_date), "reseñas": r.total}
                for r in rows if (r.total or 0) > 0
            ]).sort_values("fecha")

        with st.spinner("Consultando Cassandra..."):
            df_daily = get_daily()

        if not df_daily.empty:
            df_daily["fecha_dt"] = pd.to_datetime(df_daily["fecha"])
            _min_d = df_daily["fecha_dt"].min().date()
            _max_d = df_daily["fecha_dt"].max().date()
            _default_start = max(_min_d, (_max_d - timedelta(days=365)))

            _dcol1, _dcol2 = st.columns(2)
            with _dcol1:
                _start_d = st.date_input("Desde", value=_default_start, min_value=_min_d, max_value=_max_d, key="daily_start")
            with _dcol2:
                _end_d = st.date_input("Hasta", value=_max_d, min_value=_min_d, max_value=_max_d, key="daily_end")

            _mask = (df_daily["fecha_dt"].dt.date >= _start_d) & (df_daily["fecha_dt"].dt.date <= _end_d)
            df_plot = df_daily[_mask].copy()

            if df_plot.empty:
                st.warning("Sin datos en el rango seleccionado.")
            else:
                fig4 = go.Figure()
                fig4.add_trace(go.Bar(
                    x=df_plot["fecha"], y=df_plot["reseñas"],
                    name="Reseñas/día",
                    marker_color=df_plot["reseñas"],
                    marker_colorscale="Blues",
                ))
                if len(df_plot) >= 3:
                    x_num = list(range(len(df_plot)))
                    y_vals = df_plot["reseñas"].tolist()
                    n = len(x_num)
                    mean_x = sum(x_num) / n
                    mean_y = sum(y_vals) / n
                    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_num, y_vals)) / \
                            max(sum((x - mean_x) ** 2 for x in x_num), 1e-9)
                    intercept = mean_y - slope * mean_x
                    trend = [slope * x + intercept for x in x_num]
                    fig4.add_trace(go.Scatter(
                        x=df_plot["fecha"], y=trend,
                        mode="lines", name="Tendencia",
                        line=dict(color="#E63946", dash="dash", width=2),
                    ))
                fig4.update_layout(
                    title=f"Volumen diario de reseñas ({_start_d} → {_end_d})",
                    xaxis_title="Fecha", yaxis_title="Reseñas",
                    legend=dict(orientation="h", y=1.1),
                )
                from datetime import date as _date
                if _start_d <= _date(2020, 3, 1) <= _end_d:
                    fig4.add_vline(
                        x="2020-03-01", line_dash="dash", line_color="#e74c3c", opacity=0.8,
                        annotation_text="COVID-19 (Mar 2020)",
                        annotation_position="top right",
                        annotation_font_color="#e74c3c",
                    )
                st.plotly_chart(fig4, use_container_width=True)

                if len(df_plot) >= 2:
                    ultimo = df_plot.iloc[-1]["reseñas"]
                    penultimo = df_plot.iloc[-2]["reseñas"]
                    delta = ((ultimo - penultimo) / penultimo * 100) if penultimo else 0
                    st.metric("Último día en rango", f"{ultimo:,} reseñas", f"{delta:+.1f}% vs día anterior")
        else:
            st.info("Ejecuta el DAG para ver datos. Si ya corriste el pipeline, puede tardar unos minutos.")

    with col_r:
        st.subheader("⭐ Rating promedio por categoría")
        @st.cache_data(ttl=60)
        def get_cat_stats():
            rows = get_cassandra().execute(
                "SELECT category, avg_stars, avg_sentiment, review_count "
                "FROM category_daily_stats"
            )
            return pd.DataFrame([{
                "categoría": r.category,
                "avg_stars": round(r.avg_stars, 2),
                "avg_sentiment": round(r.avg_sentiment or 0, 3),
                "reseñas": r.review_count,
            } for r in rows])

        with st.spinner("Consultando categorías..."):
            df_cats = get_cat_stats()

        if not df_cats.empty:
            _w = df_cats.copy()
            _w["_sw"] = _w["avg_stars"]     * _w["reseñas"]
            _w["_ew"] = _w["avg_sentiment"] * _w["reseñas"]
            df_top = (_w.groupby("categoría")
                       .agg(_sw=("_sw", "sum"), _ew=("_ew", "sum"), reseñas=("reseñas", "sum"))
                       .reset_index())
            df_top["avg_stars"]     = df_top["_sw"] / df_top["reseñas"].clip(lower=1)
            df_top["avg_sentiment"] = df_top["_ew"] / df_top["reseñas"].clip(lower=1)
            df_top = df_top.drop(columns=["_sw", "_ew"]).sort_values("reseñas", ascending=False).head(15)
            fig5 = px.scatter(
                df_top, x="avg_stars", y="avg_sentiment",
                size="reseñas", color="avg_stars",
                color_continuous_scale="RdYlGn", range_color=[1, 5],
                hover_name="categoría", size_max=40,
                labels={"avg_stars": "Rating promedio", "avg_sentiment": "Sentimiento (VADER)"},
                title="Rating vs Sentimiento por categoría",
            )
            fig5.add_vline(x=3.5, line_dash="dash", line_color="gray", opacity=0.5)
            fig5.add_hline(y=0,   line_dash="dash", line_color="gray", opacity=0.5)
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.warning("Sin datos de categorías. Ejecuta `bulk_ingest.py` para cargar histórico.")

    st.subheader("🔥 Análisis de sentimiento por categoría")
    if not df_cats.empty:
        _sc = df_cats.copy()
        _sc["_ew"] = _sc["avg_sentiment"] * _sc["reseñas"]
        df_sent = (_sc.groupby("categoría")
                     .agg(_ew=("_ew", "sum"), reseñas=("reseñas", "sum"))
                     .reset_index())
        df_sent["avg_sentiment"] = df_sent["_ew"] / df_sent["reseñas"].clip(lower=1)
        df_sent = (df_sent.drop(columns=["_ew"])
                          .sort_values("avg_sentiment", ascending=False)
                          .head(top_n))
        df_sent["color"] = df_sent["avg_sentiment"].apply(
            lambda x: "positivo" if x > 0 else "negativo"
        )
        fig6 = px.bar(df_sent, x="categoría", y="avg_sentiment",
                      color="color",
                      color_discrete_map={"positivo": "#2ecc71", "negativo": "#e74c3c"},
                      title=f"Top {top_n} categorías por sentimiento promedio (VADER)")
        fig6.update_layout(xaxis_tickangle=-30, showlegend=False)
        st.plotly_chart(fig6, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 — Neo4j
# ═══════════════════════════════════════════════════════════════════════════

with tab_neo4j:
    st.header("Red social de usuarios — Neo4j")

    try:
        driver = get_neo4j()

        @st.cache_data(ttl=300)
        def get_graph_stats():
            with get_neo4j().session() as s:
                return {
                    "usuarios":   s.run("MATCH (u:User) RETURN count(u) AS n").single()["n"],
                    "negocios":   s.run("MATCH (b:Business) RETURN count(b) AS n").single()["n"],
                    "amistades":  s.run("MATCH ()-[r:FRIEND]->() RETURN count(r) AS n").single()["n"],
                    "reseñas":    s.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) AS n").single()["n"],
                    "categorias": s.run("MATCH (c:Category) RETURN count(c) AS n").single()["n"],
                }

        with st.spinner("Consultando grafo Neo4j..."):
            stats = get_graph_stats()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("👤 Nodos Usuario",    f"{stats['usuarios']:,}")
        c2.metric("🏪 Nodos Negocio",    f"{stats['negocios']:,}")
        c3.metric("🏷️ Categorías",       f"{stats['categorias']:,}")
        c4.metric("🤝 Aristas FRIEND",   f"{stats['amistades']:,}")
        c5.metric("⭐ Aristas REVIEWED", f"{stats['reseñas']:,}")

        st.divider()

        # Visualización de red
        st.subheader(f"🔵 Red social — Top {min(top_n, 20)} usuarios más conectados")

        @st.cache_data(ttl=300)
        def get_network_data(n: int):
            n = min(n, 30)
            with get_neo4j().session() as s:
                res = s.run("""
                    MATCH (u:User)-[:FRIEND]->(f:User)
                    WITH u, count(f) AS deg
                    ORDER BY deg DESC LIMIT $n
                    RETURN u.user_id AS uid,
                           coalesce(u.name, substring(u.user_id, 0, 8)) AS name,
                           deg
                """, n=n)
                nodes = [{"uid": r["uid"], "name": r["name"], "deg": r["deg"]} for r in res]

                if not nodes:
                    return [], []

                uids = [u["uid"] for u in nodes]
                res2 = s.run("""
                    MATCH (u:User)-[:FRIEND]->(f:User)
                    WHERE u.user_id IN $uids AND f.user_id IN $uids
                    RETURN u.user_id AS src, f.user_id AS tgt
                    LIMIT 200
                """, uids=uids)
                edges = [(r["src"], r["tgt"]) for r in res2]
            return nodes, edges

        with st.spinner("Generando visualización de red..."):
            net_nodes, net_edges = get_network_data(min(top_n, 20))

        if net_nodes:
            n = len(net_nodes)
            pos = {
                u["uid"]: (math.cos(2 * math.pi * i / n), math.sin(2 * math.pi * i / n))
                for i, u in enumerate(net_nodes)
            }

            edge_x, edge_y = [], []
            for src, tgt in net_edges:
                if src in pos and tgt in pos:
                    x0, y0 = pos[src]
                    x1, y1 = pos[tgt]
                    edge_x += [x0, x1, None]
                    edge_y += [y0, y1, None]

            node_x = [pos[u["uid"]][0] for u in net_nodes]
            node_y = [pos[u["uid"]][1] for u in net_nodes]
            node_sizes = [max(12, min(45, u["deg"] // 5)) for u in net_nodes]
            node_hover = [f"{u['name']}<br>Amigos directos: {u['deg']:,}" for u in net_nodes]

            fig_net = go.Figure()
            fig_net.add_trace(go.Scatter(
                x=edge_x, y=edge_y, mode="lines",
                line=dict(width=0.8, color="#555"), hoverinfo="none", showlegend=False,
            ))
            fig_net.add_trace(go.Scatter(
                x=node_x, y=node_y, mode="markers+text",
                marker=dict(
                    size=node_sizes,
                    color=[u["deg"] for u in net_nodes],
                    colorscale="Viridis", showscale=True,
                    colorbar=dict(title="N° amigos"),
                    line=dict(width=1, color="#fff"),
                ),
                text=[u["name"][:14] for u in net_nodes],
                textposition="top center",
                textfont=dict(size=9),
                hovertext=node_hover, hoverinfo="text",
                showlegend=False,
            ))
            fig_net.update_layout(
                height=500,
                showlegend=False,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                margin=dict(t=20, b=20, l=20, r=20),
                plot_bgcolor="#0e1117",
                paper_bgcolor="#0e1117",
            )
            st.plotly_chart(fig_net, use_container_width=True)
            st.caption(f"Nodos = usuarios (tamaño proporcional a conexiones) · {len(net_edges)} aristas mostradas entre el top {n}")
            _degs = sorted(u["deg"] for u in net_nodes)
            _max_deg = _degs[-1]
            _med_deg = _degs[len(_degs) // 2]
            _ratio = _max_deg // max(_med_deg, 1)
            st.caption(
                f"El usuario más conectado tiene {_max_deg:,} amigos vs {_med_deg:,} del usuario mediano "
                f"del top {n} (ratio {_ratio}×). Distribución power-law característica de redes sociales reales."
            )
        else:
            st.info("Sin datos de red. Ejecuta el DAG o `bulk_ingest.py` para cargar aristas REVIEWED.")

        st.divider()
        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("🏆 Top influencers (por nº de amigos)")
            @st.cache_data(ttl=300)
            def get_influencers(n: int):
                with get_neo4j().session() as s:
                    res = s.run("""
                        MATCH (u:User)-[:FRIEND]->(f:User)
                        RETURN u.user_id AS uid, u.name AS nombre, count(f) AS amigos
                        ORDER BY amigos DESC LIMIT $n
                    """, n=n)
                    return [{"usuario": r["nombre"] or r["uid"][:8], "amigos": r["amigos"]}
                            for r in res]
            with st.spinner(""):
                inf = get_influencers(top_n)
            if inf:
                df_inf = pd.DataFrame(inf)
                fig7 = px.bar(df_inf, x="usuario", y="amigos",
                              color="amigos", color_continuous_scale="Viridis",
                              title=f"Top {top_n} usuarios por conexiones directas")
                fig7.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig7, use_container_width=True)

        with col_r:
            st.subheader("🍕 Negocios más reseñados por la red")
            @st.cache_data(ttl=300)
            def get_network_biz(n: int):
                with get_neo4j().session() as s:
                    res = s.run("""
                        MATCH (:User)-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
                        RETURN b.name AS negocio, count(*) AS reseñas_red
                        ORDER BY reseñas_red DESC LIMIT $n
                    """, n=n)
                    return [{"negocio": r["negocio"], "reseñas_red": r["reseñas_red"]} for r in res]
            with st.spinner(""):
                biz_net = get_network_biz(top_n)
            if biz_net:
                df_net = pd.DataFrame(biz_net)
                fig8 = px.bar(df_net, x="negocio", y="reseñas_red",
                              color="reseñas_red", color_continuous_scale="Oranges",
                              title=f"Top {top_n} negocios reseñados por amigos")
                fig8.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig8, use_container_width=True)
            else:
                st.info("Sin datos de red aún. Carga más reseñas con `bulk_ingest.py`.")

        st.subheader("🔍 Explorar red de un usuario")
        uid_input = st.text_input("User ID (pega un ID de la tabla de arriba)")
        if uid_input:
            @st.cache_data(ttl=60)
            def get_user_network(uid: str):
                with get_neo4j().session() as s:
                    amigos = s.run("""
                        MATCH (:User {user_id:$uid})-[:FRIEND]->(f:User)
                        RETURN f.name AS nombre, f.review_count AS reviews, f.fans AS fans
                        ORDER BY fans DESC LIMIT 20
                    """, uid=uid)
                    negocios = s.run("""
                        MATCH (:User {user_id:$uid})-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
                        RETURN b.name AS negocio, b.stars AS stars, count(*) AS menciones
                        ORDER BY menciones DESC LIMIT 10
                    """, uid=uid)
                    return (
                        pd.DataFrame([dict(r) for r in amigos]),
                        pd.DataFrame([dict(r) for r in negocios]),
                    )
            with st.spinner("Buscando red del usuario..."):
                df_amigos, df_biz_user = get_user_network(uid_input)
            c1, c2 = st.columns(2)
            with c1:
                st.write("**Amigos del usuario:**")
                st.dataframe(df_amigos, use_container_width=True)
            with c2:
                st.write("**Negocios recomendados por su red:**")
                st.dataframe(df_biz_user, use_container_width=True)

        st.divider()
        st.subheader("📊 Red social vs ranking global — ¿descubre negocios distintos?")

        @st.cache_data(ttl=300)
        def get_net_vs_global(n: int):
            with get_neo4j().session() as s:
                net_rows = list(s.run("""
                    MATCH (:User)-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
                    RETURN b.name AS negocio, count(*) AS menciones_red
                    ORDER BY menciones_red DESC LIMIT $n
                """, n=n))
                glob_rows = list(s.run("""
                    MATCH (:User)-[:REVIEWED]->(b:Business)
                    RETURN b.name AS negocio, count(*) AS resenas_global
                    ORDER BY resenas_global DESC LIMIT $n
                """, n=n))
            df_red  = pd.DataFrame([{"negocio": r["negocio"], "menciones_red":  r["menciones_red"]}  for r in net_rows])
            df_glob = pd.DataFrame([{"negocio": r["negocio"], "resenas_global": r["resenas_global"]} for r in glob_rows])
            if df_red.empty or df_glob.empty:
                return pd.DataFrame()
            return df_red.merge(df_glob, on="negocio", how="outer").fillna(0)

        with st.spinner("Comparando popularidad en red vs global..."):
            df_comp = get_net_vs_global(top_n)

        if not df_comp.empty:
            df_comp["tipo"] = df_comp.apply(
                lambda r: "En ambos"   if r["menciones_red"] > 0 and r["resenas_global"] > 0
                else ("Solo en red"    if r["menciones_red"] > 0
                else  "Solo global"),
                axis=1,
            )
            fig_comp = px.scatter(
                df_comp, x="resenas_global", y="menciones_red",
                text="negocio", color="tipo",
                color_discrete_map={"En ambos": "#3498db", "Solo en red": "#e74c3c", "Solo global": "#2ecc71"},
                labels={"resenas_global": "Reseñas totales (global)", "menciones_red": "Menciones en red social"},
                title="¿La red del influencer descubre negocios distintos al ranking general?",
            )
            fig_comp.update_traces(textposition="top center", textfont_size=8)
            st.plotly_chart(fig_comp, use_container_width=True)
            _solo_red = df_comp[df_comp["tipo"] == "Solo en red"]["negocio"].tolist()
            if _solo_red:
                st.caption(
                    f"Negocios exclusivos de la red (no en ranking global): "
                    f"{', '.join(_solo_red[:3])}{'...' if len(_solo_red) > 3 else ''}. "
                    f"La red actúa como descubridor de nichos."
                )
        else:
            st.info("Sin datos suficientes para la comparativa.")

    except Exception as e:
        st.error(f"Neo4j no disponible: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 — KPIs
# ═══════════════════════════════════════════════════════════════════════════

with tab_kpis:
    st.header("📊 KPIs del pipeline — generados por Airflow")
    st.caption("Calculados automáticamente en la etapa `generate_kpis` del DAG `yelp_pipeline`")

    KPI_META = {
        "reviews_per_day":            ("Reseñas procesadas/día",          "Volumen de reseñas que procesó el pipeline en cada ejecución diaria."),
        "daily_growth_pct":           ("Crecimiento diario (%)",           "Variación porcentual del volumen de reseñas respecto al día anterior."),
        "top_category_avg_stars":     ("Mejor categoría del día (★)",      "Categoría con mayor rating promedio ese día (mín. 3 reseñas). Cambia a diario según qué nicho brilló más."),
        "active_categories":          ("Categorías activas",               "Número de tipos de negocio distintos con al menos una reseña ese día. Mide la diversidad de actividad comercial."),
        "avg_stars_of_day":           ("Rating promedio ponderado",        "Media ponderada de stars de todas las categorías ese día. Índice global de calidad diario."),
        "avg_sentiment":              ("Sentimiento promedio VADER",       "Índice de sentimiento medio de las reseñas del día (–1 negativo → +1 positivo)."),
        "top_category_quality_score": ("Score calidad×volumen (cat. top)", "Categoría con mejor score stars×(sentiment+1)×reviews ese día."),
        "pct_high_rated_categories":  ("% categorías ≥4★",                 "Porcentaje de categorías activas ese día con rating promedio ≥ 4.0 estrellas."),
        "pct_positive_categories":    ("% categorías sentim. positivo",    "% de categorías con sentimiento VADER > 0.1 ese día."),
    }

    @st.cache_data(ttl=60)
    def get_all_kpis():
        rows = get_cassandra().execute(
            "SELECT kpi_name, kpi_date, value, detail FROM kpi_results"
        )
        return pd.DataFrame([{
            "kpi":    r.kpi_name,
            "fecha":  str(r.kpi_date),
            "valor":  round(r.value, 4),
            "detalle": r.detail,
        } for r in rows])

    with st.spinner("Cargando KPIs..."):
        df_kpi = get_all_kpis()

    if df_kpi.empty:
        st.info("Sin KPIs todavía. Ejecuta el DAG al menos una vez.")
    else:
        # Usar la fecha más reciente con reviews reales (evita mostrar fechas del DAG
        # que corrieron fuera del rango del dataset Yelp, donde todo vale 0)
        _rpd = df_kpi[df_kpi["kpi"] == "reviews_per_day"]
        _fechas_reales = _rpd[_rpd["valor"] > 0]["fecha"]
        ultima_fecha = _fechas_reales.max() if not _fechas_reales.empty else df_kpi["fecha"].max()
        _fechas_prev = _fechas_reales[_fechas_reales < ultima_fecha]
        penultima_fecha = _fechas_prev.max() if not _fechas_prev.empty else None

        st.subheader(f"Valores más recientes — {ultima_fecha}")
        ultimos = df_kpi[df_kpi["fecha"] == ultima_fecha].set_index("kpi")
        anteriores = df_kpi[df_kpi["fecha"] == penultima_fecha].set_index("kpi") if penultima_fecha else None

        # KPIs que ya son porcentajes o escalas acotadas: delta como diferencia absoluta (pp / puntos)
        # KPIs que ya son tasas de cambio: sin delta (mostrar el cambio de un cambio es confuso)
        _NO_DELTA   = {"daily_growth_pct"}
        _ABS_DELTA  = {"avg_sentiment", "avg_stars_of_day", "top_category_avg_stars",
                       "pct_high_rated_categories", "pct_positive_categories"}
        _DELTA_UNIT = {
            "avg_sentiment":           "pts",
            "avg_stars_of_day":        "★",
            "top_category_avg_stars":  "★",
            "pct_high_rated_categories": "pp",
            "pct_positive_categories":   "pp",
        }

        cols = st.columns(3)
        for i, (kpi_id, (nombre, desc)) in enumerate(KPI_META.items()):
            with cols[i % 3]:
                if kpi_id in ultimos.index:
                    val = ultimos.loc[kpi_id, "valor"]
                    det = ultimos.loc[kpi_id, "detalle"] or ""
                    delta_str = None
                    if kpi_id not in _NO_DELTA and anteriores is not None and kpi_id in anteriores.index:
                        prev_val = anteriores.loc[kpi_id, "valor"]
                        diff = val - prev_val
                        if kpi_id in _ABS_DELTA:
                            unit = _DELTA_UNIT.get(kpi_id, "")
                            delta_str = f"{diff:+.2f}{unit} vs {penultima_fecha}"
                        elif prev_val != 0:
                            delta_str = f"{(diff / abs(prev_val) * 100):+.1f}% vs {penultima_fecha}"
                    st.metric(label=nombre, value=f"{val:,.2f}", delta=delta_str, help=desc)
                    if det:
                        st.caption(f"Detalle: {det}")
                else:
                    st.metric(label=nombre, value="—", help=desc)

        st.divider()

        # ── Insights estructurales de la red (Neo4j) — estáticos ────────────
        st.subheader("🔗 Insights estructurales de la red (Neo4j)")
        st.caption("Métricas calculadas sobre el grafo completo. No varían por fecha — representan la estructura de la red social de Yelp en Philadelphia.")

        @st.cache_data(ttl=3600)
        def get_structural_insights() -> dict:
            try:
                driver = get_neo4j()
                out: dict = {}
                with driver.session() as s:
                    r = s.run(
                        "MATCH (u:User)-[:FRIEND]->(f:User) "
                        "RETURN coalesce(u.name,'') AS name, u.user_id AS uid, count(f) AS deg "
                        "ORDER BY deg DESC LIMIT 1"
                    ).single()
                    if r:
                        out["inf_name"] = r["name"] or r["uid"][:8]
                        out["inf_deg"]  = int(r["deg"])
                    r2 = s.run(
                        "MATCH (u:User)-[:REVIEWED]->(b:Business) "
                        "RETURN b.name AS bname, count(*) AS cnt "
                        "ORDER BY cnt DESC LIMIT 1"
                    ).single()
                    if r2:
                        out["biz_name"] = r2["bname"] or "—"
                        out["biz_cnt"]  = int(r2["cnt"])
                    r3 = s.run("MATCH ()-[r:FRIEND]->() RETURN count(r) AS c").single()
                    if r3:
                        out["total_friends"] = int(r3["c"])
                    r4 = s.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) AS c").single()
                    if r4:
                        out["total_reviewed"] = int(r4["c"])
                return out
            except Exception as exc:
                return {"error": str(exc)}

        si = get_structural_insights()
        if "error" in si:
            st.warning(f"Neo4j no disponible: {si['error']}")
        else:
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric(
                "Usuario más influyente",
                si.get("inf_name", "—"),
                help="Usuario con más conexiones FRIEND directas en el grafo Neo4j",
            )
            sc1.caption(f"{si.get('inf_deg', 0):,} amigos directos")
            sc2.metric(
                "Negocio más reseñado en la red",
                si.get("biz_name", "—"),
                help="Negocio con más aristas REVIEWED apuntando a él en Neo4j",
            )
            sc2.caption(f"{si.get('biz_cnt', 0):,} reseñas en la red")
            sc3.metric(
                "Conexiones FRIEND totales",
                f"{si.get('total_friends', 0):,}",
                help="Total de relaciones de amistad cargadas en el grafo",
            )
            sc4.metric(
                "Relaciones REVIEWED totales",
                f"{si.get('total_reviewed', 0):,}",
                help="Total de aristas Usuario→Negocio por reseña en el grafo",
            )

        st.divider()
        st.subheader("Evolución temporal de KPIs")
        kpi_sel = st.selectbox(
            "Selecciona un KPI",
            options=list(KPI_META.keys()),
            format_func=lambda k: KPI_META[k][0],
        )
        df_sel = df_kpi[df_kpi["kpi"] == kpi_sel].sort_values("fecha")
        # Excluir fechas donde reviews_per_day=0 (DAG corrió fuera del rango del dataset)
        _zero_dates = set(
            df_kpi[(df_kpi["kpi"] == "reviews_per_day") & (df_kpi["valor"] == 0)]["fecha"]
        )
        df_sel = df_sel[~df_sel["fecha"].isin(_zero_dates)]
        if not df_sel.empty:
            fig9 = px.line(
                df_sel, x="fecha", y="valor", markers=True,
                title=KPI_META[kpi_sel][0],
                labels={"fecha": "Fecha", "valor": "Valor"},
            )
            fig9.update_traces(line_color="#E63946", marker_size=8)
            st.plotly_chart(fig9, use_container_width=True)
            st.caption(KPI_META[kpi_sel][1])

        st.divider()
        st.subheader("📋 Tabla comparativa de KPIs por fecha")

        _all_fechas = sorted(df_kpi["fecha"].unique())
        _max_cols = st.slider(
            "Últimas N fechas en la tabla",
            min_value=3, max_value=min(30, len(_all_fechas)),
            value=min(14, len(_all_fechas)), key="pivot_n",
        )
        _fechas_pivot = _all_fechas[-_max_cols:]
        df_pivot_data = df_kpi[df_kpi["fecha"].isin(_fechas_pivot)]

        pivot = df_pivot_data.pivot_table(index="kpi", columns="fecha", values="valor", aggfunc="mean")
        pivot.index = pivot.index.map(lambda k: KPI_META.get(k, (k,))[0])

        def color_delta(val):
            if pd.isna(val):
                return ""
            return "color: #2ecc71" if val >= 0 else "color: #e74c3c"

        if len(pivot.columns) >= 2:
            delta_col = pivot.iloc[:, -1] - pivot.iloc[:, -2]
            pivot["Δ último"] = delta_col
            styled = (
                pivot.style
                .format("{:.2f}", na_rep="—")
                .map(color_delta, subset=["Δ último"])
            )
            st.dataframe(styled, use_container_width=True)
        else:
            st.dataframe(pivot.style.format("{:.2f}", na_rep="—"), use_container_width=True)

        st.divider()
        st.subheader("Tabla completa de KPIs")
        df_display = df_kpi.copy()
        df_display["nombre"] = df_display["kpi"].map({k: v[0] for k, v in KPI_META.items()})
        st.dataframe(
            df_display[["fecha", "nombre", "valor", "detalle"]].sort_values(["fecha", "nombre"]),
            use_container_width=True, hide_index=True,
        )
