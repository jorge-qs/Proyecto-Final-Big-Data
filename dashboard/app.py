"""
Dashboard analítico — Yelp Big Data Project (Philadelphia, PA)
Arquitectura: MongoDB + Cassandra + Neo4j | Orquestado con Apache Airflow
"""
from __future__ import annotations
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

# ── CSS mínimo para cards ──────────────────────────────────────────────────
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


# ── Sidebar ────────────────────────────────────────────────────────────────

st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/a/ad/Yelp_Logo.svg", width=120)
st.sidebar.title("Yelp Big Data")
st.sidebar.caption("Philadelphia, PA · UTEC 2026")
st.sidebar.divider()
st.sidebar.subheader("Filtros")
ciudad_filtro = st.sidebar.text_input("Ciudad", "Philadelphia")
min_stars = st.sidebar.slider("Rating mínimo", 1.0, 5.0, 4.0, 0.5)
top_n = st.sidebar.slider("Top N resultados", 5, 25, 10)
st.sidebar.divider()
st.sidebar.caption("Fuente: Yelp Open Dataset")
st.sidebar.caption("Pipeline: Airflow @daily")


# ── Header ────────────────────────────────────────────────────────────────

st.title("🍽️ Yelp Open Dataset — Dashboard Analítico")
st.caption("Arquitectura multimodelo: MongoDB · Cassandra · Neo4j | Orquestado con Apache Airflow")
st.divider()

# Métricas globales rápidas
db = get_mongo()
session = get_cassandra()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("🏪 Negocios", f"{db.businesses.count_documents({}):,}")
c2.metric("⭐ Reseñas", f"{db.reviews.count_documents({}):,}")
c3.metric("👤 Usuarios", f"{db.users.count_documents({}):,}")
c4.metric("💡 Tips", f"{db.tips.count_documents({}):,}")
try:
    n_friends = get_neo4j().session().run("MATCH ()-[r:FRIEND]->() RETURN count(r) AS n").single()["n"]
    c5.metric("🤝 Amistades", f"{n_friends:,}")
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
    filtro_biz: dict = {"stars": {"$gte": min_stars}}
    if ciudad_filtro:
        filtro_biz["city"] = {"$regex": ciudad_filtro, "$options": "i"}

    @st.cache_data(ttl=120)
    def get_top_businesses(min_s: float, ciudad: str, n: int):
        f = {"stars": {"$gte": min_s}}
        if ciudad:
            f["city"] = {"$regex": ciudad, "$options": "i"}
        rows = list(get_mongo().businesses.find(
            f, {"name": 1, "stars": 1, "review_count": 1, "categories": 1}
        ).sort("review_count", -1).limit(n))
        return rows

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
        dist = get_stars_dist()
        if dist:
            df_dist = pd.DataFrame(dist).rename(columns={"_id": "estrellas", "total": "reseñas"})
            fig2 = px.pie(df_dist, names="estrellas", values="reseñas",
                          color_discrete_sequence=px.colors.sequential.RdBu,
                          title="¿Cómo califican los usuarios?")
            st.plotly_chart(fig2, use_container_width=True)

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
        cats = get_top_categories()
        if cats:
            df_cats = pd.DataFrame(cats).rename(columns={"_id": "categoría", "count": "negocios"})
            fig3 = px.bar(df_cats, x="negocios", y="categoría", orientation="h",
                          color="negocios", color_continuous_scale="Reds",
                          title="Top 15 categorías")
            fig3.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig3, use_container_width=True)

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
            return pd.DataFrame([{"fecha": str(r.review_date), "reseñas": r.total}
                                  for r in rows]).sort_values("fecha")
        df_daily = get_daily()
        if not df_daily.empty:
            fig4 = px.bar(df_daily, x="fecha", y="reseñas",
                          color="reseñas", color_continuous_scale="Blues",
                          title="Volumen diario de reseñas procesadas por el pipeline")
            st.plotly_chart(fig4, use_container_width=True)

            if len(df_daily) >= 2:
                ultimo = df_daily.iloc[-1]["reseñas"]
                penultimo = df_daily.iloc[-2]["reseñas"]
                delta = ((ultimo - penultimo) / penultimo * 100) if penultimo else 0
                st.metric("Último día procesado", f"{ultimo} reseñas",
                          f"{delta:+.1f}% vs día anterior")
        else:
            st.info("Ejecuta el DAG para ver datos.")

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
        df_cats = get_cat_stats()
        if not df_cats.empty:
            df_top = (df_cats.groupby("categoría")
                      .agg(avg_stars=("avg_stars", "mean"),
                           avg_sentiment=("avg_sentiment", "mean"),
                           reseñas=("reseñas", "sum"))
                      .reset_index()
                      .sort_values("reseñas", ascending=False)
                      .head(15))
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

    st.subheader("🔥 Análisis de sentimiento por categoría")
    if not df_cats.empty:
        df_sent = (df_cats.groupby("categoría")["avg_sentiment"]
                   .mean().reset_index()
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

        # Métricas del grafo
        @st.cache_data(ttl=300)
        def get_graph_stats():
            with get_neo4j().session() as s:
                return {
                    "usuarios": s.run("MATCH (u:User) RETURN count(u) AS n").single()["n"],
                    "negocios": s.run("MATCH (b:Business) RETURN count(b) AS n").single()["n"],
                    "amistades": s.run("MATCH ()-[r:FRIEND]->() RETURN count(r) AS n").single()["n"],
                    "reseñas":  s.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) AS n").single()["n"],
                    "categorias": s.run("MATCH (c:Category) RETURN count(c) AS n").single()["n"],
                }
        stats = get_graph_stats()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("👤 Nodos Usuario",   f"{stats['usuarios']:,}")
        c2.metric("🏪 Nodos Negocio",   f"{stats['negocios']:,}")
        c3.metric("🏷️ Categorías",      f"{stats['categorias']:,}")
        c4.metric("🤝 Aristas FRIEND",  f"{stats['amistades']:,}")
        c5.metric("⭐ Aristas REVIEWED", f"{stats['reseñas']:,}")

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
                    return [{"usuario": r["nombre"] or r["uid"][:8],
                             "amigos": r["amigos"]} for r in res]
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
                    return [{"negocio": r["negocio"], "reseñas_red": r["reseñas_red"]}
                            for r in res]
            biz_net = get_network_biz(top_n)
            if biz_net:
                df_net = pd.DataFrame(biz_net)
                fig8 = px.bar(df_net, x="negocio", y="reseñas_red",
                              color="reseñas_red", color_continuous_scale="Oranges",
                              title=f"Top {top_n} negocios reseñados por amigos")
                fig8.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig8, use_container_width=True)
            else:
                st.info("Sin datos de red aún.")

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
            df_amigos, df_biz_user = get_user_network(uid_input)
            c1, c2 = st.columns(2)
            with c1:
                st.write("**Amigos del usuario:**")
                st.dataframe(df_amigos, use_container_width=True)
            with c2:
                st.write("**Negocios recomendados por su red:**")
                st.dataframe(df_biz_user, use_container_width=True)

    except Exception as e:
        st.error(f"Neo4j no disponible: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 — KPIs
# ═══════════════════════════════════════════════════════════════════════════

with tab_kpis:
    st.header("📊 KPIs del pipeline — generados por Airflow")
    st.caption("Calculados automáticamente en la etapa `generate_kpis` del DAG `yelp_pipeline`")

    KPI_META = {
        "reviews_per_day":       ("Reseñas procesadas/día", "Volumen de reseñas que procesó el pipeline en cada ejecución diaria."),
        "daily_growth_pct":      ("Crecimiento diario (%)", "Variación porcentual del volumen de reseñas respecto al día anterior."),
        "top_category_avg_stars":("Rating promedio (top categoría)", "Rating promedio de la categoría mejor puntuada ese día."),
        "top_influencer_degree": ("Grado del influencer top", "Número de amigos directos del usuario más conectado en la red social."),
        "top_business_network":  ("Menciones del top negocio en red", "Cuántas veces fue reseñado el negocio más popular dentro de la red social."),
        "avg_sentiment":         ("Sentimiento promedio VADER", "Índice de sentimiento medio de las reseñas del día (-1 negativo, +1 positivo)."),
    }

    @st.cache_data(ttl=60)
    def get_all_kpis():
        rows = get_cassandra().execute(
            "SELECT kpi_name, kpi_date, value, detail FROM kpi_results"
        )
        return pd.DataFrame([{
            "kpi": r.kpi_name,
            "fecha": str(r.kpi_date),
            "valor": round(r.value, 4),
            "detalle": r.detail,
        } for r in rows])

    df_kpi = get_all_kpis()

    if df_kpi.empty:
        st.info("Sin KPIs todavía. Ejecuta el DAG al menos una vez.")
    else:
        # Última fecha disponible → tarjetas de KPI
        ultima_fecha = df_kpi["fecha"].max()
        st.subheader(f"Valores más recientes — {ultima_fecha}")
        ultimos = df_kpi[df_kpi["fecha"] == ultima_fecha].set_index("kpi")

        cols = st.columns(3)
        for i, (kpi_id, (nombre, desc)) in enumerate(KPI_META.items()):
            with cols[i % 3]:
                if kpi_id in ultimos.index:
                    val = ultimos.loc[kpi_id, "valor"]
                    det = ultimos.loc[kpi_id, "detalle"] or ""
                    st.metric(label=nombre, value=f"{val:,.2f}", help=desc)
                    if det:
                        st.caption(f"Detalle: {det}")
                else:
                    st.metric(label=nombre, value="—", help=desc)

        st.divider()
        st.subheader("Evolución temporal de KPIs")

        kpi_sel = st.selectbox(
            "Selecciona un KPI",
            options=list(KPI_META.keys()),
            format_func=lambda k: KPI_META[k][0],
        )
        df_sel = df_kpi[df_kpi["kpi"] == kpi_sel].sort_values("fecha")
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
        st.subheader("Tabla completa de KPIs")
        df_display = df_kpi.copy()
        df_display["nombre"] = df_display["kpi"].map(
            {k: v[0] for k, v in KPI_META.items()}
        )
        st.dataframe(
            df_display[["fecha", "nombre", "valor", "detalle"]].sort_values(["fecha", "nombre"]),
            use_container_width=True,
            hide_index=True,
        )
