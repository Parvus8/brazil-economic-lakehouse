"""Painel em Streamlit: preços de combustíveis no Brasil vs. inflação (IPCA) e dólar.

Lê os marts da camada gold que o dbt exporta como Parquet para o object storage, com uma identidade S3
somente leitura.
"""

import os

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

GOLD_URI = os.getenv("GOLD_URI", "s3://gold/marts")
DEFAULT_PRODUCT = "GASOLINA"
# O Streamlit renderiza como LaTeX o texto entre dois cifrões, então o símbolo da moeda é escapado no markdown.
BRL = "R\\$"

MONTHS = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
# Rótulos de mês em português no eixo de tempo do Vega-Lite (que usa nomes em inglês por padrão).
MONTH_LABEL_EXPR = f"{MONTHS}[month(datum.value)] + ' ' + year(datum.value)"
# Vírgula decimal nos eixos numéricos.
DECIMAL_LABEL_EXPR = "replace(format(datum.value, '~f'), '.', ',')"

PRODUCTS = {
    "GASOLINA": ("Gasolina", "f"),
    "GASOLINA ADITIVADA": ("Gasolina aditivada", "f"),
    "ETANOL": ("Etanol", "m"),
    "DIESEL": ("Diesel", "m"),
    "DIESEL S10": ("Diesel S10", "m"),
    "GNV": ("GNV", "m"),
}

# Paleta de referência (validada para daltonismo nos dois modos). Cada série mantém a cor qualquer que seja
# o filtro: combustível é sempre o slot 1, IPCA o slot 2 e dólar o slot 3.
THEMES = {
    "light": {
        "series": ["#2a78d6", "#eb6834", "#1baf7a"],
        "text": "#0b0b0b",
        "text_secondary": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
    },
    "dark": {
        "series": ["#3987e5", "#d95926", "#199e70"],
        "text": "#ffffff",
        "text_secondary": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
    },
}

st.set_page_config(page_title="Combustíveis vs. Macro", page_icon="⛽", layout="wide")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def month_year(value) -> str:
    return f"{MONTHS[value.month - 1]} {value.year}"


def number(value: float, decimals: int = 2, signed: bool = False) -> str:
    text = f"{value:{'+' if signed else ''},.{decimals}f}"
    return text.replace(",", "_").replace(".", ",").replace("_", ".")


def product_name(product: str) -> str:
    return PRODUCTS.get(product, (product.capitalize(), "m"))[0]


def product_gender(product: str) -> str:
    return PRODUCTS.get(product, ("", "m"))[1]


@st.cache_resource
def connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    endpoint = os.getenv("S3_ENDPOINT", "seaweedfs:8333")
    con.execute(
        f"""
        CREATE SECRET gold (
            TYPE s3,
            KEY_ID {sql_literal(os.environ["S3_ACCESS_KEY"])},
            SECRET {sql_literal(os.environ["S3_SECRET_KEY"])},
            ENDPOINT {sql_literal(endpoint)},
            URL_STYLE 'path',
            USE_SSL {str(os.getenv("S3_USE_SSL", "false")).lower() == "true"}
        )
        """
    )
    return con


@st.cache_data(ttl=600, show_spinner="Carregando os dados da camada gold...")
def load_mart(name: str) -> pd.DataFrame:
    # Um cursor por chamada: o Streamlit atende as sessões a partir de várias threads.
    df = connection().cursor().sql(f"select * from read_parquet({sql_literal(f'{GOLD_URI}/{name}.parquet')})").df()
    df["month"] = pd.to_datetime(df["month"])
    return df


def theme() -> dict:
    return THEMES["dark" if st.context.theme.type == "dark" else "light"]


def base_config(chart: alt.TopLevelMixin, colors: dict) -> alt.TopLevelMixin:
    return (
        chart.configure(background="transparent", font="system-ui, -apple-system, 'Segoe UI', sans-serif")
        .configure_view(strokeWidth=0)
        .configure_axis(
            gridColor=colors["grid"],
            gridWidth=1,
            domainColor=colors["axis"],
            tickColor=colors["axis"],
            labelColor=colors["muted"],
            titleColor=colors["text_secondary"],
            titleFontWeight="normal",
            labelFontSize=12,
        )
        .configure_legend(
            labelColor=colors["text_secondary"], titleColor=colors["text_secondary"], orient="top", title=None
        )
    )


def spread_labels(values: pd.Series, min_gap: float) -> pd.Series:
    """Posições dos rótulos no fim das linhas: mantém cada rótulo a pelo menos min_gap (em unidades dos dados)
    do seguinte, para que séries que terminam próximas não fiquem com os textos sobrepostos."""
    positions = {}
    previous = None
    for index, value in values.sort_values(ascending=False).items():
        position = value if previous is None else min(value, previous - min_gap)
        positions[index] = previous = position
    return pd.Series(positions)


def line_chart(
    long: pd.DataFrame, series: list[str], y_title: str, decimals: int, colors: dict, year_step: int = 1
) -> alt.Chart:
    """Linhas com cursor no mês mais próximo, tooltip compartilhado e rótulos diretos no fim de cada linha."""
    color = alt.Color("series:N", scale=alt.Scale(domain=series, range=colors["series"][: len(series)]))
    x = alt.X(
        "month:T",
        title=None,
        axis=alt.Axis(
            labelExpr=MONTH_LABEL_EXPR,
            grid=False,
            labelAngle=0,
            tickCount={"interval": "year", "step": year_step},
            labelFlush=False,
        ),
    )
    y = alt.Y(
        "value:Q",
        title=y_title,
        scale=alt.Scale(zero=False),
        axis=alt.Axis(tickCount=6, labelExpr=DECIMAL_LABEL_EXPR),
    )

    hover = alt.selection_point(nearest=True, on="pointerover", fields=["month"], empty=False, clear="pointerout")
    base = alt.Chart(long)
    lines = base.mark_line(strokeWidth=2).encode(x=x, y=y, color=color)
    points = (
        base.mark_point(size=70, filled=True, stroke="transparent")
        .encode(x=x, y=y, color=color, opacity=alt.condition(hover, alt.value(1), alt.value(0)))
        .add_params(hover)
    )

    # O tooltip usa textos já formatados em português (mês por extenso e vírgula decimal).
    wide = long.pivot(index="month", columns="series", values="value").reset_index()
    wide["Mês"] = wide["month"].map(month_year)
    for name in series:
        wide[f"{name} "] = wide[name].map(lambda value: number(value, decimals))
    rule = (
        alt.Chart(wide)
        .mark_rule(color=colors["axis"], strokeWidth=1)
        .encode(
            x="month:T",
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip("Mês:N")] + [alt.Tooltip(f"{name} :N", title=name) for name in series],
        )
    )

    last = long.sort_values("month").groupby("series").tail(1).copy()
    last["label"] = last["series"] + "  " + last["value"].map(lambda value: number(value, decimals))
    # Cerca de 16px entre rótulos num gráfico de 340px de altura.
    last["label_y"] = spread_labels(last["value"], (long["value"].max() - long["value"].min()) * 0.06)
    end_dots = alt.Chart(last).mark_point(size=50, filled=True).encode(x=x, y=y, color=color)
    end_labels = (
        alt.Chart(last)
        .mark_text(align="left", dx=8, fontSize=12, color=colors["text_secondary"])
        .encode(x=x, y=alt.Y("label_y:Q"), text=alt.Text("label:N"))
    )
    return alt.layer(lines, rule, points, end_dots, end_labels).properties(
        height=340, padding={"right": 170, "left": 4, "top": 8, "bottom": 4}
    )


def main() -> None:
    colors = theme()
    st.title("Combustíveis no Brasil vs. inflação e dólar")
    st.caption(
        "O combustível ficou mais caro em termos reais ou só acompanhou a inflação (IPCA) e o dólar? "
        "Fontes: ANP (preços coletados em postos) e Banco Central do Brasil (SGS)."
    )

    try:
        mart = load_mart("mart_fuel_vs_macro")
        by_state = load_mart("fct_fuel_prices_monthly")
    except duckdb.Error as exc:
        st.info(
            "Ainda não há dados na camada gold. Execute a DAG `economic_lakehouse` no Airflow "
            "e volte quando ela terminar."
        )
        st.caption(f"Detalhes: {exc}")
        return

    products = sorted(mart["product"].unique(), key=product_name)
    months = sorted(mart["month"].dt.date.unique())

    filters = st.columns([1, 2])
    product = filters[0].selectbox(
        "Produto",
        products,
        index=products.index(DEFAULT_PRODUCT) if DEFAULT_PRODUCT in products else 0,
        format_func=product_name,
    )
    start, end = filters[1].select_slider(
        "Período", options=months, value=(months[0], months[-1]), format_func=month_year
    )

    df = mart[(mart["product"] == product) & mart["month"].dt.date.between(start, end)].sort_values("month")
    if len(df) < 2:
        st.warning("Selecione um período com pelo menos dois meses.")
        return

    # Rebaseia todos os índices em 100 no primeiro mês selecionado, para a comparação partir do mesmo ponto.
    first = df.iloc[0]
    df = df.assign(
        fuel=100 * df["avg_price"] / first["avg_price"],
        ipca=100 * df["ipca_index"] / first["ipca_index"],
        usd=100 * df["usd_brl_index"] / first["usd_brl_index"],
    )
    df["real_price"] = df["avg_price"] / (df["ipca"] / 100)
    latest = df.iloc[-1]
    name = product_name(product)
    # Em minúsculas no meio das frases, exceto siglas como GNV.
    name_in_text = name if name.isupper() else name.lower()
    first_month, last_month = month_year(first["month"]), month_year(latest["month"])
    unit = by_state.loc[by_state["product"] == product, "unit"].mode().iat[0]
    unit_name = unit.split("/")[-1].strip()

    # Alguns produtos começam mais tarde nos dados da ANP (gasolina aditivada a partir de out/2020), então os
    # textos usam o primeiro mês que de fato tem dados, e não o início do período selecionado.
    if first["month"].date() > start:
        st.caption(f"A série de {name_in_text} na ANP começa em {first_month}; as comparações partem desse mês.")

    kpis = st.columns(4)
    mom = latest["fuel_price_mom_pct"]
    kpis[0].metric(
        f"Preço médio, {month_year(latest['month'])}",
        f"R$ {number(latest['avg_price'])}",
        f"{number(mom, 1, signed=True)}% vs. mês anterior" if pd.notna(mom) else None,
        delta_color="inverse",
        help=f"Média nacional, {BRL} por {unit_name}. Alta aparece em vermelho porque é ruim para o consumidor.",
    )
    kpis[1].metric(f"{name} desde {first_month}", f"{number(latest['fuel'] - 100, 1, signed=True)}%")
    kpis[2].metric("Inflação (IPCA) no período", f"{number(latest['ipca'] - 100, 1, signed=True)}%")
    kpis[3].metric("Dólar (USD/BRL) no período", f"{number(latest['usd'] - 100, 1, signed=True)}%")

    real_change = (latest["real_price"] / first["avg_price"] - 1) * 100
    article, ending = ("a", "a") if product_gender(product) == "f" else ("o", "o")
    verdict = f"mais car{ending}" if real_change > 0 else f"mais barat{ending}"
    st.markdown(
        f"Em termos reais, **{article} {name_in_text} ficou {number(abs(real_change), 1)}% {verdict}** "
        f"entre {first_month} e {last_month}: "
        f"o preço variou {number(latest['fuel'] - 100, 1, signed=True)}% enquanto a inflação foi de "
        f"{number(latest['ipca'] - 100, 1, signed=True)}%."
    )

    st.subheader("Índice de preço vs. inflação e dólar")
    st.caption("Primeiro mês selecionado = 100. Passe o mouse sobre o gráfico para ver os valores de cada mês.")
    names = {"fuel": name, "ipca": "IPCA", "usd": "Dólar"}
    index_long = df.melt(id_vars="month", value_vars=list(names), var_name="key", value_name="value")
    index_long["series"] = index_long["key"].map(names)
    st.altair_chart(
        base_config(line_chart(index_long, list(names.values()), "Índice", 1, colors), colors),
        width="stretch",
    )
    with st.expander("Mostrar dados"):
        table = df[["month", "fuel", "ipca", "usd"]].rename(columns=names)
        table.insert(0, "Mês", table.pop("month").map(month_year))
        st.dataframe(
            table,
            hide_index=True,
            column_config={col: st.column_config.NumberColumn(format="localized") for col in names.values()},
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Preço nominal vs. real")
        st.caption(f"{BRL} por {unit_name}; preço real em {BRL} de {first_month} (corrigido pelo IPCA).")
        price_names = {"avg_price": "Nominal", "real_price": "Real"}
        price_long = df.melt(id_vars="month", value_vars=list(price_names), var_name="key", value_name="value")
        price_long["series"] = price_long["key"].map(price_names)
        st.altair_chart(
            base_config(line_chart(price_long, list(price_names.values()), "R$", 2, colors, year_step=2), colors),
            width="stretch",
        )
        with st.expander("Mostrar dados"):
            table = df[["month", "avg_price", "real_price"]].rename(columns=price_names)
            table.insert(0, "Mês", table.pop("month").map(month_year))
            st.dataframe(
                table,
                hide_index=True,
                column_config={col: st.column_config.NumberColumn(format="localized") for col in price_names.values()},
            )

    with right:
        state_month = pd.Timestamp(end)
        states = by_state[(by_state["product"] == product) & (by_state["month"] == state_month)]
        states = states.sort_values("avg_price", ascending=False).assign(
            media=lambda d: d["avg_price"].map(number),
            mediana=lambda d: d["median_price"].map(number),
            postos=lambda d: d["stations"].map(lambda value: number(value, 0)),
        )
        st.subheader(f"Preço médio por estado, {month_year(state_month)}")
        st.caption(f"{len(states)} estados; média nacional {BRL} {number(latest['avg_price'])}.")
        bars = (
            alt.Chart(states)
            .mark_bar(color=colors["series"][0], cornerRadiusEnd=4, height={"band": 0.75})
            .encode(
                x=alt.X("avg_price:Q", title="R$", axis=alt.Axis(tickCount=5, labelExpr=DECIMAL_LABEL_EXPR)),
                y=alt.Y(
                    "state:N",
                    sort="-x",
                    title=None,
                    axis=alt.Axis(labelColor=colors["text_secondary"], labelOverlap=False, ticks=False),
                ),
                tooltip=[
                    alt.Tooltip("state:N", title="Estado"),
                    alt.Tooltip("media:N", title="Média (R$)"),
                    alt.Tooltip("mediana:N", title="Mediana (R$)"),
                    alt.Tooltip("postos:N", title="Postos"),
                ],
            )
            .properties(height=max(340, 22 * len(states)))
        )
        st.altair_chart(base_config(bars, colors), width="stretch")
        with st.expander("Mostrar dados"):
            st.dataframe(
                states[["state", "region", "avg_price", "median_price", "min_price", "max_price", "stations"]].rename(
                    columns={
                        "state": "Estado",
                        "region": "Região",
                        "avg_price": "Média",
                        "median_price": "Mediana",
                        "min_price": "Mínimo",
                        "max_price": "Máximo",
                        "stations": "Postos",
                    }
                ),
                hide_index=True,
                column_config={
                    col: st.column_config.NumberColumn(format="localized")
                    for col in ("Média", "Mediana", "Mínimo", "Máximo", "Postos")
                },
            )

    st.caption(
        "Metodologia: o preço nacional é a média das médias estaduais, ponderada pelo número de coletas. "
        "O índice do IPCA acumula a inflação mensal; o dólar usa a média mensal da taxa PTAX. "
        "Os dados são atualizados semanalmente pelo pipeline no Airflow."
    )


main()
