import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt
import os

user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")

# Connect to Postgres
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/youtube_gold")

# --- Streamlit UI ---
st.title("📊 YouTube Trends Dashboard")

regions = st.sidebar.multiselect(
    "Select regions",
    ["US","IN","GB","CA","DE","FR","RU","JP","KR","MX"],
    default=["US","IN"]
)
min_views = st.sidebar.slider("Minimum views", 0, 10_000_000, 100_000)

# --- Helper function with caching ---
@st.cache_data
def run_query(sql: str):
    return pd.read_sql(sql, engine)

# --- Top categories by views ---
sql1 = f"""
SELECT category_name, region, SUM(views) as views
FROM videos_gold
WHERE region IN ({','.join([f"'{r}'" for r in regions])})
AND views >= {min_views}
GROUP BY category_name, region
"""
top_categories = run_query(sql1)

chart1 = alt.Chart(top_categories).mark_bar().encode(
    x="category_name:N",
    y="views:Q",
    color="region:N",
    tooltip=["category_name", "region", "views"]
).properties(title="Top Categories by Views")
st.altair_chart(chart1, use_container_width=True)

# --- Views over time ---
sql2 = f"""
SELECT trending_date, region, SUM(views) as views
FROM videos_gold
WHERE region IN ({','.join([f"'{r}'" for r in regions])})
AND views >= {min_views}
GROUP BY trending_date, region
ORDER BY trending_date
"""
daily_views = run_query(sql2)

chart2 = alt.Chart(daily_views).mark_line().encode(
    x="trending_date:T",
    y="views:Q",
    color="region:N"
).properties(title="Views Over Time")
st.altair_chart(chart2, use_container_width=True)

# --- Likes vs Comments (sampled) ---
sql3 = f"""
SELECT title, region, likes, comment_count
FROM videos_gold
WHERE region IN ({','.join([f"'{r}'" for r in regions])})
AND views >= {min_views}
LIMIT 5000
"""
scatter_df = run_query(sql3)

chart3 = alt.Chart(scatter_df).mark_circle(size=60).encode(
    x="likes:Q",
    y="comment_count:Q",
    color="region:N",
    tooltip=["title", "region", "likes", "comment_count"]
).interactive().properties(title="Likes vs Comments")
st.altair_chart(chart3, use_container_width=True)

# --- Engagement ratio histogram ---
sql4 = f"""
SELECT engagement_ratio, region
FROM videos_gold
WHERE region IN ({','.join([f"'{r}'" for r in regions])})
AND views >= {min_views}
"""
hist_df = run_query(sql4)

chart4 = alt.Chart(hist_df).mark_bar().encode(
    x=alt.X("engagement_ratio:Q", bin=True),
    y="count()",
    color="region:N"
).properties(title="Engagement Ratio Distribution")
st.altair_chart(chart4, use_container_width=True)