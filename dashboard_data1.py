import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import yaml
import re

def process_dataset(path, dataset_name):
    # Books
    with open(f"{path}/books.yaml", "r") as f:
        data = yaml.safe_load(f)
    books_df = pd.DataFrame(data)
    books_df.columns = [col.lstrip(":") for col in books_df.columns]

    # Orders
    orders_df = pd.read_parquet(f"{path}/orders.parquet")
    orders_df.drop_duplicates(inplace=True)
    orders_df.dropna(subset=["user_id", "book_id"], inplace=True)
    orders_df["paid_price"] = orders_df["quantity"] * orders_df["unit_price"]

    def clean_price(x):
        if pd.isna(x): return None
        match = re.findall(r"[\d\.]+", str(x))
        return float(match[0]) if match else None
    orders_df["paid_price_clean"] = orders_df["paid_price"].apply(clean_price)

    def detect_currency(x):
        s = str(x)
        if "€" in s: return "EUR"
        if "$" in s or "USD" in s: return "USD"
        return "UNKNOWN"
    orders_df["currency"] = orders_df["paid_price"].apply(detect_currency)

    def convert_to_usd(val, currency):
        if pd.isna(val): return None
        if currency == "EUR": return round(val * 1.2, 2)
        return val
    orders_df["paid_price_usd"] = orders_df.apply(
        lambda row: convert_to_usd(row["paid_price_clean"], row["currency"]), axis=1
    )

    def clean_timestamp(x):
        if pd.isna(x): return None
        x = str(x).replace("A.M.", "AM").replace("P.M.", "PM")
        x = x.replace(",", " ")
        return x
    orders_df["timestamp"] = orders_df["timestamp"].apply(clean_timestamp)
    orders_df["timestamp"] = pd.to_datetime(orders_df["timestamp"], errors="coerce", utc=True)
    orders_df["date"] = orders_df["timestamp"].dt.strftime("%Y-%m-%d")
    print(orders_df["timestamp"].head(10))

    books_df = books_df.rename(columns={"id": "book_id"})
    merged_df = orders_df.merge(books_df, on="book_id", how="left")

    # Calculations
    daily_revenue = orders_df.groupby("date")["paid_price_usd"].sum()
    top5_days = daily_revenue.sort_values(ascending=False).head(5)
    unique_users = orders_df["user_id"].nunique()
    merged_df["authors_set"] = merged_df["author"].apply(lambda x: frozenset(x.split(",")))
    unique_author_sets = merged_df["authors_set"].nunique()
    popular_authors = merged_df.groupby("authors_set")["book_id"].count().sort_values(ascending=False).head(1)
    top_customer = orders_df.groupby("user_id")["paid_price_usd"].sum().sort_values(ascending=False).head(1)

    # Tabs
    st.subheader(f"{dataset_name} - Top 5 days by revenue")
    st.write(top5_days)

    fig, ax = plt.subplots()
    daily_revenue.plot(ax=ax)
    st.pyplot(fig)

    st.write("Unique users:", unique_users)
    st.write("Best buyer IDs:", list(top_customer.index))
    st.write("Unique author sets:", unique_author_sets)
    st.write("Most popular authors:", popular_authors.index.tolist())

# --- Main App ---
st.title("BI Dashboard - DATA1 / DATA2 / DATA3")

tab1, tab2, tab3 = st.tabs(["DATA1", "DATA2", "DATA3"])

with tab1:
    process_dataset(r"C:/Users/user/Downloads/data/data/DATA1", "DATA1")

with tab2:
    process_dataset(r"C:/Users/user/Downloads/data/data/DATA2", "DATA2")

with tab3:
    process_dataset(r"C:/Users/user/Downloads/data/data/DATA3", "DATA3")
