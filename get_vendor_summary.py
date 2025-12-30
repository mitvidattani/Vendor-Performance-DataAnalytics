# get_vendor_summary.py
import os
import logging
import sqlite3
import pandas as pd

from ingestion_db import ingest_db

# ---------------- LOGGING ----------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)

# ---------------- FUNCTIONS ----------------
def create_vendor_sales_summary(conn):
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.ActualPrice,
        ps.PurchasePrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df):
    df["Volume"] = df["Volume"].astype(float)
    df.fillna(0, inplace=True)
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # --- Derived columns (UNCHANGED) ---
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
    df["ProfitMargin"] = (df["GrossProfit"] / df["TotalSalesDollars"]) * 100
    df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"]
    df["SalestoPurchaseRatio"] = (
        df["TotalSalesDollars"] / df["TotalPurchaseDollars"]
    )

    return df


# ---------------- MAIN ----------------
if __name__ == "__main__":
    conn = sqlite3.connect("inventory.db")

    logging.info("Creating Vendor Summary Table...")
    summary_df = create_vendor_sales_summary(conn)
    print("\n--- Vendor Summary (Raw) ---")
    print(summary_df.head())

    logging.info("Cleaning data and adding derived metrics...")
    clean_df = clean_data(summary_df)
    print("\n--- Vendor Summary (Cleaned & Enriched) ---")
    print(clean_df.head())

    ingest_db(clean_df, "vendor_sales_summary")
    logging.info("Vendor sales summary ingested successfully")

    sample_df = pd.read_sql_query(
        "SELECT * FROM vendor_sales_summary LIMIT 5;",
        conn
    )
    print("\n--- Vendor Summary (Read Back From DB) ---")
    print(sample_df)

    conn.close()
# Ingestion complete