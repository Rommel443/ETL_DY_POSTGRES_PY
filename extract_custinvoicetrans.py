from config import SQL_SERVER_CONFIG, POSTGRES_CONFIG
from database_utils import fetch_data_from_sql, clean_dataframe, insert_data_to_postgres

def process_custinvoicetrans():
    query = """
    SELECT TOP 10
        SALESID, INVOICEID, INVENTDIMID, INVENTQTY, INVENTTRANSID,
        INVOICEDATE, ITEMID, LINEAMOUNT, PRICEUNIT, QTY,
        SALESPRICE, LINEDISC, LINEPERCENT, NUMBERSEQUENCEGROUP,
        ORIGSALESID, SALESUNIT, MODIFIEDDATETIME, CREATEDDATETIME, RECID
    FROM CUSTINVOICETRANS
    WHERE CONVERT(DATE, INVOICEDATE) BETWEEN '2024-01-01' AND '2024-11-30'
      AND INVOICEID <> '*'
      AND LEN(SALESID) > 0
    """
    df = fetch_data_from_sql(query, SQL_SERVER_CONFIG)
    df = clean_dataframe(df)
    insert_data_to_postgres(df, "custinvoicetrans", POSTGRES_CONFIG)
