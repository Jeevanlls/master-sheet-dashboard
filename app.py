# app_fixed.py
# Master Sheet Dashboard with Owner Payout Summary & Dispute Tracking
import streamlit as st
import pandas as pd
import numpy as np
import io
import os
import hashlib
from datetime import datetime
from sqlalchemy import create_engine

# ======================
# AUTHENTICATION
# ======================
# Define and check login credentials.  Credentials are stored as
# username: hashed password pairs for simplicity.  To add more users,
# extend this dictionary.  In a real application you might want to
# integrate with an external auth provider or store hashed passwords
# securely.
import hashlib

def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

CREDENTIALS = {
    'admin': _hash_password('admin'),
    'user': _hash_password('password123'),
}

# Helper to rerun the Streamlit app in a version‑agnostic way.
# Newer versions expose `st.rerun()` while older versions still have
# `st.experimental_rerun()`. This function attempts to call the
# experimental API first, and falls back to the stable API if
# necessary. If neither exists, it does nothing.
def _safe_rerun() -> None:
    """Attempt to rerun the Streamlit script, regardless of version."""
    # Try the experimental API first
    try:
        st.experimental_rerun()  # type: ignore[attr-defined]
    except AttributeError:
        # Fall back to the stable API in newer Streamlit versions
        try:
            st.rerun()  # type: ignore[attr-defined]
        except AttributeError:
            # As a last resort do nothing; the user can manually refresh
            pass

def require_login():
    """
    Show a login form in the sidebar and verify credentials.  If the
    user is already authenticated, optionally provide a logout button.
    Returns True when the user is authenticated and False otherwise.
    """
    # initialise session state
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if st.session_state['authenticated']:
        # authenticated user: show logout option
        with st.sidebar.expander('Account', expanded=True):
            if st.button('Logout'):
                st.session_state['authenticated'] = False
                # Re-run the app to force a refresh and show the login form again
                _safe_rerun()
        return True

    # login prompt
    with st.sidebar.expander('Login', expanded=True):
        st.write('### Please log in to continue')
        username = st.text_input('Username')
        password = st.text_input('Password', type='password')
        login_clicked = st.button('Login')
        if login_clicked:
            if username in CREDENTIALS and _hash_password(password) == CREDENTIALS[username]:
                st.session_state['authenticated'] = True
                # On successful login trigger a rerun so the main dashboard can load
                _safe_rerun()
            else:
                st.error('Invalid username or password')
    return st.session_state['authenticated']

"""
This APP is an updated version : 5.0 @ 06-8-2025 Time 22:48 Created by: Jeevan Ratnam
This app provides a dashboard for managing eBay and Prime transaction data,generating a master sheet, and summarizing owner payouts. It includes features
for uploading transaction files, processing data, and generating reports.It also allows users to clear the database and manage settings related to VAT,fees, and dispute tracking.
"""

# ======================
# CONFIGURATION
# ======================
DB_FILE = "master_sheet.db"
LOGO_FILE = "prime-dark.svg"

st.set_page_config(
    page_title="Master Sheet Dashboard",
    page_icon=LOGO_FILE,
    layout="wide"
)

# Require user to log in before using the dashboard
if not require_login():
    st.stop()


# ======================
# DATABASE INITIALIZATION
# ======================
if not os.path.exists(DB_FILE):
    open(DB_FILE, 'a').close()
engine = create_engine(f"sqlite:///{DB_FILE}", echo=False)

# ======================
# UTILITIES
# ======================
def show_logo():
    """Display logo if available."""
    if os.path.exists(LOGO_FILE):
        st.image(LOGO_FILE, width=350)

def load_settings():
    """Load settings from the database."""
    if not engine.dialect.has_table(engine.connect(), "settings"):
        return {}
    try:
        df = pd.read_sql('settings', engine)
        if not df.empty:
            return df.iloc[0].to_dict()
    except Exception:
        pass
    return {}

def to_excel_with_style(df: pd.DataFrame) -> bytes:
    """
    Convert a pandas DataFrame into an Excel file in memory with
    custom styling applied.  The styling matches the colours used in the
    Owner Payout Summary table: the 'Active listing total on Ebay'
    column is highlighted in light blue, the 'Grand Total' column in
    pale yellow, and the row labelled 'GRAND TOTAL' in light green.

    Numeric columns are formatted as currency (pounds).

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to export.  Must be the same shape as the
        displayed table, including the 'Active listing total on Ebay'
        and 'Grand Total' columns if present.

    Returns
    -------
    bytes
        The binary content of the Excel file.
    """
    output = io.BytesIO()
    # Use the xlsxwriter engine for richer formatting
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Summary')
        workbook = writer.book
        worksheet = writer.sheets['Summary']

        # Define formats
        currency_fmt = workbook.add_format({'num_format': '£#,##0.00'})
        grand_total_col_fmt = workbook.add_format({'bg_color': '#ffff99', 'bold': True})
        active_total_col_fmt = workbook.add_format({'bg_color': '#add8e6', 'bold': True})
        total_row_fmt = workbook.add_format({'bg_color': '#d1fecb', 'bold': True})

        # Determine column indices (0-based) for special columns
        cols = df.columns.tolist()
        active_col_idx = cols.index('Active listing total on Ebay') if 'Active listing total on Ebay' in cols else None
        grand_total_idx = cols.index('Grand Total') if 'Grand Total' in cols else None
        # Apply column formats
        for idx, col in enumerate(cols):
            # Format numeric columns as currency
            if pd.api.types.is_numeric_dtype(df[col]):
                worksheet.set_column(idx, idx, None, currency_fmt)
        # Set special column formatting
        if active_col_idx is not None:
            worksheet.set_column(active_col_idx, active_col_idx, None, active_total_col_fmt)
        if grand_total_idx is not None:
            worksheet.set_column(grand_total_idx, grand_total_idx, None, grand_total_col_fmt)

        # Apply row format to the grand total row if the DataFrame has a
        # 'car_details_sku' column.  For summary tables (such as the
        # owner payout summary), this column may not exist, so row
        # highlighting is skipped in that case.
        if 'car_details_sku' in df.columns:
            gt_rows = df.index[df['car_details_sku'] == 'GRAND TOTAL'].tolist()
            if gt_rows:
                # Adjust for header row offset (header row is row 0)
                worksheet.set_row(gt_rows[0] + 1, None, total_row_fmt)

    output.seek(0)
    return output.getvalue()

def save_settings(settings):
    """Save settings to the database."""
    pd.DataFrame([settings]).to_sql('settings', engine, if_exists='replace', index=False)

def store_raw_data(df, table_name, engine):
    """
    Safely adds new transaction lines by creating a robust hash to skip duplicates.
    This version creates a standardized string for each row to avoid errors from
    data types or formatting. It never deletes existing data.
    Returns the count of new rows added and duplicates skipped.
    """
    if df.empty:
        return 0, 0  # (new_rows, skipped_rows)

    # --- Robust Hashing Logic ---
    # 1. Define a consistent order of columns for the hash.
    cols_to_hash = sorted([col for col in df.columns if col != 'row_hash'])

    # 2. Create a standardized string for each row.
    # This converts all values to strings, makes them lowercase, and joins them.
    # This process is not sensitive to original data types or formatting.
    def create_canonical_string(row):
        return '|'.join(str(row[col]).strip().lower() for col in cols_to_hash)

    hash_source_strings = df.apply(create_canonical_string, axis=1)

    # 3. Create the hash from the standardized string.
    df['row_hash'] = hash_source_strings.apply(lambda x: hashlib.md5(x.encode()).hexdigest())

    # --- Deduplication Logic ---
    # Remove duplicates within the current dataframe based on the row_hash.  This ensures
    # that repeated lines in a single uploaded file are dropped before checking against
    # the database.  Keep the first occurrence of each hash.
    df = df.drop_duplicates(subset=['row_hash'], keep='first')

    new_df = df
    if engine.dialect.has_table(engine.connect(), table_name):
        try:
            # Check for 'row_hash' column to prevent errors on old DBs
            db_cols = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", engine).columns
            if 'row_hash' in db_cols:
                existing_hashes = pd.read_sql(f"SELECT row_hash FROM {table_name}", engine)['row_hash'].tolist()
                new_df = df[~df['row_hash'].isin(existing_hashes)]
        except Exception:
            # If error (e.g., table empty), assume no duplicates
            pass

    skipped_count = len(df) - len(new_df)
    new_count = len(new_df)

    if new_count > 0:
        # Save the new rows, including the robust hash column
        new_df.to_sql(table_name, engine, if_exists='append', index=False)

    return new_count, skipped_count

@st.cache_data
def read_file(uploaded_file):
    """Read uploaded CSV or Excel file into a DataFrame."""
    try:
        if uploaded_file.name.endswith('.csv'):
            return pd.read_csv(uploaded_file)
        else:
            return pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading {uploaded_file.name}: {e}")
        return pd.DataFrame()

def to_excel(df, sheet_name='Report'):
    """Convert a DataFrame to Excel format in memory."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

def validate_dataframe(df, required_columns, file_type):
    """
    Checks if a dataframe contains all the required columns.
    Returns True and an empty string if valid.
    Returns False and an error message if invalid.
    """
    if df.empty:
        return False, f"The uploaded {file_type} file appears to be empty."

    # Standardize columns by stripping whitespace before checking
    df.columns = [c.strip() for c in df.columns]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        error_msg = (
            f"Invalid {file_type} file. It's missing the following required columns: **{', '.join(missing_cols)}**. "
            "Please check if you have uploaded the correct file."
        )
        return False, error_msg
    return True, ""

# ======================
# SIDEBAR MENU & CLEAR DB
# ======================
menu = st.sidebar.radio("Navigation", ["Upload & Process", "Reports", "Owner Payout Summary"])

with st.sidebar:
    st.markdown("### ⚠ Clear Database")
    if st.button("🗑 Clear Database"):
        st.session_state['confirm_clear'] = True

    if st.session_state.get('confirm_clear', False):
        st.warning("Are you sure you want to delete ALL data? This cannot be undone.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Yes, delete"):
                if os.path.exists(DB_FILE):
                    os.remove(DB_FILE)
                    open(DB_FILE, 'a').close()
                    st.success("Database cleared successfully. Restart the app.")
                else:
                    st.warning("Database file not found.")
                st.session_state['confirm_clear'] = False
        with col2:
            if st.button("❌ Cancel"):
                st.session_state['confirm_clear'] = False

# ======================
# DATA PROCESSING
# ======================
def process_data(ebay_df, prime_df, settings):
    """
    Combine eBay and Prime transaction data into a single master sheet.  This
    version introduces a "prime-only" stream to capture orders present in
    the Prime upload but absent from the eBay export.  It preserves the
    existing logic for eBay aggregation and manual MNL orders.
    """
    # === 0. INITIAL CLEANING & RENAMING ===
    ebay_df.columns = [c.strip() for c in ebay_df.columns]
    prime_df.columns = [c.strip() for c in prime_df.columns]

    if 'Order number' in ebay_df.columns:
        ebay_df['Order number'] = ebay_df['Order number'].astype(str).str.strip()
    prime_df.rename(columns={'Order': 'Order number', 'Sku': 'car_details_sku'}, inplace=True)
    if 'Order number' in prime_df.columns:
        prime_df['Order number'] = prime_df['Order number'].astype(str).str.strip()

    # Use the correct SKU column name 'Custom label'
    if 'Custom label' in ebay_df.columns:
        ebay_df.rename(columns={'Custom label': 'car_details_sku'}, inplace=True)
    else:
        ebay_df['car_details_sku'] = 'Unknown SKU'

    # === A. MANUAL ORDER STREAM ===
    # Identify manual orders in the Prime upload via an "MNL" prefix.  These
    # orders originate outside of the eBay transaction export, so they need
    # to be represented independently in the final dataset.  We construct
    # a separate DataFrame for these rows, with only a minimal set of
    # financial columns.  Remaining numeric fields will be filled with
    # zeros later when we combine all streams.
    is_manual_prime = prime_df['Order number'].str.startswith('MNL', na=False)
    manual_orders_raw = prime_df[is_manual_prime].copy()
    manual_final_df = pd.DataFrame()
    if not manual_orders_raw.empty:
        manual_final_df['order_number'] = manual_orders_raw['Order number']
        manual_final_df['owner'] = manual_orders_raw['Owner'].fillna('Unknown')
        manual_final_df['car_details_sku'] = manual_orders_raw['car_details_sku']
        manual_final_df['item_title'] = manual_orders_raw.get('Title', 'N/A')
        # use today's date as the transaction_date for manual uploads
        manual_final_df['transaction_date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
        # convert price and shipping to numeric values; fall back to zero on errors
        price = pd.to_numeric(manual_orders_raw.get('Order Price'), errors='coerce').fillna(0)
        shipping = pd.to_numeric(manual_orders_raw.get('Shipping Cost'), errors='coerce').fillna(0)
        manual_final_df['net_amount'] = price
        manual_final_df['shipping_cost'] = shipping
        manual_final_df['customer_payout'] = price - shipping
        # Default values for missing financial columns and flags
        manual_final_df['item_subtotal'] = 0
        manual_final_df['postage_packaging'] = 0
        manual_final_df['final_value_fee_fixed'] = 0
        manual_final_df['final_value_fee_variable'] = 0
        manual_final_df['regulatory_fee'] = 0
        manual_final_df['refund_fee'] = 0
        manual_final_df['return_postage'] = 0
        manual_final_df['vat_amount'] = 0
        manual_final_df['vat_deductible'] = 0
        manual_final_df['vat_global_scheme'] = ''
        manual_final_df['has_refund'] = False
        manual_final_df['vat_applicable'] = False

    # === A2. PRIME-ONLY ORDER STREAM ===
    # In the original logic, Prime rows without a matching eBay order were
    # ignored unless they started with "MNL".  However, users may upload
    # Prime reports that contain many orders not present in the eBay export.
    # To ensure these transactions are surfaced in the final report, we
    # create a second manual-like stream for all Prime rows (non-"MNL")
    # whose order number does not appear in the eBay data.  These will be
    # treated similarly to manual orders – we capture basic details and
    # compute a simple payout using the Net Sales (or Order Price) minus
    # shipping.  Any remaining columns will be filled with defaults when
    # combining streams.
    prime_only_final_df = pd.DataFrame()
    if not prime_df.empty:
        prime_non_manual = prime_df[~is_manual_prime].copy()
        # Determine which orders are absent in the eBay export
        ebay_order_numbers = set(ebay_df['Order number'].astype(str)) if 'Order number' in ebay_df.columns else set()
        unmatched_mask = ~prime_non_manual['Order number'].astype(str).isin(ebay_order_numbers)
        prime_unmatched = prime_non_manual[unmatched_mask]
        if not prime_unmatched.empty:
            prime_only_final_df['order_number'] = prime_unmatched['Order number']
            prime_only_final_df['owner'] = prime_unmatched['Owner'].fillna('Unknown')
            prime_only_final_df['car_details_sku'] = prime_unmatched['car_details_sku']
            prime_only_final_df['item_title'] = prime_unmatched.get('Title', 'N/A')
            # Attempt to use the original order date; if missing, default to today's date
            order_dates = pd.to_datetime(prime_unmatched.get('Order Date'), errors='coerce')
            prime_only_final_df['transaction_date'] = order_dates.dt.strftime('%d/%m/%Y').fillna(pd.to_datetime('today').strftime('%d/%m/%Y'))
            # Determine net amount: prefer Net Sales if present and non-zero, else fall back to Order Price
            net_sales = pd.to_numeric(prime_unmatched.get('Net Sales'), errors='coerce')
            order_price = pd.to_numeric(prime_unmatched.get('Order Price'), errors='coerce')
            prime_only_final_df['net_amount'] = net_sales.where(net_sales > 0, order_price).fillna(0)
            # Shipping cost
            shipping_only = pd.to_numeric(prime_unmatched.get('Shipping Cost'), errors='coerce').fillna(0)
            prime_only_final_df['shipping_cost'] = shipping_only
            # Customer payout = net minus shipping
            prime_only_final_df['customer_payout'] = prime_only_final_df['net_amount'] - prime_only_final_df['shipping_cost']
            # Default values for other financial columns and flags; these
            # columns mirror those created in the eBay stream and allow
            # seamless concatenation later on.  Setting them explicitly
            # avoids type mismatches (e.g., booleans vs empty strings) that
            # would otherwise occur when filtering in the Reports tab.
            prime_only_final_df['item_subtotal'] = 0
            prime_only_final_df['postage_packaging'] = 0
            prime_only_final_df['final_value_fee_fixed'] = 0
            prime_only_final_df['final_value_fee_variable'] = 0
            prime_only_final_df['regulatory_fee'] = 0
            prime_only_final_df['refund_fee'] = 0
            prime_only_final_df['return_postage'] = 0
            prime_only_final_df['vat_amount'] = 0
            prime_only_final_df['vat_deductible'] = 0
            prime_only_final_df['vat_global_scheme'] = ''
            prime_only_final_df['has_refund'] = False
            prime_only_final_df['vat_applicable'] = False

    # === B. EBAY ORDER STREAM (Based on the successful debug logic) ===
    prime_for_ebay = prime_df[~is_manual_prime].copy()
    numeric_cols_ebay = ['Net amount', 'Item subtotal', 'Postage and packaging', 'Final value fee – fixed', 'Final value fee – variable', 'Regulatory operating fee']
    for col in numeric_cols_ebay:
        if col in ebay_df.columns:
            ebay_df[col] = pd.to_numeric(ebay_df[col], errors='coerce').fillna(0)
    ebay_df['Transaction creation date'] = pd.to_datetime(ebay_df.get('Transaction creation date'), errors='coerce')
    ebay_df['Type'] = ebay_df.get('Type', pd.Series(dtype=str)).str.lower()

    # 1. Prepare per-order info (Owner, Shipping Cost, SKU) from Prime data
    #    We aggregate the first non-null SKU per order to ensure we have a
    #    consistent identifier for each order, even when the eBay file
    #    lacks a Custom label.  Shipping cost is summed and owner is taken
    #    from the first occurrence.
    prime_info_df = prime_for_ebay.groupby('Order number').agg(
        owner=('Owner', 'first'),
        shipping_cost=('Shipping Cost', 'sum'),
        car_details_sku=('car_details_sku', 'first')
    ).reset_index()

    # 2. Merge this enrichment data onto the raw eBay transaction lines
    ebay_df_merged = pd.merge(
        ebay_df,
        prime_info_df,
        on='Order number',
        how='left',
        suffixes=('_ebay', '_prime')
    )
    # Prefer the SKU from the Prime upload, but if it is missing or appears as a placeholder
    # (e.g. '-', '--', '', or NaN), fall back to the SKU from the eBay file.  If neither
    # provides a valid SKU, assign 'Unknown SKU'.
    def choose_sku(row):
        prime_sku = row.get('car_details_sku_prime')
        ebay_sku = row.get('car_details_sku_ebay')
        invalid_vals = {None, '', '-', '--', 'Unknown SKU', 'nan', np.nan}
        # Treat NaN values as invalid strings for comparison
        if isinstance(prime_sku, float) and pd.isna(prime_sku):
            prime_sku = None
        if isinstance(ebay_sku, float) and pd.isna(ebay_sku):
            ebay_sku = None
        # Determine which SKU to use
        if prime_sku not in invalid_vals:
            return str(prime_sku).strip()
        elif ebay_sku not in invalid_vals:
            return str(ebay_sku).strip()
        else:
            return 'Unknown SKU'
    # Apply the SKU selection logic row-wise
    if 'car_details_sku_prime' in ebay_df_merged.columns or 'car_details_sku_ebay' in ebay_df_merged.columns:
        ebay_df_merged['car_details_sku'] = ebay_df_merged.apply(choose_sku, axis=1)
    else:
        ebay_df_merged['car_details_sku'] = 'Unknown SKU'
    # Clean up interim columns
    for col in ['car_details_sku_prime', 'car_details_sku_ebay']:
        if col in ebay_df_merged.columns:
            ebay_df_merged.drop(columns=[col], inplace=True)

    # 3. Group by Order Number ONLY.  We no longer include the SKU in the
    #    grouping keys because the SKU is derived from the Prime upload
    #    rather than the eBay custom label.  This prevents orders from
    #    being split into multiple records when the eBay file lacks a
    #    custom label.
    grouping_keys = ['Order number']

    def get_first_valid_title(s):
        valid_titles = s.dropna().astype(str).str.strip()[~s.isin(['', '--'])]
        return valid_titles.iloc[0] if not valid_titles.empty else 'N/A'

    # Aggregation logic uses the ORIGINAL column names
    agg_logic = {
        'Item title': get_first_valid_title,
        'Net amount': 'sum',
        'Item subtotal': 'sum',
        'Postage and packaging': 'sum',
        'Final value fee – fixed': 'sum',
        'Final value fee – variable': 'sum',
        'Regulatory operating fee': 'sum',
        'Transaction creation date': 'first',
        'owner': 'first',
        'shipping_cost': 'first',
        'car_details_sku': 'first',
        'Type': lambda x: 'refund' if 'refund' in x.values else ('postage_label' if 'postage label' in x.values else 'order')
    }

    ebay_agg_df = ebay_df_merged.groupby(grouping_keys, dropna=False).agg(agg_logic).reset_index()

    # 4. Rename columns AFTER aggregation
    ebay_final_df = ebay_agg_df
    ebay_final_df.rename(columns={
        'Order number': 'order_number', 'Item title': 'item_title', 'Net amount': 'net_amount',
        'Item subtotal': 'item_subtotal', 'Postage and packaging': 'postage_packaging',
        'Final value fee – fixed': 'final_value_fee_fixed', 'Final value fee – variable': 'final_value_fee_variable',
        'Regulatory operating fee': 'regulatory_fee', 'Transaction creation date': 'transaction_date',
        'Type': 'type'
    }, inplace=True)
    ebay_final_df['owner'] = ebay_final_df['owner'].fillna('Unknown')

    # 5. Apply the Full, Original Calculation Block
    ebay_final_df['has_refund'] = ebay_final_df['type'] == 'refund'
    ebay_final_df['is_full_refund'] = (ebay_final_df['has_refund']) & (ebay_final_df['item_subtotal'] <= 0)
    ebay_final_df['item_subtotal'] = np.where(ebay_final_df['is_full_refund'], 0, ebay_final_df['item_subtotal'])
    ebay_final_df['refund_fee'] = np.where(ebay_final_df['has_refund'], settings.get('refund_handling_fee', 0.0), 0.0)

    has_postage_label_map = ebay_df.groupby('Order number')['Type'].apply(lambda x: 'postage label' in x.values)
    ebay_final_df['has_postage_label'] = ebay_final_df['order_number'].map(has_postage_label_map).fillna(False)

    conditions = [
        (~ebay_final_df['has_refund']),
        (ebay_final_df['has_refund'] & ~ebay_final_df['is_full_refund']),
        (ebay_final_df['is_full_refund'] & ebay_final_df['has_postage_label']),
        (ebay_final_df['is_full_refund'] & ~ebay_final_df['has_postage_label'] & (ebay_final_df['shipping_cost'] > 0)),
        (ebay_final_df['is_full_refund'] & ~ebay_final_df['has_postage_label'] & (ebay_final_df['shipping_cost'] <= 0))
    ]
    choices = [0, 0, 0, ebay_final_df['shipping_cost'], settings.get('default_return_postage', 0.0)]
    ebay_final_df['return_postage'] = np.select(conditions, choices, default=0)

    vat_threshold = settings.get('vat_threshold', 500.0)
    ebay_final_df['vat_applicable'] = (ebay_final_df['item_subtotal'] >= vat_threshold) & (~ebay_final_df['has_refund'])
    vat_rate = settings.get('vat_rate', 20.0) / 100
    divisor = 1 + vat_rate
    vat_method = settings.get('vat_method', 'Based on Net Sales')
    if vat_method == 'Based on Net Sales':
        vat_values = ebay_final_df['net_amount'] / divisor * vat_rate
    elif vat_method == 'Based on Order Value':
        vat_values = ebay_final_df['item_subtotal'] / divisor * vat_rate
    else:
        temp_payout = ebay_final_df['net_amount'] - (ebay_final_df['shipping_cost'] + ebay_final_df['refund_fee'] - ebay_final_df['return_postage'])
        vat_values = temp_payout / divisor * vat_rate
    ebay_final_df['vat_amount'] = np.where(ebay_final_df['vat_applicable'], vat_values, 0)
    ebay_final_df['vat_deductible'] = ebay_final_df['vat_amount']
    ebay_final_df['vat_global_scheme'] = np.where(ebay_final_df['vat_applicable'], "", "Under Global Scheme")

    ebay_final_df['customer_payout'] = (ebay_final_df['net_amount'] - ebay_final_df['shipping_cost'].fillna(0) - ebay_final_df['refund_fee'] - ebay_final_df['return_postage'] - ebay_final_df['vat_amount'])
    ebay_final_df['transaction_date'] = pd.to_datetime(ebay_final_df['transaction_date']).dt.strftime('%d/%m/%Y')

    # === C. COMBINE THE THREE STREAMS ===
    final_df = pd.concat([ebay_final_df, manual_final_df, prime_only_final_df], ignore_index=True)

    # === D. FINALIZE AND RETURN ===
    final_columns = [
        'order_number', 'owner', 'car_details_sku', 'transaction_date', 'item_title', 'net_amount',
        'shipping_cost', 'item_subtotal', 'postage_packaging', 'final_value_fee_fixed',
        'final_value_fee_variable', 'regulatory_fee', 'refund_fee', 'return_postage',
        'vat_global_scheme', 'vat_amount', 'vat_deductible', 'customer_payout', 'has_refund', 'vat_applicable'
    ]
    for col in final_columns:
        if col not in final_df.columns:
            final_df[col] = ''
    numeric_final = [
        'net_amount', 'shipping_cost', 'item_subtotal', 'postage_packaging', 'final_value_fee_fixed',
        'final_value_fee_variable', 'regulatory_fee', 'refund_fee', 'return_postage', 'vat_amount',
        'vat_deductible', 'customer_payout'
    ]
    final_df[numeric_final] = final_df[numeric_final].fillna(0)
    return final_df[final_columns].round(2).sort_values(by='transaction_date', na_position='first')

# ======================
# UPLOAD & PROCESS TAB
# ======================
if menu == "Upload & Process":
    show_logo()
    st.title("📊 Master Sheet Generator")
    saved_settings = load_settings()
    settings = saved_settings if saved_settings else {}

    ebay_file = st.file_uploader("Upload eBay Sales Data (CSV/Excel)", type=['csv', 'xlsx', 'xls'])
    prime_file = st.file_uploader("Upload Prime Data (CSV/Excel)", type=['csv', 'xlsx', 'xls'])

    st.markdown("---")
    active_listing_file = st.file_uploader("Upload Active eBay Listing Report (Optional)", type=['csv', 'xlsx', 'xls'])
    listing_date = st.date_input("Active Listing Report Date", value=datetime.today())
    st.markdown("---")

    with st.expander("VAT, Fees, Rules"):
        settings['vat_method'] = st.selectbox(
            "VAT Method",
            ('Based on Net Sales', 'Based on Order Value', 'Based on Customer Payout'),
            index=['Based on Net Sales', 'Based on Order Value', 'Based on Customer Payout'].index(settings.get('vat_method', 'Based on Net Sales'))
        )
        settings['vat_rate'] = st.number_input("VAT Rate (%)", value=settings.get('vat_rate', 20.0), step=0.1, format="%.2f")
        settings['vat_threshold'] = st.number_input("VAT Threshold (£)", value=settings.get('vat_threshold', 500.0), step=10.0, format="%.2f")
        settings['refund_handling_fee'] = st.number_input("Refund Fee (£)", value=settings.get('refund_handling_fee', 0.0), step=0.5, format="%.2f")
        settings['default_return_postage'] = st.number_input("Default Return Postage (£)", value=settings.get('default_return_postage', 0.0), step=0.5, format="%.2f")
        settings['dispute_keywords'] = st.text_input("Dispute Keywords (comma-separated)", value=settings.get('dispute_keywords', 'hold,payment dispute,claim,cancellation'))

    if st.button("Generate Master Sheet"):
        settings['active_listing_date'] = listing_date.strftime('%d/%m/%Y')
        save_settings(settings)

        if active_listing_file is not None:
            with st.spinner("Processing Active Listings..."):
                listing_df = read_file(active_listing_file)
                listing_req_cols = ['Custom label (SKU)', 'Available quantity', 'Current price']
                is_listing_valid, listing_error = validate_dataframe(listing_df, listing_req_cols, "Active Listing")
                if is_listing_valid:
                    listing_df.to_sql('active_listings', engine, if_exists='replace', index=False)
                    st.info(f"Active Listing Report processed with {len(listing_df)} rows.")
                else:
                    st.error(listing_error)

        if ebay_file is not None and prime_file is not None:
            ebay_df = read_file(ebay_file)
            prime_df = read_file(prime_file)

            # --- UPDATED: Validator now checks for the correct 'Custom label' column ---
            ebay_req_cols = ['Order number', 'Net amount', 'Transaction creation date', 'Type', 'Custom label']
            is_ebay_valid, ebay_error = validate_dataframe(ebay_df, ebay_req_cols, "eBay")
            if not is_ebay_valid:
                st.error(ebay_error)

            prime_req_cols = ['Order', 'Owner', 'Sku', 'Shipping Cost']
            is_prime_valid, prime_error = validate_dataframe(prime_df, prime_req_cols, "Prime")
            if not is_prime_valid:
                st.error(prime_error)

            if is_ebay_valid and is_prime_valid:
                with st.spinner("Ingesting transaction data..."):
                    ebay_new, ebay_skipped = store_raw_data(ebay_df, "transactions_raw", engine)
                    prime_new, prime_skipped = store_raw_data(prime_df, "prime_raw", engine)
                st.info(f"eBay file: Added {ebay_new} new transaction lines. Skipped {ebay_skipped} existing duplicates.")
                st.info(f"Prime file: Added {prime_new} new lines. Skipped {prime_skipped} existing duplicates.")

                if ebay_new > 0 or prime_new > 0:
                    with st.spinner("Recalculating all payouts..."):
                        processed_df = process_data(pd.read_sql("transactions_raw", engine), pd.read_sql("prime_raw", engine), settings)
                        processed_df.to_sql('transactions_processed', engine, if_exists='replace', index=False)
                    st.success(f"Processing complete! Master sheet regenerated with {len(processed_df)} unique records.")
                else:
                    st.warning("No new transaction lines were found to process.")
        elif ebay_file or prime_file:
            st.warning("Please upload both eBay and Prime data files to process transactions.")

# ======================
# REPORTS TAB
# ======================
if menu == "Reports":
    show_logo()
    st.title("📊 Reports Dashboard")
    if not engine.dialect.has_table(engine.connect(), "transactions_processed"):
        st.warning("No processed data available.")
    else:
        df = pd.read_sql("transactions_processed", engine)
        settings = load_settings()

        # --- Create the verify_flag column based on dispute keywords ---
        dispute_list = [k.strip().lower() for k in settings.get("dispute_keywords", "").split(",") if k.strip()]
        if engine.dialect.has_table(engine.connect(), "transactions_raw") and dispute_list:
            raw_df = pd.read_sql("transactions_raw", engine)
            raw_df.columns = [c.strip() for c in raw_df.columns]
            raw_df['type'] = raw_df.get('Type', pd.Series(dtype=str)).astype(str).str.lower()
            if 'Order number' in raw_df.columns:
                raw_df.rename(columns={'Order number': 'order_number'}, inplace=True)
            if 'order_number' in raw_df.columns:
                dispute_orders = raw_df[raw_df['type'].isin(dispute_list)]['order_number'].unique()
                df['verify_flag'] = df['order_number'].isin(dispute_orders)
            else:
                df['verify_flag'] = False
        else:
            df['verify_flag'] = False

        st.subheader("🔍 Filters")
        owner_options = sorted(df['owner'].fillna("Unknown").unique())
        sku_options = sorted(df['car_details_sku'].fillna("Unknown").unique())

        # Convert transaction_date for filtering
        df['transaction_date_dt'] = pd.to_datetime(df['transaction_date'], format='%d/%m/%Y', errors='coerce')
        valid_dates = df['transaction_date_dt'].dropna()
        date_min = valid_dates.min().date() if not valid_dates.empty else datetime.today().date()
        date_max = valid_dates.max().date() if not valid_dates.empty else datetime.today().date()

        col1, col2 = st.columns(2)
        with col1:
            owner_filter = st.multiselect("Owner", owner_options)
        with col2:
            sku_filter = st.multiselect("SKU", sku_options)

        title_search = st.text_input("Item Title Contains")

        col3, col4, col5 = st.columns(3)
        with col3:
            vat_filter = st.selectbox("VAT Applicable", ["All", "Yes", "No"])
        with col4:
            refund_filter = st.selectbox("Has Refund", ["All", "Yes", "No"])
        with col5:
            verify_filter = st.selectbox("Flagged for Verification", ["All", "Yes", "No"])

        date_range = st.date_input("Date Range", value=(date_min, date_max))

        # Apply all filters
        if owner_filter:
            df = df[df['owner'].fillna("Unknown").isin(owner_filter)]
        if sku_filter:
            df = df[df['car_details_sku'].fillna("Unknown").isin(sku_filter)]
        if title_search:
            df = df[df['item_title'].str.contains(title_search, case=False, na=False)]
        if vat_filter != "All":
            df = df[df['vat_applicable'] == (vat_filter == "Yes")]
        if refund_filter != "All":
            df = df[df['has_refund'] == (refund_filter == "Yes")]
        if verify_filter != "All":
            df = df[df['verify_flag'] == (verify_filter == "Yes")]
        if len(date_range) == 2:
            start_date = pd.to_datetime(date_range[0])
            end_date = pd.to_datetime(date_range[1])
            df = df[(df['transaction_date_dt'] >= start_date) & (df['transaction_date_dt'] <= end_date)]

        df = df.drop(columns=['transaction_date_dt'])

        if not df.empty:
            st.markdown("---")
            totals = df.select_dtypes(include=np.number).sum()
            total_col1, total_col2, total_col3, total_col4 = st.columns(4)
            with total_col1:
                st.metric("Net Amount", f"£{totals.get('net_amount', 0):,.2f}")
            with total_col2:
                st.metric("Customer Payout", f"£{totals.get('customer_payout', 0):,.2f}")
            with total_col3:
                st.metric("VAT Amount", f"£{totals.get('vat_amount', 0):,.2f}")
            with total_col4:
                st.metric("Refund Fees", f"£{totals.get('refund_fee', 0):,.2f}")
            st.markdown("---")

            rows_per_page = 50
            total_rows = len(df)
            total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)
            page_number = st.number_input('Page', min_value=1, max_value=max(1, total_pages), value=1)
            start_idx = (page_number - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            df_paginated = df.iloc[start_idx:end_idx]

            def highlight_verify_row(row):
                return ['background-color: yellow' if row['verify_flag'] else '' for _ in row]
            def highlight_negative_payout(val):
                return 'color: red' if isinstance(val, (int, float)) and val < 0 else ''

            currency_columns = [col for col in df_paginated.select_dtypes(include=np.number).columns if 'applicable' not in col and 'flag' not in col]
            formatter = {col: "£{:,.2f}" for col in currency_columns}

            styled_df = df_paginated.style.apply(highlight_verify_row, axis=1).set_properties(subset=['customer_payout'], **{'background-color': '#d1fecb'}).applymap(highlight_negative_payout, subset=['customer_payout']).format(formatter)

            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            st.write(f"Showing rows **{start_idx + 1}** to **{min(end_idx, total_rows)}** of **{total_rows}**.")
        else:
            st.info("No orders match the selected filters.")

        st.download_button("📥 Download Filtered Excel", to_excel(df), "filtered_report.xlsx")

# ======================
# OWNER PAYOUT SUMMARY TAB
# ======================
elif menu == "Owner Payout Summary":
    show_logo()
    st.title("🏆 Owner Payout Summary")

    # Check that processed data exists
    if not engine.dialect.has_table(engine.connect(), "transactions_processed"):
        st.warning("No processed data available.")
    else:
        df = pd.read_sql("transactions_processed", engine)
        # Standardise SKU and dates
        df['car_details_sku'] = df['car_details_sku'].fillna('Unknown SKU')
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], dayfirst=True, errors='coerce')
        df['short_title'] = df['item_title'].astype(str).str[:20] + '...'
        # Prime earnings = net_amount minus customer payout
        df['prime_earnings'] = df['net_amount'] - df['customer_payout']

        # ----------------------
        # DATE RANGE AND OTHER FILTERS
        # ----------------------------
        # Consolidate all filter widgets into a single horizontal row.  This saves
        # vertical space and makes it easier to understand which controls are related.
        # The row includes: a checkbox to use the entire date range, a dropdown to
        # select an owner, a dropdown to choose the grouping period, and a multiselect
        # to filter by SKUs.  If the user unchecks "Use entire date range", a date
        # range input will appear directly beneath the filters.
        if not df['transaction_date'].isna().all():
            min_date = df['transaction_date'].min().date()
            max_date = df['transaction_date'].max().date()
            filter_cols = st.columns([1, 2, 1, 2])
            # Use entire date range checkbox
            with filter_cols[0]:
                use_all_dates = st.checkbox("Use entire date range", value=True)
            # Owner selection
            owners = ["All Owners"] + sorted(df['owner'].fillna("Unknown").unique())
            with filter_cols[1]:
                owner_selected = st.selectbox("Select Owner", owners)
            # Group by selection
            with filter_cols[2]:
                group_by = st.selectbox("Group By", ["Year", "Month", "Week", "Day"])
            # SKU filter
            sku_options = sorted(df['car_details_sku'].dropna().unique())
            with filter_cols[3]:
                sku_filter = st.multiselect("Filter SKUs", sku_options)
            # Date range picker if not using all dates
            if not use_all_dates:
                date_range = st.date_input(
                    "Date Range",
                    value=(min_date, max_date),
                    key="date_range_selector"
                )
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    start_date, end_date = date_range
                    df = df[(df['transaction_date'] >= pd.to_datetime(start_date)) & (df['transaction_date'] <= pd.to_datetime(end_date))]
        else:
            # Default values when there are no valid transaction dates
            use_all_dates = True
            owners = ["All Owners"] + sorted(df['owner'].fillna("Unknown").unique())
            owner_selected = "All Owners"
            group_by = "Year"
            sku_filter = []

        # Apply owner filter
        if owner_selected != "All Owners":
            df = df[df['owner'] == owner_selected]
        # Derive period column based on group_by choice
        if group_by == "Year":
            df['period'] = df['transaction_date'].dt.year
        elif group_by == "Month":
            df['period'] = df['transaction_date'].dt.to_period('M').astype(str)
            month_order = sorted(
                df['period'].dropna().unique(),
                key=lambda x: (int(x.split('-')[0]), int(x.split('-')[1]))
            )
            df['period'] = pd.Categorical(df['period'], categories=month_order, ordered=True)
        elif group_by == "Week":
            df['period'] = df['transaction_date'].dt.strftime('%G-W%V')
        else:
            df['period'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        # Apply SKU filter if specified
        if sku_filter:
            df = df[df['car_details_sku'].isin(sku_filter)]

        # Apply owner filter
        if owner_selected != "All Owners":
            df = df[df['owner'] == owner_selected]
        # Apply group-by selection to compute the period column
        if group_by == "Year":
            df['period'] = df['transaction_date'].dt.year
        elif group_by == "Month":
            df['period'] = df['transaction_date'].dt.to_period('M').astype(str)
            month_order = sorted(df['period'].dropna().unique(), key=lambda x: (int(x.split('-')[0]), int(x.split('-')[1])))
            df['period'] = pd.Categorical(df['period'], categories=month_order, ordered=True)
        elif group_by == "Week":
            df['period'] = df['transaction_date'].dt.strftime('%G-W%V')
        else:
            df['period'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        # Apply SKU filter
        if sku_filter:
            df = df[df['car_details_sku'].isin(sku_filter)]

        # ----------------------
        # OVERALL TOTALS & SUMMARY BY OWNER
        # ----------------------
        # After applying owner and SKU filters, compute overall metrics and a summary by owner.
        if not df.empty:
            # Compute a fees column for summarisation
            df_summary = df.copy()
            df_summary['fees'] = (
                df_summary['final_value_fee_fixed'] +
                df_summary['final_value_fee_variable'] +
                df_summary['regulatory_fee'] +
                df_summary['refund_fee']
            )
            # Aggregate totals across the entire filtered dataset
            overall_totals = {
                'Total Net Amount': df_summary['net_amount'].sum(),
                'Total Shipping Cost': df_summary['shipping_cost'].sum(),
                'Total Customer Payout': df_summary['customer_payout'].sum(),
                'Total Item Subtotal': df_summary['item_subtotal'].sum(),
                'Total Postage Packaging': df_summary['postage_packaging'].sum(),
                'Total Fees': df_summary['fees'].sum(),
                'Total Return Postage': df_summary['return_postage'].sum(),
                'Total VAT Amount': df_summary['vat_amount'].sum(),
                'Total VAT Deductible': df_summary['vat_deductible'].sum()
            }

            # Build a DataFrame for the overall totals and format values as currency
            metrics_table = pd.DataFrame.from_dict(overall_totals, orient='index', columns=['Value'])
            metrics_table['Value'] = metrics_table['Value'].apply(lambda x: f"£{x:,.2f}")
            metrics_table.reset_index(inplace=True)
            metrics_table.rename(columns={'index': 'Metric'}, inplace=True)

            # Compute percentage differences between key totals
            subtotal = overall_totals['Total Item Subtotal'] if overall_totals['Total Item Subtotal'] != 0 else 1
            net_amount = overall_totals['Total Net Amount'] if overall_totals['Total Net Amount'] != 0 else 1
            diff_subtotal_net = (overall_totals['Total Item Subtotal'] - overall_totals['Total Net Amount']) / subtotal
            diff_net_payout = (overall_totals['Total Net Amount'] - overall_totals['Total Customer Payout']) / net_amount
            diff_subtotal_payout = (overall_totals['Total Item Subtotal'] - overall_totals['Total Customer Payout']) / subtotal

            # Display difference metrics and overall totals in a compact format.
            # First, show the three percentage differences in one horizontal row.
            st.markdown("### Difference Metrics")
            diff_cols = st.columns(3)
            diff_cols[0].markdown(
                f"<div style='font-size:14px; text-align:center'><strong>Subtotal vs Net</strong><br>{diff_subtotal_net:.2%}</div>",
                unsafe_allow_html=True
            )
            diff_cols[1].markdown(
                f"<div style='font-size:14px; text-align:center'><strong>Net vs Payout</strong><br>{diff_net_payout:.2%}</div>",
                unsafe_allow_html=True
            )
            diff_cols[2].markdown(
                f"<div style='font-size:14px; text-align:center'><strong>Subtotal vs Payout</strong><br>{diff_subtotal_payout:.2%}</div>",
                unsafe_allow_html=True
            )

            # Next, display the overall totals across three columns with three metrics
            # in each column.  This reduces vertical space compared to a long table.
            st.markdown("### Overall Totals")
            totals_cols = st.columns(3)
            metrics_list = list(overall_totals.items())
            for i in range(3):
                with totals_cols[i]:
                    for j in range(i * 3, (i + 1) * 3):
                        if j < len(metrics_list):
                            name, val = metrics_list[j]
                            st.markdown(
                                f"<div style='font-size:14px; margin-bottom:4px'><strong>{name}</strong><br>£{val:,.2f}</div>",
                                unsafe_allow_html=True
                            )

        # Load settings to get active listing date if available
        settings = load_settings()
        active_listing_date = settings.get('active_listing_date')
        if active_listing_date:
            st.info(f"Active listing snapshot date: {active_listing_date}")

        # Prepare active listing totals per SKU if the table exists
        active_total_map = {}
        if engine.dialect.has_table(engine.connect(), 'active_listings'):
            act_df = pd.read_sql('active_listings', engine)
            act_df.columns = [c.strip() for c in act_df.columns]
            # Ensure required columns exist
            if 'Custom label (SKU)' in act_df.columns and 'Available quantity' in act_df.columns and 'Current price' in act_df.columns:
                # Compute total value per SKU (quantity * price)
                qty = pd.to_numeric(act_df['Available quantity'], errors='coerce').fillna(0)
                price = pd.to_numeric(act_df['Current price'], errors='coerce').fillna(0)
                act_df['active_total'] = qty * price
                # Map by SKU key
                active_total_map = act_df.groupby(act_df['Custom label (SKU)'].astype(str).str.strip())['active_total'].sum().to_dict()

        # Tabs for different metrics
        tab1, tab2, tab3 = st.tabs(["Customer Payout", "Net Amount", "Prime Earnings"])
        metrics_map = { "Customer Payout": "customer_payout", "Net Amount": "net_amount", "Prime Earnings": "prime_earnings" }

        def generate_summary(metric_name):
            # Metric summarisation per SKU and period
            if df.empty or df['period'].isna().all():
                st.warning("No data available for the selected period or filters.")
                return

            pivot_df = pd.pivot_table(
                df,
                index=['car_details_sku'],
                columns='period',
                values=metric_name,
                aggfunc='sum',
                fill_value=0
            )
            pivot_df.reset_index(inplace=True)

            # Title and first order date per SKU
            title_map = df.drop_duplicates(subset=['car_details_sku']).set_index('car_details_sku')['short_title']
            first_order_date_map = df.groupby('car_details_sku')['transaction_date'].min()
            pivot_df.insert(1, 'title', pivot_df['car_details_sku'].map(title_map).fillna('N/A'))
            pivot_df.insert(2, 'First Order Date', pivot_df['car_details_sku'].map(first_order_date_map).dt.strftime('%d/%m/%Y'))

            # Insert active listing total column if available
            if active_total_map:
                pivot_df.insert(3, 'Active listing total on Ebay', pivot_df['car_details_sku'].map(active_total_map).fillna(0))

            # Compute grand total across period columns (exclude active listing column from this sum)
            numeric_cols = pivot_df.select_dtypes(include=np.number).columns.tolist()
            if 'Active listing total on Ebay' in numeric_cols:
                numeric_cols.remove('Active listing total on Ebay')
            pivot_df['Grand Total'] = pivot_df[numeric_cols].sum(axis=1)

            # Construct a totals row.  For the Active listing column, sum only the
            # values currently displayed in the pivot table (rather than the
            # entire active_total_map) so that filtering by date, owner or SKU
            # is respected.  Place the totals row at the bottom by assigning its index.
            total_row_data = {
                'car_details_sku': 'GRAND TOTAL',
                'title': '',
                'First Order Date': ''
            }
            if 'Active listing total on Ebay' in pivot_df.columns:
                total_row_data['Active listing total on Ebay'] = pivot_df['Active listing total on Ebay'].sum()
            for col in numeric_cols + ['Grand Total']:
                total_row_data[col] = pivot_df[col].sum()
            total_row = pd.DataFrame([total_row_data])
            # Set the index to the end of pivot_df so the row appears at the bottom
            total_row.index = [len(pivot_df)]
            pivot_df = pd.concat([pivot_df, total_row])

            # Highlight functions
            def highlight_total_row(row):
                return ['background-color: #d1fecb; font-weight: bold;'] * len(row) if row['car_details_sku'] == 'GRAND TOTAL' else [''] * len(row)

            def highlight_special_col(col):
                if col.name == 'Grand Total':
                    return ['background-color: #ffff99; font-weight: bold;'] * len(col)
                elif col.name == 'Active listing total on Ebay':
                    return ['background-color: #add8e6; font-weight: bold;'] * len(col)
                else:
                    return [''] * len(col)

            # Format numeric columns as currency
            all_numeric_cols = pivot_df.select_dtypes(include=np.number).columns
            formatter = {col: "£{:,.2f}" for col in all_numeric_cols}
            styled_df = pivot_df.style.apply(highlight_total_row, axis=1).apply(highlight_special_col, axis=0).format(formatter)

            # Download the summary with formatting preserved and display
            st.download_button(
                f"📥 Download {metric_name} Summary",
                to_excel_with_style(pivot_df),
                f"{metric_name}_summary.xlsx"
            )
            st.dataframe(styled_df, use_container_width=True, hide_index=True)

        with tab1:
            generate_summary(metrics_map["Customer Payout"])
        with tab2:
            generate_summary(metrics_map["Net Amount"])
        with tab3:
            generate_summary(metrics_map["Prime Earnings"])

        # ----------------------
        # PAYOUT SUMMARY BY OWNER (BOTTOM SECTION)
        # ----------------------
        if not df.empty:
            st.markdown("---")
            st.markdown("### Payout Summary by Owner")
            # Aggregate numeric columns per owner using the same filtered dataframe
            agg_map_owner = {
                'net_amount': 'sum',
                'shipping_cost': 'sum',
                'item_subtotal': 'sum',
                'postage_packaging': 'sum',
                'return_postage': 'sum',
                'final_value_fee_fixed': 'sum',
                'final_value_fee_variable': 'sum',
                'regulatory_fee': 'sum',
                'refund_fee': 'sum',
                'vat_amount': 'sum',
                'vat_deductible': 'sum',
                'customer_payout': 'sum'
            }
            owner_summary_df = df.groupby('owner').agg(agg_map_owner)
            owner_summary_df['fees'] = (
                owner_summary_df['final_value_fee_fixed'] +
                owner_summary_df['final_value_fee_variable'] +
                owner_summary_df['regulatory_fee'] +
                owner_summary_df['refund_fee']
            )
            owner_summary_df = owner_summary_df.drop(columns=['final_value_fee_fixed', 'final_value_fee_variable', 'regulatory_fee', 'refund_fee'])
            display_cols_owner = [
                'net_amount', 'shipping_cost', 'item_subtotal', 'postage_packaging',
                'return_postage', 'fees', 'vat_amount', 'vat_deductible', 'customer_payout'
            ]
            owner_summary_df = owner_summary_df[display_cols_owner]
            currency_cols_owner = owner_summary_df.select_dtypes(include=np.number).columns
            formatter_owner = {col: "£{:,.2f}" for col in currency_cols_owner}
            styled_owner_summary = owner_summary_df.reset_index().style.format(formatter_owner)
            # Download button
            st.download_button(
                "📥 Download Owner Summary",
                to_excel_with_style(owner_summary_df.reset_index()),
                "owner_summary.xlsx"
            )
            st.dataframe(styled_owner_summary, use_container_width=True, hide_index=True)