# app.py - Shipping Curves Uploader
from dash import Dash, html, dcc, callback, Output, Input, State
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import base64
import io

import configparser
import os
from sqlalchemy import create_engine, text

############################################ PostgreSQL Connection ###################################################
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.abspath(os.path.join(script_dir, '..'))
    CONFIG_FILE_PATH = os.path.join(config_dir, 'config.ini')
except:
    CONFIG_FILE_PATH = 'config.ini'

config_reader = configparser.ConfigParser(interpolation=None)
config_reader.read(CONFIG_FILE_PATH)

DB_CONNECTION_STRING = config_reader.get('DATABASE', 'CONNECTION_STRING', fallback=None)
DB_SCHEMA = config_reader.get('DATABASE', 'SCHEMA', fallback=None)

if not DB_CONNECTION_STRING:
    raise ValueError(f"Missing DATABASE CONNECTION_STRING in {CONFIG_FILE_PATH}")

engine = create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)

#------------------------------------------- Style Constants ----------------------------------------------------------#
# Professional McKinsey Blue Styling - Following Enterprise Standards
STYLES = {
    'page_header': {
        'color': '#1f2937',  # Professional dark gray
        'font-weight': '600',
        'font-size': '32px',
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'
    },
    'icon_primary': {
        'color': '#2E86C1'  # McKinsey blue
    },
    'section_header': {
        'color': '#1f2937',
        'font-weight': '500',
        'font-size': '22px',
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'
    },
    'card_header': {
        'background': '#f8fafc',  # Light blue tint - subtle brand connection
        'border': 'none',
        'font-weight': '500',
        'font-size': '16px',
        'padding': '14px 18px',
        'border-radius': '6px'
    },
    'card_container': {
        'background': 'linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)',
        'border': 'none',
        'padding': '24px 32px',
        'border-bottom': '2px solid #2E86C1'  # McKinsey blue accent
    },
    'section_container': {
        'background': '#f8fafc',
        'border-bottom': '1px solid #cbd5e1',
        'padding': '14px 18px',
        'border-radius': '6px'
    },
    'table_container': {
        'border': '1px solid #cbd5e1',
        'border-radius': '6px',
        'padding': '16px'
    },
    'content_padding': {
        'padding': '24px'
    },
    'border_container': {
        'border': '1px solid #cbd5e1',
        'border-radius': '6px'
    },
    'author_label': {
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif',
        'font-size': '16px',
        'font-weight': '400',
        'color': '#4b5563'
    },
    'button_primary': {
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif',
        'font-size': '16px',
        'font-weight': '500'
    },
    'badge': {
        'font-size': '13px',
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'
    },
    'input': {
        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif',
        'font-size': '16px'
    }
}

############################################ Helper Functions ###################################################

def create_shipping_curves_table_if_not_exists():
    """Create uploader_shipping_curves table if it doesn't exist"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.uploader_shipping_curves (
        id SERIAL PRIMARY KEY,
        month DATE NOT NULL UNIQUE,
        west_spot_tcde_usd_day DECIMAL(10,4),
        west_term_tcde_usd_day DECIMAL(10,4),
        east_spot_tcde_usd_day DECIMAL(10,4),
        east_term_tcde_usd_day DECIMAL(10,4),
        upload_timestamp_utc TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
        uploaded_by VARCHAR(50),
        notes TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

def get_shipping_curves_data():
    """Get all shipping curves data from database"""
    try:
        df = pd.read_sql(f"""
            SELECT month, west_spot_tcde_usd_day, west_term_tcde_usd_day,
                   east_spot_tcde_usd_day, east_term_tcde_usd_day,
                   upload_timestamp_utc, uploaded_by, notes
            FROM {DB_SCHEMA}.uploader_shipping_curves
            ORDER BY month
        """, engine)

        # Format month as YYYY-MM for display
        if not df.empty:
            df['month'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')

        return df
    except:
        return pd.DataFrame()

def save_shipping_curves_changes(changed_rows, uploaded_by):
    """Save only changed shipping curves rows to database using UPDATE"""
    try:
        if not changed_rows:
            return True, "No changes to save"

        timestamp_utc = dt.datetime.utcnow()
        updated_count = 0

        with engine.begin() as conn:
            for row in changed_rows:
                # Convert month to first day of month
                month_date = pd.to_datetime(row['month'] + '-01')

                # Prepare values (handle None/null values)
                west_spot = row.get('west_spot_tcde_usd_day')
                west_term = row.get('west_term_tcde_usd_day')
                east_spot = row.get('east_spot_tcde_usd_day')
                east_term = row.get('east_term_tcde_usd_day')
                notes = row.get('notes') or ''

                # Use INSERT ... ON CONFLICT UPDATE (upsert) for PostgreSQL
                upsert_sql = f"""
                INSERT INTO {DB_SCHEMA}.uploader_shipping_curves
                    (month, west_spot_tcde_usd_day, west_term_tcde_usd_day,
                     east_spot_tcde_usd_day, east_term_tcde_usd_day,
                     upload_timestamp_utc, uploaded_by, notes)
                VALUES
                    (:month, :west_spot, :west_term, :east_spot, :east_term,
                     :timestamp, :uploaded_by, :notes)
                ON CONFLICT (month)
                DO UPDATE SET
                    west_spot_tcde_usd_day = EXCLUDED.west_spot_tcde_usd_day,
                    west_term_tcde_usd_day = EXCLUDED.west_term_tcde_usd_day,
                    east_spot_tcde_usd_day = EXCLUDED.east_spot_tcde_usd_day,
                    east_term_tcde_usd_day = EXCLUDED.east_term_tcde_usd_day,
                    upload_timestamp_utc = EXCLUDED.upload_timestamp_utc,
                    uploaded_by = EXCLUDED.uploaded_by,
                    notes = EXCLUDED.notes
                """

                conn.execute(text(upsert_sql), {
                    'month': month_date,
                    'west_spot': west_spot,
                    'west_term': west_term,
                    'east_spot': east_spot,
                    'east_term': east_term,
                    'timestamp': timestamp_utc,
                    'uploaded_by': uploaded_by or 'unknown',
                    'notes': notes
                })
                updated_count += 1

        return True, f"Successfully updated {updated_count} row(s)"

    except Exception as e:
        return False, f"Error saving changes: {str(e)}"

############################################ Layout ###################################################

# Initialize table
create_shipping_curves_table_if_not_exists()

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME], suppress_callback_exceptions=True)

app.layout = dbc.Container([
    # Hidden store for original data (for change tracking)
    dcc.Store(id='original-data-store', storage_type='memory'),
    # Store for last update timestamp
    dcc.Store(id='last-update-store', storage_type='memory'),

    # Header
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    # Title and Author Row
                    dbc.Row([
                        dbc.Col([
                            html.H2([
                                html.I(className="fas fa-ship me-3", style=STYLES['icon_primary']),
                                "Shipping Curves Uploader"
                            ], className="mb-0", style=STYLES['page_header'])
                        ], width=8),
                        dbc.Col([
                            dbc.InputGroup([
                                dbc.InputGroupText([
                                    html.I(className="fas fa-user me-2", style={'color': '#4b5563'}),
                                    html.Span("Author:", style=STYLES['author_label'])
                                ], style=STYLES['card_header']),
                                dbc.Input(
                                    id="author-input",
                                    placeholder="3 chars max",
                                    type="text",
                                    required=True,
                                    maxLength=3,
                                    style=STYLES['input']
                                )
                            ], size="sm")
                        ], width=4)
                    ], className="mb-3"),

                    # Action Buttons Row
                    dbc.Row([
                        dbc.Col([
                            dbc.Button([
                                html.I(className="fas fa-sync-alt me-1"),
                                "Refresh Data"
                            ], id="refresh-btn", color="info", size="sm", className="me-2", style=STYLES['button_primary']),
                            dbc.Button([
                                html.I(className="fas fa-download me-1"),
                                "Export to Excel"
                            ], id="export-btn", color="success", size="sm", style=STYLES['button_primary'])
                        ])
                    ])
                ], style=STYLES['card_container']),

                dbc.CardBody([
                    # Status Messages
                    html.Div(id="status-message", className="mb-3"),

                    # Data Editor Card
                    dbc.Card([
                        dbc.CardHeader([
                            dbc.Row([
                                dbc.Col([
                                    html.H5([
                                        html.I(className="fas fa-table me-2", style=STYLES['icon_primary']),
                                        "Shipping Curves Data Editor"
                                    ], className="mb-0 d-inline", style=STYLES['section_header']),
                                    html.Div(id="last-update-display", className="ms-3", style={
                                        'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif',
                                        'font-size': '16px',
                                        'display': 'inline-block',
                                        'padding': '8px 16px',
                                        'background': 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
                                        'border-left': '3px solid #2E86C1',
                                        'border-radius': '6px',
                                        'box-shadow': '0 2px 4px rgba(46, 134, 193, 0.15)'
                                    })
                                ], width="auto"),
                                dbc.Col([
                                    dbc.Button([
                                        html.I(className="fas fa-save me-1"),
                                        "Save Changes"
                                    ], id="save-btn", color="primary", size="sm", style=STYLES['button_primary'])
                                ], width="auto", className="ms-auto")
                            ], align="center", justify="between")
                        ], style=STYLES['section_container']),

                        dbc.CardBody([
                            # Data Table
                            html.Div(
                                id='table-container',
                                style=STYLES['table_container']
                            )
                        ], style=STYLES['content_padding'])
                    ], className="mb-4", style=STYLES['border_container']),

                    dcc.Download(id="download-export")
                ], style={'padding': '30px', 'background': '#ffffff'})
            ], style={'border': '1px solid #e9ecef', 'border-radius': '12px', 'box-shadow': '0 4px 6px -1px rgba(0, 0, 0, 0.1)'})
        ], width=12)
    ])
], fluid=True, style={'font-family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'})

############################################ Callbacks ###################################################

# Load data on startup and refresh
@callback(
    [Output('table-container', 'children'),
     Output('original-data-store', 'data'),
     Output('last-update-store', 'data')],
    Input('refresh-btn', 'n_clicks'),
    prevent_initial_call=False
)
def load_data(n_clicks):
    df = get_shipping_curves_data()

    if df.empty:
        # Create default data until December 2031
        current_date = dt.date.today()
        # Start from current month
        start_date = dt.date(current_date.year, current_date.month, 1)
        # Go to end of 2031
        end_date = dt.date(2031, 12, 1)

        # Generate list of months
        months = []
        current = start_date
        while current <= end_date:
            months.append(current.strftime('%Y-%m'))
            # Move to next month
            if current.month == 12:
                current = dt.date(current.year + 1, 1, 1)
            else:
                current = dt.date(current.year, current.month + 1, 1)

        # Create dataframe with default rows
        df = pd.DataFrame({
            'month': months,
            'west_spot_tcde_usd_day': [None] * len(months),
            'west_term_tcde_usd_day': [None] * len(months),
            'east_spot_tcde_usd_day': [None] * len(months),
            'east_term_tcde_usd_day': [None] * len(months),
            'upload_timestamp_utc': [None] * len(months),
            'uploaded_by': [''] * len(months),
            'notes': [''] * len(months)
        })

    grid = dag.AgGrid(
        id='data-table',
        rowData=df.to_dict('records'),
        columnDefs=[
            {
                "field": "month",
                "headerName": "Month",
                "width": 140,
                "editable": True,
            },
            {
                "headerName": "WEST",
                "children": [
                    {
                        "field": "west_spot_tcde_usd_day",
                        "headerName": "SPOT_TCDE_USD_DAY",
                        "width": 200,
                        "editable": True,
                        "type": "numericColumn",
                        "valueFormatter": {"function": "params.value != null ? d3.format('.2f')(params.value) : ''"}
                    },
                    {
                        "field": "west_term_tcde_usd_day",
                        "headerName": "TERM_TCDE_USD_DAY",
                        "width": 200,
                        "editable": True,
                        "type": "numericColumn",
                        "valueFormatter": {"function": "params.value != null ? d3.format('.2f')(params.value) : ''"}
                    }
                ]
            },
            {
                "headerName": "EAST",
                "children": [
                    {
                        "field": "east_spot_tcde_usd_day",
                        "headerName": "SPOT_TCDE_USD_DAY",
                        "width": 200,
                        "editable": True,
                        "type": "numericColumn",
                        "valueFormatter": {"function": "params.value != null ? d3.format('.2f')(params.value) : ''"}
                    },
                    {
                        "field": "east_term_tcde_usd_day",
                        "headerName": "TERM_TCDE_USD_DAY",
                        "width": 200,
                        "editable": True,
                        "type": "numericColumn",
                        "valueFormatter": {"function": "params.value != null ? d3.format('.2f')(params.value) : ''"}
                    }
                ]
            },
            {
                "field": "notes",
                "headerName": "Notes",
                "width": 900,
                "editable": True
            }
        ],
        defaultColDef={
            "sortable": True,
            "resizable": True,
        },
        dashGridOptions={
            # Excel-like behavior
            "enterNavigatesVertically": True,
            "enterNavigatesVerticallyAfterEdit": True,
            "singleClickEdit": False,  # Double-click to edit (more intentional)
            "undoRedoCellEditing": True,
            "undoRedoCellEditingLimit": 20,
            "stopEditingWhenCellsLoseFocus": True,

            # UI enhancements
            "animateRows": True,
            "enableRangeSelection": True,
            "rowHeight": 42,  # Reduced for more compact display

            # Auto height to show all rows without scroll
            "domLayout": "autoHeight",

            # Don't auto-size columns to fill container
            "suppressColumnVirtualisation": True,
        },
        getRowStyle={
            "styleConditions": [
                {
                    "condition": "params.node.rowIndex % 2 === 1",
                    "style": {"backgroundColor": "#f8f9fa", "fontSize": "14px"}
                },
                {
                    "condition": "params.node.rowIndex % 2 === 0",
                    "style": {"fontSize": "14px"}
                }
            ]
        },
        className="ag-theme-alpine",
        style={"fontSize": "14px"}
    )

    # Store original data for change tracking
    original_data = df.to_dict('records')

    # Get last update timestamp from the data (if available)
    last_update = None
    if not df.empty and 'upload_timestamp_utc' in df.columns:
        # Find the most recent timestamp
        timestamps = df['upload_timestamp_utc'].dropna()
        if not timestamps.empty:
            # Convert UTC to UAE time (UTC+4)
            utc_time = pd.to_datetime(timestamps).max()
            uae_time = utc_time + pd.Timedelta(hours=4)
            last_update = uae_time.strftime('%Y-%m-%d %H:%M')

    return grid, original_data, last_update

# Save changes
@callback(
    Output('status-message', 'children'),
    Input('save-btn', 'n_clicks'),
    State('data-table', 'rowData'),
    State('original-data-store', 'data'),
    State('author-input', 'value'),
    prevent_initial_call=True
)
def save_changes(n_clicks, table_data, original_data, author_name):
    if not table_data:
        return dbc.Alert("No data to save", color="warning")

    if not author_name or author_name.strip() == "":
        return dbc.Alert([
            html.I(className="fas fa-exclamation-triangle me-2"),
            "Author name is required. Please enter your name before saving."
        ], color="danger")

    # Compare current data with original to find changed rows
    changed_rows = []
    if original_data:
        # Create a lookup dictionary for original data by month
        original_lookup = {row['month']: row for row in original_data}

        for current_row in table_data:
            month = current_row.get('month')
            if month in original_lookup:
                original_row = original_lookup[month]
                # Check if any editable field has changed
                if (current_row.get('west_spot_tcde_usd_day') != original_row.get('west_spot_tcde_usd_day') or
                    current_row.get('west_term_tcde_usd_day') != original_row.get('west_term_tcde_usd_day') or
                    current_row.get('east_spot_tcde_usd_day') != original_row.get('east_spot_tcde_usd_day') or
                    current_row.get('east_term_tcde_usd_day') != original_row.get('east_term_tcde_usd_day') or
                    current_row.get('notes') != original_row.get('notes')):
                    changed_rows.append(current_row)
            else:
                # New row (not in original data)
                changed_rows.append(current_row)
    else:
        # No original data to compare, save all
        changed_rows = table_data

    if not changed_rows:
        return dbc.Alert([
            html.I(className="fas fa-info-circle me-2"),
            "No changes detected"
        ], color="info")

    # Save only changed rows
    success, message = save_shipping_curves_changes(changed_rows, author_name.strip())

    if success:
        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            message
        ], color="success")
    else:
        return dbc.Alert([
            html.I(className="fas fa-exclamation-triangle me-2"),
            message
        ], color="danger")

# Export to Excel
@callback(
    Output("download-export", "data"),
    Input("export-btn", "n_clicks"),
    State('data-table', 'rowData'),
    prevent_initial_call=True
)
def export_data(n_clicks, table_data):
    if not table_data:
        return None

    df = pd.DataFrame(table_data)
    return dcc.send_data_frame(df.to_excel, "shipping_curves_export.xlsx", index=False, sheet_name="Shipping Curves")

# Update author input styling
@callback(
    Output('author-input', 'style'),
    Input('author-input', 'value'),
    prevent_initial_call=True
)
def update_author_style(author_value):
    if author_value and author_value.strip():
        return {'border-color': '#28a745'}
    else:
        return {'border-color': '#dc3545'}

# Display last update timestamp
@callback(
    Output('last-update-display', 'children'),
    Input('last-update-store', 'data'),
    prevent_initial_call=False
)
def display_last_update(last_update):
    if last_update:
        return [
            html.I(className="fas fa-clock me-2", style={'color': '#2E86C1'}),
            html.Span("Last updated: ", style={'font-weight': 'bold', 'color': '#1f2937'}),
            html.Span(f"{last_update} UAE", style={'color': '#4b5563'})
        ]
    else:
        return ""

############################################ Run App ###################################################

if __name__ == '__main__':
    app.run(debug=True, port=8053)
