# blueprints/testpads.py
from flask import Blueprint, jsonify, request, Response
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
from datetime import timezone, timedelta
import csv, io
import json
import config
from config import jsonpath


# Create a blueprint for testpads
testpads_bp = Blueprint('testpads', __name__)

# Load the JSON schemas
with open(jsonpath / 'testpads.json') as f:
    testpads_schema = json.load(f)
with open(jsonpath / 'labels.json') as f:
    labels_schema = json.load(f)
with open(jsonpath / 'measurements.json') as f:  
    measurements_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

MST = timezone(timedelta(hours=-7), name="MST")
def _attach_mst(rows):
    for row in rows:
        if row.get("measurement_date") is not None and row["measurement_date"].tzinfo is None:
            row["measurement_date"] = row["measurement_date"].replace(tzinfo=MST)
    return rows

@testpads_bp.route('/v1/testpads', methods=['GET'])
def get_all_testpads():
    """Return a list of all testpads."""
    print("get_all_testpads called")
    
    columns = testpads_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(testpads_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        unit_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in unit_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_testpads.csv"})

@testpads_bp.route('/v1/testpads/<string:testpad_name>', methods=['GET'])
def get_testpad_data(testpad_name):
    """Return details for a specific testpad."""
    print(f"get_testpad_data called for testpad_name: {testpad_name}")
    
    columns = testpads_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(testpads_schema['table_info']['name'])} WHERE testpad_name = :testpad_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"testpad_name": testpad_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Testpad not found"}, 404
        return jsonify(rows[0])
    
@testpads_bp.route('/v1/testpads/<string:testpad_name>/measurements', methods=['GET'])
def get_testpad_measurements(testpad_name: str):
    """Return measurement data for a given testpad in a date range."""

    # 1) pull required query‐string params
    start_date = request.args.get('start_date')
    end_date   = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"message": "start_date and end_date are required"}), 400

    # 2) pull optional multi‐value params
    label_names     = request.args.getlist('label_names')     or None

    # 3) resolve each table name + suffix
    tpad_table = make_filename(testpads_schema['table_info']['name'])
    lbl_table  = make_filename(labels_schema['table_info']['name'])
    meas_table = measurements_schema['table_info']['name'] #DO NOT ADD SUFFIX TO MEASUREMENTS TABLE

    # 4) grab your measurement columns from the JSON so you never mistype
    meas_cols = measurements_schema['table_info']['columns']
    # we know we want these two for the CSV
    date_col  = 'measurement_date'
    value_col = 'measurement_value'

    # 5) pull out the FK column‐names from your measurements schema by pattern
    #    (if your JSON schemas actually tag FKs you can do it more robustly;
    #     here I'm just demonstrating the heuristic)
    fk_cols = list(meas_cols.keys())
    label_fk     = next(c for c in fk_cols if c.lower().startswith('label_')     or c == 'label_id')
    tpad_fk    = next(c for c in fk_cols if c.lower().startswith('testpad_')    or c == 'testpad_id')
    
    # 6) build the SQL with every name coming from JSON + make_filename()
    query = f"""
        SELECT
          m.[{date_col}]      AS measurement_date,
          m.[{value_col}]     AS measurement_value,
          l.[label_name]      AS label_name,
          t.[testpad_name]     AS testpad_name
        FROM {meas_table} AS m
        JOIN {lbl_table}   AS l   ON m.[{label_fk}]     = l.[label_id]
        JOIN {tpad_table}  AS t   ON m.[{tpad_fk}]    = t.[testpad_id]
        WHERE
          m.[{date_col}] BETWEEN :start_date AND :end_date
          AND t.[testpad_name] = :testpad_name
    """

    params = {
      "start_date":  start_date,
      "end_date":    end_date,
      "testpad_name": testpad_name
    }

    # 7) inject your optional IN‐clauses
    if label_names:
        ph = ", ".join(f":lab_{i}" for i in range(len(label_names)))
        query += f" AND l.[label_name] IN ({ph})"
        params.update({f"lab_{i}": v for i, v in enumerate(label_names)})

    query += " ORDER BY m.[measurement_date]"

    # 8) execute
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(r._mapping) for r in result]

    # 9) re‐attach MST timezone if missing
    rows = _attach_mst(rows)

    # 10) pivot & write CSV just like you had before
    output = io.StringIO()
    writer = csv.writer(output)

    if label_names:
        header_keys = list(label_names)
    else:
        header_keys = sorted({r['label_name'] for r in rows})

    writer.writerow(['measurement_date'] + header_keys)

    by_date = {}
    for r in rows:
        ts  = r['measurement_date'].isoformat()
        key = f"{r['label_name']}"
        by_date.setdefault(ts, {h: None for h in header_keys})[key] = r['measurement_value']

    for ts, vals in sorted(by_date.items()):
        writer.writerow([ts] + [vals[k] for k in header_keys])

    output.seek(0)
    return Response(
      output.getvalue(),
      mimetype='text/csv',
      headers={"Content-Disposition": "attachment;filename=testpad_measurements.csv"}
    )

@testpads_bp.route('/v1/testpads/<string:testpad_name>/measurement_date_range',
                  methods=['GET'])
def get_measurement_date_range(testpad_name: str):
    """Return the min/max measurement_date for a given testpad."""

    # resolve tables with suffix
    tpad_table = make_filename(testpads_schema['table_info']['name'])
    meas_table = measurements_schema['table_info']['name'] #DO NOT ADD SUFFIX TO MEASUREMENTS TABLE

    # pull out measurement columns from the JSON schema
    meas_cols = measurements_schema['table_info']['columns']

    # heuristics to find the system‐FK and the date column
    testpad_fk = next(
        c for c in meas_cols
        if c.lower() == 'testpad_id' or c.lower().startswith('testpad_')
    )
    date_col  = next(
        c for c in meas_cols
        if c.lower().endswith('_date') and c != testpad_fk
    )
    print(f"meas_table= {meas_table}")

    # build the SQL
    sql = f"""
        SELECT
          MIN(m.[{date_col}]) AS start_date,
          MAX(m.[{date_col}]) AS end_date
        FROM {meas_table} AS m
        JOIN {tpad_table} AS t
          ON m.[{testpad_fk}] = t.[testpad_id]
        WHERE t.[testpad_name] = :testpad_name
    """

    params = {"testpad_name": testpad_name}

    # execute
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).first()

    # if no data, return empty
    if row is None or (row.start_date is None and row.end_date is None):
        return jsonify({"start_date": None, "end_date": None}), 404

    # attach timezone / isoformat
    start_iso = row.start_date.isoformat() if row.start_date else None
    end_iso   = row.end_date.isoformat()   if row.end_date   else None

    return jsonify({
        "start_date": start_iso,
        "end_date":   end_iso
    })