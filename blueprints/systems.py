# blueprints/systems.py
from flask import Blueprint, jsonify, request, Response
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
import config
from config import jsonpath
import json
from datetime import timezone, timedelta
import os
from blueprints.module_models import get_module_model_data_by_id

# Create a blueprint for systems
systems_bp = Blueprint('systems', __name__)

# load all of the JSON schemas once
#BASE = '/Users/jsstein/bin/db25_api2/src/blueprints'
with open(jsonpath / 'systems.json')     as f:  systems_schema    = json.load(f)
with open(jsonpath / 'subsystems.json')  as f:  subsystems_schema = json.load(f)
with open(jsonpath / 'labels.json')      as f:  labels_schema     = json.load(f)
with open(jsonpath / 'measurements.json') as f:  measurements_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

MST = timezone(timedelta(hours=-7), name="MST")
def _attach_mst(rows):
    for row in rows:
        if row.get("measurement_date") is not None and row["measurement_date"].tzinfo is None:
            row["measurement_date"] = row["measurement_date"].replace(tzinfo=MST)
    return rows

@systems_bp.route('/v1/systems', methods=['GET'])
def get_all_systems():

    """Return a list of all systems."""
    print("get_all_systems called")
    
    columns = systems_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    quoted_column_names = [f"[{col}]" for col in column_names]
    query = f"SELECT {', '.join(quoted_column_names)} FROM {make_filename(systems_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        system_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in system_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_systems.csv"})

    
    # """Return a list of all systems."""
    # print("get_all_systems called")
    
    # columns = systems_schema['table_info']['columns']
    # column_names = [col for col in columns if columns[col]['keep']]
    
    # # Quote column names to handle special characters
    # quoted_column_names = [f"[{col}]" for col in column_names]
    
    # # Construct the SQL query
    # query = f"SELECT {', '.join(quoted_column_names)} FROM {make_filename(systems_schema['table_info']['name'])}"
    
    # engine = get_engine()
    # with engine.connect() as conn:
    #     result: Result = conn.execute(text(query))
    #     system_rows = [dict(row._mapping) for row in result]

    # # Create a CSV response
    # output = io.StringIO()
    # writer = csv.writer(output)

    # # Create header based on the fields we want to include. ****THIS NEEDS TO BE UPDATED****
    # header = ['system_id', 'system_name', 'module_manufacturer', 'module_model', 
    #           'nstrings', 'system_mounting', 'fixed_azimuth', 'fixed_tilt', 
    #           'axis_azimuth', 'axis_tilt', 'tilt_max']
    # writer.writerow(header)  # Write header

    # # Write rows to CSV
    # for row in system_rows:
    #     writer.writerow([row['system_id'], row['system_name'], row['system_description'], 
    #                      row['module_model_id'], row['number_of_strings'], row['system_mounting'], 
    #                      row['fixed_azimuth_deg'], row['fixed_tilt_deg'], 
    #                      row['axis_azimuth_deg'], row['axis_tilt_deg'], row['tilt_max_deg']])

    # # Prepare the response
    # output.seek(0)  # Move to the beginning of the StringIO object
    # return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_systems.csv"})

@systems_bp.route('/v1/systems/<string:system_name>', methods=['GET'])
def get_system_data(system_name):
    """Return metadata for a specific system."""
    print(f"get_system_data called for system: {system_name}")
    
    columns = systems_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    
    # Quote column names to handle special characters
    quoted_column_names = [f"[{col}]" for col in column_names]
    
    # Construct the SQL query
    query = f"SELECT {', '.join(quoted_column_names)} FROM {make_filename(systems_schema['table_info']['name'])} WHERE [system_name] = :system_name" 
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"system_name": system_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "System not found"}, 404
        return jsonify(rows[0])  # Return single object instead of list

# GET modules per system
@systems_bp.route('/v1/systems/<string:system_name>/number_of_modules', methods=['GET'])
def get_number_of_modules_per_system(system_name):
    """Return total number of modules for a specific system."""
    print(f"get_number_of_modules_per_system called for system: {system_name}")
    
    sub_data = get_subsystem_data_from_system(system_name)
    #print(sub_data)
    number_of_modules = 0  # initial value
    for sub in sub_data:
        number_of_modules += (sub['strings_per_subsystem'] * sub['modules_per_string'])
    
    # Return as a JSON response
    return jsonify({'number_of_modules': int(number_of_modules)})

# GET modules per system/subsystem
@systems_bp.route('/v1/systems/<string:system_name>/<string:subsystem_name>/number_of_modules', methods=['GET'])
def get_number_of_modules_per_subsystem(system_name, subsystem_name):
    """Return total number of modules for a specific system/subsystem."""
    print(f"get_number_of_modules_per_system: {system_name}, subsystem: {subsystem_name})")
    
    sub_data = get_subsystem_data_from_system(system_name)
    number_of_modules = None
    for sub in sub_data:
        if sub['subsystem_name'] == subsystem_name:
            number_of_modules = int(sub['strings_per_subsystem']) * int(sub['modules_per_string'])
            print('number of modules =', number_of_modules)
            break
    if number_of_modules is None:
        return {"message": "Subsystem not found"}, 404

    # Return as a JSON response
    return jsonify({'number_of_modules': number_of_modules})

# GET dc capacity of system
@systems_bp.route('/v1/systems/<string:system_name>/dc_capacity', methods=['GET'])
def get_system_dc_capacity(system_name):
    """Return dc capacity for a specific system."""
    print(f"get_system_capacity called for system: {system_name}")
    
    # Get the system data as a Response object
    response = get_system_data(system_name)
    
    # Check if the response is a valid JSON response
    if isinstance(response, Response):
        if response.status_code != 200:
            return response  # Return the error response directly

        # Extract the JSON data from the response
        sysdata = response.get_json()
    else:
        return {"message": "Unexpected get_system_data response type"}, 500

    # Check if module_model_id is present and not None
    module_model_id = sysdata.get('module_model_id')
    if module_model_id is None:
        return {"message": "module_model_id is not defined for the specified system."}, 400

    # Get the number of modules
    number_of_modules_response = get_number_of_modules_per_system(system_name)
    if isinstance(number_of_modules_response, Response):
        if number_of_modules_response.status_code != 200:
            return number_of_modules_response  # Return the error response directly
        number_of_modules = number_of_modules_response.get_json()['number_of_modules']
        print('****Number of modules= ',number_of_modules)
    else:
        return {"message": "Unexpected response type from get_number_of_modules_per_system"}, 500

    # Get module model data
    module_model_data_response = get_module_model_data_by_id(int(module_model_id))
    if isinstance(module_model_data_response, Response):
        if module_model_data_response.status_code != 200:
            return module_model_data_response  # Return the error response directly
        module_model_data = module_model_data_response.get_json()
    else:
        return {"message": "Unexpected get_module_model_data_by_id response type"}, 500

    # Calculate DC capacity
    module_power_rating_W = module_model_data['power_rating_W']
    print('module_power_rating_W', module_power_rating_W)
    print('number_of_modules', number_of_modules)
    dc_capacity = module_power_rating_W * number_of_modules
    return {'dc_capacity_W': dc_capacity}

# ******************************************************************************************************************
# GET dc capacity of system/subsystem ******************************************************************************
# ******************************************************************************************************************
@systems_bp.route('/v1/systems/<string:system_name>/<string:subsystem_name>/dc_capacity', methods=['GET'])
def get_system_subsystem_dc_capacity(system_name, subsystem_name):
    """Return dc capacity for a specific system/subsystem."""
    print(f"get_subsystem_capacity called for system: {system_name}, subsystem: {subsystem_name} ")
    
    # Get the system data as a Response object
    response = get_system_data(system_name)
    
    # Check if the response is a valid JSON response
    if isinstance(response, Response):
        if response.status_code != 200:
            return response  # Return the error response directly

        # Extract the JSON data from the response
        sysdata = response.get_json()
    else:
        return {"message": "Unexpected get_system_data response type"}, 500

    # Check if module_model_id is present and not None
    module_model_id = sysdata.get('module_model_id')
    if module_model_id is None:
        return {"message": "module_model_id is not defined for the specified system."}, 400

    # Get the number of modules from subsystem
    number_of_modules_response = get_number_of_modules_per_subsystem(system_name, subsystem_name)
    if isinstance(number_of_modules_response, Response):
        if number_of_modules_response.status_code != 200:
            return number_of_modules_response  # Return the error response directly
        number_of_modules = number_of_modules_response.get_json()['number_of_modules']
        print(f'Number of modules in subsystem: {subsystem_name} = {number_of_modules}')
    else:
        return {"message": "Unexpected response type from get_number_of_modules_per_system"}, 500



    # Get module model data
    module_model_data_response = get_module_model_data_by_id(int(module_model_id))
    if isinstance(module_model_data_response, Response):
        if module_model_data_response.status_code != 200:
            return module_model_data_response  # Return the error response directly
        module_model_data = module_model_data_response.get_json()
    else:
        return {"message": "Unexpected get_module_model_data_by_id response type"}, 500

    # Calculate DC capacity of subsystem
    module_power_rating_W = module_model_data['power_rating_W']
    print('module_power_rating_W', module_power_rating_W)
    print('number_of_modules', number_of_modules)
    dc_capacity = module_power_rating_W * number_of_modules
    return {'dc_capacity_subsystem_W': dc_capacity}


###########################################################################################################
# Test url for Postman 
#http://127.0.0.1:8080/v1/systems/SLTE_PSEL_LG1/measurements?start_date=2019-01-01T12:00:00-07:00&end_date=2019-01-01T13:00:00-07:00&subsystem_names=S1&label_names=Vdc&label_names=Idc

@systems_bp.route('/v1/systems/<string:system_name>/measurements', methods=['GET'])
def get_system_measurements(system_name: str):
    """Return measurement data for a given system in a date range (plus linked testpad data)."""

    # 1) pull required query‐string params
    start_date = request.args.get('start_date')
    end_date   = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"message": "start_date and end_date are required"}), 400

    # 2) pull optional multi‐value params
    subsystem_names = request.args.getlist('subsystem_names') or None
    label_names     = request.args.getlist('label_names')     or None

    # 3) resolve each table name + suffix
    syst_table = make_filename(systems_schema['table_info']['name'])
    sub_table  = make_filename(subsystems_schema['table_info']['name'])
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
    system_fk    = next(c for c in fk_cols if c.lower().startswith('system_')    or c == 'system_id')
    subsystem_fk = next(c for c in fk_cols if c.lower().startswith('subsystem_') or c == 'subsystem_id')

    # 6) build the SQL with every name coming from JSON + make_filename()
    query = f"""
        SELECT
          m.[{date_col}]      AS measurement_date,
          m.[{value_col}]     AS measurement_value,
          l.[label_name]      AS label_name,
          sub.[subsystem_name]AS subsystem_name,
          s.[system_name]     AS system_name
        FROM {meas_table} AS m
        JOIN {lbl_table}   AS l   ON m.[{label_fk}]     = l.[label_id]
        JOIN {syst_table}  AS s   ON m.[{system_fk}]    = s.[system_id]
        JOIN {sub_table}   AS sub ON m.[{subsystem_fk}] = sub.[subsystem_id]
        WHERE
          m.[{date_col}] BETWEEN :start_date AND :end_date
          AND s.[system_name] = :system_name
    """

    params = {
      "start_date":  start_date,
      "end_date":    end_date,
      "system_name": system_name
    }

    # 7) inject your optional IN‐clauses
    if subsystem_names:
        ph = ", ".join(f":sub_{i}" for i in range(len(subsystem_names)))
        query += f" AND sub.[subsystem_name] IN ({ph})"
        params.update({f"sub_{i}": v for i, v in enumerate(subsystem_names)})

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

    if label_names and subsystem_names:
        # fixed header order
        header_keys = [f"{lab}_{sub}" for sub in subsystem_names for lab in label_names]
    else:
        # discover all pairs present
        pairs = sorted({(r['label_name'], r['subsystem_name']) for r in rows})
        header_keys = [f"{lab}_{sub}" for lab, sub in pairs]

    writer.writerow(['measurement_date'] + header_keys)

    by_date = {}
    for r in rows:
        ts  = r['measurement_date'].isoformat()
        key = f"{r['label_name']}_{r['subsystem_name']}"
        by_date.setdefault(ts, {h: None for h in header_keys})[key] = r['measurement_value']

    for ts, vals in sorted(by_date.items()):
        writer.writerow([ts] + [vals[k] for k in header_keys])

    output.seek(0)
    return Response(
      output.getvalue(),
      mimetype='text/csv',
      headers={"Content-Disposition": "attachment;filename=system_measurements.csv"}
    )

@systems_bp.route('/v1/systems/<string:system_name>/subsystems', methods=['GET'])
def get_subsystem_data_from_system(system_name: str):
    print(f"get_subsystem_data called for system: {system_name}")
    
    columns = subsystems_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    
    # Quote column names to handle special characters
    quoted_column_names = [f"[{col}]" for col in column_names]
    
    # Construct the SQL query
    query = f"SELECT {', '.join(quoted_column_names)} FROM {make_filename(subsystems_schema['table_info']['name'])} WHERE [system_name] = :system_name" 
        
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"system_name": system_name})
        return [dict(row._mapping) for row in result]

##############################################################################################
# Here is a test URL for this function:
# http://127.0.0.1:8080/v1/systems/SLTE_PSEL_LG1/measurement_date_range
@systems_bp.route('/v1/systems/<string:system_name>/measurement_date_range',
                  methods=['GET'])
def get_measurement_date_range(system_name: str):
    """Return the min/max measurement_date for a given system."""

    # resolve tables with suffix
    syst_table = make_filename(systems_schema['table_info']['name'])
    meas_table = measurements_schema['table_info']['name'] #DO NOT ADD SUFFIX TO MEASUREMENTS TABLE

    # pull out measurement columns from the JSON schema
    meas_cols = measurements_schema['table_info']['columns']

    # heuristics to find the system‐FK and the date column
    system_fk = next(
        c for c in meas_cols
        if c.lower() == 'system_id' or c.lower().startswith('system_')
    )
    date_col  = next(
        c for c in meas_cols
        if c.lower().endswith('_date') and c != system_fk
    )

    # build the SQL
    sql = f"""
        SELECT
          MIN(m.[{date_col}]) AS start_date,
          MAX(m.[{date_col}]) AS end_date
        FROM {meas_table} AS m
        JOIN {syst_table} AS s
          ON m.[{system_fk}] = s.[system_id]
        WHERE s.[system_name] = :system_name
    """

    params = {"system_name": system_name}

    # execute
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).first()

    # if no data, return empty
    if row is None or (row.start_date is None and row.end_date is None):
        return jsonify({"start_date": None, "end_date": None})

    # attach timezone if naive, then isoformat
    def _iso(dt):
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MST)
        return dt.isoformat()

    return jsonify({
        "start_date": _iso(row.start_date),
        "end_date":   _iso(row.end_date),
    })

@systems_bp.route('/v1/systems/<string:system_name>/measurements/last_measurement_date', methods=['GET'])
def get_last_measurement_date(system_name: str):
    """Return the most recent measurement_date for a given system."""

    print(f"get_last_measurement_date called for system: {system_name}")

    # resolve tables with suffix
    syst_table = make_filename(systems_schema['table_info']['name'])
    meas_table = measurements_schema['table_info']['name']  # DO NOT ADD SUFFIX

    # pull out measurement columns from the JSON schema
    meas_cols = measurements_schema['table_info']['columns']

    # heuristics to find the system‐FK and the date column
    system_fk = next(
        c for c in meas_cols
        if c.lower() == 'system_id' or c.lower().startswith('system_')
    )

    date_col = next(
        c for c in meas_cols
        if c.lower().endswith('_date') and c != system_fk
    )

    # build the SQL
    sql = f"""
        SELECT
          MAX(m.[{date_col}]) AS last_measurement_date
        FROM {meas_table} AS m
        JOIN {syst_table} AS s
          ON m.[{system_fk}] = s.[system_id]
        WHERE s.[system_name] = :system_name
    """

    params = {"system_name": system_name}

    # execute
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).first()

    # if no data, return empty
    if row is None or row.last_measurement_date is None:
        return jsonify({"last_measurement_date": None}), 404

    last_dt = row.last_measurement_date
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=MST)
    return jsonify({
        "last_measurement_date": last_dt.isoformat()
    })
