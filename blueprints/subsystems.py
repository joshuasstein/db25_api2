# blueprints/subsystems.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for subsystems
subsystems_bp = Blueprint('subsystems', __name__)
# Load the JSON schema
with open(jsonpath / 'subsystems.json') as f:
    subsystems_schema = json.load(f)

@subsystems_bp.route('/v1/subsystems', methods=['GET'])
def get_all_subsystems():
    """Return a list of all subsystems."""
    print("get_all_subsystems called")
    
    columns = subsystems_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(subsystems_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        subsystem_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in subsystem_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_subsystems.csv"})
# def get_all_subsystems():
#     """Return a list of all subsystems."""
#     print("get_all_subsystems called")
    
#     query = """
#         SELECT subsystem_name, subsystem_description, system_name, modules_per_string
#         FROM db25_subsystems
#     """
    
#     engine = get_engine()
#     with engine.connect() as conn:
#         result: Result = conn.execute(text(query))
#         subsystem_rows = [dict(row._mapping) for row in result]

#     # Create a CSV response
#     output = io.StringIO()
#     writer = csv.writer(output)

#     # Create header based on the fields we want to include
#     header = ['subsystem_name', 'subsystem_description', 'system_name', 'modules_per_string']
#     writer.writerow(header)  # Write header

#     # Write rows to CSV
#     for row in subsystem_rows:
#         writer.writerow([row['subsystem_name'], row['subsystem_description'], 
#                          row['system_name'], row['modules_per_string']])

#     # Prepare the response
#     output.seek(0)  # Move to the beginning of the StringIO object
#     return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_subsystems.csv"})

@subsystems_bp.route('/v1/subsystems/<string:subsystem_name>', methods=['GET'])
def get_subsystem_data(subsystem_name):
    """Return details for a specific subsystem."""
    print(f"get_subsystem_data called for subsystem_name: {subsystem_name}")
    
    columns = subsystems_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(subsystems_schema['table_info']['name'])} WHERE subsystem_name = :subsystem_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"subsystem_name": subsystem_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Subsystem name not found"}, 404
    return jsonify(rows[0])
# def get_subsystem_data(subsystem_name):
#     """Return details for a specific site."""
#     print(f"get_subsystem_data called for subsystem_name: {subsystem_name}")
    
#     columns = subsystem_schema['table_info']['columns']
#     column_names = [col for col in columns if columns[col]['keep']]
#     query = f"SELECT {', '.join(column_names)} FROM {make_filename(subsystem_schema['table_info']['name'])} WHERE system_name = :{system_name} AND subsystem_name = {subsystem_name}"
    
#     engine = get_engine()
#     with engine.connect() as conn:
#         result: Result = conn.execute(text(query), {"subsystem_name": subsystem_name})
#         rows = [dict(row._mapping) for row in result]
#         if not rows:
#             return {"message": "Subsystem name not found"}, 404
#     return jsonify(rows[0])