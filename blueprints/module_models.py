# blueprints/module_models.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# def encase_names(names):
#     name_list = []
#     for name in names:
#         name_list.append('\"'+name+'\"')
#     return name_list   

# Create a blueprint for module models
module_models_bp = Blueprint('module_models', __name__)

# Load the JSON schema
with open(jsonpath / "module_models.json", "r", encoding="utf-8") as f:
    module_models_schema = json.load(f)
    
def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@module_models_bp.route('/v1/module_models', methods=['GET'])
def get_all_module_models():
    """Return a list of all module models."""
    print("get_all_module_models called")
    
    columns = module_models_schema['table_info']['columns']
    column_names = [f"{col}" for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(module_models_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        module_model_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in module_model_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_module_models.csv"})

@module_models_bp.route('/v1/module_models/<string:module_model_name>', methods=['GET'])
def get_module_model_data(module_model_name):
    """Return details for a specific module model."""
    print(f"get_module_model_data called for module_model_name: {module_model_name}")
    
    columns = module_models_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(module_models_schema['table_info']['name'])} WHERE module_model_name = :module_model_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"module_model_name": module_model_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Module model name not found"}, 404
        return jsonify(rows[0])
    
@module_models_bp.route('/v1/module_models/<int:module_model_id>', methods=['GET'])
def get_module_model_data_by_id(module_model_id):
    """Return details for a specific module model."""
    print(f"get_module_model_data called for module_model_id: {module_model_id}")
    
    columns = module_models_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(module_models_schema['table_info']['name'])} WHERE module_model_id = :module_model_id"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"module_model_id": module_model_id})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Module model ID not found"}, 404
        return jsonify(rows[0])