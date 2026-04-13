# blueprints/projects.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for projects
projects_bp = Blueprint('projects', __name__)

# Load the JSON schema
with open(jsonpath / 'projects.json') as f:
    projects_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@projects_bp.route('/v1/projects', methods=['GET'])
def get_all_projects():
    """Return a list of all projects."""
    print("get_all_projects called")
    
    columns = projects_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(projects_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        project_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in project_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_projects.csv"})

@projects_bp.route('/v1/projects/<string:project_name>', methods=['GET'])
def get_project_data(project_name):
    """Return details for a specific project."""
    print(f"get_project_data called for project_name: {project_name}")
    
    columns = projects_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(projects_schema['table_info']['name'])} WHERE project_name = :project_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"project_name": project_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Project not found"}, 404
        return jsonify(rows[0])