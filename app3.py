import os
import logging
import datetime
import argparse
#from sqlalchemy.engine import Result
from datetime import timezone, timedelta
import connexion
from connexion.exceptions import OAuthProblem
from flask import jsonify, request
import jwt
from db import get_engine
import config


# Import blueprints
from blueprints.systems import systems_bp
from blueprints.subsystems import subsystems_bp
from blueprints.testpads import testpads_bp
from blueprints.units import units_bp
from blueprints.aggregations import aggregations_bp
from blueprints.alignments import alignments_bp
from blueprints.durations import durations_bp
from blueprints.labels import labels_bp
from blueprints.manufacturers import manufacturers_bp
from blueprints.module_models import module_models_bp
from blueprints.modules import modules_bp
from blueprints.projects import projects_bp
from blueprints.sites import sites_bp
from blueprints.loads import loads_bp



# Secret key (use environment variable in production!)
JWT_SECRET = os.getenv('API_JWT_SERVER_KEY')
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 3600  # token validity (1 hour)

# Example users (replace with real DB or LDAP)
USERS = {
    "tester": "password",
    "josh": "stein",
    "brent": "thompson",
    "norman": "jost",
    "bruce": "king"
}

def login(body):
    """Authenticate user and return JWT token."""
    print(f"Login attempt with body: {body}")
    username = body.get("username")
    password = body.get("password")
    
    print(f"Username: {username}, Password: {password}")

    if username not in USERS or USERS[username] != password:
        print("Authentication failed")
        return {"message": "Invalid credentials"}, 401

    payload = {
        "sub": username,
        "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    print(f"Token generated: {token}")
    return {"access_token": token}

def bearer_auth(token):
    """Security function for bearer token authentication."""
    print(f"bearer_auth called with token: {token[:20] if token else 'None'}...")
    
    if not token:
        raise OAuthProblem("No token provided")
    
    try:
        decoded_token = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        print(f"Token decoded successfully for user: {decoded_token.get('sub')}")
        return {"sub": decoded_token["sub"], "uid": decoded_token["sub"]}
        
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        raise OAuthProblem("Token has expired")
    except jwt.InvalidTokenError as e:
        print(f"Invalid token: {e}")
        raise OAuthProblem("Invalid token")


# Utility to add MST timezone offset
MST = timezone(timedelta(hours=-7), name="MST")
def _attach_mst(rows):
    for row in rows:
        if row.get("measurement_date") is not None and row["measurement_date"].tzinfo is None:
            row["measurement_date"] = row["measurement_date"].replace(tzinfo=MST)
    return rows

def print_registered_routes(connexion_app):
    """Print all registered routes in the Flask application."""
    print("Registered Routes:")
    for rule in connexion_app.app.url_map.iter_rules():
        print(f"{rule.endpoint}: {rule.methods} -> {rule}")

def create_app():
    # Ensure JWT_SECRET is set
    if not JWT_SECRET:
        raise RuntimeError("API_JWT_SERVER_KEY environment variable must be set")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    app = connexion.App(
        __name__,
        specification_dir=BASE_DIR
    )

    app.add_api(
        "api.yaml",
        validate_responses=True,
        strict_validation=True
    )

    # Register blueprints
    app.app.register_blueprint(systems_bp)
    app.app.register_blueprint(subsystems_bp)
    app.app.register_blueprint(testpads_bp)
    app.app.register_blueprint(units_bp)
    app.app.register_blueprint(aggregations_bp)
    app.app.register_blueprint(alignments_bp)
    app.app.register_blueprint(durations_bp)
    app.app.register_blueprint(labels_bp)
    app.app.register_blueprint(manufacturers_bp)
    app.app.register_blueprint(module_models_bp)
    app.app.register_blueprint(modules_bp)
    app.app.register_blueprint(projects_bp)
    app.app.register_blueprint(sites_bp)
    app.app.register_blueprint(loads_bp)

    return app

app = create_app()

def main():
    parser = argparse.ArgumentParser(description="Start the API server.")
    parser.add_argument("--suffix", type=str, help="Table Suffix (e.g., datetime)")
    args = parser.parse_args()

    if args.suffix:
        import config
        config.SUFFIX = args.suffix
        print(f"Suffix value: {config.SUFFIX}")

    print("Starting API server...")
    print(f"JWT_SECRET is set: {JWT_SECRET is not None}")

    app.run(host="127.0.0.1", port=8001)
    
if __name__ == "__main__":
    main()
