from flask import Flask, request, jsonify, render_template_string
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from dashboard import html_temp

def host_db(username, password, host, port, db_name, db_type='sqlite'):
    """
    Hosts a database and exposes it as an API.

    Args:
        db_type (str): Database type ('mysql', 'postgresql', 'sqlite').
        username (str): Database username.
        password (str): Database password.
        host (str): Database host address.
        port (int): Database port.
        db_name (str): Database name.

    Returns:
        Flask app object that serves the API.
    """
    app = Flask(__name__)
    
    # Set up SQLAlchemy with the specified database connection
    if db_type == 'sqlite':
        # SQLite does not require username/password or a host
        app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_name}.db'
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = f'{db_type}://{username}:{password}@{host}:{port}/{db_name}'
    
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)
    
    # Activity log to store connection events
    connection_activity_log = []

    # HTML template for displaying database info and activity log
    dashboard_template = html_temp

    # Define a sample database model (this can be extended)
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(100), nullable=False)
        email = db.Column(db.String(120), unique=True, nullable=False)

        def to_dict(self):
            return {"id": self.id, "name": self.name, "email": self.email}

    # Initialize the database (workaround for before_first_request)
    with app.app_context():
        db.create_all()
        connection_activity_log.append({
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "message": "Database initialized."
        })

    # Dashboard route
    @app.route('/dashboard')
    def dashboard():
        """Displays the dashboard with database info and connection activity."""
        return render_template_string(dashboard_template, 
                                      db_name=db_name, 
                                      connection_activity=connection_activity_log)

    # API endpoints
    @app.route('/users', methods=['GET'])
    def get_users():
        """Fetch all users."""
        users = User.query.all()
        connection_activity_log.append({
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "message": "Fetched all users."
        })
        return jsonify([user.to_dict() for user in users])

    @app.route('/users', methods=['POST'])
    def create_user():
        """Add a new user."""
        data = request.json
        if not data.get('name') or not data.get('email'):
            return jsonify({"error": "Name and email are required"}), 400
        try:
            new_user = User(name=data['name'], email=data['email'])
            db.session.add(new_user)
            db.session.commit()
            connection_activity_log.append({
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Added new user: {data['name']}."
            })
            return jsonify(new_user.to_dict()), 201
        except Exception as e:
            db.session.rollback()
            connection_activity_log.append({
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Error adding user: {str(e)}."
            })
            return jsonify({"error": str(e)}), 500

    @app.route('/users/<int:user_id>', methods=['GET'])
    def get_user(user_id):
        """Fetch a single user by ID."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404
        connection_activity_log.append({
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "message": f"Fetched user with ID: {user_id}."
        })
        return jsonify(user.to_dict())

    @app.route('/users/<int:user_id>', methods=['DELETE'])
    def delete_user(user_id):
        """Delete a user by ID."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404
        try:
            db.session.delete(user)
            db.session.commit()
            connection_activity_log.append({
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Deleted user with ID: {user_id}."
            })
            return jsonify({"message": "User deleted successfully"}), 200
        except Exception as e:
            db.session.rollback()
            connection_activity_log.append({
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Error deleting user: {str(e)}."
            })
            return jsonify({"error": str(e)}), 500

    return app
