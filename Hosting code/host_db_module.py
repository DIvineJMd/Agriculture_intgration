from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

def host_db(username, password, host, port, db_name, db_type = 'sqlite'):
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

    # Define a sample database model (this can be extended)
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(100), nullable=False)
        email = db.Column(db.String(120), unique=True, nullable=False)

        def to_dict(self):
            return {"id": self.id, "name": self.name, "email": self.email}

    # Initialize the database
    @app.before_first_request
    def initialize_database():
        db.create_all()

    # API endpoints
    @app.route('/users', methods=['GET'])
    def get_users():
        """Fetch all users."""
        users = User.query.all()
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
            return jsonify(new_user.to_dict()), 201
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    @app.route('/users/<int:user_id>', methods=['GET'])
    def get_user(user_id):
        """Fetch a single user by ID."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404
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
            return jsonify({"message": "User deleted successfully"}), 200
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    return app

# Example Usage
if __name__ == '__main__':
    # Hosting a SQLite database and exposing it via an API
    app = host_db(
        # db_type='sqlite',       # Use 'mysql', 'postgresql' for other databases
        username='',            # Not required for SQLite
        password='',            # Not required for SQLite
        host='',                # Not required for SQLite
        port=0,                 # Not required for SQLite
        db_name='example_db'    # SQLite database file name
    )
    app.run(host='0.0.0.0', port=5000)
