html_temp = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; padding: 20px; }
        h1 { color: #333; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
        th { background-color: #f4f4f4; }
    </style>
</head>
<body>
    <h1>Database Dashboard</h1>
    <p><strong>Database Name:</strong> {{ db_name }}</p>
    <h2>Connection Activity</h2>
    <table>
        <thead>
            <tr>
                <th>Timestamp</th>
                <th>Activity</th>
            </tr>
        </thead>
        <tbody>
            {% for activity in connection_activity %}
            <tr>
                <td>{{ activity.timestamp }}</td>
                <td>{{ activity.message }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""