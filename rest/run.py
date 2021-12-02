import __init__

app = __init__.create_app()
app.run(host="0.0.0.0", port=5000, debug=True)