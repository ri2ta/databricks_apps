# app.py
import logging
import atexit
from flask import Flask, render_template
import db

# Basic logging setup so db.py logs show up on console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
db.logger = app.logger  # unify db.py logging with Flask's logger

# Initialize DB connection pool once on startup
def _init_db_pool():
    try:
        db.init_pool()
    except Exception:
        app.logger.exception("Failed to initialize DB pool")

_init_db_pool()

# Close pool only when process exits; keep it warm between requests
atexit.register(db.close_pool)

@app.route('/')
def index():
    return render_template('layout.html')

@app.route('/list')
def list_view():
    customers = db.get_customers()
    return render_template('partials/list.html', customers=customers)

@app.route('/detail/<int:customer_id>')
def detail_view(customer_id):
    customer = db.get_customer_detail(customer_id=customer_id)
    if customer is None:
        return "Customer not found", 404
    return render_template('partials/detail.html', customer=customer)