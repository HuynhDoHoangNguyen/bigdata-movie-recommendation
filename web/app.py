"""Flask Application for Big Data Movie Recommendation System (TV4).

Provides Web UI and REST APIs for:
  - System Overview & Architecture Dashboard
  - Data Processing & Exploratory Data Analysis (EDA)
  - ALS Model Evaluation & Top-N Movie Recommendations (Raw & Reranked)
"""

import json
import logging
from pathlib import Path
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data storage in memory
app_data = {
    'dataset_summary': {},
    'rating_distribution': [],
    'movie_popularity': [],
    'genre_statistics': [],
    'rating_trend': [],
    'tag_statistics': [],
    'user_activity_summary': {},
    'model_metrics': {},
    'ranking_comparison': [],
    'recommendations': {},
    'recommendations_raw': {},
    'demo_users': []
}

DATA_DIR = Path(__file__).parent / 'data'


def load_data():
    """Load JSON files into memory on startup."""
    for key in app_data.keys():
        file_path = DATA_DIR / f"{key}.json"
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    app_data[key] = json.load(f)
                logger.info(f"Loaded {key}.json ({len(app_data[key]) if isinstance(app_data[key], (list, dict)) else '1'} records)")
            else:
                logger.warning(f"File not found: {file_path}, using default empty value.")
        except Exception as e:
            logger.error(f"Error loading {key}.json: {e}")


# Load data immediately
load_data()


@app.template_filter('format_number')
def format_number_filter(val):
    """Format numbers with thousand separators (e.g. 32.000.204)."""
    if val is None or val == 'N/A' or val == '':
        return 'N/A'
    try:
        return f"{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return str(val)


# --- View Routes ---

@app.route('/')
def index():
    """Dashboard page."""
    stats = app_data.get('dataset_summary', {})
    model = app_data.get('model_metrics', {})
    ranking = app_data.get('ranking_comparison', [])
    return render_template('index.html', stats=stats, model=model, ranking=ranking)


@app.route('/analytics')
def analytics():
    """Analytics page."""
    return render_template('analytics.html')


@app.route('/recommend')
def recommend():
    """Recommendations page."""
    demo_users = app_data.get('demo_users', [])
    return render_template('recommend.html', demo_users=demo_users)


# --- REST API Endpoints ---

@app.route('/api/stats')
def api_stats():
    """JSON API for dataset summary and overall metrics."""
    return jsonify({
        'dataset_summary': app_data['dataset_summary'],
        'user_activity_summary': app_data['user_activity_summary']
    })


@app.route('/api/analytics/<chart_type>')
def api_analytics(chart_type):
    """JSON API for specific chart data."""
    valid_charts = [
        'rating_distribution',
        'genre_statistics',
        'rating_trend',
        'movie_popularity',
        'tag_statistics'
    ]
    if chart_type in valid_charts:
        return jsonify(app_data.get(chart_type, []))
    else:
        return jsonify({'error': 'Invalid chart type'}), 404


@app.route('/api/recommend/<int:user_id>')
def api_recommend(user_id):
    """JSON API for user recommendations (supports ?mode=rerank or ?mode=raw)."""
    demo_users = app_data.get('demo_users', [])
    if user_id not in demo_users and str(user_id) not in [str(u) for u in demo_users]:
        return jsonify({
            'error': f'User {user_id} không nằm trong phạm vi 20 demo users đã lưu',
            'demo_users': demo_users
        }), 404

    mode = request.args.get('mode', 'rerank').lower()
    str_user_id = str(user_id)

    raw_list = app_data.get('recommendations_raw', {}).get(str_user_id, [])
    reranked_list = app_data.get('recommendations', {}).get(str_user_id, [])

    if mode == 'raw':
        active_recs = raw_list
    else:
        active_recs = reranked_list

    return jsonify({
        'user_id': user_id,
        'mode': mode,
        'recommendations': active_recs,
        'recommendations_raw': raw_list,
        'recommendations_reranked': reranked_list
    })


@app.route('/api/demo-users')
def api_demo_users():
    """JSON API for list of demo user IDs."""
    return jsonify(app_data.get('demo_users', []))


@app.route('/api/model-metrics')
def api_model_metrics():
    """JSON API for model metrics."""
    return jsonify(app_data.get('model_metrics', {}))


@app.route('/api/ranking-comparison')
def api_ranking_comparison():
    """JSON API for ranking comparison."""
    return jsonify(app_data.get('ranking_comparison', []))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
