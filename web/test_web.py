"""Automated Verification Test for TV4 Web Demo.

Tests:
  1. Root Dashboard (/) -> 200 OK, checks for MovieLens 32M, stats, pipeline architecture, ALS info
  2. Analytics (/analytics) -> 200 OK, checks for chart canvas elements
  3. Recommendations (/recommend) -> 200 OK, checks for user 806 and demo selector
  4. API Stats (/api/stats) -> 200 OK, JSON format
  5. API Analytics endpoints (/api/analytics/...) -> 200 OK for 5 charts
  6. API Recommendations (/api/recommend/806) -> 200 OK, 10 reranked and raw recommendations
  7. API Recommendations for invalid user (/api/recommend/9999999) -> 404 with demo_users list
  8. API Demo Users (/api/demo-users) -> 200 OK, exactly 20 users
  9. API Model Metrics (/api/model-metrics) -> 200 OK
  10. API Ranking Comparison (/api/ranking-comparison) -> 200 OK
"""

import sys
import unittest
from pathlib import Path

# Add web directory to sys.path
sys.path.insert(0, str(Path(__file__).parent))

from app import app, app_data


class WebDemoTestCase(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_dashboard_view(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Hệ thống gợi ý phim dựa trên Big Data", html)
        self.assertIn("MovieLens 32M", html)
        self.assertIn("Thông tin mô hình ALS", html)
        self.assertIn("So sánh phương pháp", html)
        self.assertIn("32.000.204", html)

    def test_analytics_view(self):
        response = self.client.get('/analytics')
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Phân tích dữ liệu MovieLens 32M", html)
        self.assertIn("ratingDistChart", html)
        self.assertIn("genreChart", html)
        self.assertIn("trendChart", html)
        self.assertIn("moviePopChart", html)
        self.assertIn("tagChart", html)

    def test_recommend_view(self):
        response = self.client.get('/recommend')
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Gợi ý phim cho bạn", html)
        self.assertIn("SAMPLE model", html)
        self.assertIn("User 806", html)

    def test_api_stats(self):
        response = self.client.get('/api/stats')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('dataset_summary', data)
        self.assertEqual(data['dataset_summary']['total_ratings'], 32000204)
        self.assertEqual(data['dataset_summary']['total_users'], 200948)

    def test_api_analytics_endpoints(self):
        charts = ['rating_distribution', 'genre_statistics', 'rating_trend', 'movie_popularity', 'tag_statistics']
        for c in charts:
            res = self.client.get(f'/api/analytics/{c}')
            self.assertEqual(res.status_code, 200, f"Failed for chart {c}")
            data = res.get_json()
            self.assertTrue(len(data) > 0, f"Chart data empty for {c}")

        # Test invalid chart type
        res_invalid = self.client.get('/api/analytics/invalid_chart_name')
        self.assertEqual(res_invalid.status_code, 404)

    def test_api_recommend_valid_user(self):
        res = self.client.get('/api/recommend/806?mode=rerank')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data['user_id'], 806)
        self.assertEqual(len(data['recommendations']), 10)
        first_rec = data['recommendations'][0]
        self.assertEqual(first_rec['rank'], 1)
        self.assertIn('title', first_rec)
        self.assertIn('adjusted_score', first_rec)
        self.assertIn('train_support', first_rec)

        # Test raw mode
        res_raw = self.client.get('/api/recommend/806?mode=raw')
        self.assertEqual(res_raw.status_code, 200)
        data_raw = res_raw.get_json()
        self.assertEqual(len(data_raw['recommendations']), 10)

    def test_api_recommend_invalid_user(self):
        res = self.client.get('/api/recommend/9999999')
        self.assertEqual(res.status_code, 404)
        data = res.get_json()
        self.assertIn('error', data)
        self.assertEqual(len(data['demo_users']), 20)

    def test_api_demo_users(self):
        res = self.client.get('/api/demo-users')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(len(data), 20)
        self.assertIn(806, data)

    def test_api_model_metrics(self):
        res = self.client.get('/api/model-metrics')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data['rmse'], 0.8146)
        self.assertEqual(data['mae'], 0.6201)
        self.assertEqual(data['sample_users'], 10000)

    def test_api_ranking_comparison(self):
        res = self.client.get('/api/ranking-comparison')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(len(data), 3)


if __name__ == '__main__':
    unittest.main()
