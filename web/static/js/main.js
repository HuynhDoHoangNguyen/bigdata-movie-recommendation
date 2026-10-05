/* ===== Big Data Movie Recommendation - Frontend Logic (TV4) ===== */

// --- Color Palette ---
const COLORS = {
    blue: '#4361ee',
    green: '#2ec4b6',
    orange: '#ff9f1c',
    purple: '#7209b7',
    red: '#e63946',
    pink: '#f72585',
    cyan: '#4cc9f0',
    yellow: '#fee440',
    teal: '#06d6a0',
    indigo: '#3f37c9'
};

const CHART_COLORS = [
    COLORS.blue, COLORS.green, COLORS.orange, COLORS.purple,
    COLORS.red, COLORS.pink, COLORS.cyan, COLORS.yellow,
    COLORS.teal, COLORS.indigo,
    '#8338ec', '#fb5607', '#3a86a7', '#606c38', '#dda15e',
    '#bc6c25', '#264653', '#2a9d8f', '#e9c46a', '#f4a261', '#e76f51'
];

// --- Utility Functions ---
function formatNumber(num) {
    if (num === null || num === undefined) return 'N/A';
    return Number(num).toLocaleString('vi-VN');
}

// --- Chart Loading ---
async function loadAllCharts() {
    await Promise.all([
        loadRatingDistribution(),
        loadMoviePopularity(),
        loadGenreStatistics(),
        loadRatingTrend(),
        loadTagStatistics()
    ]);
}

// --- 1. Rating Distribution Chart ---
async function loadRatingDistribution() {
    try {
        const res = await fetch('/api/analytics/rating_distribution');
        const data = await res.json();
        if (!data || data.length === 0) return;

        const ctx = document.getElementById('ratingDistChart');
        if (!ctx) return;

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.rating.toString()),
                datasets: [{
                    label: 'Số lượt đánh giá',
                    data: data.map(d => d.count),
                    backgroundColor: data.map((_, i) => {
                        const hue = 210 + (i * 14);
                        return `hsla(${hue}, 70%, 55%, 0.8)`;
                    }),
                    borderColor: data.map((_, i) => {
                        const hue = 210 + (i * 14);
                        return `hsla(${hue}, 70%, 45%, 1)`;
                    }),
                    borderWidth: 1,
                    borderRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const pct = data[ctx.dataIndex].percentage;
                                return `${formatNumber(ctx.raw)} lượt (${pct ? pct.toFixed(1) : 0}%)`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: val => formatNumber(val)
                        },
                        title: { display: true, text: 'Số lượt đánh giá' }
                    },
                    x: {
                        title: { display: true, text: 'Rating (0.5 - 5.0)' }
                    }
                }
            }
        });
    } catch (e) {
        console.error('Error loading rating distribution:', e);
    }
}

// --- 2. Movie Popularity Chart ---
async function loadMoviePopularity() {
    try {
        const res = await fetch('/api/analytics/movie_popularity');
        const data = await res.json();
        if (!data || data.length === 0) return;

        const ctx = document.getElementById('moviePopChart');
        if (!ctx) return;

        const top20 = data.slice(0, 20);

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: top20.map(d => {
                    const title = d.title || 'Unknown';
                    return title.length > 28 ? title.substring(0, 26) + '...' : title;
                }),
                datasets: [{
                    label: 'Số lượt đánh giá',
                    data: top20.map(d => d.rating_count),
                    backgroundColor: COLORS.blue + 'CC',
                    borderColor: COLORS.blue,
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            afterLabel: (ctx) => {
                                const movie = top20[ctx.dataIndex];
                                const avg = movie.average_rating ? movie.average_rating.toFixed(2) : 'N/A';
                                return `Rating TB: ${avg} | ${movie.genres || ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: { callback: val => formatNumber(val) },
                        title: { display: true, text: 'Số lượt đánh giá' }
                    }
                }
            }
        });
    } catch (e) {
        console.error('Error loading movie popularity:', e);
    }
}

// --- 3. Genre Statistics Chart ---
async function loadGenreStatistics() {
    try {
        const res = await fetch('/api/analytics/genre_statistics');
        const data = await res.json();
        if (!data || data.length === 0) return;

        const ctx = document.getElementById('genreChart');
        if (!ctx) return;

        const sorted = [...data].sort((a, b) => b.rating_count - a.rating_count);

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: sorted.map(d => d.genre),
                datasets: [
                    {
                        label: 'Số lượt đánh giá',
                        data: sorted.map(d => d.rating_count),
                        backgroundColor: COLORS.green + 'CC',
                        borderColor: COLORS.green,
                        borderWidth: 1,
                        borderRadius: 4,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Rating trung bình',
                        data: sorted.map(d => d.average_rating),
                        type: 'line',
                        borderColor: COLORS.orange,
                        backgroundColor: COLORS.orange + '33',
                        pointRadius: 4,
                        pointBackgroundColor: COLORS.orange,
                        tension: 0.3,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    tooltip: {
                        callbacks: {
                            afterLabel: (ctx) => {
                                const g = sorted[ctx.dataIndex];
                                return `Số phim: ${formatNumber(g.movie_count)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        position: 'left',
                        ticks: { callback: val => formatNumber(val) },
                        title: { display: true, text: 'Số lượt đánh giá' }
                    },
                    y1: {
                        beginAtZero: false,
                        position: 'right',
                        min: 2.5,
                        max: 4.5,
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Rating TB' }
                    },
                    x: {
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45
                        }
                    }
                }
            }
        });
    } catch (e) {
        console.error('Error loading genre statistics:', e);
    }
}

// --- 4. Rating Trend Chart ---
async function loadRatingTrend() {
    try {
        const res = await fetch('/api/analytics/rating_trend');
        const data = await res.json();
        if (!data || data.length === 0) return;

        const ctx = document.getElementById('trendChart');
        if (!ctx) return;

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(d => d.year.toString()),
                datasets: [
                    {
                        label: 'Số lượt đánh giá',
                        data: data.map(d => d.rating_count),
                        borderColor: COLORS.purple,
                        backgroundColor: COLORS.purple + '22',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 3,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Rating trung bình',
                        data: data.map(d => d.average_rating),
                        borderColor: COLORS.orange,
                        backgroundColor: 'transparent',
                        tension: 0.3,
                        pointRadius: 3,
                        borderDash: [5, 5],
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        position: 'left',
                        ticks: { callback: val => formatNumber(val) },
                        title: { display: true, text: 'Số lượt đánh giá' }
                    },
                    y1: {
                        position: 'right',
                        min: 2.5,
                        max: 4.5,
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Rating TB' }
                    },
                    x: {
                        title: { display: true, text: 'Năm' }
                    }
                }
            }
        });
    } catch (e) {
        console.error('Error loading rating trend:', e);
    }
}

// --- 5. Tag Statistics Chart ---
async function loadTagStatistics() {
    try {
        const res = await fetch('/api/analytics/tag_statistics');
        const data = await res.json();
        if (!data || data.length === 0) return;

        const ctx = document.getElementById('tagChart');
        if (!ctx) return;

        const top20 = data.slice(0, 20);

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: top20.map(d => d.tag),
                datasets: [{
                    label: 'Số lần gắn tag',
                    data: top20.map(d => d.tag_count),
                    backgroundColor: CHART_COLORS.slice(0, 20).map(c => c + 'CC'),
                    borderColor: CHART_COLORS.slice(0, 20),
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: { callback: val => formatNumber(val) },
                        title: { display: true, text: 'Số lần gắn tag' }
                    }
                }
            }
        });
    } catch (e) {
        console.error('Error loading tag statistics:', e);
    }
}

// --- Recommendation Functions ---
async function loadRecommendations(userId, mode) {
    const resultsDiv = document.getElementById('recommendResults');
    const spinner = document.getElementById('btnSpinner');
    if (!resultsDiv) return;

    if (!userId) {
        resultsDiv.innerHTML = '<div class="text-center text-muted py-5"><p class="fs-5 mb-0">Chọn một User ID để xem gợi ý phim</p></div>';
        return;
    }

    if (spinner) spinner.classList.remove('d-none');

    resultsDiv.innerHTML = `
        <div class="loading-spinner py-5 text-center">
            <div class="spinner-border text-primary" role="status"></div>
            <div class="mt-2 text-muted">Đang tải danh sách phim gợi ý cho User #${userId}...</div>
        </div>
    `;

    try {
        const queryMode = (mode === 'raw' || mode === 'modeRaw') ? 'raw' : 'rerank';
        const res = await fetch(`/api/recommend/${userId}?mode=${queryMode}`);
        const data = await res.json();

        if (spinner) spinner.classList.add('d-none');

        if (data.error) {
            resultsDiv.innerHTML = `
                <div class="alert alert-warning shadow-sm">
                    <h6 class="alert-heading fw-bold mb-2">${data.error}</h6>
                    <p class="mb-2 small">Hệ thống chỉ lưu sẵn danh sách gợi ý cho 20 demo users sau đây:</p>
                    <div class="d-flex flex-wrap gap-1">
                        ${(data.demo_users || []).map(u =>
                            `<button type="button" class="btn btn-sm btn-outline-primary" onclick="selectDemoUser(${u})">${u}</button>`
                        ).join('')}
                    </div>
                </div>
            `;
            return;
        }

        const recommendations = data.recommendations || [];
        const isReranked = queryMode !== 'raw';
        const userIdDisplay = data.user_id || userId;

        if (recommendations.length === 0) {
            resultsDiv.innerHTML = '<div class="alert alert-info shadow-sm">Không có dữ liệu recommendation cho user này.</div>';
            return;
        }

        let html = `
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h5 class="mb-0 fw-bold">
                    Top ${recommendations.length} phim gợi ý cho User #${userIdDisplay}
                </h5>
                <span class="badge ${isReranked ? 'bg-primary' : 'bg-secondary'} p-2">
                    ${isReranked ? 'Reranked (Đề xuất)' : 'Raw ALS (Chưa rerank)'}
                </span>
            </div>
        `;

        recommendations.forEach(movie => {
            const genres = (movie.genres || '').split('|').filter(g => g);
            const genreBadges = genres.map(g =>
                `<span class="badge bg-white text-secondary border me-1">${g}</span>`
            ).join(' ');

            const scoreLabel = isReranked ? 'Adjusted Score' : 'ALS Prediction';
            const scoreValue = isReranked
                ? (movie.adjusted_score !== undefined ? movie.adjusted_score.toFixed(4) : (movie.prediction ? movie.prediction.toFixed(4) : 'N/A'))
                : (movie.prediction !== undefined ? movie.prediction.toFixed(4) : 'N/A');

            let extraInfo = '';
            if (isReranked && movie.train_support !== undefined) {
                extraInfo = `<span class="badge bg-light text-muted border ms-2">Độ phổ biến (Support): ${formatNumber(movie.train_support)} ratings</span>`;
            }
            if (isReranked && movie.prediction !== undefined) {
                extraInfo += `<span class="badge bg-light text-muted border ms-1">ALS raw: ${movie.prediction.toFixed(4)}</span>`;
            }

            html += `
                <div class="card mb-3 shadow-sm border-0 border-start border-4 ${isReranked ? 'border-primary' : 'border-secondary'}">
                    <div class="card-body d-flex align-items-center gap-3">
                        <div class="rank-badge bg-primary text-white rounded-circle d-flex align-items-center justify-content-center fw-bold fs-5" style="width: 44px; height: 44px; min-width: 44px;">
                            #${movie.rank}
                        </div>
                        <div class="flex-grow-1">
                            <h6 class="fw-bold mb-1 text-dark">${movie.title || 'Unknown'}</h6>
                            <div class="mb-2">${genreBadges}</div>
                            <div class="small">
                                <span class="text-primary fw-semibold">${scoreLabel}: <strong>${scoreValue}</strong></span>
                                ${extraInfo}
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });

        resultsDiv.innerHTML = html;
    } catch (e) {
        if (spinner) spinner.classList.add('d-none');
        console.error('Error loading recommendations:', e);
        resultsDiv.innerHTML = '<div class="alert alert-danger shadow-sm">Lỗi khi tải dữ liệu gợi ý. Vui lòng kiểm tra lại backend.</div>';
    }
}

// --- Helper: Select demo user from buttons or dropdown ---
function selectDemoUser(userId) {
    const input = document.getElementById('userIdInput');
    const select = document.getElementById('userIdSelect');
    if (input) input.value = userId;
    if (select) select.value = userId;

    const mode = getActiveMode();
    loadRecommendations(userId, mode);
}

function getActiveMode() {
    const modeRadio = document.querySelector('input[name="modeRadio"]:checked') || document.querySelector('input[name="recMode"]:checked');
    if (!modeRadio) return 'rerank';
    return (modeRadio.value === 'raw' || modeRadio.id === 'modeRaw') ? 'raw' : 'rerank';
}

// --- Initialize Recommendation Page ---
function initRecommendPage() {
    const btn = document.getElementById('btnRecommend') || document.getElementById('searchBtn');
    const userIdInput = document.getElementById('userIdInput');
    const userIdSelect = document.getElementById('userIdSelect');

    function triggerSearch() {
        const uid = (userIdInput && userIdInput.value) ? userIdInput.value : (userIdSelect ? userIdSelect.value : '');
        if (uid) {
            loadRecommendations(uid.trim(), getActiveMode());
        }
    }

    if (btn) {
        btn.addEventListener('click', triggerSearch);
    }

    if (userIdInput) {
        userIdInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                triggerSearch();
            }
        });
    }

    if (userIdSelect) {
        userIdSelect.addEventListener('change', function() {
            if (this.value) {
                if (userIdInput) userIdInput.value = this.value;
                triggerSearch();
            }
        });
    }

    document.querySelectorAll('input[name="modeRadio"], input[name="recMode"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const uid = (userIdInput && userIdInput.value) ? userIdInput.value : (userIdSelect ? userIdSelect.value : '');
            if (uid) {
                loadRecommendations(uid.trim(), getActiveMode());
            }
        });
    });

    // Auto-select user 806 by default on initial page load if select exists
    if (userIdSelect && !userIdSelect.value) {
        userIdSelect.value = '806';
        if (userIdInput) userIdInput.value = '806';
        loadRecommendations('806', 'rerank');
    }
}

// Auto-initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('recommendResults')) {
        initRecommendPage();
    }
    if (document.getElementById('ratingDistChart')) {
        loadAllCharts();
    }
});
