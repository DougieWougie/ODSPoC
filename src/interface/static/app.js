document.addEventListener('DOMContentLoaded', () => {
    // Theme toggle
    const themeBtn = document.getElementById('theme-btn');
    const themeIcon = document.getElementById('theme-icon');
    const htmlEl = document.documentElement;

    themeBtn.addEventListener('click', () => {
        htmlEl.classList.toggle('dark');
        if (htmlEl.classList.contains('dark')) {
            themeIcon.textContent = '☀️';
        } else {
            themeIcon.textContent = '🌙';
        }
    });

    // Experiment selection
    const cards = document.querySelectorAll('.card');
    const generateBtn = document.getElementById('generate-btn');
    const statusMsg = document.getElementById('status-msg');
    let currentExperiment = null;
    let isSpinningUp = false;

    cards.forEach(card => {
        card.addEventListener('click', async () => {
            if (isSpinningUp) return;

            // UI Update
            cards.forEach(c => c.classList.remove('active'));
            card.classList.add('active');
            
            const expId = card.getAttribute('data-exp');
            currentExperiment = expId;
            generateBtn.disabled = true;
            isSpinningUp = true;
            statusMsg.textContent = `Starting Experiment ${expId}... Please wait.`;
            cards.forEach(c => c.style.opacity = '0.5');
            card.style.opacity = '1';

            // API Call
            try {
                const res = await fetch(`/api/experiment/${expId}`, { method: 'POST' });
                if (res.ok) {
                    statusMsg.textContent = `Experiment ${expId} is running. Ready to generate data.`;
                    generateBtn.disabled = false;
                } else {
                    statusMsg.textContent = `Failed to start Experiment ${expId}.`;
                }
            } catch (e) {
                statusMsg.textContent = `Error: ${e.message}`;
            } finally {
                isSpinningUp = false;
                cards.forEach(c => c.style.opacity = '1');
            }
        });
    });

    // Generate Data
    generateBtn.addEventListener('click', async () => {
        if (!currentExperiment) return;
        statusMsg.textContent = 'Generating 2000 transactions...';
        try {
            const res = await fetch('/api/generate', { method: 'POST' });
            if (res.ok) {
                statusMsg.textContent = 'Data generation triggered. Watch the metrics below.';
            }
        } catch (e) {
            statusMsg.textContent = `Error generating data: ${e.message}`;
        }
    });

    // Chart.js Setup
    const ctx = document.getElementById('latencyChart').getContext('2d');
    
    // Set default Chart text color to CSS variable for theming
    Chart.defaults.color = getComputedStyle(htmlEl).getPropertyValue('--text-color').trim();

    const latencyChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Avg Latency (s)',
                    borderColor: '#818cf8',
                    backgroundColor: 'rgba(129, 140, 248, 0.2)',
                    data: [],
                    fill: true,
                    tension: 0.4
                },
                {
                    label: 'Max Latency (s)',
                    borderColor: '#f43f5e',
                    backgroundColor: 'rgba(244, 63, 94, 0.2)',
                    data: [],
                    fill: true,
                    tension: 0.4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Seconds'
                    }
                }
            },
            animation: {
                duration: 0 // optimize for real-time updates
            }
        }
    });

    // WebSocket for Metrics
    let ws;
    const avgEl = document.getElementById('avg-latency');
    const maxEl = document.getElementById('max-latency');
    const pendingEl = document.getElementById('pending-txns');
    const MAX_DATA_POINTS = 60;

    function connectWebSocket() {
        ws = new WebSocket(`ws://${window.location.host}/ws/metrics`);

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.error) {
                console.error(data.error);
                return;
            }

            // Update DOM
            avgEl.textContent = `${data.avg.toFixed(3)}s`;
            maxEl.textContent = `${data.max.toFixed(3)}s`;
            pendingEl.textContent = data.missing;

            // Update Chart
            const now = new Date().toLocaleTimeString();
            latencyChart.data.labels.push(now);
            latencyChart.data.datasets[0].data.push(data.avg);
            latencyChart.data.datasets[1].data.push(data.max);

            if (latencyChart.data.labels.length > MAX_DATA_POINTS) {
                latencyChart.data.labels.shift();
                latencyChart.data.datasets[0].data.shift();
                latencyChart.data.datasets[1].data.shift();
            }

            latencyChart.update();
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected. Reconnecting in 2 seconds...');
            setTimeout(connectWebSocket, 2000);
        };
        
        ws.onerror = (err) => {
            console.error('WebSocket error:', err);
            ws.close();
        };
    }

    connectWebSocket();
});
