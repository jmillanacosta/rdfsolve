// RDFSolve Coverage Visualization Module
class CoverageVisualization {
    constructor(datasets) {
        this.datasets = datasets;
        this.currentDataset = null;
        this.coverageData = null;
        this.githubRawBase = 'https://raw.githubusercontent.com/jmillanacosta/rdfsolve/main/docs/';
        this.init();
    }

    init() {
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Delegated click handlers for coverage buttons
        document.body.addEventListener('click', (e) => {
            if (e.target.classList.contains('view-coverage-btn')) {
                this.loadDataset(e.target.dataset.dataset);
            }
            if (e.target.classList.contains('close-panel-btn') && e.target.dataset.panel === 'coverage') {
                this.closeSidebar();
            }
        });

        // Show all checkboxes
        document.getElementById('show-all-patterns')?.addEventListener('change', () => {
            this.renderTopPatterns();
        });
        document.getElementById('show-all-properties')?.addEventListener('change', () => {
            this.renderPropertyDistribution();
        });
    }

    async loadDataset(datasetName) {
        if (!datasetName) {
            this.closeSidebar();
            return;
        }

        this.currentDataset = this.datasets.find(d => d.name === datasetName);
        if (!this.currentDataset?.dataFiles?.coverage) {
            console.error('Dataset not found or no coverage data');
            return;
        }

        this.showLoading();
        this.openSidebar();

        try {
            const coveragePath = this.currentDataset.dataFiles.coverage
                .replace(/^(\.\.\/)+/, '')
                .replace(/^\.?\//, '');
            const coverageUrl = this.githubRawBase + coveragePath;

            const response = await fetch(coverageUrl);
            if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);

            const csvText = await response.text();
            this.coverageData = this.parseCSV(csvText);
            
            this.renderVisualizations();
        } catch (error) {
            console.error('Failed to load coverage data:', error);
            this.showError('Failed to load coverage data. The file may not be available.');
        }
    }

    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        const headers = lines[0].split(',');
        
        const data = lines.slice(1).map(line => {
            const values = this.parseCSVLine(line);
            const row = {};
            headers.forEach((header, i) => {
                row[header] = values[i];
            });
            return row;
        });

        return data;
    }

    parseCSVLine(line) {
        // Handle CSV with potential commas in quoted fields
        const result = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                result.push(current);
                current = '';
            } else {
                current += char;
            }
        }
        result.push(current);
        
        return result;
    }

    renderVisualizations() {
        this.renderStats();
        this.renderCoverageHistogram();
        this.renderTopPatterns();
        this.renderPropertyDistribution();
    }

    renderStats() {
        const statsContainer = document.getElementById('coverage-stats');
        if (!statsContainer || !this.coverageData) return;

        const totalPatterns = this.coverageData.length;
        const avgCoverage = this.coverageData.reduce((sum, row) => 
            sum + parseFloat(row.coverage_percent || 0), 0) / totalPatterns;
        
        const uniqueSubjects = new Set(this.coverageData.map(row => row.subject_class)).size;
        const uniqueProperties = new Set(this.coverageData.map(row => row.property)).size;
        const uniqueObjects = new Set(this.coverageData.map(row => row.object_class)).size;
        
        const totalOccurrences = this.coverageData.reduce((sum, row) => 
            sum + parseInt(row.occurrence_count || 0), 0);

        statsContainer.innerHTML = `
            <div class="stat-grid">
                <div class="mini-stat">
                    <div class="mini-stat-value">${totalPatterns}</div>
                    <div class="mini-stat-label">Total Patterns</div>
                </div>
                <div class="mini-stat">
                    <div class="mini-stat-value">${avgCoverage.toFixed(1)}%</div>
                    <div class="mini-stat-label">Avg Coverage</div>
                </div>
                <div class="mini-stat">
                    <div class="mini-stat-value">${uniqueSubjects}</div>
                    <div class="mini-stat-label">Subject Classes</div>
                </div>
                <div class="mini-stat">
                    <div class="mini-stat-value">${uniqueProperties}</div>
                    <div class="mini-stat-label">Properties</div>
                </div>
                <div class="mini-stat">
                    <div class="mini-stat-value">${uniqueObjects}</div>
                    <div class="mini-stat-label">Object Classes</div>
                </div>
                <div class="mini-stat">
                    <div class="mini-stat-value">${totalOccurrences.toLocaleString()}</div>
                    <div class="mini-stat-label">Total Occurrences</div>
                </div>
            </div>
        `;
    }

    renderCoverageHistogram() {
        const container = document.getElementById('coverage-histogram');
        if (!container || !this.coverageData) return;

        // Clear loading state
        container.innerHTML = '';

        // Prepare data for histogram
        const coverageValues = this.coverageData.map(row => 
            parseFloat(row.coverage_percent || 0)
        );

        const occurrences = this.coverageData.map(row => 
            parseInt(row.occurrence_count || 0)
        );

        const patterns = this.coverageData.map(row => 
            row.shape_pattern || 'Unknown'
        );

        const trace = {
            x: coverageValues,
            y: occurrences,
            text: patterns,
            mode: 'markers',
            type: 'scatter',
            marker: {
                size: 8,
                color: coverageValues,
                colorscale: 'Viridis',
                showscale: true,
                colorbar: {
                    title: 'Coverage %',
                    thickness: 15,
                    len: 0.7
                },
                line: {
                    color: 'white',
                    width: 0.5
                }
            },
            hovertemplate: '<b>Pattern:</b> %{text}<br>' +
                          '<b>Coverage:</b> %{x:.1f}%<br>' +
                          '<b>Occurrences:</b> %{y}<br>' +
                          '<extra></extra>'
        };

        const layout = {
            title: {
                text: 'Coverage Distribution',
                font: { size: 16, weight: 'bold' }
            },
            xaxis: {
                title: 'Coverage Percentage (%)',
                range: [-5, 105]
            },
            yaxis: {
                title: 'Occurrence Count',
                type: 'log'
            },
            hovermode: 'closest',
            margin: { t: 50, r: 80, b: 50, l: 60 },
            autosize: true
        };

        const config = {
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['lasso2d', 'select2d']
        };

        Plotly.newPlot(container, [trace], layout, config);
    }

    renderTopPatterns() {
        const container = document.getElementById('top-patterns');
        if (!container || !this.coverageData) return;

        // Clear loading state
        container.innerHTML = '';

        // Check if "show all" is enabled
        const showAll = document.getElementById('show-all-patterns')?.checked;
        
        // Sort by occurrence count
        let topPatterns = [...this.coverageData]
            .sort((a, b) => parseInt(b.occurrence_count || 0) - parseInt(a.occurrence_count || 0));
        
        // Limit to top 15 unless "show all" is checked
        if (!showAll) {
            topPatterns = topPatterns.slice(0, 15);
        }

        const patterns = topPatterns.map(row => 
            this.truncatePattern(row.shape_pattern || 'Unknown')
        );
        const occurrences = topPatterns.map(row => 
            parseInt(row.occurrence_count || 0)
        );
        const coverages = topPatterns.map(row => 
            parseFloat(row.coverage_percent || 0)
        );

        const trace = {
            type: 'bar',
            x: occurrences,
            y: patterns,
            orientation: 'h',
            marker: {
                color: coverages,
                colorscale: 'Viridis',
                showscale: false,
                line: {
                    color: 'white',
                    width: 1
                }
            },
            hovertemplate: '<b>%{y}</b><br>' +
                          'Occurrences: %{x}<br>' +
                          '<extra></extra>'
        };

        const layout = {
            title: {
                text: showAll ? `All ${patterns.length} Patterns by Occurrence` : 'Top 15 Patterns by Occurrence',
                font: { size: 16, weight: 'bold' }
            },
            xaxis: {
                title: 'Occurrence Count'
            },
            yaxis: {
                automargin: true
            },
            margin: { t: 50, r: 20, b: 50, l: 200 },
            height: showAll ? Math.max(500, patterns.length * 25) : 500,
            autosize: true
        };

        const config = {
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['lasso2d', 'select2d', 'zoom2d', 'pan2d']
        };

        Plotly.newPlot(container, [trace], layout, config);
    }

    renderPropertyDistribution() {
        const container = document.getElementById('property-distribution');
        if (!container || !this.coverageData) return;

        // Clear loading state
        container.innerHTML = '';

        // Check if "show all" is enabled
        const showAll = document.getElementById('show-all-properties')?.checked;

        // Group by property and sum occurrences
        const propertyMap = new Map();
        this.coverageData.forEach(row => {
            const prop = row.property || 'Unknown';
            const count = parseInt(row.occurrence_count || 0);
            propertyMap.set(prop, (propertyMap.get(prop) || 0) + count);
        });

        // Sort by count
        let sortedProps = Array.from(propertyMap.entries())
            .sort((a, b) => b[1] - a[1]);
        
        // Limit to top 20 unless "show all" is checked
        if (!showAll) {
            sortedProps = sortedProps.slice(0, 20);
        }

        const trace = {
            type: 'pie',
            labels: sortedProps.map(([prop]) => this.formatPropertyLabel(prop)),
            values: sortedProps.map(([_, count]) => count),
            hovertemplate: '<b>%{label}</b><br>' +
                          'Occurrences: %{value}<br>' +
                          'Percentage: %{percent}<br>' +
                          '<extra></extra>',
            textposition: 'auto',
            marker: {
                line: {
                    color: 'white',
                    width: 2
                }
            }
        };

        const layout = {
            title: {
                text: showAll ? `All ${sortedProps.length} Properties` : 'Property Distribution (Top 20)',
                font: { size: 16, weight: 'bold' }
            },
            height: showAll ? Math.max(450, sortedProps.length * 20) : 450,
            showlegend: true,
            legend: {
                orientation: 'v',
                x: 1.05,
                y: 0.5
            },
            autosize: true
        };

        const config = {
            responsive: true,
            displayModeBar: false
        };

        Plotly.newPlot(container, [trace], layout, config);
    }

    truncatePattern(pattern, maxLength = 50) {
        if (pattern.length <= maxLength) return pattern;
        return pattern.substring(0, maxLength) + '...';
    }

    formatPropertyLabel(property) {
        // Extract local name from URI or full property name
        const parts = property.split(/[#\/]/);
        return parts[parts.length - 1] || property;
    }

    showLoading() {
        const containers = [
            'coverage-stats',
            'coverage-histogram',
            'top-patterns',
            'property-distribution'
        ];

        containers.forEach(id => {
            const container = document.getElementById(id);
            if (container) {
                container.innerHTML = '<div class="loading">Loading...</div>';
            }
        });
    }

    showError(message) {
        const containers = [
            'coverage-stats',
            'coverage-histogram',
            'top-patterns',
            'property-distribution'
        ];

        containers.forEach(id => {
            const container = document.getElementById(id);
            if (container) {
                container.innerHTML = `<div class="error-message">${message}</div>`;
            }
        });
    }

    openSidebar() {
        // Close schema panel if open
        document.getElementById('schema-panel')?.classList.remove('open');
        
        document.getElementById('coverage-panel')?.classList.add('open');
        document.getElementById('main-content')?.classList.add('panel-open');
    }

    closeSidebar() {
        document.getElementById('coverage-panel')?.classList.remove('open');
        document.getElementById('main-content')?.classList.remove('panel-open');
    }
}

// Export for use in main app
window.CoverageVisualization = CoverageVisualization;
