// RDFSolve Results Dashboard
class RDFSolveDashboard {
    constructor() {
        this.data = null;
        this.githubBaseUrl = 'https://github.com/jmillanacosta/rdfsolve/blob/main/docs/';
        this.init();
    }

    async init() {
        try {
            await this.loadData();
            this.renderStats();
            this.renderSchemaResults();
            this.renderPydanticResults();
            this.updateLastUpdated();
        } catch (error) {
            console.error('Failed to initialize dashboard:', error);
            this.showError('Failed to load results data');
        }
    }

    async loadData() {
        try {
            const response = await fetch('results.json');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            this.data = await response.json();
        } catch (error) {
            // Fallback: try to extract data from existing content or use mock data
            console.warn('Could not load results.json, using fallback data');
            this.data = this.generateFallbackData();
        }
    }

    generateFallbackData() {
        // This will be populated by the workflow, but provides structure
        return {
            lastUpdated: new Date().toISOString(),
            stats: {
                totalDatasets: 0,
                schemaSuccessful: 0,
                schemaFailed: 0,
                pydanticSuccessful: 0,
                pydanticFailed: 0,
                namespaceSuccessful: 0,
                namespaceFailed: 0,
                totalDataFiles: 0
            },
            datasets: []
        };
    }

    renderStats() {
        const stats = this.data.stats || {};
        
        document.getElementById('total-datasets').textContent = stats.totalDatasets || 0;
        document.getElementById('successful-datasets').textContent = stats.schemaSuccessful || 0;
        document.getElementById('failed-datasets').textContent = stats.schemaFailed || 0;
        document.getElementById('total-data-files').textContent = stats.totalDataFiles || 0;
    }

    renderSchemaResults() {
        const container = document.getElementById('datasets-grid');
        const datasets = this.data.datasets || [];
        
        // Filter datasets that have schema notebooks
        const results = datasets.filter(d => d.notebooks && d.notebooks.schema);

        if (results.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No Schema Results</h3>
                    <p>No schema analysis results available yet.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = results.map(dataset => this.createDatasetCard(dataset, 'schema')).join('');
    }

    renderPydanticResults() {
        const container = document.getElementById('pydantic-grid');
        const datasets = this.data.datasets || [];
        
        // Filter datasets that have pydantic notebooks and aren't missing
        const results = datasets.filter(d => d.notebooks && d.notebooks.pydantic && d.notebooks.pydantic.status !== 'missing');

        if (results.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No Pydantic Results</h3>
                    <p>No pydantic model results available yet.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = results.map(dataset => this.createPydanticCard(dataset)).join('');
    }

    createDatasetCard(dataset, notebookType) {
        const notebook = dataset.notebooks[notebookType];
        const statusClass = notebook.status === 'success' ? 'success' : 'error';
        const statusIcon = notebook.status === 'success' ? '✓' : '✗';
        const statusText = notebook.status === 'success' ? 'Completed Successfully' : 'Analysis Failed';

        const dataFilesHTML = this.createDataFilesSection(dataset.dataFiles || {});

        return `
            <div class="dataset-card">
                <div class="dataset-name">
                    <span class="status-icon ${statusClass}">${statusIcon}</span>
                    ${dataset.name}
                </div>
                
                <div class="status-badge ${statusClass}">${statusText}</div>
                
                <div class="dataset-meta">
                    <div>Report Size: ${notebook.reportSize || 'Unknown'}</div>
                    <div>Generated: ${this.formatDate(dataset.generated)}</div>
                </div>
                
                ${dataFilesHTML}
                
                <a href="${notebook.reportUrl}" class="view-link">
                    View Analysis Report →
                </a>
            </div>
        `;
    }

    createPydanticCard(dataset) {
        const notebook = dataset.notebooks.pydantic;
        const statusClass = notebook.status === 'success' ? 'success' : 'error';
        const statusIcon = notebook.status === 'success' ? '✓' : '✗';
        const statusText = notebook.status === 'success' ? 'Models Generated' : 'Generation Failed';

        return `
            <div class="dataset-card">
                <div class="dataset-name">
                    <span class="status-icon ${statusClass}">${statusIcon}</span>
                    ${dataset.name}
                </div>
                
                <div class="status-badge ${statusClass}">${statusText}</div>
                
                <div class="dataset-meta">
                    <div>Report Size: ${notebook.reportSize || 'Unknown'}</div>
                    <div>Generated: ${this.formatDate(dataset.generated)}</div>
                </div>
                
                <a href="${notebook.reportUrl}" class="view-link">
                    View Pydantic Models →
                </a>
            </div>
        `;
    }

    createDataFilesSection(dataFiles) {
        const files = Object.entries(dataFiles).filter(([key, url]) => url);
        
        if (files.length === 0) {
            return '';
        }

        const fileTypeNames = {
            jsonld: 'JSON-LD',
            linkml: 'LinkML',
            coverage: 'Coverage',
            void: 'VoID',
            instances: 'Instances',
            nquads: 'N-Quads',
            schema_json: 'JSON',
            schema_csv: 'CSV',
            queries: 'SPARQL Queries'
        };

        const linksHTML = files.map(([type, url]) => {
            const name = fileTypeNames[type] || type.toUpperCase();
            // Convert relative path to GitHub blob URL
            const githubUrl = this.toGithubUrl(url);
            return `<a href="${githubUrl}" class="data-link" target="_blank">${name}</a>`;
        }).join('');

        return `
            <div class="data-files">
                <div class="data-files-title">📁 Generated Data Files</div>
                <div class="data-links">
                    ${linksHTML}
                </div>
            </div>
        `;
    }

    toGithubUrl(relativePath) {
        // If already a full URL, return as-is
        if (relativePath.startsWith('http://') || relativePath.startsWith('https://')) {
            return relativePath;
        }
        // Remove leading ../, ./, or / and normalize to docs/ base
        // Paths like ../data/... should become data/...
        let cleanPath = relativePath.replace(/^(\.\.\/)+/, '').replace(/^\.?\//, '');
        return this.githubBaseUrl + cleanPath;
    }

    formatDate(dateString) {
        if (!dateString) return 'Unknown';
        
        try {
            const date = new Date(dateString);
            return date.toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch {
            return dateString;
        }
    }

    updateLastUpdated() {
        const lastUpdated = this.data.lastUpdated;
        const element = document.getElementById('last-updated');
        
        if (lastUpdated) {
            element.textContent = `Last updated: ${this.formatDate(lastUpdated)}`;
        } else {
            element.textContent = 'Last updated: Unknown';
        }
    }

    showError(message) {
        const containers = ['datasets-grid', 'pydantic-grid'];
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            container.innerHTML = `
                <div class="empty-state">
                    <h3>Error Loading Data</h3>
                    <p>${message}</p>
                </div>
            `;
        });
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new RDFSolveDashboard();
});

// Add refresh functionality
document.addEventListener('keydown', (e) => {
    if (e.key === 'F5' || (e.ctrlKey && e.key === 'r')) {
        window.location.reload();
    }
});