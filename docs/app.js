// RDFSolve Results Dashboard
class RDFSolveDashboard {
    constructor() {
        this.data = null;
        this.sourcesData = null;
        this.githubBaseUrl = 'https://github.com/jmillanacosta/rdfsolve/blob/main/docs/';
        this.githubRawBase = 'https://raw.githubusercontent.com/jmillanacosta/rdfsolve/main/';
        this.init();
    }

    async init() {
        try {
            await Promise.all([
                this.loadData(),
                this.loadSources()
            ]);
            this.renderStats();
            this.renderDatasets();
            this.updateLastUpdated();
            this.initializeVisualizations();
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

    async loadSources() {
        try {
            const response = await fetch(this.githubRawBase + 'data/sources.csv');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const csvText = await response.text();
            this.sourcesData = this.parseSourcesCSV(csvText);
        } catch (error) {
            console.warn('Could not load sources.csv:', error);
            this.sourcesData = {};
        }
    }

    parseSourcesCSV(csvText) {
        const lines = csvText.trim().split('\n');
        const sources = {};
        // Skip header: dataset_name,void_iri,graph_uri,endpoint_url,use_graph
        for (let i = 1; i < lines.length; i++) {
            const cols = lines[i].split(',');
            if (cols.length >= 4) {
                const datasetName = cols[0].trim();
                sources[datasetName] = {
                    voidIri: cols[1]?.trim(),
                    graphUri: cols[2]?.trim(),
                    endpointUrl: cols[3]?.trim(),
                    useGraph: cols[4]?.trim() === 'True'
                };
            }
        }
        return sources;
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
        
        const totalDatasets = stats.totalDatasets || 0;
        const successfulDatasets = stats.schemaSuccessful || 0;
        const failedDatasets = stats.schemaFailed || 0;
        const totalDataFiles = stats.totalDataFiles || 0;
        
        document.getElementById('total-datasets').textContent = totalDatasets;
        document.getElementById('successful-datasets').textContent = successfulDatasets;
        document.getElementById('failed-datasets').textContent = failedDatasets;
        document.getElementById('total-data-files').textContent = totalDataFiles;
    }

    renderDatasets() {
        const container = document.getElementById('datasets-grid');
        const datasets = this.data.datasets || [];

        if (datasets.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No Results</h3>
                    <p>No dataset analysis results available yet.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = datasets.map(dataset => this.createDatasetCard(dataset)).join('');
    }

    createDatasetCard(dataset) {
        const schemaNotebook = dataset.notebooks?.schema;
        const statusClass = schemaNotebook?.status === 'success' ? 'success' : 'error';
        const statusIcon = schemaNotebook?.status === 'success' ? '✓' : '✗';
        const statusText = schemaNotebook?.status === 'success' ? 'Analysis Complete' : 'Analysis Failed';

        const dataFilesHTML = this.createDataFilesSection(dataset.dataFiles || {});
        const notebooksHTML = this.createNotebooksSection(dataset.notebooks || {});
        const endpointHTML = this.createEndpointSection(dataset.name);

        return `
            <div class="dataset-card">
                <div class="dataset-name">
                    <span class="status-icon ${statusClass}">${statusIcon}</span>
                    ${dataset.name}
                </div>
                
                <div class="status-badge ${statusClass}">${statusText}</div>
                
                <div class="dataset-meta">
                    <div>Generated: ${this.formatDate(dataset.generated)}</div>
                </div>
                
                ${endpointHTML}
                ${notebooksHTML}
                ${dataFilesHTML}
                
                <div class="dataset-actions">
                    <button class="view-schema-btn action-btn" data-dataset="${dataset.name}">
                        Schema Diagram
                    </button>
                    <button class="view-coverage-btn action-btn" data-dataset="${dataset.name}">
                        Coverage Statistics
                    </button>
                </div>
            </div>
        `;
    }

    createEndpointSection(datasetName) {
        const source = this.sourcesData?.[datasetName];
        if (!source?.endpointUrl) {
            return '';
        }
        
        const graphLine = source.useGraph && source.graphUri 
            ? `<div>Graph: <code class="graph-uri">${source.graphUri}</code></div>` 
            : '';
        
        return `
            <div class="dataset-meta">
                <div>Endpoint: <a href="${source.endpointUrl}" class="endpoint-link" target="_blank">${source.endpointUrl}</a></div>
                ${graphLine}
            </div>
        `;
    }

    createNotebooksSection(notebooks) {
        const notebookTypes = [
            { key: 'schema', label: 'Schema Analysis' },
            { key: 'pydantic', label: 'Pydantic Models' },
            { key: 'namespace', label: 'Namespaces' }
        ];

        const notebookButtons = notebookTypes.map(({ key, label }) => {
            const notebook = notebooks[key];
            if (!notebook || notebook.status === 'missing') {
                return `<span class="notebook-btn disabled" title="Not available">${label}</span>`;
            }
            
            const statusClass = notebook.status === 'success' ? 'success' : 'error';
            const statusIcon = notebook.status === 'success' ? '✓' : '✗';
            
            return `<a href="${notebook.reportUrl}" class="notebook-btn ${statusClass}" target="_blank" title="${notebook.reportSize}">
                ${label} ${statusIcon}
            </a>`;
        }).join('');

        return `
            <div class="notebooks-section">
                <div class="section-label">Generated Jupyter Notebooks</div>
                <div class="notebook-buttons">
                    ${notebookButtons}
                </div>
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
            queries: 'SPARQL Queries',
            //subjectIndex: 'Subject Index',
            //objectIndex: 'Object Index'
        };

        const linksHTML = files.map(([type, url]) => {
            const name = fileTypeNames[type] || type.toUpperCase();
            // Convert relative path to GitHub blob URL
            const githubUrl = this.toGithubUrl(url);
            return `<a href="${githubUrl}" class="data-link" target="_blank">${name}</a>`;
        }).join('');

        return `
            <div class="data-files">
                <div class="section-label">Generated Data Files</div>
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
        const container = document.getElementById('datasets-grid');
        if (container) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>Error Loading Data</h3>
                    <p>${message}</p>
                </div>
            `;
        }
    }
    
    initializeVisualizations() {
        const datasets = this.data.datasets || [];
        
        // Initialize coverage visualization
        if (window.CoverageVisualization) {
            this.coverageViz = new window.CoverageVisualization(datasets);
        }
        
        // Initialize schema diagram
        if (window.SchemaDiagram) {
            this.schemaDiagram = new window.SchemaDiagram(datasets);
            // Add schema buttons to dataset cards
            if (window.addSchemaButtons) {
                setTimeout(() => window.addSchemaButtons(), 100);
            }
        }
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