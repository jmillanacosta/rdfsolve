/**
 * IRI Resolver Module
 * 
 * Resolves IRIs against available SPARQL endpoints to detect their rdf:type,
 * manages selections, and generates filtered schema subgraphs.
 */

class IriResolver {
    constructor(options = {}) {
        this.sourcesData = options.sourcesData || {};
        this.schemaData = options.schemaData || {};
        this.onResolve = options.onResolve || (() => {});
        this.onApply = options.onApply || (() => {});
        
        // State
        this.resolvedIris = new Map(); // IRI -> { datasets: Map<dataset, { types: Set, endpoint, graph }> }
        this.selectedIris = new Set();
        this.neighborDepth = 4;
        this.selectedDatasets = new Set(); // Which datasets to query (empty = all)
        
        // Query timeout
        this.queryTimeout = 15000;
        
        // Track failed endpoints to show status
        this.failedEndpoints = [];
        
        this._initUI();
    }

    _initUI() {
        this._populateDatasetSelector();
        this._bindEvents();
    }
    
    /**
     * Populate dataset selector with available endpoints
     */
    _populateDatasetSelector() {
        const selector = document.getElementById('iri-dataset-select');
        if (!selector) return;
        
        const datasets = Object.entries(this.sourcesData)
            .filter(([_, info]) => info.endpointUrl)
            .map(([name, _]) => name)
            .sort();
        
        selector.innerHTML = datasets.map(name => 
            `<option value="${name}">${name}</option>`
        ).join('');
        
        // Initialize Bootstrap Select if available
        if (typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(selector).selectpicker('refresh');
            $(selector).on('changed.bs.select', () => {
                this.selectedDatasets = new Set($(selector).val() || []);
            });
        }
    }

    _bindEvents() {
        // Input area
        const textarea = document.getElementById('iri-input-textarea');
        const resolveBtn = document.getElementById('iri-resolve-btn');
        const clearBtn = document.getElementById('iri-clear-btn');
        const applyBtn = document.getElementById('iri-apply-btn');
        const depthInput = document.getElementById('iri-neighbor-depth');
        const resultsList = document.getElementById('iri-results-list');
        const selectAllBtn = document.getElementById('iri-select-all-btn');
        const deselectAllBtn = document.getElementById('iri-deselect-all-btn');

        if (resolveBtn) {
            resolveBtn.addEventListener('click', () => this.resolveIris());
        }
        
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clear());
        }
        
        if (applyBtn) {
            applyBtn.addEventListener('click', () => this.applySelection());
        }
        
        if (depthInput) {
            depthInput.addEventListener('change', (e) => {
                const val = e.target.value.trim();
                this.neighborDepth = val === '' || val === 'all' ? Infinity : parseInt(val, 10) || 4;
            });
        }
        
        if (selectAllBtn) {
            selectAllBtn.addEventListener('click', () => this._selectAll());
        }
        
        if (deselectAllBtn) {
            deselectAllBtn.addEventListener('click', () => this._deselectAll());
        }
        
        // Multi-select behavior via click/ctrl/shift on results
        if (resultsList) {
            resultsList.addEventListener('click', (e) => this._handleResultClick(e));
        }
    }

    /**
     * Parse IRIs from textarea (one per line)
     */
    _parseInput() {
        const textarea = document.getElementById('iri-input-textarea');
        if (!textarea) return [];
        
        return textarea.value
            .split('\n')
            .map(line => line.trim())
            .filter(line => line && (line.startsWith('http://') || line.startsWith('https://')));
    }

    /**
     * Resolve all IRIs against available endpoints
     * Accumulates results - doesn't clear previous resolutions
     */
    async resolveIris() {
        const iris = this._parseInput();
        if (iris.length === 0) {
            this._showStatus('No valid IRIs provided', 'warning');
            return;
        }
        
        this._showStatus(`Resolving ${iris.length} IRI(s)...`, 'info');
        this._setLoading(true);
        
        // Get endpoints from sourcesData
        const endpoints = this._getEndpoints();
        if (endpoints.length === 0) {
            this._showStatus('No endpoints available', 'error');
            this._setLoading(false);
            return;
        }
        
        // DON'T clear previous results - accumulate them
        // Only clear failures tracking
        this.failedEndpoints = [];
        
        // Track which IRIs are new in this batch
        const newIris = iris.filter(iri => !this.resolvedIris.has(iri));
        const existingIris = iris.filter(iri => this.resolvedIris.has(iri));
        
        // Query each endpoint (parallel with concurrency limit)
        let successCount = 0;
        let corsCount = 0;
        let timeoutCount = 0;
        
        for (const { dataset, endpoint, graph } of endpoints) {
            this._showStatus(`Querying ${dataset}... (${successCount}/${endpoints.length})`, 'info');
            try {
                const results = await this._queryEndpoint(endpoint, graph, iris);
                this._mergeResults(dataset, endpoint, graph, results);
                successCount++;
            } catch (err) {
                const reason = err.message || 'Unknown error';
                this.failedEndpoints.push({ dataset, reason });
                if (reason === 'CORS blocked') corsCount++;
                if (reason === 'Timeout') timeoutCount++;
                console.warn(`Failed to query ${dataset}:`, reason);
            }
        }
        
        this._setLoading(false);
        
        // Count how many of the queried IRIs were found
        const foundCount = iris.filter(iri => this.resolvedIris.has(iri)).length;
        
        // Build status message
        let statusMsg = '';
        if (foundCount === 0) {
            statusMsg = `No types found for ${iris.length} IRI(s) in ${successCount}/${endpoints.length} endpoint(s)`;
        } else {
            statusMsg = `Resolved ${foundCount}/${iris.length} IRI(s) (${this.resolvedIris.size} total)`;
        }
        
        if (corsCount > 0 || timeoutCount > 0) {
            const issues = [];
            if (corsCount > 0) issues.push(`${corsCount} CORS`);
            if (timeoutCount > 0) issues.push(`${timeoutCount} timeout`);
            statusMsg += ` (${issues.join(', ')})`;
        }
        
        this._showStatus(statusMsg, foundCount > 0 ? 'success' : 'warning');
        
        this._renderResults();
        this.onResolve(this.resolvedIris);
    }

    /**
     * Get unique endpoints from sourcesData (filtered by selected datasets)
     */
    _getEndpoints() {
        const seen = new Set();
        const endpoints = [];
        
        for (const [dataset, info] of Object.entries(this.sourcesData)) {
            if (!info.endpointUrl) continue;
            
            // Filter by selected datasets if any are selected
            if (this.selectedDatasets.size > 0 && !this.selectedDatasets.has(dataset)) {
                continue;
            }
            
            const key = `${info.endpointUrl}|${info.graphUri || ''}`;
            if (seen.has(key)) continue;
            seen.add(key);
            
            endpoints.push({
                dataset,
                endpoint: info.endpointUrl,
                graph: info.useGraph ? info.graphUri : null
            });
        }
        
        return endpoints;
    }

    /**
     * Query an endpoint for types of given IRIs
     * Handles CORS errors gracefully
     */
    async _queryEndpoint(endpoint, graph, iris) {
        // Build VALUES clause
        const values = iris.map(iri => `<${iri}>`).join(' ');
        
        // Build query - get rdf:type for each IRI
        let query;
        if (graph) {
            query = `
                SELECT ?iri ?type WHERE {
                    VALUES ?iri { ${values} }
                    GRAPH <${graph}> {
                        ?iri a ?type .
                    }
                }
            `;
        } else {
            query = `
                SELECT ?iri ?type WHERE {
                    VALUES ?iri { ${values} }
                    ?iri a ?type .
                }
            `;
        }
        
        const url = `${endpoint}?query=${encodeURIComponent(query)}&format=json`;
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.queryTimeout);
        
        try {
            const response = await fetch(url, {
                headers: { 'Accept': 'application/sparql-results+json' },
                signal: controller.signal,
                mode: 'cors'
            });
            clearTimeout(timeoutId);
            
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            return data.results?.bindings || [];
        } catch (err) {
            clearTimeout(timeoutId);
            
            // Classify error for better reporting
            if (err.name === 'AbortError') {
                throw new Error('Timeout');
            } else if (err.name === 'TypeError' && err.message.includes('Failed to fetch')) {
                throw new Error('CORS blocked');
            }
            throw err;
        }
    }

    /**
     * Merge query results into resolvedIris
     */
    _mergeResults(dataset, endpoint, graph, bindings) {
        for (const binding of bindings) {
            const iri = binding.iri?.value;
            const type = binding.type?.value;
            if (!iri || !type) continue;
            
            if (!this.resolvedIris.has(iri)) {
                this.resolvedIris.set(iri, { datasets: new Map() });
            }
            
            const iriData = this.resolvedIris.get(iri);
            if (!iriData.datasets.has(dataset)) {
                iriData.datasets.set(dataset, { types: new Set(), endpoint, graph });
            }
            
            iriData.datasets.get(dataset).types.add(type);
        }
    }

    /**
     * Render resolved IRIs as selectable list
     */
    _renderResults() {
        const container = document.getElementById('iri-results-list');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (this.resolvedIris.size === 0) {
            container.innerHTML = '<div class="text-muted small p-2">No results</div>';
            return;
        }
        
        for (const [iri, data] of this.resolvedIris) {
            const item = document.createElement('div');
            item.className = 'iri-result-item';
            item.dataset.iri = iri;
            
            // Shorten IRI for display
            const shortIri = this._shortenIri(iri);
            
            // Collect all types and datasets
            const allTypes = new Set();
            const datasets = [];
            for (const [dsName, dsData] of data.datasets) {
                datasets.push(dsName);
                dsData.types.forEach(t => allTypes.add(t));
            }
            
            const typesStr = [...allTypes].map(t => this._shortenIri(t)).join(', ');
            const datasetsStr = datasets.join(', ');
            
            item.innerHTML = `
                <div class="iri-main">
                    <span class="iri-uri" title="${iri}">${shortIri}</span>
                </div>
                <div class="iri-meta">
                    <span class="iri-types" title="Types: ${[...allTypes].join(', ')}">${typesStr}</span>
                    <span class="iri-datasets text-muted" title="Found in: ${datasetsStr}">[${datasets.length} dataset(s)]</span>
                </div>
            `;
            
            container.appendChild(item);
        }
        
        // Show apply section
        const applySection = document.getElementById('iri-apply-section');
        if (applySection) applySection.style.display = 'block';
    }

    /**
     * Shorten IRI using common prefixes
     */
    _shortenIri(iri) {
        const prefixes = {
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#': 'rdf:',
            'http://www.w3.org/2000/01/rdf-schema#': 'rdfs:',
            'http://www.w3.org/2002/07/owl#': 'owl:',
            'http://purl.org/dc/elements/1.1/': 'dc:',
            'http://purl.org/dc/terms/': 'dcterms:',
            'http://xmlns.com/foaf/0.1/': 'foaf:',
            'http://www.w3.org/2004/02/skos/core#': 'skos:',
            'http://schema.org/': 'schema:',
            'http://purl.obolibrary.org/obo/': 'obo:',
            'http://identifiers.org/': 'identifiers:',
            'https://identifiers.org/': 'identifiers:',
        };
        
        for (const [uri, prefix] of Object.entries(prefixes)) {
            if (iri.startsWith(uri)) {
                return prefix + iri.substring(uri.length);
            }
        }
        
        // Get local name
        const hashIdx = iri.lastIndexOf('#');
        const slashIdx = iri.lastIndexOf('/');
        const splitIdx = Math.max(hashIdx, slashIdx);
        
        if (splitIdx > 0 && splitIdx < iri.length - 1) {
            return '...' + iri.substring(splitIdx);
        }
        
        return iri.length > 50 ? iri.substring(0, 47) + '...' : iri;
    }

    /**
     * Handle click on result item (with ctrl/shift for multi-select)
     */
    _handleResultClick(e) {
        const item = e.target.closest('.iri-result-item');
        if (!item) return;
        
        const iri = item.dataset.iri;
        if (!iri) return;
        
        if (e.ctrlKey || e.metaKey) {
            // Toggle selection
            if (this.selectedIris.has(iri)) {
                this.selectedIris.delete(iri);
                item.classList.remove('selected');
            } else {
                this.selectedIris.add(iri);
                item.classList.add('selected');
            }
        } else if (e.shiftKey && this._lastClickedIri) {
            // Range selection
            const items = [...document.querySelectorAll('.iri-result-item')];
            const startIdx = items.findIndex(i => i.dataset.iri === this._lastClickedIri);
            const endIdx = items.findIndex(i => i.dataset.iri === iri);
            
            if (startIdx >= 0 && endIdx >= 0) {
                const [from, to] = startIdx < endIdx ? [startIdx, endIdx] : [endIdx, startIdx];
                for (let i = from; i <= to; i++) {
                    const itemIri = items[i].dataset.iri;
                    this.selectedIris.add(itemIri);
                    items[i].classList.add('selected');
                }
            }
        } else {
            // Single selection (clear others)
            this.selectedIris.clear();
            document.querySelectorAll('.iri-result-item.selected').forEach(el => el.classList.remove('selected'));
            this.selectedIris.add(iri);
            item.classList.add('selected');
        }
        
        this._lastClickedIri = iri;
        this._updateSelectionCount();
    }

    _selectAll() {
        for (const iri of this.resolvedIris.keys()) {
            this.selectedIris.add(iri);
        }
        document.querySelectorAll('.iri-result-item').forEach(el => el.classList.add('selected'));
        this._updateSelectionCount();
    }

    _deselectAll() {
        this.selectedIris.clear();
        document.querySelectorAll('.iri-result-item.selected').forEach(el => el.classList.remove('selected'));
        this._updateSelectionCount();
    }

    _updateSelectionCount() {
        const countEl = document.getElementById('iri-selection-count');
        if (countEl) {
            countEl.textContent = `${this.selectedIris.size} selected`;
        }
    }

    /**
     * Apply selection - generate filtered diagram
     */
    applySelection() {
        if (this.selectedIris.size === 0) {
            this._showStatus('No IRIs selected', 'warning');
            return;
        }
        
        // Collect types and datasets for selected IRIs
        const types = new Set();
        const datasets = new Set();
        const iriTypeMap = new Map(); // IRI -> Set<type>
        
        for (const iri of this.selectedIris) {
            const data = this.resolvedIris.get(iri);
            if (!data) continue;
            
            const iriTypes = new Set();
            for (const [dsName, dsData] of data.datasets) {
                datasets.add(dsName);
                dsData.types.forEach(t => {
                    types.add(t);
                    iriTypes.add(t);
                });
            }
            iriTypeMap.set(iri, iriTypes);
        }
        
        this.onApply({
            iris: [...this.selectedIris],
            types: [...types],
            datasets: [...datasets],
            iriTypeMap,
            neighborDepth: this.neighborDepth
        });
    }

    /**
     * Get VALUES clause for SPARQL query
     */
    getValuesClause(variableName = 'entity') {
        if (this.selectedIris.size === 0) return '';
        
        const values = [...this.selectedIris].map(iri => `<${iri}>`).join(' ');
        return `VALUES ?${variableName} { ${values} }`;
    }

    /**
     * Get VALUES clause grouped by type
     */
    getValuesClauseByType(nodeVarMap) {
        // nodeVarMap: Map<typeIri, variableName>
        const clauses = [];
        
        for (const [iri, data] of this.resolvedIris) {
            if (!this.selectedIris.has(iri)) continue;
            
            for (const [dsName, dsData] of data.datasets) {
                for (const typeIri of dsData.types) {
                    const varName = nodeVarMap.get(typeIri);
                    if (varName) {
                        clauses.push({ varName, iri });
                    }
                }
            }
        }
        
        // Group by variable
        const grouped = new Map();
        for (const { varName, iri } of clauses) {
            if (!grouped.has(varName)) grouped.set(varName, new Set());
            grouped.get(varName).add(iri);
        }
        
        // Build VALUES clauses
        const parts = [];
        for (const [varName, iris] of grouped) {
            const values = [...iris].map(i => `<${i}>`).join(' ');
            parts.push(`VALUES ?${varName} { ${values} }`);
        }
        
        return parts.join('\n');
    }

    /**
     * Clear all state
     */
    clear() {
        this.resolvedIris.clear();
        this.selectedIris.clear();
        
        const textarea = document.getElementById('iri-input-textarea');
        if (textarea) textarea.value = '';
        
        const container = document.getElementById('iri-results-list');
        if (container) container.innerHTML = '';
        
        const applySection = document.getElementById('iri-apply-section');
        if (applySection) applySection.style.display = 'none';
        
        const statusEl = document.getElementById('iri-status');
        if (statusEl) statusEl.textContent = '';
        
        this._updateSelectionCount();
    }

    _showStatus(message, type = 'info') {
        const statusEl = document.getElementById('iri-status');
        if (!statusEl) return;
        
        statusEl.textContent = message;
        statusEl.className = `iri-status text-${type === 'error' ? 'danger' : type === 'success' ? 'success' : type === 'warning' ? 'warning' : 'muted'}`;
    }

    _setLoading(loading) {
        const btn = document.getElementById('iri-resolve-btn');
        if (btn) {
            btn.disabled = loading;
            btn.innerHTML = loading 
                ? '<span class="spinner-border spinner-border-sm me-1"></span>Resolving...'
                : 'Resolve IRIs';
        }
    }
}

// Export for use in other modules
window.IriResolver = IriResolver;
