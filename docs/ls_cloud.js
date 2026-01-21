/**
 * RDFSolve Linked Schema Cloud Visualization
 * 
 * Class diagram layout with:
 * - D3 tree layout for hierarchical positioning
 * - Orthogonal edges (right angles only)
 * - OWL/RDF(S)/FOAF node exclusion
 * - Path finding between nodes
 * - Adjustable X/Y spacing
 */

class lsCloudVisualization {
    constructor(datasets) {
        this.datasets = datasets;
        this.githubRawBase = 'https://raw.githubusercontent.com/jmillanacosta/rdfsolve/main/docs/';
        
        this.selectedDatasets = new Set();
        this.mergedData = [];
        this.datasetColorMap = new Map();
        this.allDatasetNames = [];
    this.legendSubset = new Set();
        
        this.svg = null;
        this.zoom = null;
        this.graphData = { nodes: [], links: [] };
        this.fullGraphData = { nodes: [], links: [] };
        
    this.pathFindingMode = false;
    this.selectedNodes = [];
    this.highlightedPath = [];
    this.allPaths = []; // Store all found paths for accumulation
    this.intersectionMode = false;
    
    // Path color palette (light, visible colors that work well on white background)
    this.pathColors = [
        '#4285f4', // Blue
        '#ea4335', // Red
        '#34a853', // Green
        '#fbbc04', // Yellow/Gold
        '#9c27b0', // Purple
        '#00acc1', // Cyan
        '#ff7043', // Deep Orange
        '#7cb342', // Light Green
        '#5c6bc0', // Indigo
        '#f06292', // Pink
    ];
        
        // Spacing multipliers (controlled by buttons) - start with high Y for readability
        this.xSpacing = 1.0;
        this.ySpacing = 1.0;
        
        // Edge style: false = orthogonal, true = curved
        this.curvedEdges = false;
        
        // Prefixes to exclude
        this.excludedPrefixes = [
            'rdf',
            'rdfs',
            'owl',
            'sh',
            'sparqlserv',
            'foaf'
        ];
        
        // Common terms to exclude by local name
        this.excludedLocalNames = [
            'Resource', 'Class', 'Literal', 'Property', 'Thing', 
            'NamedIndividual', 'Ontology', 'ObjectProperty', 'DatatypeProperty',
            'AnnotationProperty', 'TransitiveProperty', 'SymmetricProperty',
            'FunctionalProperty', 'InverseFunctionalProperty', 'Restriction',
            'AllDifferent', 'AllDisjointClasses', 'AllDisjointProperties'
        ];

        this.init();
    }

    // Dim nodes and links not present in the strict intersection across selected datasets
    applyIntersectionDimming() {
        if (!this.intersectionMode) {
            d3.selectAll('.node-group').classed('dimmed', false);
            d3.selectAll('.link-group').classed('dimmed', false);
            return;
        }

        const selected = Array.from(this.selectedDatasets || []);
        if (selected.length < 2) return;

        // Strict intersection: nodes/links present in ALL selected datasets
        const intersectionNodeIds = new Set(
            (this.graphData.nodes || [])
                .filter(n => selected.every(s => (n.datasets || []).includes(s)))
                .map(n => n.id)
        );
        const intersectionLinks = new Set(
            (this.graphData.links || [])
                .filter(l => selected.every(s => (l.datasets || []).includes(s)))
                .map(l => `${l.source}|||${l.target}`)
        );

        d3.selectAll('.node-group').classed('dimmed', d => !intersectionNodeIds.has(d.id));
        d3.selectAll('.link-group').classed('dimmed', d => {
            const key = `${d.source}|||${d.target}`;
            return !intersectionLinks.has(key);
        });
    }

    // Get color for path at given index
    getPathColor(index) {
        return this.pathColors[index % this.pathColors.length];
    }

    init() {
        this.allDatasetNames = this.datasets
            .filter(d => d.dataFiles?.coverage && d.notebooks?.schema?.status === 'success')
            .map(d => d.name)
            .sort();
        
        this.setupDatasetSelector();
        this.setupEventListeners();
        this.setupSpacingControls();
        this.setupPathAutocomplete();
    }

    // ========== Dataset Selector ==========
    
    setupDatasetSelector() {
        const selectEl = document.getElementById('ls-dataset-select');
        if (!selectEl) return;

        selectEl.innerHTML = this.allDatasetNames.map(name => 
            `<option value="${name}">${name}</option>`
        ).join('');

        if (typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(selectEl).selectpicker('refresh');
            $(selectEl).on('changed.bs.select', () => {
                this.selectedDatasets = new Set($(selectEl).val() || []);
                this.renderSelectedPreview();
            });
        }
    }

    renderSelectedPreview() {
        const container = document.getElementById('ls-selected-preview');
        if (!container) return;

        if (this.selectedDatasets.size === 0) {
            container.innerHTML = '<span style="font-size: 12px; color: #8b949e;">No datasets selected</span>';
            return;
        }

        const chips = Array.from(this.selectedDatasets).slice(0, 10).map(name => {
            const color = this.getDatasetColor(name);
            return `<span style="display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; background: ${color.replace(')', ', 0.15)').replace('hsl', 'hsla')}; border: 1px solid ${color.replace(')', ', 0.5)').replace('hsl', 'hsla')}; border-radius: 14px; font-size: 11px; margin-right: 4px; margin-bottom: 4px;">
                <span style="width: 8px; height: 8px; background: ${color}; border-radius: 50%;"></span>
                ${name}
            </span>`;
        }).join('');

        const moreCount = this.selectedDatasets.size - 10;
        container.innerHTML = chips + (moreCount > 0 ? `<span style="font-size: 11px; color: #656d76;">+${moreCount} more</span>` : '');
    }

    // ========== Colors ==========
    
    getDatasetColor(name) {
        // Use a deterministic hash-based hue so colors are well-distributed
        if (!this.datasetColorMap.has(name)) {
            const hash = Array.from(name).reduce((h, c) => ((h << 5) - h) + c.charCodeAt(0), 0) >>> 0;
            // multiplicative hashing to spread values, then modulo 360
            const hue = Math.floor(((hash * 2654435761) % 360));
            const color = `hsl(${hue}, 65%, 50%)`;
            this.datasetColorMap.set(name, color);
        }
        return this.datasetColorMap.get(name);
    }

    // ========== Event Listeners ==========
    
    setupEventListeners() {
        document.getElementById('render-ls-cloud')?.addEventListener('click', () => this.renderCloud());
        
        // Node filter with Bootstrap Select (focus)
        const nodeFilter = document.getElementById('ls-node-filter');
        if (nodeFilter && typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(nodeFilter).on('changed.bs.select', (e) => {
                this.filterByNode($(nodeFilter).val() || '');
            });
        } else if (nodeFilter) {
            nodeFilter.addEventListener('change', e => this.filterByNode(e.target.value));
        }

        // Node highlight filter (multi-select, highlights and zooms)
        const nodeHighlightFilter = document.getElementById('ls-node-highlight-filter');
        if (nodeHighlightFilter && typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(nodeHighlightFilter).on('changed.bs.select', () => {
                const selected = $(nodeHighlightFilter).val() || [];
                this.applyNodeHighlightFilter(selected);
            });
        } else if (nodeHighlightFilter) {
            nodeHighlightFilter.addEventListener('change', e => {
                const selected = Array.from(e.target.selectedOptions).map(o => o.value);
                this.applyNodeHighlightFilter(selected);
            });
        }
        
        document.getElementById('ls-neighbor-depth')?.addEventListener('change', () => {
            const nodeFilter = document.getElementById('ls-node-filter');
            const node = (typeof $ !== 'undefined' && $.fn.selectpicker) 
                ? ($(nodeFilter).val() || '') 
                : nodeFilter?.value;
            if (node) this.filterByNode(node);
        });
        document.getElementById('ls-find-path-btn')?.addEventListener('click', () => this.togglePathFindingMode());
        
        // Add path button (for accumulating multiple paths)
        document.getElementById('ls-add-path-btn')?.addEventListener('click', () => this.startAddPath());
        
        // Clear paths button
        document.getElementById('ls-clear-paths-btn')?.addEventListener('click', () => this.clearAllPaths());
        
        // Fullscreen toggle (both buttons)
        document.getElementById('ls-fullscreen-btn')?.addEventListener('click', () => this.toggleFullscreen());
        document.getElementById('ls-exit-fullscreen-btn')?.addEventListener('click', () => this.toggleFullscreen());
        
        // Collapsible section toggle (use ID selector)
        document.getElementById('ls-cloud-header')?.addEventListener('click', (e) => {
            // Don't collapse if clicking on buttons in the header
            if (e.target.closest('button')) return;
            this.toggleCollapse();
        });
        
        // Path sidebar toggle
        document.getElementById('ls-path-toggle-btn')?.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent header click from firing
            this.togglePathSidebar();
        });
        document.getElementById('ls-path-sidebar-close')?.addEventListener('click', () => this.togglePathSidebar(false));
        
        // Edge style toggle (orthogonal vs curved)
        document.getElementById('ls-edge-style-btn')?.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleEdgeStyle();
        });
        
        // Rebuild diagram when exclude-OWL checkbox changes so exclusion affects construction
        const excludeChk = document.getElementById('ls-exclude-owl');
        if (excludeChk) {
            excludeChk.addEventListener('change', () => {
                // If we have merged data already loaded, rebuild graph data and re-render
                if (!this.mergedData || this.mergedData.length === 0) return;
                this.fullGraphData = this.buildGraphData(150);

                // If selection splits, render largest connected component if available
                const components = this.getDatasetComponents(this.fullGraphData);
                if (components.length > 1) {
                    components.sort((a, b) => b.size - a.size);
                    const largest = components[0];
                    if (largest.size <= 1) {
                        this.showMessage('Selected datasets are not connected via any shared node or relationship. Please select datasets that share nodes/edges to render a combined diagram.');
                        this.intersectionMode = false;
                        return;
                    }
                    const allowed = largest;
                    const nodes = this.fullGraphData.nodes.filter(n => (n.datasets || []).some(d => allowed.has(d)));
                    const links = this.fullGraphData.links.filter(l => (l.datasets || []).some(d => allowed.has(d)));
                    this.graphData = { nodes, links };
                    const names = Array.from(largest).join(', ');
                    this.showMessage(`Selection splits into disconnected groups — rendering largest connected group: ${names}`);
                } else {
                    this.graphData = { nodes: this.fullGraphData.nodes || [], links: this.fullGraphData.links || [] };
                }

                this.populateNodeFilter();
                this.renderInfo();
                this.renderDiagram();
                // clear any intersection mode when rebuilding
                this.intersectionMode = false;
            });
        }

        // Intersection dimming button
        const intersectionBtn = document.getElementById('ls-show-intersection');
        if (intersectionBtn) {
            intersectionBtn.addEventListener('click', () => {
                const status = document.getElementById('ls-path-status');
                const selectedCount = this.selectedDatasets.size;
                if (selectedCount < 2) {
                    if (status) {
                        status.textContent = 'Select 2 or more datasets to compute intersection';
                        setTimeout(() => { status.textContent = ''; }, 3500);
                    } else {
                        alert('Select 2 or more datasets to compute intersection');
                    }
                    return;
                }

                this.intersectionMode = !this.intersectionMode;
                intersectionBtn.textContent = this.intersectionMode ? 'Hide intersection' : 'Show intersection';

                // Apply dimming immediately if diagram exists
                if (this.svg) {
                    this.applyIntersectionDimming();
                }
            });
        }
        // No union/intersection toggle — diagram shows union of nodes/links by default.
    }

    setupSpacingControls() {
        // Use shared spacing control utility from D3DiagramUtils
        D3DiagramUtils.setupSpacingControls({
            xPlusId: 'ls-x-spacing-plus',
            xMinusId: 'ls-x-spacing-minus',
            xValId: 'ls-x-spacing-val',
            yPlusId: 'ls-y-spacing-plus',
            yMinusId: 'ls-y-spacing-minus',
            yValId: 'ls-y-spacing-val',
            initialX: 6.0,
            initialY: 3.0,
            step: 0.2,
            onChange: (xSpacing, ySpacing) => {
                this.xSpacing = xSpacing;
                this.ySpacing = ySpacing;
                if (this.graphData.nodes.length > 0) {
                    this.renderDiagram();
                }
            }
        });

        // Copy path button
        const copyBtn = document.getElementById('ls-copy-path-btn');
        const txtarea = document.getElementById('ls-path-query-txtarea');
        if (copyBtn && txtarea) {
            copyBtn.addEventListener('click', () => {
                navigator.clipboard.writeText(txtarea.value).then(() => {
                    copyBtn.textContent = 'Copied!';
                    setTimeout(() => { copyBtn.textContent = 'Copy'; }, 1500);
                }).catch(() => {
                    txtarea.select();
                    document.execCommand('copy');
                    copyBtn.textContent = 'Copied!';
                    setTimeout(() => { copyBtn.textContent = 'Copy'; }, 1500);
                });
            });
        }

        // SPARQL Modal Setup
        this.setupSparqlModal();
    }

    // ========== SPARQL Modal ==========
    
    setupSparqlModal() {
        const modal = document.getElementById('sparql-modal');
        const closeBtn = document.getElementById('sparql-modal-close');
        const generateBtn = document.getElementById('ls-generate-sparql-btn');
        const copyBtn = document.getElementById('sparql-copy-btn');
        const sendBtn = document.getElementById('sparql-send-btn');
        const queryTextarea = document.getElementById('sparql-query-textarea');
        const requireTypeCheckbox = document.getElementById('sparql-require-type');

        if (!modal) return;

        // Open modal
        if (generateBtn) {
            generateBtn.addEventListener('click', () => {
                this.openSparqlModal();
            });
        }

        // Close modal
        if (closeBtn) {
            closeBtn.addEventListener('click', () => {
                modal.style.display = 'none';
            });
        }

        // Close on backdrop click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        });

        // Close on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && modal.style.display === 'flex') {
                modal.style.display = 'none';
            }
        });

        // Copy query
        if (copyBtn) {
            copyBtn.addEventListener('click', () => {
                navigator.clipboard.writeText(queryTextarea.value).then(() => {
                    copyBtn.textContent = 'Copied!';
                    setTimeout(() => { copyBtn.textContent = 'Copy Query'; }, 1500);
                });
            });
        }

        // Send query - open primary endpoint with query
        if (sendBtn) {
            sendBtn.addEventListener('click', () => {
                if (this.primaryEndpoint) {
                    const query = queryTextarea?.value || '';
                    const encodedQuery = encodeURIComponent(query);
                    window.open(`${this.primaryEndpoint}?query=${encodedQuery}`, '_blank');
                }
            });
        }

        // Regenerate query when require-type checkbox changes
        if (requireTypeCheckbox) {
            requireTypeCheckbox.addEventListener('change', () => {
                this.regenerateSparqlQuery();
            });
        }
    }

    regenerateSparqlQuery() {
        const queryTextarea = document.getElementById('sparql-query-textarea');
        const requireTypeCheckbox = document.getElementById('sparql-require-type');
        if (!queryTextarea) return;
        
        const requireType = requireTypeCheckbox?.checked ?? true;
        const query = this.generateSparqlFromPaths(requireType);
        queryTextarea.value = query;
    }

    openSparqlModal() {
        const modal = document.getElementById('sparql-modal');
        const queryTextarea = document.getElementById('sparql-query-textarea');
        const primaryEndpointEl = document.getElementById('sparql-primary-endpoint');
        const servicesSection = document.getElementById('sparql-services-section');
        const servicesList = document.getElementById('sparql-services-list');
        const graphsSection = document.getElementById('sparql-graphs-section');
        const graphsList = document.getElementById('sparql-graphs-list');
        const datasetsList = document.getElementById('sparql-datasets-list');
        const infoEl = document.getElementById('sparql-modal-info');
        const sendBtn = document.getElementById('sparql-send-btn');

        if (!modal) return;

        // Get sources data and selected datasets
        const sourcesData = window.rdfsolve?.sourcesData || {};
        const selectedDatasets = Array.from(this.selectedDatasets || []);
        
        // Analyze selected datasets for endpoint/graph configuration
        const datasetSources = selectedDatasets
            .map(name => ({ name, source: sourcesData[name] }))
            .filter(d => d.source);
        
        // Group datasets by endpoint
        const endpointMap = new Map(); // endpoint -> { datasets: [], graphs: Set }
        datasetSources.forEach(({ name, source }) => {
            const endpoint = source.endpointUrl || '';
            if (!endpointMap.has(endpoint)) {
                endpointMap.set(endpoint, { datasets: [], graphs: new Set() });
            }
            endpointMap.get(endpoint).datasets.push(name);
            if (source.useGraph && source.graphUri) {
                endpointMap.get(endpoint).graphs.add(source.graphUri);
            }
        });

        // Get ordered list of endpoints (first selected dataset's endpoint is primary)
        const endpoints = [...endpointMap.keys()].filter(Boolean);
        const primaryEndpoint = endpoints[0] || '';
        const serviceEndpoints = endpoints.slice(1); // All other endpoints are services
        
        // Store for query generation and send button
        this.primaryEndpoint = primaryEndpoint;
        this.endpointMap = endpointMap;
        this.currentDatasetSources = datasetSources;

        // Populate info panel - Primary Endpoint
        if (primaryEndpointEl) {
            primaryEndpointEl.textContent = primaryEndpoint || '(none)';
            primaryEndpointEl.title = primaryEndpoint;
        }

        // Populate info panel - SERVICE endpoints
        if (servicesSection && servicesList) {
            servicesList.innerHTML = '';
            if (serviceEndpoints.length > 0) {
                servicesSection.style.display = 'block';
                serviceEndpoints.forEach(ep => {
                    const group = endpointMap.get(ep);
                    const div = document.createElement('div');
                    div.style.cssText = 'color: var(--text-secondary); word-break: break-all; font-family: "SF Mono", Consolas, monospace; padding: 6px 8px; background: var(--card-background); border-radius: 4px; border: 1px solid var(--border-color);';
                    div.textContent = ep;
                    div.title = `Datasets: ${group.datasets.join(', ')}`;
                    servicesList.appendChild(div);
                });
            } else {
                servicesSection.style.display = 'none';
            }
        }

        // Populate info panel - Graphs
        const allGraphs = new Map(); // graph -> endpoint
        endpointMap.forEach((group, endpoint) => {
            group.graphs.forEach(g => allGraphs.set(g, endpoint));
        });
        
        if (graphsSection && graphsList) {
            graphsList.innerHTML = '';
            if (allGraphs.size > 0) {
                graphsSection.style.display = 'block';
                allGraphs.forEach((endpoint, graph) => {
                    const div = document.createElement('div');
                    div.style.cssText = 'color: var(--text-secondary); word-break: break-all; font-family: "SF Mono", Consolas, monospace; padding: 6px 8px; background: var(--card-background); border-radius: 4px; border: 1px solid var(--border-color); font-size: 10px;';
                    div.textContent = graph;
                    div.title = `Endpoint: ${endpoint}`;
                    graphsList.appendChild(div);
                });
            } else {
                graphsSection.style.display = 'none';
            }
        }

        // Populate info panel - Datasets
        if (datasetsList) {
            datasetsList.innerHTML = '';
            selectedDatasets.forEach(name => {
                const span = document.createElement('span');
                span.style.cssText = 'padding: 2px 6px; background: var(--primary-color); color: #fff; border-radius: 3px; font-size: 10px;';
                span.textContent = name;
                const source = sourcesData[name];
                if (source) {
                    span.title = `Endpoint: ${source.endpointUrl || '?'}\nGraph: ${source.graphUri || '—'}`;
                }
                datasetsList.appendChild(span);
            });
        }

        // Generate query using regenerate method (respects checkbox state)
        this.regenerateSparqlQuery();

        // Update info text
        if (infoEl) {
            const parts = [`${this.allPaths.length} path(s)`];
            if (serviceEndpoints.length > 0) {
                parts.push(`${serviceEndpoints.length} federated`);
            }
            if (allGraphs.size > 0) {
                parts.push(`${allGraphs.size} graph(s)`);
            }
            infoEl.textContent = parts.join(' · ');
        }

        // Enable/disable send button
        if (sendBtn) {
            sendBtn.disabled = !primaryEndpoint;
            sendBtn.style.opacity = primaryEndpoint ? '1' : '0.5';
        }

        // Show modal
        modal.style.display = 'flex';
    }

    /**
     * Generate SPARQL query from accumulated paths
     * Automatically uses endpoint/graph info from sources.csv for selected datasets
     * @param {boolean} requireType - Whether to include rdf:type assertions (default true)
     * @returns {string} SPARQL query
     */
    generateSparqlFromPaths(requireType = true) {
        if (this.allPaths.length === 0) {
            return '# No paths selected';
        }

        const sourcesData = window.rdfsolve?.sourcesData || {};
        const primaryEndpoint = this.primaryEndpoint || '';

        // Helper to get endpoint/graph info for a dataset
        const getDatasetInfo = (datasetName) => {
            const src = sourcesData[datasetName];
            return src ? {
                endpoint: src.endpointUrl || '',
                graph: (src.useGraph && src.graphUri) ? src.graphUri : null
            } : { endpoint: '', graph: null };
        };

        // Collect all unique types (node labels) to create variables
        const varCounter = {};
        
        const getVarName = (typeLabel) => {
            let baseName = typeLabel.replace(/^[^:]+:/, '');
            baseName = baseName.replace(/[^a-zA-Z0-9_]/g, '');
            if (!baseName) baseName = 'node';
            baseName = baseName.charAt(0).toLowerCase() + baseName.slice(1);
            return baseName;
        };

        // Build patterns grouped by endpoint -> graph -> patterns
        // Structure: Map<endpoint, Map<graph|'', patterns[]>>
        const endpointPatterns = new Map();
        let selectVars = new Set();

        const addPattern = (endpoint, graph, pattern) => {
            if (!endpointPatterns.has(endpoint)) {
                endpointPatterns.set(endpoint, new Map());
            }
            const graphKey = graph || '';
            if (!endpointPatterns.get(endpoint).has(graphKey)) {
                endpointPatterns.get(endpoint).set(graphKey, []);
            }
            endpointPatterns.get(endpoint).get(graphKey).push(pattern);
        };

        this.allPaths.forEach((pathData, pathIdx) => {
            const path = Array.isArray(pathData) ? pathData : pathData.path;
            const storedEdges = pathData.edges || [];
            if (!path || path.length < 2) return;

            // Map each node in this path to a variable
            const nodeVarMap = new Map();
            
            path.forEach((nodeId) => {
                if (nodeVarMap.has(nodeId)) return;
                
                const node = this.graphData.nodes.find(n => n.id === nodeId);
                const typeLabel = node ? node.label : nodeId;
                
                let varName = getVarName(typeLabel);
                if (!varCounter[varName]) varCounter[varName] = 0;
                const varWithSuffix = varCounter[varName] === 0 ? varName : `${varName}${varCounter[varName]}`;
                varCounter[varName]++;
                
                nodeVarMap.set(nodeId, varWithSuffix);
                selectVars.add(varWithSuffix);
            });

            // Process each edge
            for (let i = 0; i < path.length - 1; i++) {
                const fromId = path[i];
                const toId = path[i + 1];
                const fromVar = nodeVarMap.get(fromId);
                const toVar = nodeVarMap.get(toId);
                
                // Get the edge - prefer stored edge which has correct direction
                const edge = storedEdges[i] || this.graphData.links.find(l => {
                    const s = l.source?.id || l.source;
                    const t = l.target?.id || l.target;
                    return (s === fromId && t === toId) || (s === toId && t === fromId);
                });

                if (!edge) continue;

                // CRITICAL: Determine actual edge direction
                // Edge source/target define the TRUE direction of the predicate
                const edgeSource = edge.source?.id || edge.source;
                const edgeTarget = edge.target?.id || edge.target;
                
                // Check if path direction matches edge direction
                const isForward = (edgeSource === fromId && edgeTarget === toId);
                
                // Subject and object based on edge's actual direction, NOT path direction
                const subjVar = isForward ? fromVar : toVar;
                const objVar = isForward ? toVar : fromVar;
                const subjNodeId = isForward ? fromId : toId;
                const objNodeId = isForward ? toId : fromId;

                const predIri = edge.property;
                const predSparql = predIri ? `<${predIri}>` : `?p${pathIdx}_${i}`;
                
                // Get datasets this edge belongs to
                const edgeDatasets = edge.datasets || [];
                
                // For each dataset this edge is in, add pattern to that endpoint/graph
                if (edgeDatasets.length > 0) {
                    // Use first dataset to determine placement (edges typically belong to one dataset)
                    const dsName = edgeDatasets[0];
                    const info = getDatasetInfo(dsName);
                    
                    // Add type assertions if required
                    if (requireType) {
                        const subjNode = this.graphData.nodes.find(n => n.id === subjNodeId);
                        const objNode = this.graphData.nodes.find(n => n.id === objNodeId);
                        if (subjNode) {
                            addPattern(info.endpoint, info.graph, `?${subjVar} a <${subjNode.id}> .`);
                        }
                        if (objNode) {
                            addPattern(info.endpoint, info.graph, `?${objVar} a <${objNode.id}> .`);
                        }
                    }
                    
                    // Add the triple pattern - ALWAYS edge.source pred edge.target
                    addPattern(info.endpoint, info.graph, `?${subjVar} ${predSparql} ?${objVar} .`);
                } else {
                    // No dataset info - add to primary endpoint without graph
                    if (requireType) {
                        const subjNode = this.graphData.nodes.find(n => n.id === subjNodeId);
                        const objNode = this.graphData.nodes.find(n => n.id === objNodeId);
                        if (subjNode) {
                            addPattern(primaryEndpoint, null, `?${subjVar} a <${subjNode.id}> .`);
                        }
                        if (objNode) {
                            addPattern(primaryEndpoint, null, `?${objVar} a <${objNode.id}> .`);
                        }
                    }
                    addPattern(primaryEndpoint, null, `?${subjVar} ${predSparql} ?${objVar} .`);
                }
            }
        });

        // Build SELECT clause
        const selectVarsList = Array.from(selectVars).map(v => `?${v}`).join(' ');

        // Build WHERE clause with proper SERVICE/GRAPH wrapping
        const endpoints = [...endpointPatterns.keys()].filter(Boolean);
        
        // Deduplicate patterns within each graph
        const dedupePatterns = (patterns) => [...new Set(patterns)];

        let whereLines = [];
        
        endpoints.forEach((endpoint, epIdx) => {
            const graphMap = endpointPatterns.get(endpoint);
            const isService = epIdx > 0; // First endpoint is primary, others are SERVICE
            
            const graphPatterns = [];
            graphMap.forEach((patterns, graph) => {
                const uniquePatterns = dedupePatterns(patterns);
                if (graph) {
                    // Wrap in GRAPH
                    graphPatterns.push(`GRAPH <${graph}> {\n      ${uniquePatterns.join('\n      ')}\n    }`);
                } else {
                    // No graph wrapper
                    graphPatterns.push(uniquePatterns.join('\n    '));
                }
            });
            
            const innerContent = graphPatterns.join('\n    ');
            
            if (isService) {
                whereLines.push(`  SERVICE <${endpoint}> {\n    ${innerContent}\n  }`);
            } else {
                // Primary endpoint - no SERVICE wrapper
                if ([...graphMap.keys()].some(g => g)) {
                    // Has graph wrappers
                    whereLines.push(`  ${innerContent}`);
                } else {
                    whereLines.push(`  ${innerContent}`);
                }
            }
        });

        // Handle patterns with no endpoint (fallback)
        if (endpointPatterns.has('')) {
            const noEpGraphMap = endpointPatterns.get('');
            noEpGraphMap.forEach((patterns, graph) => {
                const uniquePatterns = dedupePatterns(patterns);
                if (graph) {
                    whereLines.push(`  GRAPH <${graph}> {\n    ${uniquePatterns.join('\n    ')}\n  }`);
                } else {
                    whereLines.push(`  ${uniquePatterns.join('\n  ')}`);
                }
            });
        }

        // Build complete query
        let query = `SELECT DISTINCT ${selectVarsList}\nWHERE {\n${whereLines.join('\n\n')}\n}`;
        query += '\nLIMIT 100';
        
        return query;
    }

    /**
     * Expand a CURIE to full URI using known namespaces
     */
    expandCurie(curie) {
        if (!curie || curie.startsWith('http://') || curie.startsWith('https://')) {
            return curie;
        }
        
        const [prefix, local] = curie.split(':');
        const namespaces = D3DiagramUtils?.namespaces || {};
        
        // Reverse lookup: find namespace by prefix
        for (const [uri, pref] of Object.entries(namespaces)) {
            if (pref === prefix) {
                return uri + local;
            }
        }
        
        // Common namespaces fallback
        const commonNs = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'skos': 'http://www.w3.org/2004/02/skos/core#',
            'schema': 'http://schema.org/',
            'aopo': 'http://aopkb.org/aop_ontology#',
            'pato': 'http://purl.obolibrary.org/obo/PATO_'
        };
        
        if (commonNs[prefix]) {
            return commonNs[prefix] + local;
        }
        
        // Return as-is if can't expand
        return curie;
    }

    // ========== Path Finding Autocomplete ==========
    
    setupPathAutocomplete() {
        const fromInput = document.getElementById('ls-path-from');
        const toInput = document.getElementById('ls-path-to');
        const fromSuggestions = document.getElementById('ls-path-from-suggestions');
        const toSuggestions = document.getElementById('ls-path-to-suggestions');
        const findBtn = document.getElementById('ls-find-path-btn');
        const addBtn = document.getElementById('ls-add-path-btn');
        const clearBtn = document.getElementById('ls-clear-paths-btn');

        // Store selected node IDs
        this.pathFromNodeId = null;
        this.pathToNodeId = null;

        // Setup autocomplete for both inputs
        if (fromInput && fromSuggestions) {
            this.setupAutocompleteInput(fromInput, fromSuggestions, (nodeId) => {
                this.pathFromNodeId = nodeId;
            });
        }
        if (toInput && toSuggestions) {
            this.setupAutocompleteInput(toInput, toSuggestions, (nodeId) => {
                this.pathToNodeId = nodeId;
            });
        }

        // Find Path button - now uses autocomplete values
        if (findBtn) {
            findBtn.addEventListener('click', () => this.findPathFromInputs());
        }

        // Add Path button
        if (addBtn) {
            addBtn.addEventListener('click', () => this.findPathFromInputs());
        }

        // Clear Paths button
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clearAllPaths());
        }

        // Close suggestions when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.path-autocomplete') && !e.target.closest('.autocomplete-suggestions')) {
                fromSuggestions?.style && (fromSuggestions.style.display = 'none');
                toSuggestions?.style && (toSuggestions.style.display = 'none');
            }
        });
    }

    setupAutocompleteInput(input, suggestionsDiv, onSelect) {
        let activeIndex = -1;

        input.addEventListener('input', () => {
            const query = input.value.toLowerCase().trim();
            if (query.length < 1 || !this.graphData.nodes || this.graphData.nodes.length === 0) {
                suggestionsDiv.style.display = 'none';
                return;
            }

            // Filter nodes matching query
            const matches = this.graphData.nodes
                .filter(n => n.label.toLowerCase().includes(query) || n.id.toLowerCase().includes(query))
                .slice(0, 15); // Limit results

            if (matches.length === 0) {
                suggestionsDiv.style.display = 'none';
                return;
            }

            // Render suggestions
            suggestionsDiv.innerHTML = matches.map((n, i) => {
                const datasets = n.datasets || [];
                const badges = datasets.slice(0, 3).map(ds => 
                    `<span class="dataset-badge" style="background: ${this.getDatasetColor(ds)};" title="${ds}"></span>`
                ).join('');
                return `<div class="suggestion-item" data-index="${i}" data-id="${n.id}">
                    <span class="node-label">${this.highlightMatch(n.label, query)}</span>
                    <div class="dataset-badges">${badges}</div>
                </div>`;
            }).join('');

            suggestionsDiv.style.display = 'block';
            activeIndex = -1;

            // Add click handlers to suggestions
            suggestionsDiv.querySelectorAll('.suggestion-item').forEach(item => {
                item.addEventListener('click', () => {
                    const nodeId = item.dataset.id;
                    const node = this.graphData.nodes.find(n => n.id === nodeId);
                    if (node) {
                        input.value = node.label;
                        onSelect(nodeId);
                        suggestionsDiv.style.display = 'none';
                    }
                });
            });
        });

        // Keyboard navigation
        input.addEventListener('keydown', (e) => {
            const items = suggestionsDiv.querySelectorAll('.suggestion-item');
            if (items.length === 0) return;

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                activeIndex = Math.min(activeIndex + 1, items.length - 1);
                this.updateActiveItem(items, activeIndex);
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                activeIndex = Math.max(activeIndex - 1, 0);
                this.updateActiveItem(items, activeIndex);
            } else if (e.key === 'Enter' && activeIndex >= 0) {
                e.preventDefault();
                items[activeIndex].click();
            } else if (e.key === 'Escape') {
                suggestionsDiv.style.display = 'none';
            }
        });

        input.addEventListener('focus', () => {
            if (input.value.trim().length >= 1 && this.graphData.nodes?.length > 0) {
                input.dispatchEvent(new Event('input'));
            }
        });
    }

    updateActiveItem(items, activeIndex) {
        items.forEach((item, i) => {
            item.classList.toggle('active', i === activeIndex);
        });
        if (items[activeIndex]) {
            items[activeIndex].scrollIntoView({ block: 'nearest' });
        }
    }

    highlightMatch(text, query) {
        if (!query) return text;
        const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        return text.replace(regex, '<strong>$1</strong>');
    }

    findPathFromInputs() {
        const status = document.getElementById('ls-path-status');
        
        if (!this.pathFromNodeId || !this.pathToNodeId) {
            status.textContent = 'Please select both From and To nodes';
            return;
        }

        if (this.pathFromNodeId === this.pathToNodeId) {
            status.textContent = 'From and To nodes must be different';
            return;
        }

        // Get path settings from UI
        const shortestOnly = document.getElementById('ls-path-shortest-only')?.checked ?? true;
        const maxDepth = parseInt(document.getElementById('ls-path-max-depth')?.value) || 5;

        let nodePaths;
        if (shortestOnly) {
            // Use BFS to find shortest path only
            const shortestPath = this.findPath(this.pathFromNodeId, this.pathToNodeId);
            nodePaths = shortestPath ? [shortestPath] : [];
        } else {
            // Find all paths with the configured max depth
            nodePaths = this.findAllPaths(this.pathFromNodeId, this.pathToNodeId, 50, maxDepth);
        }
        
        // Expand node-paths to edge-paths (each unique edge sequence is a separate path)
        let expandedPaths = [];
        nodePaths.forEach(nodePath => {
            const edgePaths = this.expandPathToEdgePaths(nodePath);
            expandedPaths.push(...edgePaths);
        });
        
        if (expandedPaths && expandedPaths.length > 0) {
            const fromNode = this.graphData.nodes.find(n => n.id === this.pathFromNodeId);
            const toNode = this.graphData.nodes.find(n => n.id === this.pathToNodeId);
            const fromLabel = fromNode?.label || this.pathFromNodeId;
            const toLabel = toNode?.label || this.pathToNodeId;
            
            // Add all paths with same color (they share the same endpoint pair)
            const colorIndex = this.allPaths.length > 0 
                ? Math.max(...this.allPaths.map(p => p.colorIndex || 0)) + 1 
                : 0;
            
            expandedPaths.forEach((edgePath, i) => {
                this.allPaths.push({
                    path: edgePath.nodes,
                    edges: edgePath.edges,
                    fromLabel: fromLabel,
                    toLabel: toLabel,
                    colorIndex: colorIndex, // All paths between same nodes share color
                    pathNumber: i + 1,
                    totalPaths: expandedPaths.length
                });
            });
            
            const modeText = shortestOnly ? 'shortest path' : `${expandedPaths.length} path(s)`;
            status.textContent = `Found ${modeText} (${this.allPaths.length} total)`;
            this.highlightAllPaths();
            this.showAllPathsQueryPanel();
            
            // Show add/clear buttons
            document.getElementById('ls-add-path-btn').style.display = 'inline-block';
            document.getElementById('ls-clear-paths-btn').style.display = 'inline-block';
            
            // Clear inputs for next path
            document.getElementById('ls-path-from').value = '';
            document.getElementById('ls-path-to').value = '';
            this.pathFromNodeId = null;
            this.pathToNodeId = null;
        } else {
            status.textContent = 'No path found between selected nodes';
        }
    }

    removePath(index) {
        if (index >= 0 && index < this.allPaths.length) {
            this.allPaths.splice(index, 1);
            
            if (this.allPaths.length === 0) {
                this.clearAllPaths();
            } else {
                this.highlightAllPaths();
                this.showAllPathsQueryPanel();
                document.getElementById('ls-path-status').textContent = `${this.allPaths.length} path(s)`;
            }
        }
    }

    // ========== Data Loading ==========
    
    async renderCloud() {
        if (this.selectedDatasets.size === 0) {
            this.showMessage('Please select at least one dataset.');
            return;
        }

        this.showMessage('Loading schema data...');

        try {
            const results = await Promise.all(
                Array.from(this.selectedDatasets).map(name => this.loadDatasetCoverage(name))
            );
            
            this.mergedData = [];
            results.filter(r => r).forEach(r => {
                r.data.forEach(row => this.mergedData.push({ ...row, source_dataset: r.name }));
            });

            if (this.mergedData.length === 0) {
                this.showMessage('No schema data found.');
                return;
            }

            this.fullGraphData = this.buildGraphData(150);

            // Compute dataset components - if selection splits into disjoint groups,
            // pick the largest connected component to render and inform the user.
            const components = this.getDatasetComponents(this.fullGraphData);
            if (components.length > 1) {
                // find largest component by number of datasets
                components.sort((a, b) => b.size - a.size);
                const largest = components[0];
                if (largest.size <= 1) {
                    this.showMessage('Selected datasets are not connected via any shared node or relationship. Please select datasets that share nodes/edges to render a combined diagram.');
                    return;
                }

                // Filter fullGraphData to nodes/links that include datasets from the largest component
                const allowed = largest;
                const nodes = this.fullGraphData.nodes.filter(n => (n.datasets || []).some(d => allowed.has(d)));
                const links = this.fullGraphData.links.filter(l => (l.datasets || []).some(d => allowed.has(d)));

                this.graphData = { nodes, links };
                const names = Array.from(largest).join(', ');
                this.showMessage(`Selection splits into disconnected groups — rendering largest connected group: ${names}`);
            } else {
                // Default: show union (all nodes and links across selected datasets)
                this.graphData = {
                    nodes: this.fullGraphData.nodes || [],
                    links: this.fullGraphData.links || []
                };
            }
            this.populateNodeFilter();
            this.renderInfo();
            this.renderDiagram();

        } catch (error) {
            console.error('Failed to render:', error);
            this.showMessage(`Error: ${error.message}`);
        }
    }

    async loadDatasetCoverage(datasetName) {
        const dataset = this.datasets.find(d => d.name === datasetName);
        if (!dataset?.dataFiles?.coverage) return null;

        try {
            const path = dataset.dataFiles.coverage.replace(/^(\.\.\/)+|^\.?\//g, '');
            const response = await fetch(this.githubRawBase + path);
            if (!response.ok) return null;
            return { name: datasetName, data: this.parseCSV(await response.text()) };
        } catch {
            return null;
        }
    }

    parseCSV(text) {
        const lines = text.trim().split('\n');
        if (lines.length < 2) return [];
        const headers = lines[0].split(',').map(h => h.trim());
        
        return lines.slice(1).map(line => {
            const values = [];
            let current = '', inQuotes = false;
            for (const char of line) {
                if (char === '"') inQuotes = !inQuotes;
                else if (char === ',' && !inQuotes) { values.push(current.trim()); current = ''; }
                else current += char;
            }
            values.push(current.trim());
            
            const row = {};
            headers.forEach((h, i) => row[h] = values[i] || '');
            return row;
        });
    }

    // ========== Node Exclusion ==========
    
    isExcludedNode(uri) {
        if (!uri) return true;
        if (!document.getElementById('ls-exclude-owl')?.checked) return false;
        
        // Known namespace URIs to exclude
        const excludedNamespaces = [
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'http://www.w3.org/2000/01/rdf-schema#',
            'http://www.w3.org/2002/07/owl#',
            'http://www.w3.org/ns/shacl#',
            'http://www.w3.org/2001/XMLSchema#'
        ];
        
        // Check if URI starts with excluded namespace
        if (excludedNamespaces.some(ns => uri.startsWith(ns))) return true;
        
        // Also check CURIE-style prefixes (for backward compatibility)
        if (this.excludedPrefixes.some(p => uri.startsWith(p + ':'))) return true;
        
        // Check local name for common OWL/RDF terms
        const localName = uri.split(/[#\/]/).pop();
        if (this.excludedLocalNames.includes(localName)) return true;
        
        return false;
    }

    // ========== Graph Building ==========
    
    buildGraphData(maxPatterns = 150) {
        // Strict union: include every node and every link from all merged data rows.
        const nodeMap = new Map();
        const linkMap = new Map();

        const patterns = [...this.mergedData];

        patterns.forEach(row => {
            const subjectUri = row.subject_uri || row.subject_class || 'Unknown';
            const objectUri = row.object_uri || row.object_class || 'Unknown';
            const propertyUri = row.property_uri || row.property || 'unknown';

            const subjectLabel = row.subject_class || this.getLocalName(subjectUri);
            const objectLabel = row.object_class || this.getLocalName(objectUri);
            const propertyLabel = row.property || this.getLocalName(propertyUri);

            const dataset = row.source_dataset;

            const subjectExcluded = this.isExcludedNode(subjectUri);
            const objectExcluded = this.isExcludedNode(objectUri);

            // add subject node unless excluded
            if (!subjectExcluded) {
                if (!nodeMap.has(subjectUri)) {
                    nodeMap.set(subjectUri, {
                        id: subjectUri,
                        label: subjectLabel,
                        datasets: new Set(),
                        properties: new Set(),
                        inDegree: 0,
                        outDegree: 0
                    });
                }
                nodeMap.get(subjectUri).datasets.add(dataset);
                nodeMap.get(subjectUri).properties.add(propertyLabel);
            }

            // add object node unless excluded
            if (!objectExcluded) {
                if (!nodeMap.has(objectUri)) {
                    nodeMap.set(objectUri, {
                        id: objectUri,
                        label: objectLabel,
                        datasets: new Set(),
                        properties: new Set(),
                        inDegree: 0,
                        outDegree: 0
                    });
                }
                nodeMap.get(objectUri).datasets.add(dataset);
                nodeMap.get(objectUri).properties.add(propertyLabel);
            }

            // add link between non-excluded nodes
            // Each subject-property-object is a unique edge (not merged by subject-object pair)
            if (!subjectExcluded && !objectExcluded && subjectUri !== objectUri) {
                const key = `${subjectUri}|||${propertyUri}|||${objectUri}`;
                if (!linkMap.has(key)) {
                    linkMap.set(key, {
                        source: subjectUri,
                        target: objectUri,
                        property: propertyUri,      // Full IRI
                        label: propertyLabel,       // Short label for display
                        datasets: new Set()
                    });
                }
                linkMap.get(key).datasets.add(dataset);
                // increment degrees
                const sNode = nodeMap.get(subjectUri);
                const oNode = nodeMap.get(objectUri);
                if (sNode) sNode.outDegree = (sNode.outDegree || 0) + 1;
                if (oNode) oNode.inDegree = (oNode.inDegree || 0) + 1;
            }
        });

        const nodes = Array.from(nodeMap.values()).map(n => ({
            ...n,
            datasets: Array.from(n.datasets),
            properties: Array.from(n.properties).slice(0, 4),
            degree: (n.inDegree || 0) + (n.outDegree || 0)
        }));

        const links = Array.from(linkMap.values()).map(l => ({
            ...l,
            datasets: Array.from(l.datasets)
        }));

        return { nodes, links };
    }

    // Check whether the currently selected datasets are connected via shared
    // nodes or links in the provided graph data. Returns true if all
    // selected datasets are reachable from each other.
    selectedDatasetsConnected(graphData) {
        const selected = Array.from(this.selectedDatasets || []);
        if (selected.length <= 1) return true;

        // Initialize adjacency map for selected datasets
        const adj = new Map();
        selected.forEach(s => adj.set(s, new Set()));

        // Helper to add undirected edge between two datasets if both are selected
        const addEdge = (a, b) => {
            if (!adj.has(a) || !adj.has(b) || a === b) return;
            adj.get(a).add(b);
            adj.get(b).add(a);
        };

        // For each node, connect every pair of datasets that appear on that node
        (graphData.nodes || []).forEach(n => {
            const ds = (n.datasets || []).filter(d => adj.has(d));
            for (let i = 0; i < ds.length; i++) {
                for (let j = i + 1; j < ds.length; j++) {
                    addEdge(ds[i], ds[j]);
                }
            }
        });

        // For completeness, also connect datasets that co-occur on the same link
        (graphData.links || []).forEach(l => {
            const ds = (l.datasets || []).filter(d => adj.has(d));
            for (let i = 0; i < ds.length; i++) {
                for (let j = i + 1; j < ds.length; j++) {
                    addEdge(ds[i], ds[j]);
                }
            }
        });

        // BFS from the first selected dataset
        const start = selected[0];
        const visited = new Set([start]);
        const q = [start];
        while (q.length > 0) {
            const cur = q.shift();
            (adj.get(cur) || []).forEach(nb => {
                if (!visited.has(nb)) { visited.add(nb); q.push(nb); }
            });
        }

        return visited.size === selected.length;
    }

    // Return array of dataset components (each component is a Set of dataset names)
    getDatasetComponents(graphData) {
        const selected = Array.from(this.selectedDatasets || []);
        if (selected.length <= 1) return [new Set(selected)];

        // adjacency among selected datasets
        const adj = new Map();
        selected.forEach(s => adj.set(s, new Set()));

        const addEdge = (a, b) => {
            if (!adj.has(a) || !adj.has(b) || a === b) return;
            adj.get(a).add(b);
            adj.get(b).add(a);
        };

        (graphData.nodes || []).forEach(n => {
            const ds = (n.datasets || []).filter(d => adj.has(d));
            for (let i = 0; i < ds.length; i++) {
                for (let j = i + 1; j < ds.length; j++) addEdge(ds[i], ds[j]);
            }
        });

        (graphData.links || []).forEach(l => {
            const ds = (l.datasets || []).filter(d => adj.has(d));
            for (let i = 0; i < ds.length; i++) {
                for (let j = i + 1; j < ds.length; j++) addEdge(ds[i], ds[j]);
            }
        });

        const components = [];
        const visited = new Set();
        selected.forEach(s => {
            if (visited.has(s)) return;
            const comp = new Set();
            const q = [s];
            visited.add(s);
            comp.add(s);
            while (q.length) {
                const cur = q.shift();
                (adj.get(cur) || []).forEach(nb => {
                    if (!visited.has(nb)) { visited.add(nb); q.push(nb); comp.add(nb); }
                });
            }
            components.push(comp);
        });

        return components;
    }

    getLocalName(uri) {
        if (!uri) return 'Unknown';
        const local = uri.split(/[#\/]/).pop();
        try { return decodeURIComponent(local).substring(0, 20); } 
        catch { return local.substring(0, 20); }
    }

    // ========== Node Filter ==========
    
    populateNodeFilter() {
        const select = document.getElementById('ls-node-filter');
        if (!select) return;

        // Map node id to degree in the current diagram
        const currentNodeDegrees = {};
        for (const n of this.graphData.nodes) {
            currentNodeDegrees[n.id] = n.degree || 0;
        }

        // Use all nodes from the full graph, but show their degree in the current diagram (or 0)
        const sorted = [...this.fullGraphData.nodes].sort((a, b) => b.degree - a.degree);
        select.innerHTML = '<option value="">All nodes</option>' +
            sorted.map(n => {
                const curDegree = currentNodeDegrees[n.id] !== undefined ? currentNodeDegrees[n.id] : 0;
                return `<option value="${n.id}">${n.label} (${curDegree})</option>`;
            }).join('');

        // Refresh Bootstrap Select if available
        if (typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(select).selectpicker('refresh');
        }

        // Also populate node highlight filter
        this.populateNodeHighlightFilter();
    }

    // Populate the node highlight filter (multi-select for highlighting)
    populateNodeHighlightFilter() {
        const select = document.getElementById('ls-node-highlight-filter');
        if (!select) return;

        const sorted = [...this.fullGraphData.nodes].sort((a, b) => b.degree - a.degree);
        select.innerHTML = sorted.map(n => {
            return `<option value="${n.id}">${n.label}</option>`;
        }).join('');

        if (typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(select).selectpicker('refresh');
        }
    }

    // Apply highlight filter: highlight selected nodes, dim others, and zoom to last selected
    applyNodeHighlightFilter(selectedIds) {
        if (!this.svg) return;
        const nodeWidth = 200; // must match renderDiagram

        if (!selectedIds || selectedIds.length === 0) {
            // Clear highlight: show all nodes/links normally
            d3.selectAll('.node-group').classed('dimmed', false);
            d3.selectAll('.link-group').classed('dimmed', false);
            return;
        }

        const selectedSet = new Set(selectedIds);

        // Highlight selected nodes, dim others
        d3.selectAll('.node-group').classed('dimmed', d => !selectedSet.has(d.id));
        d3.selectAll('.link-group').classed('dimmed', l => {
            const s = typeof l.source === 'object' ? l.source.id : l.source;
            const t = typeof l.target === 'object' ? l.target.id : l.target;
            return !(selectedSet.has(s) || selectedSet.has(t));
        });

        // Zoom to last selected node
        const lastId = selectedIds[selectedIds.length - 1];
        const targetNode = this.graphData.nodes.find(n => n.id === lastId);
        if (targetNode && this.zoom) {
            this.zoomToNode(targetNode, nodeWidth);
        }
    }

    // Zoom and center on a specific node
    zoomToNode(node, nodeWidth = 200) {
        if (!this.svg || !this.zoom || !node) return;
        const container = document.getElementById('ls-diagram');
        const containerRect = container?.getBoundingClientRect();
        const width = containerRect?.width || 1200;
        const height = containerRect?.height || 600;

        // Get current viewBox
        const viewBox = this.svg.attr('viewBox')?.split(' ').map(Number) || [0, 0, width, height];
        const [vbX, vbY, vbW, vbH] = viewBox;

        // Node center in viewBox coordinates
        const nodeCenterX = node.x + nodeWidth / 2;
        const nodeCenterY = node.y + (node.height || 80) / 2;

        // Calculate scale to fit node nicely (zoom in to 2x)
        const scale = 2;

        // Translate to center the node
        const tx = width / 2 - nodeCenterX * scale;
        const ty = height / 2 - nodeCenterY * scale;

        this.svg.transition().duration(500).call(
            this.zoom.transform,
            d3.zoomIdentity.translate(tx, ty).scale(scale)
        );
    }

    filterByNode(nodeId) {
        if (!nodeId) {
            this.graphData = { ...this.fullGraphData };
        } else {
            const depth = parseInt(document.getElementById('ls-neighbor-depth')?.value || '2');
            this.graphData = this.getNeighborhood(nodeId, depth);
        }
        this.renderDiagram();
    }

    getNeighborhood(startId, maxDepth) {
        if (maxDepth === 0) return { ...this.fullGraphData };

        const adjacency = new Map();
        this.fullGraphData.links.forEach(l => {
            if (!adjacency.has(l.source)) adjacency.set(l.source, []);
            if (!adjacency.has(l.target)) adjacency.set(l.target, []);
            adjacency.get(l.source).push(l.target);
            adjacency.get(l.target).push(l.source);
        });

        const visited = new Set([startId]);
        const queue = [{ id: startId, depth: 0 }];

        while (queue.length > 0) {
            const { id, depth } = queue.shift();
            if (depth >= maxDepth) continue;
            (adjacency.get(id) || []).forEach(neighbor => {
                if (!visited.has(neighbor)) {
                    visited.add(neighbor);
                    queue.push({ id: neighbor, depth: depth + 1 });
                }
            });
        }

        return {
            nodes: this.fullGraphData.nodes.filter(n => visited.has(n.id)),
            links: this.fullGraphData.links.filter(l => visited.has(l.source) && visited.has(l.target))
        };
    }

    // ========== Path Finding ==========
    
    togglePathFindingMode() {
        this.pathFindingMode = !this.pathFindingMode;
        this.selectedNodes = [];
        
        const btn = document.getElementById('ls-find-path-btn');
        const addBtn = document.getElementById('ls-add-path-btn');
        const clearBtn = document.getElementById('ls-clear-paths-btn');
        const status = document.getElementById('ls-path-status');
        
        if (this.pathFindingMode) {
            btn.style.background = '#0969da';
            btn.style.color = 'white';
            status.textContent = 'Click first node...';
        } else {
            btn.style.background = '';
            btn.style.color = '';
            status.textContent = '';
            // Don't clear paths when exiting mode, just stop selecting
        }
    }

    startAddPath() {
        // Start adding another path without clearing existing ones
        this.pathFindingMode = true;
        this.selectedNodes = [];
        const btn = document.getElementById('ls-find-path-btn');
        const status = document.getElementById('ls-path-status');
        btn.style.background = '#0969da';
        btn.style.color = 'white';
        status.textContent = 'Click first node for new path...';
    }

    clearAllPaths() {
        this.allPaths = [];
        this.highlightedPath = [];
        this.pathFromNodeId = null;
        this.pathToNodeId = null;
        this.clearHighlights();
        this.hidePathQueryPanel();
        
        // Clear UI elements
        const addBtn = document.getElementById('ls-add-path-btn');
        const clearBtn = document.getElementById('ls-clear-paths-btn');
        const status = document.getElementById('ls-path-status');
        const pathsList = document.getElementById('ls-paths-list');
        const fromInput = document.getElementById('ls-path-from');
        const toInput = document.getElementById('ls-path-to');
        
        if (addBtn) addBtn.style.display = 'none';
        if (clearBtn) clearBtn.style.display = 'none';
        if (status) status.textContent = '';
        if (pathsList) {
            pathsList.style.display = 'none';
            pathsList.innerHTML = '';
        }
        if (fromInput) fromInput.value = '';
        if (toInput) toInput.value = '';
    }

    handleNodeClick(node) {
        if (!this.pathFindingMode) return;
        const status = document.getElementById('ls-path-status');

        if (this.selectedNodes.length === 0) {
            this.selectedNodes.push(node.id);
            status.textContent = `From: ${node.label} → Click second node`;
            d3.selectAll('.node-group').classed('path-start', d => d.id === node.id);
        } else if (this.selectedNodes[0] !== node.id) {
            const path = this.findPath(this.selectedNodes[0], node.id);
            if (path) {
                this.allPaths.push(path);
                status.textContent = `${this.allPaths.length} path(s) found`;
                this.highlightAllPaths();
                this.showAllPathsQueryPanel();
                // Show add/clear buttons
                document.getElementById('ls-add-path-btn').style.display = 'inline-block';
                document.getElementById('ls-clear-paths-btn').style.display = 'inline-block';
            } else {
                status.textContent = 'No path found between selected nodes';
            }
            this.selectedNodes = [];
            // Exit path finding mode after finding a path
            this.pathFindingMode = false;
            const btn = document.getElementById('ls-find-path-btn');
            btn.style.background = '';
            btn.style.color = '';
        }
    }

    // Show all accumulated paths as SPARQL-like text in colored bubbles
    showAllPathsQueryPanel() {
        const panel = document.getElementById('ls-path-query-text');
        const bubblesContainer = document.getElementById('ls-path-bubbles');
        const txtarea = document.getElementById('ls-path-query-txtarea');
        if (!panel || !bubblesContainer) return;
        
        if (this.allPaths.length === 0) {
            panel.style.display = 'none';
            bubblesContainer.innerHTML = '';
            if (txtarea) txtarea.value = '';
            return;
        }

        // Build both visual bubbles and plain text (for copy)
        let allPlainText = [];
        let bubblesHtml = [];
        
        this.allPaths.forEach((pathData, pathIdx) => {
            const path = Array.isArray(pathData) ? pathData : pathData.path;
            const storedEdges = pathData.edges; // May have pre-computed edges from expansion
            if (!path || path.length < 2) return;
            
            // Use colorIndex if available (paths between same nodes share color)
            const colorIdx = pathData.colorIndex !== undefined ? pathData.colorIndex : pathIdx;
            const color = this.getPathColor(colorIdx);
            const lightBg = this.hexToRgba(color, 0.1);
            const borderColor = this.hexToRgba(color, 0.4);
            
            let lines = [];
            for (let i = 0; i < path.length - 1; i++) {
                const fromId = path[i];
                const toId = path[i+1];
                
                const fromNode = this.graphData.nodes.find(n => n.id === fromId);
                const toNode = this.graphData.nodes.find(n => n.id === toId);
                
                const fromLabel = fromNode ? fromNode.label : fromId;
                const toLabel = toNode ? toNode.label : toId;
                
                // Get edge - prefer stored edge, fallback to graph lookup
                let edge = storedEdges && storedEdges[i] ? storedEdges[i] : null;
                if (!edge) {
                    edge = this.graphData.links.find(l => 
                        (l.source === fromId && l.target === toId) || 
                        (l.source === toId && l.target === fromId) ||
                        (l.source?.id === fromId && l.target?.id === toId) ||
                        (l.source?.id === toId && l.target?.id === fromId)
                    );
                }
                
                const predLabel = edge ? (edge.property || edge.label || '?p') : '?p';
                
                // CRITICAL: Respect edge directionality
                // Edge source/target define the TRUE direction of the predicate
                if (edge) {
                    const edgeSource = edge.source?.id || edge.source;
                    const edgeTarget = edge.target?.id || edge.target;
                    const isForward = (edgeSource === fromId && edgeTarget === toId);
                    
                    const subjLabel = isForward ? fromLabel : toLabel;
                    const objLabel = isForward ? toLabel : fromLabel;
                    lines.push(`${subjLabel}  ${predLabel}  ${objLabel} .`);
                } else {
                    // No edge found - use path order as fallback
                    lines.push(`${fromLabel}  ${predLabel}  ${toLabel} .`);
                }
            }
            
            // Show path number if there are multiple paths between same nodes
            const pathInfo = pathData.totalPaths > 1 
                ? ` (${pathData.pathNumber}/${pathData.totalPaths})`
                : '';
            const pathLabel = `${pathData.fromLabel || 'Start'} → ${pathData.toLabel || 'End'}${pathInfo}`;
            
            // Path length = number of nodes in path, edges = nodes - 1
            const pathLength = path.length;
            const edgeCount = lines.length; // Number of triples (includes multiple edges between same nodes)
            const lengthInfo = `${pathLength} nodes, ${edgeCount} edges`;
            
            allPlainText.push(`# Path ${pathIdx + 1}: ${pathLabel} [${lengthInfo}]`);
            allPlainText.push(...lines);
            allPlainText.push('');
            
            bubblesHtml.push(`
                <div class="path-bubble" data-path-index="${pathIdx}" style="background: ${lightBg}; border: 1px solid ${borderColor}; border-left: 4px solid ${color}; border-radius: 6px; padding: 8px 12px; position: relative;">
                    <button class="remove-path-bubble-btn" data-index="${pathIdx}" style="position: absolute; top: 4px; right: 4px; width: 18px; height: 18px; border: none; background: ${borderColor}; color: ${color}; border-radius: 50%; cursor: pointer; font-size: 12px; line-height: 1; display: flex; align-items: center; justify-content: center;" title="Remove this path">×</button>
                    <div style="font-size: 11px; font-weight: 600; color: ${color}; margin-bottom: 6px; display: flex; align-items: center; gap: 6px; padding-right: 20px;">
                        <span style="width: 8px; height: 8px; border-radius: 50%; background: ${color};"></span>
                        Path ${pathIdx + 1}: ${pathLabel}
                        <span style="font-weight: 400; font-size: 10px; color: var(--text-secondary); margin-left: auto;">${lengthInfo}</span>
                    </div>
                    <pre style="margin: 0; font-family: 'SF Mono', Consolas, monospace; font-size: 10px; line-height: 1.5; color: var(--text-primary); white-space: pre-wrap; word-break: break-word;">${lines.join('\n')}</pre>
                </div>
            `);
        });
        
        bubblesContainer.innerHTML = bubblesHtml.join('');
        
        // Add click handlers for delete buttons on bubbles
        bubblesContainer.querySelectorAll('.remove-path-bubble-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const index = parseInt(btn.dataset.index);
                this.removePath(index);
            });
        });
        
        if (txtarea) txtarea.value = allPlainText.join('\n').trim();
        panel.style.display = 'block';
    }
    
    // Helper to convert hex color to rgba
    hexToRgba(hex, alpha) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        if (result) {
            const r = parseInt(result[1], 16);
            const g = parseInt(result[2], 16);
            const b = parseInt(result[3], 16);
            return `rgba(${r}, ${g}, ${b}, ${alpha})`;
        }
        return hex;
    }

    hidePathQueryPanel() {
        const panel = document.getElementById('ls-path-query-text');
        const bubblesContainer = document.getElementById('ls-path-bubbles');
        const txtarea = document.getElementById('ls-path-query-txtarea');
        if (panel) panel.style.display = 'none';
        if (bubblesContainer) bubblesContainer.innerHTML = '';
        if (txtarea) txtarea.value = '';
    }

    findPath(startId, endId) {
        const adjacency = new Map();
        this.graphData.links.forEach(l => {
            const s = l.source.id || l.source;
            const t = l.target.id || l.target;
            if (!adjacency.has(s)) adjacency.set(s, []);
            if (!adjacency.has(t)) adjacency.set(t, []);
            adjacency.get(s).push(t);
            adjacency.get(t).push(s);
        });

        const visited = new Set();
        const queue = [[startId]];

        while (queue.length > 0) {
            const path = queue.shift();
            const node = path[path.length - 1];
            if (node === endId) return path;
            if (!visited.has(node)) {
                visited.add(node);
                (adjacency.get(node) || []).forEach(n => {
                    if (!visited.has(n)) queue.push([...path, n]);
                });
            }
        }
        return null;
    }

    /**
     * Find ALL paths between two nodes using DFS with backtracking
     * @param {string} startId - Start node ID
     * @param {string} endId - End node ID
     * @param {number} maxPaths - Maximum number of paths to return (default 50)
     * @param {number} maxDepth - Maximum path length to explore (default 10)
     * @returns {Array} Array of paths (each path is an array of node IDs)
     */
    findAllPaths(startId, endId, maxPaths = 50, maxDepth = 10) {
        const adjacency = new Map();
        this.graphData.links.forEach(l => {
            const s = l.source.id || l.source;
            const t = l.target.id || l.target;
            if (!adjacency.has(s)) adjacency.set(s, []);
            if (!adjacency.has(t)) adjacency.set(t, []);
            adjacency.get(s).push(t);
            adjacency.get(t).push(s);
        });

        const allPaths = [];
        const visited = new Set();

        const dfs = (currentId, path) => {
            if (allPaths.length >= maxPaths) return;
            if (path.length > maxDepth) return;
            
            if (currentId === endId) {
                allPaths.push([...path]);
                return;
            }

            visited.add(currentId);
            const neighbors = adjacency.get(currentId) || [];
            for (const neighbor of neighbors) {
                if (!visited.has(neighbor)) {
                    path.push(neighbor);
                    dfs(neighbor, path);
                    path.pop();
                    if (allPaths.length >= maxPaths) break;
                }
            }
            visited.delete(currentId);
        };

        dfs(startId, [startId]);
        return allPaths;
    }

    /**
     * Expand a node-path into multiple edge-paths when there are multiple edges between nodes.
     * Each unique sequence of edges becomes a separate path.
     * @param {Array} nodePath - Array of node IDs representing a path
     * @returns {Array} Array of edge-paths, each containing { nodes: [...], edges: [...] }
     */
    expandPathToEdgePaths(nodePath) {
        if (!nodePath || nodePath.length < 2) return [];
        
        // For each consecutive node pair, find all edges between them
        const edgeOptions = [];
        for (let i = 0; i < nodePath.length - 1; i++) {
            const fromId = nodePath[i];
            const toId = nodePath[i + 1];
            const edges = this.graphData.links.filter(l => {
                const s = l.source?.id || l.source;
                const t = l.target?.id || l.target;
                return (s === fromId && t === toId) || (s === toId && t === fromId);
            });
            edgeOptions.push(edges.length > 0 ? edges : [{ property: '?p' }]);
        }
        
        // Generate all combinations (Cartesian product)
        const expandedPaths = [];
        const generateCombinations = (index, currentEdges) => {
            if (index === edgeOptions.length) {
                expandedPaths.push({
                    nodes: nodePath,
                    edges: [...currentEdges]
                });
                return;
            }
            for (const edge of edgeOptions[index]) {
                currentEdges.push(edge);
                generateCombinations(index + 1, currentEdges);
                currentEdges.pop();
            }
        };
        
        generateCombinations(0, []);
        return expandedPaths;
    }

    highlightPath(path) {
        const pathSet = new Set(path);
        d3.selectAll('.node-group')
            .classed('path-node', d => pathSet.has(d.id))
            .classed('dimmed', d => !pathSet.has(d.id));
        
        d3.selectAll('.link-group')
            .classed('dimmed', d => {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                for (let i = 0; i < path.length - 1; i++) {
                    if ((path[i] === s && path[i+1] === t) || (path[i] === t && path[i+1] === s)) return false;
                }
                return true;
            })
            .classed('path-link', d => {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                for (let i = 0; i < path.length - 1; i++) {
                    if ((path[i] === s && path[i+1] === t) || (path[i] === t && path[i+1] === s)) return true;
                }
                return false;
            });
    }

    // Highlight all accumulated paths with distinct colors
    highlightAllPaths() {
        // Build maps of node/edge to their path colors (for multi-path, first path wins)
        const nodeColorMap = new Map(); // nodeId -> color
        const edgeColorMap = new Map(); // "source|||target|||property" -> color
        const allPathNodes = new Set();
        const allPathEdges = new Set(); // Set of "source|||target|||property" keys
        
        this.allPaths.forEach((pathData, pathIdx) => {
            const path = Array.isArray(pathData) ? pathData : pathData.path;
            const storedEdges = pathData.edges;
            if (!path) return;
            // Use colorIndex if available (paths between same nodes share color)
            const colorIdx = pathData.colorIndex !== undefined ? pathData.colorIndex : pathIdx;
            const color = this.getPathColor(colorIdx);
            
            path.forEach(nodeId => {
                allPathNodes.add(nodeId);
                if (!nodeColorMap.has(nodeId)) {
                    nodeColorMap.set(nodeId, color);
                }
            });
            
            for (let i = 0; i < path.length - 1; i++) {
                // If we have stored edges, use the specific edge property
                const edgeProp = storedEdges && storedEdges[i] 
                    ? (storedEdges[i].property || storedEdges[i].label || '') 
                    : '';
                
                // Create edge keys that include the property for specific matching
                const edgeKey1 = `${path[i]}|||${path[i+1]}|||${edgeProp}`;
                const edgeKey2 = `${path[i+1]}|||${path[i]}|||${edgeProp}`;
                allPathEdges.add(edgeKey1);
                allPathEdges.add(edgeKey2);
                if (!edgeColorMap.has(edgeKey1)) {
                    edgeColorMap.set(edgeKey1, color);
                    edgeColorMap.set(edgeKey2, color);
                }
            }
        });

        // Apply colors to nodes
        d3.selectAll('.node-group')
            .classed('path-node', d => allPathNodes.has(d.id))
            .classed('dimmed', d => allPathNodes.size > 0 && !allPathNodes.has(d.id))
            .each(function(d) {
                const color = nodeColorMap.get(d.id);
                const rect = d3.select(this).select('rect');
                if (color && allPathNodes.has(d.id)) {
                    rect.style('stroke', color).style('stroke-width', '3px');
                } else {
                    rect.style('stroke', null).style('stroke-width', null);
                }
            });
        
        // Apply colors to links
        d3.selectAll('.link-group')
            .classed('path-link', d => {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                const prop = d.property || d.label || '';
                return allPathEdges.has(`${s}|||${t}|||${prop}`);
            })
            .classed('dimmed', d => {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                const prop = d.property || d.label || '';
                return allPathEdges.size > 0 && !allPathEdges.has(`${s}|||${t}|||${prop}`);
            })
            .each(function(d) {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                const prop = d.property || d.label || '';
                const color = edgeColorMap.get(`${s}|||${t}|||${prop}`);
                const path = d3.select(this).select('path');
                if (color) {
                    path.style('stroke', color).style('stroke-width', '3px');
                } else {
                    path.style('stroke', null).style('stroke-width', null);
                }
            });
    }

    clearHighlights() {
        d3.selectAll('.node-group')
            .classed('path-node path-start dimmed', false)
            .each(function() {
                d3.select(this).select('rect').style('stroke', null).style('stroke-width', null);
            });
        d3.selectAll('.link-group')
            .classed('path-link dimmed', false)
            .each(function() {
                d3.select(this).select('path').style('stroke', null).style('stroke-width', null);
            });
    }

    // Toggle fullscreen mode for the LS Cloud section
    toggleFullscreen() {
        const container = document.getElementById('ls-cloud-container');
        const diagram = document.getElementById('ls-diagram');
        const btn = document.getElementById('ls-fullscreen-btn');
        const exitBtn = document.getElementById('ls-exit-fullscreen-btn');
        if (!container) return;
        
        const isCurrentlyFullscreen = container.classList.contains('ls-cloud-fullscreen');
        
        if (isCurrentlyFullscreen) {
            // Exit fullscreen
            container.classList.remove('ls-cloud-fullscreen');
            if (btn) {
                btn.style.display = '';
                btn.textContent = 'Fullscreen';
            }
            if (exitBtn) exitBtn.style.display = 'none';
            document.body.style.overflow = '';
            
            // Reset any inline styles that might have been set during fullscreen
            if (diagram) {
                diagram.style.height = '';
                diagram.style.minHeight = '';
            }
        } else {
            // Enter fullscreen
            container.classList.add('ls-cloud-fullscreen');
            if (btn) btn.style.display = 'none';
            if (exitBtn) exitBtn.style.display = '';
            document.body.style.overflow = 'hidden';
        }
        
        // Re-render diagram to fit new size
        if (this.graphData.nodes.length > 0) {
            setTimeout(() => this.renderDiagram(), 100);
        }
    }

    // Toggle collapse state for the LS Cloud section
    toggleCollapse(forceCollapse = null) {
        const container = document.getElementById('ls-cloud-container');
        const icon = container?.querySelector('.collapse-icon');
        if (!container) return;
        
        const shouldCollapse = forceCollapse !== null ? forceCollapse : !container.classList.contains('collapsed');
        
        if (shouldCollapse) {
            container.classList.add('collapsed');
            if (icon) icon.style.transform = 'rotate(-90deg)';
        } else {
            container.classList.remove('collapsed');
            if (icon) icon.style.transform = 'rotate(0deg)';
        }
    }
    
    // Toggle path finder sidebar visibility
    togglePathSidebar(forceOpen = null) {
        const sidebar = document.getElementById('ls-path-sidebar');
        const toggleBtn = document.getElementById('ls-path-toggle-btn');
        if (!sidebar) return;
        
        const isCurrentlyOpen = sidebar.classList.contains('open');
        const shouldOpen = forceOpen !== null ? forceOpen : !isCurrentlyOpen;
        
        if (shouldOpen) {
            sidebar.classList.add('open');
            toggleBtn?.classList.add('active');
            toggleBtn.style.background = 'var(--primary-color)';
            toggleBtn.style.color = '#fff';
            toggleBtn.style.borderColor = 'var(--primary-color)';
        } else {
            sidebar.classList.remove('open');
            toggleBtn?.classList.remove('active');
            toggleBtn.style.background = '';
            toggleBtn.style.color = '';
            toggleBtn.style.borderColor = '';
        }
    }
    
    // Toggle edge style between orthogonal and curved
    toggleEdgeStyle() {
        this.curvedEdges = !this.curvedEdges;
        const btn = document.getElementById('ls-edge-style-btn');
        if (btn) {
            btn.textContent = this.curvedEdges ? 'Curved' : 'Orthogonal';
            if (this.curvedEdges) {
                btn.style.background = 'var(--primary-color)';
                btn.style.color = '#fff';
                btn.style.borderColor = 'var(--primary-color)';
            } else {
                btn.style.background = '';
                btn.style.color = '';
                btn.style.borderColor = '';
            }
        }
        // Re-render diagram with new edge style immediately
        if (this.graphData && this.graphData.nodes.length > 0) {
            this.renderDiagram();
        }
    }
    
    // Collapse LS Cloud (called when schema/coverage panels open)
    collapseForPanel() {
        this.toggleCollapse(true);
    }

    // ========== Rendering ==========
    
    renderInfo() {
        const info = document.getElementById('ls-info');
        if (!info) return;

        // Find datasets present in the current diagram (nodes or links)
        const presentDatasets = new Set();
        this.graphData.nodes.forEach(n => (n.datasets || []).forEach(d => presentDatasets.add(d)));
        this.graphData.links.forEach(l => (l.datasets || []).forEach(d => presentDatasets.add(d)));

        // Build clickable legend items (they toggle a subset filter)
        const legend = Array.from(presentDatasets).map(name => {
            const color = this.getDatasetColor(name);
            return `<span class="ls-legend-item" data-dataset="${name}" style="display: inline-flex; align-items: center; gap: 6px; margin-right: 10px; font-size: 11px; cursor: pointer;">
                <span class="ls-legend-swatch" style="width: 12px; height: 12px; background: ${color}; border-radius: 2px; display:inline-block;"></span>
                <span class="ls-legend-label">${name}</span>
            </span>`;
        }).join('');

        info.style.display = 'block';
        info.innerHTML = `<div style="display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px;">
            <span><strong>${this.graphData.nodes.length}</strong> classes, <strong>${this.graphData.links.length}</strong> relationships</span>
            <div>${legend}</div>
        </div>`;

        // Attach click handlers to legend items to toggle subset/dimming
        setTimeout(() => {
            const items = info.querySelectorAll('.ls-legend-item');
            items.forEach(it => {
                it.onclick = (e) => {
                    const ds = it.getAttribute('data-dataset');
                    if (!ds) return;
                    if (this.legendSubset.has(ds)) this.legendSubset.delete(ds);
                    else this.legendSubset.add(ds);

                    // update visual active state
                    items.forEach(i => i.classList.toggle('active', this.legendSubset.has(i.getAttribute('data-dataset'))));

                    // Re-render diagram styling without rebuilding layout
                    this.renderDiagram();
                };
            });
        }, 0);
    }

    renderDiagram() {
        const container = document.getElementById('ls-diagram');
        if (!container) return;
        container.innerHTML = '';

        const nodes = this.graphData.nodes;
        const links = this.graphData.links;

        if (nodes.length === 0) {
            this.showMessage('No nodes to display.');
            return;
        }

        // Node dimensions - wider for readability and ensure all text fits
        const nodeWidth = 200;
        const nodeHeaderHeight = 32;
        const propertyLineHeight = 18;
        const nodePadding = 16;

        // Calculate node heights based on content
        nodes.forEach(node => {
            const propsToShow = Math.min(node.properties.length, 4);
            node.height = nodeHeaderHeight + propsToShow * propertyLineHeight + nodePadding + 20; // +20 for dataset dots
        });

        // Get container dimensions for fitting
        const containerRect = container.getBoundingClientRect();
        const containerWidth = containerRect.width || 1200;
        const containerHeight = containerRect.height || 400;

        // Use D3 tree layout with spacing multipliers
        D3DiagramUtils.computeTreeLayout(nodes, links, {
            nodeWidth: nodeWidth,
            containerWidth: containerWidth,
            containerHeight: containerHeight,
            xSpacing: this.xSpacing,
            ySpacing: this.ySpacing
        });

        // Calculate actual content bounds
        const padding = 40;
        const minX = Math.min(...nodes.map(n => n.x)) - padding;
        const minY = Math.min(...nodes.map(n => n.y)) - padding;
        const maxX = Math.max(...nodes.map(n => n.x + nodeWidth)) + padding;
        const maxY = Math.max(...nodes.map(n => n.y + n.height)) + padding;
        
        const contentWidth = maxX - minX;
        const contentHeight = maxY - minY;

        // Create SVG - use 100% height so it adapts to container
        this.svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', [minX, minY, contentWidth, contentHeight])
            .attr('preserveAspectRatio', 'xMidYMid meet');

        // Styles
        this.svg.append('style').text(`
            .node-group { cursor: grab; }
            .node-group:active { cursor: grabbing; }
            .node-group:hover .node-rect { stroke-width: 2px; stroke: #0969da; }
            .node-group.dimmed { opacity: 0.15; }
            .node-group.path-node .node-rect { stroke: #e85d04; stroke-width: 3px; }
            .node-group.path-start .node-rect { stroke: #2da44e; stroke-width: 3px; }
            .link-group { pointer-events: none; }
            .link-group.dimmed { opacity: 0.1; }
            .link-group.path-link path { stroke: #e85d04; stroke-width: 2.5px; }
            .node-header { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; }
            .node-property { font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace; }
            .node-group.legend-dimmed { opacity: 0.12; }
            .link-group.legend-dimmed { opacity: 0.08; }
        `);

        // Zoom
        this.zoom = d3.zoom()
            .scaleExtent([0.15, 100])
            .on('zoom', e => g.attr('transform', e.transform));
        this.svg.call(this.zoom);

        const g = this.svg.append('g');

        // Arrowhead marker - refX set to 0 so arrow tip is at path end
        this.svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 8)
            .attr('markerHeight', 8)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-4L10,0L0,4Z')
            .attr('fill', '#666');

        // Build node lookup
        const nodeById = new Map(nodes.map(n => [n.id, n]));

        // Links layer (drawn first, behind nodes)
        const linkLayer = g.append('g').attr('class', 'links-layer');
        
        // Nodes layer
        const nodeLayer = g.append('g').attr('class', 'nodes-layer');

        // Assign edge offsets to prevent overlapping edges from same source
        D3DiagramUtils.assignEdgeOffsets(links);

        // Draw links
        const linkGroups = linkLayer.selectAll('g')
            .data(links)
            .join('g')
            .attr('class', 'link-group');

        linkGroups.each((d, i, elements) => {
            const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
            const targetId = typeof d.target === 'object' ? d.target.id : d.target;
            const source = nodeById.get(sourceId);
            const target = nodeById.get(targetId);
            if (!source || !target) return;

            // Choose edge path function based on curvedEdges setting
            const pathFn = this.curvedEdges 
                ? D3DiagramUtils.createCurvedEdgePath 
                : D3DiagramUtils.createEdgePath;

            d3.select(elements[i]).append('path')
                .attr('class', 'link-path')
                .attr('d', pathFn(source, target, nodeWidth, d.edgeOffset || 0))
                .attr('fill', 'none')
                .attr('stroke', '#aaa')
                .attr('stroke-width', 1.5)
                .attr('marker-end', 'url(#arrow)');

            // Place label with offset to avoid overlapping when multiple edges exist
            const labelPos = D3DiagramUtils.getEdgeLabelPosition(
                source, target, nodeWidth, 
                d.edgeOffset || 0, d.edgeIndex || 0, d.edgeCount || 1
            );
            d3.select(elements[i]).append('text')
                .attr('x', labelPos.x)
                .attr('y', labelPos.y)
                .attr('font-size', '10px')
                .attr('fill', '#555')
                .attr('text-anchor', 'middle')
                .attr('class', 'link-label')
                .text(d.label.length > 22 ? d.label.substring(0, 20) + '..' : d.label);
        });

        // Draw nodes
        const self = this;
        const nodeGroups = nodeLayer.selectAll('g')
            .data(nodes)
            .join('g')
            .attr('class', 'node-group')
            .attr('transform', d => `translate(${d.x}, ${d.y})`)
            .on('click', (e, d) => this.handleNodeClick(d))
            .call(this.createDrag(nodeById, linkGroups, nodeWidth));

        // Node shadow
        nodeGroups.append('rect')
            .attr('x', 2)
            .attr('y', 2)
            .attr('width', nodeWidth)
            .attr('height', d => d.height)
            .attr('fill', '#00000010')
            .attr('rx', 6);

        // Node rectangle (main body)
        nodeGroups.append('rect')
            .attr('class', 'node-rect')
            .attr('width', nodeWidth)
            .attr('height', d => d.height)
            .attr('fill', 'white')
            .attr('stroke', '#c9d1d9')
            .attr('stroke-width', 1)
            .attr('rx', 6);

        // Header background
        nodeGroups.append('rect')
            .attr('width', nodeWidth)
            .attr('height', nodeHeaderHeight)
            .attr('fill', '#f0f3f6')
            .attr('rx', 6);

        // Fix header corners at bottom
        nodeGroups.append('rect')
            .attr('y', nodeHeaderHeight - 6)
            .attr('width', nodeWidth)
            .attr('height', 6)
            .attr('fill', '#f0f3f6');

        // Header separator line
        nodeGroups.append('line')
            .attr('x1', 0)
            .attr('x2', nodeWidth)
            .attr('y1', nodeHeaderHeight)
            .attr('y2', nodeHeaderHeight)
            .attr('stroke', '#d0d7de');

        // Class name (larger, readable, full width)
        nodeGroups.append('text')
            .attr('class', 'node-header')
            .attr('x', nodeWidth / 2)
            .attr('y', 22)
            .attr('text-anchor', 'middle')
            .attr('font-size', '13px')
            .attr('font-weight', '600')
            .attr('fill', '#1f2328')
            .text(d => d.label.length > 24 ? d.label.substring(0, 22) + '..' : d.label);

        // Properties section
        nodeGroups.each(function(d) {
            const propsToShow = d.properties.slice(0, 4);
            propsToShow.forEach((prop, i) => {
                d3.select(this).append('text')
                    .attr('class', 'node-property')
                    .attr('x', 10)
                    .attr('y', nodeHeaderHeight + 16 + i * propertyLineHeight)
                    .attr('font-size', '11px')
                    .attr('fill', '#57606a')
                    .text(prop.length > 26 ? prop.substring(0, 24) + '..' : prop);
            });
        });

        // Dataset provenance dots at bottom
        nodeGroups.each(function(d) {
            const dotRadius = 5;
            const spacing = 12;
            // Only show dots for datasets this node actually appears in
            const datasets = Array.isArray(d.datasets) ? d.datasets : [];
            const totalWidth = (datasets.length - 1) * spacing;
            const startX = (nodeWidth - totalWidth) / 2;
            const y = d.height - 10;

            datasets.forEach((ds, i) => {
                const color = window.lsCloud.getDatasetColor(ds);
                d3.select(this).append('circle')
                    .attr('cx', startX + i * spacing)
                    .attr('cy', y)
                    .attr('r', dotRadius)
                    .attr('fill', color)
                    .attr('stroke', 'white')
                    .attr('stroke-width', 1.5)
                    .append('title').text(ds);
            });
        });

        // Tooltips
        nodeGroups.append('title')
            .text(d => `${d.id}\n\nDatasets: ${d.datasets.join(', ')}\nConnections: ${d.degree}`);

        // Apply legend-based dimming if a subset is active
        if (this.legendSubset && this.legendSubset.size > 0) {
            nodeGroups.classed('legend-dimmed', d => {
                // keep nodes that belong to any selected legend dataset; dim others
                return !((d.datasets || []).some(ds => this.legendSubset.has(ds)));
            });

            linkGroups.classed('legend-dimmed', l => {
                return !((l.datasets || []).some(ds => this.legendSubset.has(ds)));
            });
        } else {
            // clear legend dimming
            nodeGroups.classed('legend-dimmed', false);
            linkGroups.classed('legend-dimmed', false);
        }

        // Zoom controls
        const zoomFit = document.getElementById('ls-zoom-fit');
        const zoomIn = document.getElementById('ls-zoom-in');
        const zoomOut = document.getElementById('ls-zoom-out');
        
        if (zoomFit) zoomFit.onclick = () => this.svg.transition().duration(300).call(this.zoom.transform, d3.zoomIdentity);
        if (zoomIn) zoomIn.onclick = () => this.svg.transition().duration(200).call(this.zoom.scaleBy, 1.3);
        if (zoomOut) zoomOut.onclick = () => this.svg.transition().duration(200).call(this.zoom.scaleBy, 0.7);
        
        // If a node is selected in the node filter, focus (center) it in the viewport
        try {
            const nodeFilter = document.getElementById('ls-node-filter');
            if (nodeFilter) {
                let sel = null;
                if (typeof $ !== 'undefined' && $.fn.selectpicker) sel = $(nodeFilter).val();
                else sel = nodeFilter.value;
                if (Array.isArray(sel)) sel = sel[0] || null;
                if (sel) {
                    const targetNode = nodeById.get(sel);
                    if (targetNode) {
                        // compute translation to center the node in viewBox coordinates
                        const nodeCenterX = targetNode.x + nodeWidth / 2;
                        const nodeCenterY = targetNode.y + (targetNode.height || 0) / 2;
                        const tx = (width / 2) - (nodeCenterX - minX);
                        const ty = (height / 2) - (nodeCenterY - minY);
                        this.svg.transition().duration(600).call(this.zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(1));
                    }
                }
            }
        } catch (e) {
            // non-fatal
            console.warn('Focus on node failed:', e);
        }
        // If intersection mode is active, apply dimming after drawing
        if (this.intersectionMode) this.applyIntersectionDimming();
    }

    // Create drag behavior for nodes using shared D3DiagramUtils
    createDrag(nodeById, linkGroups, nodeWidth) {
        const self = this;
        return D3DiagramUtils.createDragBehavior({
            nodeById: nodeById,
            linkGroups: linkGroups,
            nodeWidth: nodeWidth,
            getCurvedEdges: () => self.curvedEdges
        });
    }

    showMessage(msg) {
        const container = document.getElementById('ls-diagram');
        if (container) {
            container.innerHTML = `<div style="display: flex; align-items: center; justify-content: center; height: 300px; color: #656d76;">${msg}</div>`;
        }
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    fetch('results.json')
        .then(r => r.json())
        .then(data => { window.lsCloud = new lsCloudVisualization(data.datasets || []); })
        .catch(err => console.error('Failed to load results.json:', err));
});
