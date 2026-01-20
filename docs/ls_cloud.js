/**
 * RDFSolve Linked Schema Cloud Visualization
 * 
 * Class diagram layout with:
 * - D3 tree layout for hierarchical positioning
 * - Orthogonal edges (right angles only)
 * - OWL/RDF(S)/XSD node exclusion
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
    this.intersectionMode = false;
        
        // Spacing multipliers (controlled by sliders)
        this.xSpacing = 1.0;
        this.ySpacing = 1.0;
        
        // Prefixes to exclude
        this.excludedPrefixes = [
            'rdf',
            'rdfs',
            'owl',
            'sh',
            'sparqlserv'
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

    init() {
        this.allDatasetNames = this.datasets
            .filter(d => d.dataFiles?.coverage && d.notebooks?.schema?.status === 'success')
            .map(d => d.name)
            .sort();
        
        this.setupDatasetSelector();
        this.setupEventListeners();
        this.setupSpacingSliders();
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
        
        // Node filter with Bootstrap Select
        const nodeFilter = document.getElementById('ls-node-filter');
        if (nodeFilter && typeof $ !== 'undefined' && $.fn.selectpicker) {
            $(nodeFilter).on('changed.bs.select', (e) => {
                this.filterByNode($(nodeFilter).val() || '');
            });
        } else if (nodeFilter) {
            nodeFilter.addEventListener('change', e => this.filterByNode(e.target.value));
        }
        
        document.getElementById('ls-neighbor-depth')?.addEventListener('change', () => {
            const nodeFilter = document.getElementById('ls-node-filter');
            const node = (typeof $ !== 'undefined' && $.fn.selectpicker) 
                ? ($(nodeFilter).val() || '') 
                : nodeFilter?.value;
            if (node) this.filterByNode(node);
        });
        document.getElementById('ls-find-path-btn')?.addEventListener('click', () => this.togglePathFindingMode());
        
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

    setupSpacingSliders() {
        const xSlider = document.getElementById('ls-x-spacing');
        const ySlider = document.getElementById('ls-y-spacing');
        const xVal = document.getElementById('ls-x-spacing-val');
        const yVal = document.getElementById('ls-y-spacing-val');

        if (xSlider) {
            xSlider.addEventListener('input', (e) => {
                this.xSpacing = parseInt(e.target.value) / 100;
                if (xVal) xVal.textContent = `${e.target.value}%`;
                if (this.graphData.nodes.length > 0) this.renderDiagram();
            });
        }

        if (ySlider) {
            ySlider.addEventListener('input', (e) => {
                this.ySpacing = parseInt(e.target.value) / 100;
                if (yVal) yVal.textContent = `${e.target.value}%`;
                if (this.graphData.nodes.length > 0) this.renderDiagram();
            });
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
            if (!subjectExcluded && !objectExcluded && subjectUri !== objectUri) {
                const key = `${subjectUri}|||${objectUri}`;
                if (!linkMap.has(key)) {
                    linkMap.set(key, {
                        source: subjectUri,
                        target: objectUri,
                        properties: new Set(),
                        datasets: new Set()
                    });
                }
                linkMap.get(key).properties.add(propertyLabel);
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
            properties: Array.from(l.properties),
            datasets: Array.from(l.datasets),
            label: Array.from(l.properties).slice(0, 2).join(', ')
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
        this.highlightedPath = [];
        
        const btn = document.getElementById('ls-find-path-btn');
        const status = document.getElementById('ls-path-status');
        
        if (this.pathFindingMode) {
            btn.style.background = '#0969da';
            btn.style.color = 'white';
            status.textContent = 'Click first node...';
        } else {
            btn.style.background = '';
            btn.style.color = '';
            status.textContent = '';
            this.clearHighlights();
        }
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
                status.textContent = `Path: ${path.length} nodes`;
                this.highlightPath(path);
            } else {
                status.textContent = 'No path found';
            }
            this.selectedNodes = [];
        }
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

    clearHighlights() {
        d3.selectAll('.node-group').classed('path-node path-start dimmed', false);
        d3.selectAll('.link-group').classed('path-link dimmed', false);
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

        // Node dimensions - wider for readability
        const nodeWidth = 160;
        const nodeHeaderHeight = 28;
        const propertyLineHeight = 16;
        const nodePadding = 12;

        // Calculate node heights based on content
        nodes.forEach(node => {
            const propsToShow = Math.min(node.properties.length, 4);
            node.height = nodeHeaderHeight + propsToShow * propertyLineHeight + nodePadding + 16; // +16 for dataset dots
        });

        // Get container dimensions for fitting
        const containerRect = container.getBoundingClientRect();
        const containerWidth = containerRect.width || 1200;
        const containerHeight = Math.max(500, containerRect.height || 600);

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
        
        // Use container size as viewport, content size as viewBox
        const width = containerWidth;
        const height = containerHeight;

        // Create SVG
        this.svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', height)
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

        // Arrowhead marker
        this.svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10)
            .attr('refY', 0)
            .attr('markerWidth', 7)
            .attr('markerHeight', 7)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-4L10,0L0,4')
            .attr('fill', '#666');

        // Build node lookup
        const nodeById = new Map(nodes.map(n => [n.id, n]));

        // Links layer (drawn first, behind nodes)
        const linkLayer = g.append('g').attr('class', 'links-layer');
        
        // Nodes layer
        const nodeLayer = g.append('g').attr('class', 'nodes-layer');

        // Assign edge offsets to prevent overlapping edges from same source
        this.assignEdgeOffsets(links);

        // Draw links
        const linkGroups = linkLayer.selectAll('g')
            .data(links)
            .join('g')
            .attr('class', 'link-group');

        linkGroups.each((d, i, elements) => {
            const source = nodeById.get(d.source);
            const target = nodeById.get(d.target);
            if (!source || !target) return;
            
            d3.select(elements[i]).append('path')
                .attr('class', 'link-path')
                .attr('d', this.createEdgePath(source, target, nodeWidth, d.edgeOffset || 0))
                .attr('fill', 'none')
                .attr('stroke', '#aaa')
                .attr('stroke-width', 1.5)
                .attr('marker-end', 'url(#arrow)');
        });

        // Link labels (on the path midpoint)
        linkGroups.each((d, i, elements) => {
            const source = nodeById.get(d.source);
            const target = nodeById.get(d.target);
            if (!source || !target) return;
            
            const labelPos = this.getEdgeLabelPosition(source, target, nodeWidth);
            
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

        // Class name (larger, readable)
        nodeGroups.append('text')
            .attr('class', 'node-header')
            .attr('x', nodeWidth / 2)
            .attr('y', 18)
            .attr('text-anchor', 'middle')
            .attr('font-size', '12px')
            .attr('font-weight', '600')
            .attr('fill', '#1f2328')
            .text(d => d.label.length > 18 ? d.label.substring(0, 16) + '..' : d.label);

        // Properties section
        nodeGroups.each(function(d) {
            const propsToShow = d.properties.slice(0, 4);
            propsToShow.forEach((prop, i) => {
                d3.select(this).append('text')
                    .attr('class', 'node-property')
                    .attr('x', 8)
                    .attr('y', nodeHeaderHeight + 14 + i * propertyLineHeight)
                    .attr('font-size', '10px')
                    .attr('fill', '#57606a')
                    .text(prop.length > 20 ? prop.substring(0, 18) + '..' : prop);
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

    // Assign offsets to edges sharing the same source to prevent overlap
    assignEdgeOffsets(links) {
        // Group links by source
        const bySource = new Map();
        links.forEach(link => {
            if (!bySource.has(link.source)) bySource.set(link.source, []);
            bySource.get(link.source).push(link);
        });

        // Assign offsets within each group
        bySource.forEach((group, sourceId) => {
            if (group.length <= 1) {
                group.forEach(l => l.edgeOffset = 0);
                return;
            }
            // Center the offsets around 0
            const mid = (group.length - 1) / 2;
            group.forEach((link, i) => {
                link.edgeOffset = i - mid;
            });
        });

        // Also offset edges going to the same target
        const byTarget = new Map();
        links.forEach(link => {
            if (!byTarget.has(link.target)) byTarget.set(link.target, []);
            byTarget.get(link.target).push(link);
        });

        byTarget.forEach((group, targetId) => {
            if (group.length <= 1) return;
            const mid = (group.length - 1) / 2;
            group.forEach((link, i) => {
                // Add to existing offset
                link.edgeOffset = (link.edgeOffset || 0) + (i - mid) * 0.5;
            });
        });
    }

    // Create drag behavior for nodes
    createDrag(nodeById, linkGroups, nodeWidth) {
        const self = this;
        
        return d3.drag()
            .on('start', function(event, d) {
                d3.select(this).raise();
            })
            .on('drag', function(event, d) {
                d.x = event.x;
                d.y = event.y;
                d3.select(this).attr('transform', `translate(${d.x}, ${d.y})`);
                
                // Update connected links
                linkGroups.each(function(link) {
                    const source = nodeById.get(link.source);
                    const target = nodeById.get(link.target);
                    if (!source || !target) return;
                    
                    d3.select(this).select('.link-path')
                        .attr('d', self.createEdgePath(source, target, nodeWidth, link.edgeOffset || 0));
                    
                    // Update label position
                    const labelPos = self.getEdgeLabelPosition(source, target, nodeWidth);
                    d3.select(this).select('.link-label')
                        .attr('x', labelPos.x)
                        .attr('y', labelPos.y);
                });
            });
    }

    // Create edge path with offset to avoid overlaps
    createEdgePath(source, target, nodeWidth, offset = 0) {
        const sx = source.x + nodeWidth / 2;
        const sy = source.y + source.height;
        const tx = target.x + nodeWidth / 2;
        const ty = target.y;

        // Apply horizontal offset to avoid overlapping edges
        const offsetX = offset * 15;

        // Target is below source (normal tree flow)
        if (ty > sy + 10) {
            // Stagger the vertical midpoint based on offset to separate parallel edges
            const midY = sy + (ty - sy) * (0.4 + offset * 0.1);
            return `M ${sx} ${sy} V ${midY} H ${tx + offsetX} V ${ty}`;
        }
        
        // Back-edge (target is above or same level)
        const loopOffset = 40 + Math.abs(offset) * 20;
        const goRight = tx >= sx;
        const routeX = goRight ? 
            Math.max(source.x + nodeWidth, target.x + nodeWidth) + loopOffset :
            Math.min(source.x, target.x) - loopOffset;
        
        return `M ${sx} ${sy} V ${sy + 25} H ${routeX} V ${ty - 25} H ${tx} V ${ty}`;
    }

    // Get label position for an edge - positioned right next to the edge path
    getEdgeLabelPosition(source, target, nodeWidth) {
        const sx = source.x + nodeWidth / 2;
        const sy = source.y + source.height;
        const tx = target.x + nodeWidth / 2;
        const ty = target.y;

        // For vertical edges (going down), place label on the right side of the vertical segment
        if (ty > sy) {
            // Orthogonal path goes: down from source, then horizontal, then down to target
            // Place label at the midpoint of the path, slightly offset from the line
            const midY = sy + (ty - sy) / 2;
            const midX = (sx + tx) / 2;
            
            // If mostly vertical, place label just to the right of the line
            if (Math.abs(tx - sx) < 50) {
                return { x: sx + 8, y: midY };
            }
            // If there's a horizontal segment, place at the corner
            return { x: midX + 8, y: sy + 30 };
        }
        
        // For edges going up or same level
        return { x: (sx + tx) / 2 + 8, y: Math.min(sy, ty) - 8 };
    }

    // Wrapper for orthogonalPath that assigns edge offsets
    orthogonalPath(source, target, nodeWidth) {
        return this.createEdgePath(source, target, nodeWidth, 0);
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
