/**
 * RDFSolve Schema Diagram Module using D3.js
 * Single-dataset class diagram visualization in side panel
 */

class SchemaDiagram {
    constructor(datasets) {
        this.datasets = datasets;
        this.currentDataset = null;
        this.coverageData = null;
        this.githubRawBase = 'https://raw.githubusercontent.com/jmillanacosta/rdfsolve/main/docs/';
        
        this.svg = null;
        this.zoom = null;
        this.graphData = { nodes: [], links: [] };
        
        // Configuration
        this.config = {
            nodeWidth: 180,
            nodeHeaderHeight: 28,
            propertyLineHeight: 16,
            nodePadding: 12,
            maxPatterns: 80
        };
        
        this.excludeOwl = true;
        
        // Spacing multipliers (controlled by sliders)
        this.xSpacing = 1.0;
        this.ySpacing = 1.0;
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupSpacingSliders();
    }

    setupEventListeners() {
        // Delegated click handlers for view buttons
        document.body.addEventListener('click', (e) => {
            if (e.target.classList.contains('view-schema-btn')) {
                this.loadSchema(e.target.dataset.dataset);
            }
            if (e.target.classList.contains('close-panel-btn') && e.target.dataset.panel === 'schema') {
                this.closeSidebar();
            }
        });

        // Exclude OWL checkbox
        document.getElementById('schema-exclude-owl')?.addEventListener('change', (e) => {
            this.excludeOwl = e.target.checked;
            if (this.coverageData) this.renderDiagram();
        });

        // Zoom controls
        document.getElementById('zoom-in')?.addEventListener('click', () => this.zoomBy(1.3));
        document.getElementById('zoom-out')?.addEventListener('click', () => this.zoomBy(0.7));
        document.getElementById('zoom-fit')?.addEventListener('click', () => this.zoomFit());
    }

    setupSpacingSliders() {
        const xSlider = document.getElementById('schema-x-spacing');
        const ySlider = document.getElementById('schema-y-spacing');
        const xVal = document.getElementById('schema-x-spacing-val');
        const yVal = document.getElementById('schema-y-spacing-val');

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

    async loadSchema(datasetName) {
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
            this.coverageData = D3DiagramUtils.parseCSV(csvText);
            
            this.renderSchemaInfo();
            this.renderDiagram();
        } catch (error) {
            console.error('Failed to load schema data:', error);
            this.showError('Failed to load schema data. The file may not be available.');
        }
    }

    renderSchemaInfo() {
        const infoContainer = document.getElementById('schema-info');
        if (!infoContainer || !this.coverageData) return;

        const uniqueClasses = new Set();
        const uniqueProperties = new Set();
        this.coverageData.forEach(row => {
            if (row.subject_class) uniqueClasses.add(row.subject_class);
            if (row.object_class) uniqueClasses.add(row.object_class);
            if (row.property) uniqueProperties.add(row.property);
        });

        infoContainer.innerHTML = `
            <div class="schema-info-grid">
                <div class="info-item"><strong>Dataset:</strong> ${this.currentDataset.name}</div>
                <div class="info-item"><strong>Classes:</strong> ${uniqueClasses.size}</div>
                <div class="info-item"><strong>Properties:</strong> ${uniqueProperties.size}</div>
                <div class="info-item"><strong>Relationships:</strong> ${this.coverageData.length}</div>
            </div>
        `;
    }

    renderDiagram() {
        const container = document.getElementById('schema-diagram');
        if (!container || !this.coverageData) return;

        container.innerHTML = '';

        // Build graph data
        this.graphData = D3DiagramUtils.buildGraphFromCoverage(this.coverageData, {
            maxPatterns: this.config.maxPatterns,
            excludeOwl: this.excludeOwl
        });

        const { nodes, links } = this.graphData;

        if (nodes.length === 0) {
            container.innerHTML = '<div class="empty-message">No data to display. Try unchecking "Exclude OWL/RDF(S)" if enabled.</div>';
            return;
        }

        // Calculate node heights
        nodes.forEach(node => {
            const propsToShow = Math.min(node.properties.length, 4);
            node.height = this.config.nodeHeaderHeight + propsToShow * this.config.propertyLineHeight + this.config.nodePadding;
        });

        // Compute layout
        D3DiagramUtils.computeHierarchicalLayout(nodes, links, this.config);

        // Calculate SVG dimensions
        const padding = 40;
        const maxX = Math.max(...nodes.map(n => n.x + this.config.nodeWidth)) + padding;
        const maxY = Math.max(...nodes.map(n => n.y + n.height)) + padding;
        const width = Math.max(600, maxX);
        const height = Math.max(400, maxY);

        // Create SVG
        this.svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', [0, 0, width, height])
            .attr('preserveAspectRatio', 'xMidYMid meet');

        // Add styles
        this.svg.append('style').text(D3DiagramUtils.getSvgStyles());

        // Setup zoom
        this.zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', e => g.attr('transform', e.transform));
        this.svg.call(this.zoom);

        const g = this.svg.append('g');

        // Arrowhead marker
        this.svg.append('defs').append('marker')
            .attr('id', 'schema-arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 8)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#57606a');

        // Render edges
        const linkGroups = g.selectAll('.link-group')
            .data(links)
            .enter()
            .append('g')
            .attr('class', 'link-group');

        linkGroups.append('path')
            .attr('d', d => {
                const source = nodes.find(n => n.id === d.source);
                const target = nodes.find(n => n.id === d.target);
                if (!source || !target) return '';
                return D3DiagramUtils.createEdgePath(source, target, this.config.nodeWidth);
            })
            .attr('fill', 'none')
            .attr('stroke', '#8b949e')
            .attr('stroke-width', 1.5)
            .attr('marker-end', 'url(#schema-arrowhead)');

        // Edge labels
        linkGroups.append('text')
            .attr('x', d => {
                const source = nodes.find(n => n.id === d.source);
                const target = nodes.find(n => n.id === d.target);
                if (!source || !target) return 0;
                return D3DiagramUtils.getEdgeLabelPosition(source, target, this.config.nodeWidth).x;
            })
            .attr('y', d => {
                const source = nodes.find(n => n.id === d.source);
                const target = nodes.find(n => n.id === d.target);
                if (!source || !target) return 0;
                return D3DiagramUtils.getEdgeLabelPosition(source, target, this.config.nodeWidth).y;
            })
            .attr('font-size', '9px')
            .attr('fill', '#57606a')
            .text(d => d.label.length > 20 ? d.label.substring(0, 18) + '...' : d.label);

        // Render nodes
        const nodeGroups = g.selectAll('.node-group')
            .data(nodes)
            .enter()
            .append('g')
            .attr('class', 'node-group')
            .attr('transform', d => `translate(${d.x}, ${d.y})`);

        // Node background
        nodeGroups.append('rect')
            .attr('class', 'node-rect')
            .attr('width', this.config.nodeWidth)
            .attr('height', d => d.height)
            .attr('rx', 4)
            .attr('fill', '#ffffff')
            .attr('stroke', '#d0d7de')
            .attr('stroke-width', 1);

        // Node header background
        nodeGroups.append('rect')
            .attr('width', this.config.nodeWidth)
            .attr('height', this.config.nodeHeaderHeight)
            .attr('rx', 4)
            .attr('fill', '#f6f8fa');

        // Cover bottom corners of header
        nodeGroups.append('rect')
            .attr('y', this.config.nodeHeaderHeight - 4)
            .attr('width', this.config.nodeWidth)
            .attr('height', 4)
            .attr('fill', '#f6f8fa');

        // Node header text
        nodeGroups.append('text')
            .attr('class', 'node-header')
            .attr('x', this.config.nodeWidth / 2)
            .attr('y', 18)
            .attr('text-anchor', 'middle')
            .attr('font-size', '11px')
            .attr('font-weight', '600')
            .attr('fill', '#24292f')
            .text(d => d.label.length > 22 ? d.label.substring(0, 20) + '...' : d.label);

        // Node properties
        nodeGroups.each((d, i, nodeElements) => {
            const nodeEl = d3.select(nodeElements[i]);
            d.properties.slice(0, 4).forEach((prop, j) => {
                nodeEl.append('text')
                    .attr('class', 'node-property')
                    .attr('x', 8)
                    .attr('y', this.config.nodeHeaderHeight + 14 + j * this.config.propertyLineHeight)
                    .attr('font-size', '10px')
                    .attr('fill', '#57606a')
                    .text(prop.length > 22 ? prop.substring(0, 20) + '...' : prop);
            });
        });

        // Tooltips
        nodeGroups.append('title')
            .text(d => `${d.id}\n\nProperties: ${d.properties.join(', ')}\nConnections: ${d.degree}`);

        // Make nodes draggable
        this.setupDrag(nodeGroups, nodes, links, linkGroups, this.config.nodeWidth);
    }

    setupDrag(nodeGroups, nodes, links, linkGroups, nodeWidth) {
        const self = this;
        
        const drag = d3.drag()
            .on('start', function(event, d) {
                d3.select(this).raise().classed('dragging', true);
            })
            .on('drag', function(event, d) {
                d.x = event.x;
                d.y = event.y;
                d3.select(this).attr('transform', `translate(${d.x}, ${d.y})`);

                // Update connected edges
                linkGroups.select('path')
                    .attr('d', l => {
                        const source = nodes.find(n => n.id === l.source);
                        const target = nodes.find(n => n.id === l.target);
                        if (!source || !target) return '';
                        return D3DiagramUtils.createEdgePath(source, target, nodeWidth);
                    });

                linkGroups.select('text')
                    .attr('x', l => {
                        const source = nodes.find(n => n.id === l.source);
                        const target = nodes.find(n => n.id === l.target);
                        if (!source || !target) return 0;
                        return D3DiagramUtils.getEdgeLabelPosition(source, target, nodeWidth).x;
                    })
                    .attr('y', l => {
                        const source = nodes.find(n => n.id === l.source);
                        const target = nodes.find(n => n.id === l.target);
                        if (!source || !target) return 0;
                        return D3DiagramUtils.getEdgeLabelPosition(source, target, nodeWidth).y;
                    });
            })
            .on('end', function(event, d) {
                d3.select(this).classed('dragging', false);
            });

        nodeGroups.call(drag);
    }

    zoomBy(factor) {
        if (this.svg && this.zoom) {
            this.svg.transition().duration(200).call(this.zoom.scaleBy, factor);
        }
    }

    zoomFit() {
        if (this.svg && this.zoom) {
            this.svg.transition().duration(300).call(this.zoom.transform, d3.zoomIdentity);
        }
    }

    showLoading() {
        const container = document.getElementById('schema-diagram');
        if (container) {
            container.innerHTML = '<div class="loading" style="display: flex; align-items: center; justify-content: center; height: 200px; color: #656d76;">Loading schema...</div>';
        }
    }

    showError(message) {
        const container = document.getElementById('schema-diagram');
        if (container) {
            container.innerHTML = `<div class="error-message" style="padding: 20px; color: #cf222e;">${message}</div>`;
        }
    }

    openSidebar() {
        // Close coverage panel if open
        document.getElementById('coverage-panel')?.classList.remove('open');
        
        document.getElementById('schema-panel')?.classList.add('open');
        document.getElementById('main-content')?.classList.add('panel-open');
    }

    closeSidebar() {
        document.getElementById('schema-panel')?.classList.remove('open');
        document.getElementById('main-content')?.classList.remove('panel-open');
    }
}

// Add schema and coverage buttons to dataset cards dynamically
function addSchemaButtons() {
    document.querySelectorAll('.dataset-card').forEach(card => {
        const datasetName = card.querySelector('.dataset-name')?.textContent.trim();
        if (datasetName && !card.querySelector('.view-schema-btn')) {
            const actionsDiv = card.querySelector('.dataset-actions') || card;
            
            const schemaBtn = document.createElement('button');
            schemaBtn.className = 'view-schema-btn action-btn';
            schemaBtn.dataset.dataset = datasetName;
            schemaBtn.textContent = 'Schema';
            
            const coverageBtn = document.createElement('button');
            coverageBtn.className = 'view-coverage-btn action-btn';
            coverageBtn.dataset.dataset = datasetName;
            coverageBtn.textContent = 'Coverage';
            
            actionsDiv.appendChild(schemaBtn);
            actionsDiv.appendChild(coverageBtn);
        }
    });
}

window.SchemaDiagram = SchemaDiagram;
window.addSchemaButtons = addSchemaButtons;
