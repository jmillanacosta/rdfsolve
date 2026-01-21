/**
 * RDFSolve Schema Diagram Module using D3.js
 * Single-dataset class diagram visualization in side panel
 * Uses shared utilities from D3DiagramUtils
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
            nodeWidth: 200,
            nodeHeaderHeight: 32,
            propertyLineHeight: 18,
            nodePadding: 14,
            maxPatterns: 80
        };
        
        this.excludeOwl = true;
        
        // Spacing multipliers (controlled by +/- buttons)
        this.xSpacing = 1.0;
        this.ySpacing = 2.0;
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupSpacingControls();
        this.setupFullscreenButton();
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

        // Zoom controls using shared utility
        D3DiagramUtils.setupZoomControls({
            zoomInId: 'zoom-in',
            zoomOutId: 'zoom-out',
            zoomFitId: 'zoom-fit',
            getSvgAndZoom: () => ({ svg: this.svg, zoom: this.zoom })
        });
    }

    setupSpacingControls() {
        // Use shared spacing control utility
        const spacingController = D3DiagramUtils.setupSpacingControls({
            xPlusId: 'schema-x-plus',
            xMinusId: 'schema-x-minus',
            xValId: 'schema-x-spacing-val',
            yPlusId: 'schema-y-plus',
            yMinusId: 'schema-y-minus',
            yValId: 'schema-y-spacing-val',
            initialX: 1.0,
            initialY: 8.0,
            step: 0.2,
            onChange: (xSpacing, ySpacing) => {
                this.xSpacing = xSpacing;
                this.ySpacing = ySpacing;
                if (this.graphData.nodes.length > 0) {
                    this.renderDiagram();
                }
            }
        });

        // Keep reference for external access
        this.spacingController = spacingController;
    }

    setupFullscreenButton() {
        const fullscreenBtn = document.getElementById('schema-fullscreen-btn');
        const container = document.getElementById('schema-panel');
        
        if (fullscreenBtn && container) {
            fullscreenBtn.addEventListener('click', () => {
                D3DiagramUtils.toggleFullscreen(container, 'schema-fullscreen', (isFullscreen) => {
                    fullscreenBtn.textContent = isFullscreen ? ' Exit' : ' Fullscreen';
                    // Re-render to fit new size
                    if (this.graphData.nodes.length > 0) {
                        setTimeout(() => this.renderDiagram(), 100);
                    }
                });
            });
        }
    }

    async loadSchema(datasetName) {
        console.log('loadSchema called with:', datasetName);
        this.currentDataset = this.datasets.find(d => d.name === datasetName);
        if (!this.currentDataset?.dataFiles?.coverage) {
            console.error('Dataset not found or no coverage data');
            return;
        }

        // Set the dataset name in the panel header
        const nameSpan = document.getElementById('schema-dataset-name');
        if (nameSpan) nameSpan.textContent = datasetName;

        this.showLoading();
        this.openSidebar();

        try {
            const coveragePath = this.currentDataset.dataFiles.coverage
                .replace(/^(\.\.\/)+/, '')
                .replace(/^\.?\//, '');
            const coverageUrl = this.githubRawBase + coveragePath;
            console.log('Fetching coverage from:', coverageUrl);

            const response = await fetch(coverageUrl);
            console.log('Fetch response:', response.status, response.ok);
            if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);

            const csvText = await response.text();
            this.coverageData = D3DiagramUtils.parseCSV(csvText);
            
            this.renderSchemaInfo();
            // Delay render to allow panel transition to complete and get proper dimensions
            setTimeout(() => this.renderDiagram(), 150);
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
        try {
            const container = document.getElementById('schema-diagram');
            console.log('SchemaDiagram.renderDiagram called', {
                container: !!container,
                coverageData: !!this.coverageData,
                dataLength: this.coverageData?.length,
            containerWidth: container?.clientWidth,
            containerHeight: container?.clientHeight
        });
        if (!container || !this.coverageData) return;

        container.innerHTML = '';

        // Build graph data using shared utility
        this.graphData = D3DiagramUtils.buildGraphFromCoverage(this.coverageData, {
            maxPatterns: this.config.maxPatterns,
            excludeOwl: this.excludeOwl
        });

        const { nodes, links } = this.graphData;
        console.log('Graph data built:', { nodesCount: nodes.length, linksCount: links.length });
        if (nodes.length === 0) {
            container.innerHTML = '<div style="padding: 20px; text-align: center; color: #656d76;">No data to display</div>';
            return;
        }

        // Calculate node heights
        nodes.forEach(n => {
            const propsCount = Math.min(n.properties.length, 4);
            n.height = this.config.nodeHeaderHeight + this.config.nodePadding * 2 + 
                       propsCount * this.config.propertyLineHeight;
        });

        const width = container.clientWidth || 800;
        const height = container.clientHeight || 500;

        // Compute layout using shared utility
        D3DiagramUtils.computeTreeLayout(nodes, links, {
            nodeWidth: this.config.nodeWidth,
            containerWidth: width,
            containerHeight: height,
            xSpacing: this.xSpacing,
            ySpacing: this.ySpacing
        });

        // Calculate actual content bounds after layout
        const padding = 40;
        const minX = Math.min(...nodes.map(n => n.x)) - padding;
        const minY = Math.min(...nodes.map(n => n.y)) - padding;
        const maxX = Math.max(...nodes.map(n => n.x + this.config.nodeWidth)) + padding;
        const maxY = Math.max(...nodes.map(n => n.y + (n.height || 80))) + padding;
        const contentWidth = maxX - minX;
        const contentHeight = maxY - minY;

        console.log('Layout bounds:', { minX, minY, maxX, maxY, contentWidth, contentHeight });

        // Create SVG with viewBox matching actual content
        this.svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', `${minX} ${minY} ${contentWidth} ${contentHeight}`)
            .attr('preserveAspectRatio', 'xMidYMid meet');

        console.log('SVG created:', { width, height, svgNode: this.svg.node() });

        this.zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => g.attr('transform', event.transform));

        this.svg.call(this.zoom);

        const g = this.svg.append('g');

        // Arrowhead marker using shared utility
        D3DiagramUtils.createArrowheadMarker(this.svg, 'schema-arrowhead', '#57606a');

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

        // Edge labels using shared utility for positioning
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
            .attr('font-size', '10px')
            .attr('fill', '#555')
            .attr('text-anchor', 'middle')
            .attr('class', 'link-label')
            .text(d => d.label.length > 25 ? d.label.substring(0, 23) + '..' : d.label);

        // Render nodes
        const nodeGroups = g.selectAll('.node-group')
            .data(nodes)
            .enter()
            .append('g')
            .attr('class', 'node-group')
            .attr('transform', d => `translate(${d.x}, ${d.y})`);

        // Node background
        nodeGroups.append('rect')
            .attr('width', this.config.nodeWidth)
            .attr('height', d => d.height)
            .attr('rx', 6)
            .attr('ry', 6)
            .attr('fill', '#fff')
            .attr('stroke', '#d0d7de')
            .attr('stroke-width', 1.5);

        // Node header background
        nodeGroups.append('rect')
            .attr('width', this.config.nodeWidth)
            .attr('height', this.config.nodeHeaderHeight)
            .attr('rx', 6)
            .attr('ry', 6)
            .attr('fill', '#f6f8fa');

        // Fix rounded corners at bottom of header
        nodeGroups.append('rect')
            .attr('y', this.config.nodeHeaderHeight - 6)
            .attr('width', this.config.nodeWidth)
            .attr('height', 6)
            .attr('fill', '#f6f8fa');

        // Header separator line
        nodeGroups.append('line')
            .attr('x1', 0)
            .attr('y1', this.config.nodeHeaderHeight)
            .attr('x2', this.config.nodeWidth)
            .attr('y2', this.config.nodeHeaderHeight)
            .attr('stroke', '#d0d7de')
            .attr('stroke-width', 1);

        // Node header text
        nodeGroups.append('text')
            .attr('class', 'node-header')
            .attr('x', this.config.nodeWidth / 2)
            .attr('y', 20)
            .attr('text-anchor', 'middle')
            .attr('font-size', '11px')
            .attr('font-weight', '600')
            .attr('fill', '#24292f')
            .text(d => d.label.length > 25 ? d.label.substring(0, 23) + '...' : d.label);

        // Node properties
        nodeGroups.each((d, i, nodeElements) => {
            const nodeEl = d3.select(nodeElements[i]);
            d.properties.slice(0, 4).forEach((prop, j) => {
                nodeEl.append('text')
                    .attr('class', 'node-property')
                    .attr('x', 10)
                    .attr('y', this.config.nodeHeaderHeight + 16 + j * this.config.propertyLineHeight)
                    .attr('font-size', '10px')
                    .attr('fill', '#57606a')
                    .text(prop.length > 28 ? prop.substring(0, 26) + '...' : prop);
            });
        });

        // Tooltips
        nodeGroups.append('title')
            .text(d => `${d.id}\n\nProperties: ${d.properties.join(', ')}\nConnections: ${d.degree}`);

        // Make nodes draggable
        this.setupDrag(nodeGroups, nodes, links, linkGroups);
        } catch (error) {
            console.error('Error in renderDiagram:', error);
        }
    }

    setupDrag(nodeGroups, nodes, links, linkGroups) {
        const nodeWidth = this.config.nodeWidth;
        const drag = d3.drag()
            .on('start', function(event, d) {
                d3.select(this).raise().classed('dragging', true);
            })
            .on('drag', function(event, d) {
                d.x = event.x;
                d.y = event.y;
                d3.select(this).attr('transform', `translate(${d.x}, ${d.y})`);

                // Update edges
                linkGroups.select('path')
                    .attr('d', l => {
                        const source = nodes.find(n => n.id === l.source);
                        const target = nodes.find(n => n.id === l.target);
                        if (!source || !target) return '';
                        return D3DiagramUtils.createEdgePath(source, target, nodeWidth);
                    });

                // Update edge labels
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
        
        // Collapse LS Cloud section when panel opens
        if (window.lsCloud) {
            window.lsCloud.toggleCollapse(true);
        }
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
