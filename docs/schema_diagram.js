// RDFSolve Schema Diagram Module using Mermaid
class SchemaDiagram {
    constructor(datasets) {
        this.datasets = datasets;
        this.currentDataset = null;
        this.coverageData = null;
        this.githubRawBase = 'https://raw.githubusercontent.com/jmillanacosta/rdfsolve/main/docs/';
        
        // Zoom/pan state
        this.scale = 1;
        this.translateX = 0;
        this.translateY = 0;
        this.isPanning = false;
        this.startX = 0;
        this.startY = 0;
        
        this.init();
    }

    init() {
        this.initializeMermaid();
        this.setupEventListeners();
    }

    initializeMermaid() {
        if (typeof mermaid !== 'undefined') {
            mermaid.initialize({
                startOnLoad: false,
                theme: 'default',
                securityLevel: 'loose',
                flowchart: { 
                    useMaxWidth: false, 
                    htmlLabels: true, 
                    curve: 'stepBefore',
                    rankSpacing: 80,
                    nodeSpacing: 40
                },
                class: {
                    useMaxWidth: false
                }
            });
        }
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

        // Diagram type selector
        document.getElementById('diagram-type')?.addEventListener('change', () => {
            if (this.coverageData) this.renderSchemaDiagram();
        });

        // Zoom controls
        document.getElementById('zoom-in')?.addEventListener('click', () => this.zoom(1.25));
        document.getElementById('zoom-out')?.addEventListener('click', () => this.zoom(0.8));
        document.getElementById('zoom-fit')?.addEventListener('click', () => this.fitToContainer());
        
        // Zoom slider
        const slider = document.getElementById('zoom-slider');
        if (slider) {
            slider.addEventListener('input', (e) => {
                this.scale = parseInt(e.target.value) / 100;
                this.applyTransform();
                this.updateZoomDisplay();
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
        this.resetZoom();

        try {
            const coveragePath = this.currentDataset.dataFiles.coverage
                .replace(/^(\.\.\/)+/, '')
                .replace(/^\.?\//, '');
            const coverageUrl = this.githubRawBase + coveragePath;

            const response = await fetch(coverageUrl);
            if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);

            const csvText = await response.text();
            this.coverageData = this.parseCSV(csvText);
            
            this.renderSchemaInfo();
            this.renderSchemaDiagram();
        } catch (error) {
            console.error('Failed to load schema data:', error);
            this.showError('Failed to load schema data. The file may not be available.');
        }
    }

    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        const headers = lines[0].split(',');
        return lines.slice(1).map(line => {
            const values = this.parseCSVLine(line);
            const row = {};
            headers.forEach((h, i) => row[h] = values[i]);
            return row;
        });
    }

    parseCSVLine(line) {
        const result = [];
        let current = '', inQuotes = false;
        for (const char of line) {
            if (char === '"') inQuotes = !inQuotes;
            else if (char === ',' && !inQuotes) { result.push(current); current = ''; }
            else current += char;
        }
        result.push(current);
        return result;
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

    renderSchemaDiagram() {
        const container = document.getElementById('schema-diagram');
        if (!container || !this.coverageData) return;

        container.innerHTML = '<div class="loading">Rendering diagram...</div>';
        const diagramType = document.getElementById('diagram-type')?.value || 'class';
        const maxNodes = 50;

        try {
            const diagram = diagramType === 'class' 
                ? this.generateClassDiagram(maxNodes) 
                : this.generateGraphDiagram(maxNodes);

            if (!diagram || diagram.length < 20) throw new Error('Generated diagram is too short');

            // Hide container during rendering to prevent visual glitches
            const mermaidDiv = document.createElement('div');
            mermaidDiv.className = 'mermaid';
            mermaidDiv.style.visibility = 'hidden';
            mermaidDiv.textContent = diagram;
            container.innerHTML = '';
            container.appendChild(mermaidDiv);
            
            if (typeof mermaid !== 'undefined') {
                mermaid.init(undefined, mermaidDiv)
                    .then(() => {
                        // Show diagram after rendering is complete
                        mermaidDiv.style.visibility = 'visible';
                        this.setupDiagramInteraction(container);
                        setTimeout(() => this.fitToContainer(), 100);
                    })
                    .catch(err => {
                        console.error('Mermaid rendering error:', err);
                        container.innerHTML = `<div class="error-message"><strong>Diagram Rendering Failed</strong><br>Try switching diagram type.<br><small>${err.message || 'Parse error'}</small></div>`;
                    });
            }
        } catch (error) {
            console.error('Failed to generate diagram:', error);
            container.innerHTML = `<div class="error-message"><strong>Failed to Generate Diagram</strong><br>${error.message}</div>`;
        }
    }

    setupDiagramInteraction(container) {
        const svg = container.querySelector('svg');
        if (!svg) return;

        // Make SVG fill its container naturally
        svg.style.cssText = 'display:block;max-width:100%;height:auto;';
        svg.removeAttribute('width');
        svg.removeAttribute('height');
        
        // Use the mermaid div as the zoomable element
        const mermaidDiv = container.querySelector('.mermaid');
        if (mermaidDiv) {
            mermaidDiv.style.transformOrigin = 'center center';
        }

        // Mouse wheel zoom
        container.addEventListener('wheel', (e) => {
            e.preventDefault();
            this.zoom(e.deltaY > 0 ? 0.9 : 1.1);
        }, { passive: false });

        // Pan with mouse drag
        container.addEventListener('mousedown', (e) => {
            if (e.button === 0) {
                this.isPanning = true;
                this.startX = e.clientX - this.translateX;
                this.startY = e.clientY - this.translateY;
                container.style.cursor = 'grabbing';
            }
        });

        container.addEventListener('mousemove', (e) => {
            if (this.isPanning) {
                this.translateX = e.clientX - this.startX;
                this.translateY = e.clientY - this.startY;
                this.applyTransform();
            }
        });

        const endPan = () => { this.isPanning = false; container.style.cursor = 'grab'; };
        container.addEventListener('mouseup', endPan);
        container.addEventListener('mouseleave', endPan);
        container.style.cursor = 'grab';
    }

    zoom(factor) {
        const newScale = Math.min(Math.max(this.scale * factor, 0.1), 5);
        this.scale = newScale;
        this.applyTransform();
        this.updateZoomDisplay();
    }

    resetZoom() {
        this.scale = 1;
        this.translateX = 0;
        this.translateY = 0;
        this.applyTransform();
        this.updateZoomDisplay();
    }

    fitToContainer() {
        this.scale = 1;
        this.translateX = 0;
        this.translateY = 0;
        this.applyTransform();
        this.updateZoomDisplay();
    }

    applyTransform() {
        const mermaidDiv = document.querySelector('#schema-diagram .mermaid');
        if (mermaidDiv) {
            mermaidDiv.style.transform = `translate(${this.translateX}px, ${this.translateY}px) scale(${this.scale})`;
        }
        
        // Sync slider
        const slider = document.getElementById('zoom-slider');
        if (slider) slider.value = Math.round(this.scale * 100);
    }

    updateZoomDisplay() {
        const display = document.getElementById('zoom-level');
        if (display) display.textContent = `${Math.round(this.scale * 100)}%`;
    }

    generateClassDiagram(maxNodes) {
        const classMap = new Map();
        const topPatterns = [...this.coverageData]
            .sort((a, b) => parseInt(b.occurrence_count || 0) - parseInt(a.occurrence_count || 0))
            .slice(0, maxNodes);

        topPatterns.forEach(row => {
            const subject = this.sanitizeClassName(row.subject_class);
            const object = this.sanitizeClassName(row.object_class);
            const subjectLabel = this.formatLabel(row.subject_class);
            const objectLabel = this.formatLabel(row.object_class);
            const propLabel = this.formatPropertyLabel(row.property);

            if (!classMap.has(subject)) classMap.set(subject, { label: subjectLabel, properties: new Set() });
            if (subject !== object && !classMap.has(object)) classMap.set(object, { label: objectLabel, properties: new Set() });
            classMap.get(subject).properties.add(propLabel);
        });

        let diagram = 'classDiagram\n';
        
        classMap.forEach((info, className) => {
            diagram += `    class ${className}["${info.label}"] {\n`;
            const props = Array.from(info.properties).slice(0, 5);
            if (props.length > 0) props.forEach(p => diagram += `        ${p.replace(/[<>{}()\[\]]/g, '')}\n`);
            else diagram += `        .\n`;
            if (info.properties.size > 5) diagram += `        +${info.properties.size - 5} more\n`;
            diagram += `    }\n`;
        });

        const relationships = new Map();
        topPatterns.forEach(row => {
            const subject = this.sanitizeClassName(row.subject_class);
            const object = this.sanitizeClassName(row.object_class);
            if (subject !== object) {
                const key = `${subject}-->${object}`;
                if (!relationships.has(key)) relationships.set(key, this.formatPropertyLabel(row.property).replace(/[<>{}()\[\]]/g, ''));
            }
        });

        relationships.forEach((label, key) => {
            const [subject, object] = key.split('-->');
            diagram += `    ${subject} --> ${object} : ${label}\n`;
        });

        return diagram;
    }

    generateGraphDiagram(maxNodes) {
        const edges = new Map();
        const nodeLabels = new Map();
        const topPatterns = [...this.coverageData]
            .sort((a, b) => parseInt(b.occurrence_count || 0) - parseInt(a.occurrence_count || 0))
            .slice(0, maxNodes);

        topPatterns.forEach(row => {
            const subject = this.sanitizeClassName(row.subject_class);
            const object = this.sanitizeClassName(row.object_class);
            nodeLabels.set(subject, this.formatLabel(row.subject_class));
            nodeLabels.set(object, this.formatLabel(row.object_class));
            
            const key = `${subject}->${object}`;
            if (!edges.has(key)) edges.set(key, []);
            edges.get(key).push(this.formatPropertyLabel(row.property));
        });

        let diagram = '%%{init: {"flowchart": {"curve": "stepBefore"}}}%%\ngraph TD\n';
        edges.forEach((properties, key) => {
            const [subject, object] = key.split('->');
            const cleanProps = properties.slice(0, 2).map(p => p.replace(/[<>{}()\[\]|]/g, ''));
            const displayLabel = properties.length > 2 ? `"${cleanProps.join(', ')} +${properties.length - 2}"` : `"${cleanProps.join(', ')}"`;
            diagram += `    ${subject}["${nodeLabels.get(subject)}"] -->|${displayLabel}| ${object}["${nodeLabels.get(object)}"]\n`;
        });

        return diagram;
    }

    sanitizeClassName(name) {
        if (!name) return 'Unknown';
        let s = name.replace(/[^a-zA-Z0-9_]/g, '_');
        if (/^[0-9]/.test(s)) s = 'C' + s;
        return s.replace(/_+/g, '_').replace(/^_+|_+$/g, '') || 'Unknown';
    }

    formatLabel(text) {
        if (!text) return 'Unknown';
        const parts = text.split(/[#\/]/);
        let localName = parts[parts.length - 1];
        try { localName = decodeURIComponent(localName); } catch {}
        // Replace colons and other problematic chars with spaces
        return localName.substring(0, 22).replace(/["'`\\:]/g, ' ').replace(/\s+/g, ' ').trim() + (localName.length > 22 ? '...' : '');
    }

    // Format property with prefix localname notation
    formatPropertyLabel(uri) {
        if (!uri) return 'unknown';
        
        // Common namespace prefixes
        const prefixes = {
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#': 'rdf',
            'http://www.w3.org/2000/01/rdf-schema#': 'rdfs',
            'http://www.w3.org/2002/07/owl#': 'owl',
            'http://www.w3.org/2001/XMLSchema#': 'xsd',
            'http://purl.org/dc/elements/1.1/': 'dc',
            'http://purl.org/dc/terms/': 'dcterms',
            'http://xmlns.com/foaf/0.1/': 'foaf',
            'http://www.w3.org/2004/02/skos/core#': 'skos',
            'http://schema.org/': 'schema',
            'http://purl.org/ontology/bibo/': 'bibo',
            'http://semanticscience.org/resource/': 'sio',
            'http://purl.obolibrary.org/obo/': 'obo',
            'http://www.w3.org/ns/prov#': 'prov',
            'http://rdfs.org/ns/void#': 'void',
            'http://purl.org/pav/': 'pav',
            'http://www.wikidata.org/prop/direct/': 'wdt',
            'http://www.wikidata.org/entity/': 'wd',
        };
        
        for (const [namespace, prefix] of Object.entries(prefixes)) {
            if (uri.startsWith(namespace)) {
                const localName = uri.substring(namespace.length);
                // Use space instead of colon to avoid Mermaid parsing issues
                return `${prefix} ${localName.substring(0, 18)}${localName.length > 18 ? '...' : ''}`;
            }
        }
        
        // Fallback: extract local name from URI
        const parts = uri.split(/[#\/]/);
        let localName = parts[parts.length - 1];
        try { localName = decodeURIComponent(localName); } catch {}
        return localName.substring(0, 22).replace(/["'`\\:]/g, ' ').replace(/\s+/g, ' ').trim() + (localName.length > 22 ? '...' : '');
    }

    showLoading() {
        const container = document.getElementById('schema-diagram');
        if (container) container.innerHTML = '<div class="loading">Loading schema...</div>';
    }

    showError(message) {
        const container = document.getElementById('schema-diagram');
        if (container) container.innerHTML = `<div class="error-message">${message}</div>`;
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
