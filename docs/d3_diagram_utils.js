/**
 * RDFSolve D3 Diagram Utilities
 * Shared functionality for D3-based class diagram visualizations
 */

const D3DiagramUtils = {
    // Default configuration
    config: {
        nodeWidth: 180,
        nodeHeaderHeight: 28,
        propertyLineHeight: 16,
        nodePadding: 12,
        rowSpacing: 140,
        nodeSpacing: 60,
        verticalPadding: 40,
        horizontalPadding: 40,
        maxProperties: 4
    },

    // Spacing multipliers (controlled by sliders)
    spacingMultipliers: {
        x: 1.0,
        y: 1.0
    },

    // Common namespace prefixes for URI to CURIE conversion
    namespaces: {
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
        'http://www.w3.org/ns/shacl#': 'sh'
    },

    // Excluded namespaces for OWL/RDF(S) filtering
    excludedNamespaces: [
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'http://www.w3.org/2000/01/rdf-schema#',
        'http://www.w3.org/2002/07/owl#',
        'http://www.w3.org/ns/shacl#',
        'http://www.w3.org/2001/XMLSchema#'
    ],

    excludedLocalNames: [
        'Resource', 'Class', 'Literal', 'Property', 'Thing',
        'NamedIndividual', 'Ontology', 'ObjectProperty', 'DatatypeProperty',
        'AnnotationProperty', 'TransitiveProperty', 'SymmetricProperty',
        'FunctionalProperty', 'InverseFunctionalProperty', 'Restriction',
        'AllDifferent', 'AllDisjointClasses', 'AllDisjointProperties'
    ],

    /**
     * Convert URI to CURIE (prefix:localName) format
     */
    uriToCurie(uri) {
        if (!uri) return 'Unknown';
        
        for (const [namespace, prefix] of Object.entries(this.namespaces)) {
            if (uri.startsWith(namespace)) {
                const localName = uri.substring(namespace.length);
                return `${prefix}:${localName}`;
            }
        }
        
        // Fallback: extract local name
        return this.getLocalName(uri);
    },

    /**
     * Get local name from URI
     */
    getLocalName(uri) {
        if (!uri) return 'Unknown';
        const local = uri.split(/[#\/]/).pop();
        try { return decodeURIComponent(local).substring(0, 25); }
        catch { return local.substring(0, 25); }
    },

    /**
     * Check if a URI should be excluded (OWL/RDF(S) terms)
     */
    isExcludedUri(uri, excludeOwl = true) {
        if (!uri) return true;
        if (!excludeOwl) return false;

        if (this.excludedNamespaces.some(ns => uri.startsWith(ns))) return true;
        
        const localName = uri.split(/[#\/]/).pop();
        if (this.excludedLocalNames.includes(localName)) return true;

        return false;
    },

    /**
     * Parse CSV text to array of objects
     */
    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        if (lines.length === 0) return [];
        
        const headers = lines[0].split(',').map(h => h.trim());
        
        return lines.slice(1).map(line => {
            const values = this.parseCSVLine(line);
            const row = {};
            headers.forEach((h, i) => row[h] = values[i] || '');
            return row;
        });
    },

    parseCSVLine(line) {
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (const char of line) {
            if (char === '"') inQuotes = !inQuotes;
            else if (char === ',' && !inQuotes) {
                values.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        values.push(current.trim());
        return values;
    },

    /**
     * Build graph data from coverage CSV rows
     */
    buildGraphFromCoverage(rows, options = {}) {
        const {
            maxPatterns = 100,
            excludeOwl = true,
            datasetName = null
        } = options;

        const nodeMap = new Map();
        const linkMap = new Map();

        const patterns = [...rows]
            .sort((a, b) => parseInt(b.occurrence_count || 0) - parseInt(a.occurrence_count || 0))
            .slice(0, maxPatterns);

        patterns.forEach(row => {
            const subjectUri = row.subject_uri || row.subject_class || 'Unknown';
            const objectUri = row.object_uri || row.object_class || 'Unknown';
            const propertyUri = row.property_uri || row.property || 'unknown';

            const subjectLabel = row.subject_class || this.getLocalName(subjectUri);
            const objectLabel = row.object_class || this.getLocalName(objectUri);
            const propertyLabel = row.property || this.getLocalName(propertyUri);

            const subjectExcluded = this.isExcludedUri(subjectUri, excludeOwl);
            const objectExcluded = this.isExcludedUri(objectUri, excludeOwl);

            // Add subject node
            if (!subjectExcluded) {
                if (!nodeMap.has(subjectUri)) {
                    nodeMap.set(subjectUri, {
                        id: subjectUri,
                        label: subjectLabel,
                        properties: new Set(),
                        outDegree: 0,
                        inDegree: 0
                    });
                }
                nodeMap.get(subjectUri).properties.add(propertyLabel);
                nodeMap.get(subjectUri).outDegree++;
            }

            // Add object node
            if (!objectExcluded) {
                if (!nodeMap.has(objectUri)) {
                    nodeMap.set(objectUri, {
                        id: objectUri,
                        label: objectLabel,
                        properties: new Set(),
                        outDegree: 0,
                        inDegree: 0
                    });
                }
                nodeMap.get(objectUri).inDegree++;
            }

            // Add link
            if (!subjectExcluded && !objectExcluded && subjectUri !== objectUri) {
                const key = `${subjectUri}|||${objectUri}`;
                if (!linkMap.has(key)) {
                    linkMap.set(key, {
                        source: subjectUri,
                        target: objectUri,
                        properties: new Set()
                    });
                }
                linkMap.get(key).properties.add(propertyLabel);
            }
        });

        const nodes = Array.from(nodeMap.values()).map(n => ({
            ...n,
            properties: Array.from(n.properties).slice(0, this.config.maxProperties),
            degree: n.inDegree + n.outDegree
        }));

        const links = Array.from(linkMap.values()).map(l => ({
            ...l,
            properties: Array.from(l.properties),
            label: Array.from(l.properties).slice(0, 2).join(', ')
        }));

        return { nodes, links };
    },

    /**
     * Compute hierarchical tree layout using D3's tree layout
     * Automatically fits to container height and spreads X as needed
     */
    computeTreeLayout(nodes, links, config = {}) {
        const {
            nodeWidth = this.config.nodeWidth,
            containerWidth = 1200,
            containerHeight = 600,
            xSpacing = 1.0,  // multiplier for horizontal spacing
            ySpacing = 1.0   // multiplier for vertical spacing
        } = config;

        if (nodes.length === 0) return;

        // Build adjacency maps
        const outgoing = new Map();
        const incoming = new Map();
        const nodeById = new Map();
        
        nodes.forEach(n => {
            outgoing.set(n.id, []);
            incoming.set(n.id, []);
            nodeById.set(n.id, n);
        });
        links.forEach(l => {
            if (outgoing.has(l.source)) outgoing.get(l.source).push(l.target);
            if (incoming.has(l.target)) incoming.get(l.target).push(l.source);
        });

        // Find roots (nodes with no incoming edges)
        let roots = nodes.filter(n => incoming.get(n.id).length === 0);
        if (roots.length === 0) {
            roots = [...nodes].sort((a, b) => b.outDegree - a.outDegree).slice(0, Math.max(1, Math.ceil(nodes.length / 5)));
        }

        // Build hierarchy data structure for D3
        const buildHierarchy = (rootId, visited = new Set()) => {
            if (visited.has(rootId)) return null;
            visited.add(rootId);
            
            const node = nodeById.get(rootId);
            if (!node) return null;
            
            const children = (outgoing.get(rootId) || [])
                .filter(childId => !visited.has(childId))
                .map(childId => buildHierarchy(childId, visited))
                .filter(c => c !== null);
            
            return {
                id: rootId,
                data: node,
                children: children.length > 0 ? children : undefined
            };
        };

        // Create a virtual root that contains all actual roots
        const visited = new Set();
        const hierarchyData = {
            id: '__root__',
            children: roots.map(r => buildHierarchy(r.id, visited)).filter(c => c !== null)
        };

        // Add orphan nodes (not connected to any root)
        nodes.forEach(n => {
            if (!visited.has(n.id)) {
                hierarchyData.children.push({ id: n.id, data: n });
                visited.add(n.id);
            }
        });

        // Create D3 hierarchy
        const root = d3.hierarchy(hierarchyData);
        
        // Calculate layout dimensions
        const padding = 40;
        const avgNodeHeight = 80;
        const numLevels = root.height;
        
        // Height: fit to container (minus padding for top/bottom)
        const availableHeight = (containerHeight - padding * 2) * ySpacing;
        
        // Width: based on maximum nodes at any level, scaled by xSpacing
        const levelWidths = [];
        root.each(node => {
            const depth = node.depth;
            if (!levelWidths[depth]) levelWidths[depth] = 0;
            levelWidths[depth]++;
        });
        const maxNodesAtLevel = Math.max(...levelWidths.filter(w => w), 1);
        const baseNodeSpacing = nodeWidth + 40;
        const availableWidth = Math.max(containerWidth - padding * 2, maxNodesAtLevel * baseNodeSpacing) * xSpacing;

        // Use D3 tree layout
        const treeLayout = d3.tree()
            .size([availableWidth, availableHeight])
            .separation((a, b) => (a.parent === b.parent ? 1 : 1.2));

        treeLayout(root);

        // Apply positions to nodes (skip the virtual root)
        root.descendants().forEach(d => {
            if (d.data.id === '__root__') return;
            
            const node = nodeById.get(d.data.id);
            if (node) {
                node.x = padding + d.x;
                node.y = padding + d.y;
                node.level = d.depth - 1; // -1 to account for virtual root
            }
        });

        // Final validation
        nodes.forEach(n => {
            if (typeof n.x !== 'number' || isNaN(n.x)) n.x = padding;
            if (typeof n.y !== 'number' || isNaN(n.y)) n.y = padding;
        });
    },

    /**
     * Legacy layout function - wraps the new tree layout
     */
    computeHierarchicalLayout(nodes, links, config = {}) {
        this.computeTreeLayout(nodes, links, {
            ...config,
            containerWidth: 1200,
            containerHeight: 600,
            xSpacing: config.xSpacing || 1.0,
            ySpacing: config.ySpacing || 1.0
        });
    },

    /**
     * Create orthogonal edge path between two nodes
     */
    createEdgePath(source, target, nodeWidth, edgeOffset = 0) {
        const offsetSpacing = 8;
        const offset = edgeOffset * offsetSpacing;

        const sx = source.x + nodeWidth / 2 + offset;
        const sy = source.y + (source.height || 80);
        const tx = target.x + nodeWidth / 2 + offset;
        const ty = target.y;

        // Simple orthogonal path
        if (ty > sy + 20) {
            const midY = sy + (ty - sy) / 2;
            if (Math.abs(tx - sx) < 10) {
                return `M${sx},${sy} L${sx},${ty}`;
            }
            return `M${sx},${sy} L${sx},${midY} L${tx},${midY} L${tx},${ty}`;
        }

        // Edges going up or same level
        const detour = 40;
        const goRight = tx > sx;
        const sideX = goRight ? Math.max(sx, tx) + detour : Math.min(sx, tx) - detour;
        return `M${sx},${sy} L${sx},${sy + 20} L${sideX},${sy + 20} L${sideX},${ty - 20} L${tx},${ty - 20} L${tx},${ty}`;
    },

    /**
     * Get label position for an edge
     */
    getEdgeLabelPosition(source, target, nodeWidth) {
        const sx = source.x + nodeWidth / 2;
        const sy = source.y + (source.height || 80);
        const tx = target.x + nodeWidth / 2;
        const ty = target.y;

        if (ty > sy) {
            const midY = sy + (ty - sy) / 2;
            if (Math.abs(tx - sx) < 50) {
                return { x: sx + 8, y: midY };
            }
            return { x: (sx + tx) / 2 + 8, y: sy + 30 };
        }

        return { x: (sx + tx) / 2 + 8, y: Math.min(sy, ty) - 8 };
    },

    /**
     * Generate a color based on index
     */
    getColor(index) {
        const hue = (index * 137.508) % 360;
        return `hsl(${hue}, 65%, 50%)`;
    },

    /**
     * Create D3 SVG styles for diagram
     */
    getSvgStyles() {
        return `
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
        `;
    }
};

// Export for use in other modules
if (typeof window !== 'undefined') {
    window.D3DiagramUtils = D3DiagramUtils;
}
