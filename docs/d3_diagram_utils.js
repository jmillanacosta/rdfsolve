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
     * Assign edge offsets to prevent overlapping labels when multiple edges exist
     * between the same pair of nodes. Groups edges by source-target pair and assigns
     * sequential offsets so paths and labels are visually separated.
     */
    assignEdgeOffsets(links) {
        // Group edges by unordered node pair
        const pairMap = new Map();
        links.forEach(link => {
            const s = typeof link.source === 'object' ? link.source.id : link.source;
            const t = typeof link.target === 'object' ? link.target.id : link.target;
            // Use sorted key so A->B and B->A are grouped together
            const key = [s, t].sort().join('|||');
            if (!pairMap.has(key)) {
                pairMap.set(key, []);
            }
            pairMap.get(key).push(link);
        });

        // Assign offsets within each group
        pairMap.forEach(group => {
            const count = group.length;
            group.forEach((link, idx) => {
                // Center the group: offsets are -1, 0, 1 for 3 edges, etc.
                link.edgeOffset = idx - Math.floor(count / 2);
                link.edgeIndex = idx;
                link.edgeCount = count;
            });
        });
    },

    /**
     * Create a drag behavior for diagram nodes
     * @param {Object} options - Configuration options
     * @param {Map|Function} options.nodeById - Map of nodeId -> node, or function to get node by id
     * @param {Object} options.linkGroups - D3 selection of link groups
     * @param {number} options.nodeWidth - Width of nodes
     * @param {Function} options.getCurvedEdges - Function that returns current curvedEdges setting
     * @param {Function} [options.onDragStart] - Optional callback on drag start
     * @param {Function} [options.onDragEnd] - Optional callback on drag end
     * @returns {Object} D3 drag behavior
     */
    createDragBehavior(options) {
        const { nodeById, linkGroups, nodeWidth, getCurvedEdges, onDragStart, onDragEnd } = options;
        const self = this;
        
        // Helper to get node from nodeById (supports Map or Function)
        const getNode = (id) => {
            if (typeof nodeById === 'function') return nodeById(id);
            if (nodeById instanceof Map) return nodeById.get(id);
            return nodeById[id];
        };

        return d3.drag()
            .on('start', function(event, d) {
                d3.select(this).raise().classed('dragging', true);
                if (onDragStart) onDragStart.call(this, event, d);
            })
            .on('drag', function(event, d) {
                d.x = event.x;
                d.y = event.y;
                d3.select(this).attr('transform', `translate(${d.x}, ${d.y})`);

                // Choose edge path function based on curvedEdges setting
                const useCurved = getCurvedEdges ? getCurvedEdges() : false;
                const pathFn = useCurved 
                    ? self.createCurvedEdgePath.bind(self)
                    : self.createEdgePath.bind(self);

                // Update connected links
                linkGroups.each(function(link) {
                    const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
                    const targetId = typeof link.target === 'object' ? link.target.id : link.target;
                    const source = getNode(sourceId);
                    const target = getNode(targetId);
                    if (!source || !target) return;

                    // Update path
                    d3.select(this).select('path, .link-path')
                        .attr('d', pathFn(source, target, nodeWidth, link.edgeOffset || 0));

                    // Update label position
                    const labelPos = self.getEdgeLabelPosition(
                        source, target, nodeWidth,
                        link.edgeOffset || 0, link.edgeIndex || 0, link.edgeCount || 1
                    );
                    d3.select(this).select('text, .link-label')
                        .attr('x', labelPos.x)
                        .attr('y', labelPos.y);
                });
            })
            .on('end', function(event, d) {
                d3.select(this).classed('dragging', false);
                if (onDragEnd) onDragEnd.call(this, event, d);
            });
    },

    /**
     * Create orthogonal edge path between two nodes
     * Ends before target node to leave room for arrowhead
     */
    createEdgePath(source, target, nodeWidth, edgeOffset = 0) {
        const offsetSpacing = 8;
        const offset = edgeOffset * offsetSpacing;
        const arrowPadding = 12; // Space for arrowhead

        const sx = source.x + nodeWidth / 2 + offset;
        const sy = source.y + (source.height || 80);
        const tx = target.x + nodeWidth / 2 + offset;
        const ty = target.y - arrowPadding; // End before target for arrow

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
     * Create curved edge path between two nodes (Bezier version)
     * Ends before target node to leave room for arrowhead
     */
    createCurvedEdgePath(source, target, nodeWidth, edgeOffset = 0) {
        const offsetSpacing = 15;
        const offset = edgeOffset * offsetSpacing;
        const arrowPadding = 12; // Space for arrowhead

        const sx = source.x + nodeWidth / 2;
        const sy = source.y + (source.height || 80);
        const tx = target.x + nodeWidth / 2;
        const ty = target.y - arrowPadding;

        // Calculate control point offset based on edge offset
        const curveOffset = offset;

        // For edges going down (normal case)
        if (ty > sy + 20) {
            const midY = sy + (ty - sy) / 2;
            // Cubic Bezier curve with offset control points
            const cy1 = sy + (midY - sy) / 2;
            const cy2 = midY + (ty - midY) / 2;
            return `M${sx},${sy} C${sx + curveOffset},${cy1} ${tx + curveOffset},${cy2} ${tx},${ty}`;
        }

        // Back-edges or same level: curve around
        const detour = 60 + Math.abs(offset);
        const goRight = tx > sx || offset > 0;
        const sideOffset = goRight ? detour : -detour;
        const cx = (sx + tx) / 2 + sideOffset;
        const cy = Math.min(sy, ty) - 40;
        
        return `M${sx},${sy} Q${cx},${cy} ${tx},${ty}`;
    },

    /**
     * Get label position for an edge (anchored to edge path, not just midpoint)
     * @param {Object} source - Source node
     * @param {Object} target - Target node
     * @param {number} nodeWidth - Width of nodes
     * @param {number} edgeOffset - Offset index for this edge (0 = center, negative = left, positive = right)
     * @param {number} edgeIndex - Index of this edge within its group
     * @param {number} edgeCount - Total edges between this node pair
     */
    getEdgeLabelPosition(source, target, nodeWidth, edgeOffset = 0, edgeIndex = 0, edgeCount = 1) {
        const offsetSpacing = 8;
        const labelVerticalSpacing = 14; // Vertical spacing between stacked labels
        const offset = edgeOffset * offsetSpacing;
        
        const sx = source.x + nodeWidth / 2 + offset;
        const sy = source.y + (source.height || 80);
        const tx = target.x + nodeWidth / 2 + offset;
        const ty = target.y;

        // Orthogonal edges: place label on the horizontal segment, or midpoint for vertical
        if (ty > sy + 20) {
            // Edge goes down: horizontal segment is at midY
            const midY = sy + (ty - sy) / 2;
            // Stack labels vertically when multiple edges exist
            const labelY = midY - 6 + (edgeIndex - Math.floor(edgeCount / 2)) * labelVerticalSpacing;
            
            // For mostly vertical edges, place label just to the right of the vertical line
            if (Math.abs(tx - sx) < 30) {
                return { x: sx + 12, y: labelY };
            }
            // For edges with a horizontal segment, place label above the horizontal segment
            return { x: (sx + tx) / 2, y: labelY };
        }
        // Back-edge or same-level: place label above the topmost point, stacked vertically
        const labelY = Math.min(sy, ty) - 14 + (edgeIndex - Math.floor(edgeCount / 2)) * labelVerticalSpacing;
        return { x: (sx + tx) / 2, y: labelY };
    },
    /**
     * Filter nodes by a predicate, highlight them, and return filtered/hidden nodes.
     * Optionally zoom to the last filtered node (provide a zoom callback).
     * @param {Array} nodes - All nodes in the diagram
     * @param {Function} predicate - Function(node) => boolean
     * @param {Function} [highlightCallback] - Called with filtered nodes for highlighting
     * @param {Function} [zoomCallback] - Called with last filtered node for zooming
     * @returns {Object} { filteredNodes, hiddenNodes }
     */
    filterAndHighlightNodes(nodes, predicate, highlightCallback, zoomCallback) {
        const filteredNodes = nodes.filter(predicate);
        const hiddenNodes = nodes.filter(n => !predicate(n));
        if (highlightCallback) highlightCallback(filteredNodes);
        if (zoomCallback && filteredNodes.length > 0) {
            zoomCallback(filteredNodes[filteredNodes.length - 1]);
        }
        return { filteredNodes, hiddenNodes };
    },

    /**
     * Utility to clear node filtering/highlighting (reset all nodes to normal state)
     * @param {Array} nodes
     * @param {Function} [highlightCallback]
     */
    clearNodeFiltering(nodes, highlightCallback) {
        if (highlightCallback) highlightCallback(nodes);
    },

    /**
     * Generate a color based on index
     */
    getColor(index) {
        const hue = (index * 137.508) % 360;
        return `hsl(${hue}, 65%, 50%)`;
    },

    /**
     * Setup +/- spacing controls for a visualization
     * @param {Object} options - Configuration options
     * @param {string} options.xPlusId - ID for X+ button
     * @param {string} options.xMinusId - ID for X- button
     * @param {string} options.xValId - ID for X value display
     * @param {string} options.yPlusId - ID for Y+ button
     * @param {string} options.yMinusId - ID for Y- button
     * @param {string} options.yValId - ID for Y value display
     * @param {number} options.initialX - Initial X spacing value (default 1.0)
     * @param {number} options.initialY - Initial Y spacing value (default 1.0)
     * @param {number} options.step - Step increment (default 0.2)
     * @param {Function} options.onChange - Callback with (xSpacing, ySpacing) when values change
     * @returns {Object} { xSpacing, ySpacing, getValues() }
     */
    setupSpacingControls(options) {
        const {
            xPlusId, xMinusId, xValId,
            yPlusId, yMinusId, yValId,
            initialX = 1,
            initialY = 1,
            step = 0.2,
            onChange = () => {}
        } = options;

        const state = {
            xSpacing: initialX,
            ySpacing: initialY
        };

        const updateDisplay = () => {
            const xValEl = document.getElementById(xValId);
            const yValEl = document.getElementById(yValId);
            if (xValEl) xValEl.textContent = state.xSpacing.toFixed(1);
            if (yValEl) yValEl.textContent = state.ySpacing.toFixed(1);
        };

        // X controls
        const xPlus = document.getElementById(xPlusId);
        const xMinus = document.getElementById(xMinusId);
        if (xPlus) {
            xPlus.addEventListener('click', () => {
                state.xSpacing += step;
                updateDisplay();
                onChange(state.xSpacing, state.ySpacing);
            });
        }
        if (xMinus) {
            xMinus.addEventListener('click', () => {
                state.xSpacing = Math.max(step, state.xSpacing - step);
                updateDisplay();
                onChange(state.xSpacing, state.ySpacing);
            });
        }

        // Y controls
        const yPlus = document.getElementById(yPlusId);
        const yMinus = document.getElementById(yMinusId);
        if (yPlus) {
            yPlus.addEventListener('click', () => {
                state.ySpacing += step;
                updateDisplay();
                onChange(state.xSpacing, state.ySpacing);
            });
        }
        if (yMinus) {
            yMinus.addEventListener('click', () => {
                state.ySpacing = Math.max(step, state.ySpacing - step);
                updateDisplay();
                onChange(state.xSpacing, state.ySpacing);
            });
        }

        // Initialize display
        updateDisplay();

        return {
            get xSpacing() { return state.xSpacing; },
            get ySpacing() { return state.ySpacing; },
            set xSpacing(v) { state.xSpacing = v; updateDisplay(); },
            set ySpacing(v) { state.ySpacing = v; updateDisplay(); },
            getValues() { return { xSpacing: state.xSpacing, ySpacing: state.ySpacing }; }
        };
    },

    /**
     * Setup zoom controls for a visualization
     * @param {Object} options - Configuration options
     * @param {string} options.zoomInId - ID for zoom in button
     * @param {string} options.zoomOutId - ID for zoom out button
     * @param {string} options.zoomFitId - ID for zoom fit button
     * @param {Function} options.getSvgAndZoom - Callback that returns { svg, zoom }
     */
    setupZoomControls(options) {
        const { zoomInId, zoomOutId, zoomFitId, getSvgAndZoom } = options;

        const zoomBy = (factor) => {
            const { svg, zoom } = getSvgAndZoom();
            if (svg && zoom) {
                svg.transition().duration(200).call(zoom.scaleBy, factor);
            }
        };

        const zoomFit = () => {
            const { svg, zoom } = getSvgAndZoom();
            if (svg && zoom) {
                svg.transition().duration(300).call(zoom.transform, d3.zoomIdentity);
            }
        };

        document.getElementById(zoomInId)?.addEventListener('click', () => zoomBy(1.3));
        document.getElementById(zoomOutId)?.addEventListener('click', () => zoomBy(0.7));
        document.getElementById(zoomFitId)?.addEventListener('click', zoomFit);

        return { zoomBy, zoomFit };
    },

    /**
     * Toggle fullscreen mode for a container element
     * @param {HTMLElement} container - The container to toggle fullscreen
     * @param {string} fullscreenClass - CSS class to add when in fullscreen mode
     * @param {Function} [onToggle] - Optional callback with (isFullscreen) parameter
     */
    toggleFullscreen(container, fullscreenClass = 'fullscreen-mode', onToggle) {
        if (!container) return;

        if (document.fullscreenElement) {
            document.exitFullscreen().then(() => {
                container.classList.remove(fullscreenClass);
                if (onToggle) onToggle(false);
            });
        } else {
            container.requestFullscreen().then(() => {
                container.classList.add(fullscreenClass);
                if (onToggle) onToggle(true);
            }).catch(err => {
                console.warn('Fullscreen not available:', err);
            });
        }
    },

    /**
     * Create arrowhead marker definition for SVG
     * @param {Object} svg - D3 SVG selection
     * @param {string} id - Marker ID
     * @param {string} [color='#57606a'] - Arrow fill color
     */
    createArrowheadMarker(svg, id, color = '#57606a') {
        svg.append('defs').append('marker')
            .attr('id', id)
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', color);
    }
};
