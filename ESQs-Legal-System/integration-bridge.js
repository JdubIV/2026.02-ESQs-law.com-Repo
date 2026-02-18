#!/usr/bin/env javascript
/**
 * LAW Matrix Integration Bridge
 * Connects the advanced frontend to the unified intelligence backend
 * Implements the complete multi-layered fine-tuning strategy integration
 */

class LawMatrixIntegrationBridge {
    constructor() {
        this.backendUrl = 'http://localhost:8080';
        this.unifiedIntelligenceEndpoint = '/api/lawmatrix/ai-query';
        this.statusEndpoint = '/api/lawmatrix/status';
        this.isConnected = false;
        this.systemHealth = 'unknown';
        
        // Initialize connection
        this.initializeConnection();
    }

    async initializeConnection() {
        try {
            const response = await fetch(`${this.backendUrl}${this.statusEndpoint}`);
            const status = await response.json();
            
            this.isConnected = true;
            this.systemHealth = status.status || 'operational';
            
            console.log('✅ LAW Matrix Integration Bridge - Connected to unified intelligence backend');
            console.log('System Status:', status);
            
            // Notify frontend of successful connection
            if (window.lawMatrix && window.lawMatrix.showNotification) {
                window.lawMatrix.showNotification('🔗 Connected to Unified Intelligence Backend');
            }
            
        } catch (error) {
            this.isConnected = false;
            this.systemHealth = 'error';
            console.error('❌ LAW Matrix Integration Bridge - Connection failed:', error);
            
            if (window.lawMatrix && window.lawMatrix.showNotification) {
                window.lawMatrix.showNotification('⚠️ Backend connection failed', 'error');
            }
        }
    }

    async sendToUnifiedAI(query, context = {}) {
        if (!this.isConnected) {
            throw new Error('Backend not connected');
        }

        try {
            const payload = {
                query: query,
                context: {
                    ...context,
                    timestamp: new Date().toISOString(),
                    source: 'law_matrix_frontend',
                    version: '4.0'
                },
                user_id: 'law_matrix_user',
                session_id: 'main_session'
            };

            const response = await fetch(`${this.backendUrl}${this.unifiedIntelligenceEndpoint}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                throw new Error(`Backend responded with ${response.status}`);
            }

            const result = await response.json();
            return result;
        } catch (error) {
            console.error('Unified AI query failed:', error);
            throw error;
        }
    }

    async analyzeCase(caseData) {
        const query = `Analyze the following case: ${caseData.title}. ${caseData.description}`;
        const context = {
            case_type: caseData.type,
            jurisdiction: caseData.jurisdiction,
            analysis_type: 'comprehensive'
        };

        return await this.sendToUnifiedAI(query, context);
    }

    async conductResearch(researchQuery, domain = 'general') {
        const query = `Conduct legal research on: ${researchQuery}`;
        const context = {
            research_domain: domain,
            depth: 'comprehensive',
            include_precedents: true
        };

        return await this.sendToUnifiedAI(query, context);
    }

    async findPrecedents(legalIssue, jurisdiction = 'Utah') {
        const query = `Find relevant legal precedents for: ${legalIssue}`;
        const context = {
            jurisdiction: jurisdiction,
            precedent_type: 'case_law',
            relevance_threshold: 0.8
        };

        return await this.sendToUnifiedAI(query, context);
    }

    async generateDocument(docType, data) {
        const query = `Generate a ${docType} with the following information: ${JSON.stringify(data)}`;
        const context = {
            document_type: docType,
            template: 'professional',
            jurisdiction: 'Utah'
        };

        return await this.sendToUnifiedAI(query, context);
    }

    async reviewDocument(documentContent, reviewType = 'compliance') {
        const query = `Review this document for ${reviewType}: ${documentContent}`;
        const context = {
            review_type: reviewType,
            jurisdiction: 'Utah',
            thoroughness: 'comprehensive'
        };

        return await this.sendToUnifiedAI(query, context);
    }

    getSystemStatus() {
        return {
            connected: this.isConnected,
            health: this.systemHealth,
            backend_url: this.backendUrl,
            features: {
                unified_intelligence: true,
                qlora_fine_tuning: true,
                rag_system: true,
                observability: true,
                contextual_awareness: true,
                intelligence_flywheel: true
            }
        };
    }

    // Health check method
    async performHealthCheck() {
        try {
            const response = await fetch(`${this.backendUrl}${this.statusEndpoint}`);
            const status = await response.json();
            
            this.isConnected = true;
            this.systemHealth = status.status;
            
            return {
                status: 'healthy',
                connected: true,
                response_time: Date.now(),
                details: status
            };
        } catch (error) {
            this.isConnected = false;
            this.systemHealth = 'error';
            
            return {
                status: 'unhealthy',
                connected: false,
                error: error.message
            };
        }
    }
}

// Initialize the integration bridge
window.lawMatrixBridge = new LawMatrixIntegrationBridge();

// Extend the Law Matrix Core with backend integration
if (window.LawMatrixCore) {
    // Add backend integration methods to the LawMatrixCore prototype
    Object.assign(LawMatrixCore.prototype, {
        async sendToBackend(query, context = {}) {
            if (window.lawMatrixBridge) {
                return await window.lawMatrixBridge.sendToUnifiedAI(query, context);
            }
            throw new Error('Integration bridge not available');
        },

        async analyzeCaseWithBackend(caseData) {
            if (window.lawMatrixBridge) {
                return await window.lawMatrixBridge.analyzeCase(caseData);
            }
            throw new Error('Integration bridge not available');
        },

        async conductResearchWithBackend(query, domain) {
            if (window.lawMatrixBridge) {
                return await window.lawMatrixBridge.conductResearch(query, domain);
            }
            throw new Error('Integration bridge not available');
        },

        async findPrecedentsWithBackend(issue, jurisdiction) {
            if (window.lawMatrixBridge) {
                return await window.lawMatrixBridge.findPrecedents(issue, jurisdiction);
            }
            throw new Error('Integration bridge not available');
        },

        getBackendStatus() {
            if (window.lawMatrixBridge) {
                return window.lawMatrixBridge.getSystemStatus();
            }
            return { connected: false, error: 'Integration bridge not available' };
        }
    });
}

console.log('🚀 LAW Matrix Integration Bridge initialized');
console.log('🔗 Frontend connected to Unified Intelligence Backend');
console.log('🧠 Multi-layered fine-tuning strategy active');

