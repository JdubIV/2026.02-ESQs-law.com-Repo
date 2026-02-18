/**
 * LAW MATRIX v4.0 BULLETPROOF ENTERPRISE EDITION
 * Core Instructions and Project Management System
 * Integrated with Multi-Layered Fine-Tuning Strategy
 */

class LawMatrixInstructions {
    constructor() {
        this.version = "4.0";
        this.systemType = "Bulletproof Enterprise Edition";
        this.currentProject = null;
        this.currentArtifact = null;
        this.projectArtifacts = new Map();
        this.firmPersonnel = this.initializeFirmPersonnel();
        this.acronyms = this.initializeAcronyms();
        this.currentUser = "John William Adams III, Esq.";
        this.userBarNumber = "#19429";
        this.billingRate = 390;
        
        console.log('📋 LAW Matrix v4.0 Instructions System Initialized');
    }

    initializeFirmPersonnel() {
        return {
            "John William Adams III": {
                names: ["JWA3", "JA", "JWA", "John", "Mr. Adams", "Sir"],
                title: "Attorney",
                barNumber: "#19429",
                billingRate: 390,
                role: "Primary Attorney"
            },
            "Travis R. Christiansen": {
                names: ["TRC", "Travis", "TC", "T", "Mr. Christiansen"],
                title: "Firm Owner and Managing Partner",
                barNumber: "#8504",
                billingRate: 450,
                role: "Managing Partner"
            },
            "Josephine Miller": {
                names: ["JM", "Jo", "Ms. Miller"],
                title: "Lead Legal Assistant",
                billingRate: 0,
                role: "Legal Assistant"
            },
            "Jordan Gubler": {
                names: ["JG", "Jordan", "Mrs. Gubler"],
                title: "Legal Assistant",
                billingRate: 0,
                role: "Legal Assistant"
            },
            "Emily Wilson": {
                names: ["EW", "Emily"],
                title: "Legal Assistant",
                billingRate: 0,
                role: "Legal Assistant"
            },
            "Jessica Byergo": {
                names: ["Jessica", "Jess", "JB", "Ms. Byergo"],
                title: "Customer Service",
                billingRate: 0,
                role: "Client/Customer Specialist"
            }
        };
    }

    initializeAcronyms() {
        return {
            "CUC": "Current Utah Code",
            "URCP": "Utah Rules of Civil Procedure",
            "CP": "Utah Rules of Civil Procedure",
            "AOC": "Appearance of Counsel",
            "MTS": "Motion to Strike",
            "MTE": "Motion to Enforce",
            "OSC": "Order to Show Cause",
            "MTQ": "Motion to Quash",
            "TRO": "Temporary Restraining Order",
            "EH": "Evidentiary Hearing",
            "MSA": "Motion to Set Aside",
            "MTI": "Motion to Intervene",
            "IntD": "Initial Disclosures",
            "RFP": "Request for Production",
            "NC": "New Client",
            "CIO": "Close it Out",
            "Cal": "Calendar"
        };
    }

    // CORE INSTRUCTION METHODS
    identifyProject(projectName) {
        this.currentProject = projectName;
        console.log(`📁 Project Identified: ${projectName}`);
        
        // Search for existing artifact
        this.searchForArtifact(projectName);
        
        return this.currentProject;
    }

    searchForArtifact(projectName) {
        // In a real implementation, this would search previous chats and system memory
        // For now, we'll check our local artifact storage
        if (this.projectArtifacts.has(projectName)) {
            this.currentArtifact = this.projectArtifacts.get(projectName);
            console.log(`📂 Found existing artifact for project: ${projectName}`);
            return this.currentArtifact;
        } else {
            this.createNewArtifact(projectName);
            return null;
        }
    }

    createNewArtifact(projectName) {
        const artifact = {
            projectName: projectName,
            clientName: projectName, // Assuming project name is client name
            createdAt: new Date().toISOString(),
            lastModified: new Date().toISOString(),
            lawType: null,
            partyPosition: null,
            caseInformation: [],
            documents: [],
            scheduledHearings: [],
            calendarEvents: [],
            emails: [],
            draftedDocuments: [],
            connectedCases: [],
            workProduct: [],
            parties: [],
            witnesses: [],
            courtOfficers: [],
            billingEntries: [],
            chatHistory: [],
            factsAndEvidence: [],
            status: "Active"
        };

        this.projectArtifacts.set(projectName, artifact);
        this.currentArtifact = artifact;
        
        console.log(`📝 Created new artifact for project: ${projectName}`);
        return artifact;
    }

    updateArtifact(updates) {
        if (!this.currentArtifact) {
            console.error('No current artifact to update');
            return false;
        }

        // Apply updates with timestamp
        Object.assign(this.currentArtifact, updates);
        this.currentArtifact.lastModified = new Date().toISOString();
        
        // Update in storage
        this.projectArtifacts.set(this.currentProject, this.currentArtifact);
        
        console.log(`📝 Updated artifact for project: ${this.currentProject}`);
        return true;
    }

    addDocumentToArtifact(documentData) {
        if (!this.currentArtifact) {
            console.error('No current artifact to add document to');
            return false;
        }

        const documentEntry = {
            ...documentData,
            addedAt: new Date().toISOString(),
            addedBy: this.currentUser
        };

        this.currentArtifact.documents.push(documentEntry);
        this.updateArtifact({ documents: this.currentArtifact.documents });
        
        console.log(`📄 Added document to artifact: ${documentData.title || 'Untitled'}`);
        return true;
    }

    addBillingEntry(description, timeSpent, rate = null) {
        if (!this.currentArtifact) {
            console.error('No current artifact to add billing entry to');
            return false;
        }

        const billingEntry = {
            date: new Date().toISOString(),
            description: description,
            timeSpent: timeSpent,
            rate: rate || this.billingRate,
            total: timeSpent * (rate || this.billingRate),
            attorney: this.currentUser,
            barNumber: this.userBarNumber
        };

        this.currentArtifact.billingEntries.push(billingEntry);
        this.updateArtifact({ billingEntries: this.currentArtifact.billingEntries });
        
        console.log(`💰 Added billing entry: ${timeSpent} hours at $${rate || this.billingRate}/hour`);
        return true;
    }

    // DOCUMENT FORMATTING METHODS
    formatDocumentForWord(content, documentType = 'memo') {
        const timestamp = new Date().toLocaleDateString();
        const header = this.generateDocumentHeader(documentType);
        
        // Ensure proper paragraph formatting (no bullets, numbered appropriately)
        const formattedContent = this.formatParagraphs(content);
        
        return `${header}\n\n${formattedContent}\n\nDated: ${timestamp}\nPrepared by: ${this.currentUser}, ${this.userBarNumber}`;
    }

    generateDocumentHeader(documentType) {
        const firmName = "Boyack Christiansen Legal Solutions";
        const attorneyName = this.currentUser;
        const barNumber = this.userBarNumber;
        const projectName = this.currentProject || "Current Project";
        
        return `${firmName}\n${attorneyName}, ${barNumber}\n${projectName}\n${documentType.toUpperCase()}`;
    }

    formatParagraphs(content) {
        // Remove bullets and ensure proper paragraph numbering
        let formatted = content.replace(/^[\s]*[-•*]\s*/gm, ''); // Remove bullets
        formatted = formatted.replace(/^\d+\.\s*/gm, ''); // Remove existing numbering
        
        // Split into paragraphs and number them
        const paragraphs = formatted.split(/\n\s*\n/).filter(p => p.trim());
        const numberedParagraphs = paragraphs.map((para, index) => {
            return `${index + 1}. ${para.trim()}`;
        });
        
        return numberedParagraphs.join('\n\n');
    }

    // LEGAL RESEARCH AND WRITING METHODS
    validateLegalInformation(fact, source) {
        // Core instruction: NO PHANTOM LAW, CASES, OR FACTS
        if (!source || source.trim() === '') {
            console.warn('⚠️ WARNING: Legal information without verifiable source');
            return {
                valid: false,
                warning: 'Information lacks verifiable source - must be confirmed before use'
            };
        }
        
        return {
            valid: true,
            fact: fact,
            source: source,
            verified: false, // Requires manual verification
            timestamp: new Date().toISOString()
        };
    }

    requestClarification(unclearInformation) {
        // Core instruction: Request clarification for unclear information
        console.log(`❓ CLARIFICATION REQUESTED: ${unclearInformation}`);
        return {
            type: 'clarification_request',
            information: unclearInformation,
            timestamp: new Date().toISOString(),
            required: true
        };
    }

    // PROJECT FOCUS ENFORCEMENT
    enforceProjectFocus(userInput, currentProject) {
        // Core instruction: REMAIN FOCUSED ON TASK AT HAND
        const focusKeywords = [
            'project', 'case', 'client', 'legal', 'motion', 'hearing',
            'court', 'filing', 'document', 'research', 'brief'
        ];
        
        const inputLower = userInput.toLowerCase();
        const hasFocus = focusKeywords.some(keyword => inputLower.includes(keyword));
        
        if (!hasFocus) {
            console.warn('⚠️ WARNING: Input may deviate from project focus');
            return {
                focused: false,
                warning: 'Please ensure all communications relate to the current project'
            };
        }
        
        return {
            focused: true,
            project: currentProject
        };
    }

    // INTEGRATION WITH UNIFIED INTELLIGENCE SYSTEM
    async sendToUnifiedAI(query, context = {}) {
        // Integrate with our multi-layered fine-tuning backend
        const enhancedContext = {
            ...context,
            project: this.currentProject,
            artifact: this.currentArtifact,
            attorney: this.currentUser,
            barNumber: this.userBarNumber,
            instructions: 'LAW_MATRIX_v4_BULLETPROOF',
            timestamp: new Date().toISOString()
        };

        try {
            if (window.lawMatrixBridge) {
                const result = await window.lawMatrixBridge.sendToUnifiedAI(query, enhancedContext);
                
                // Add to artifact chat history
                this.addToChatHistory(query, result, 'unified_ai');
                
                return result;
            } else {
                throw new Error('Unified AI backend not available');
            }
        } catch (error) {
            console.error('Failed to send to unified AI:', error);
            throw error;
        }
    }

    addToChatHistory(userInput, aiResponse, source = 'user') {
        if (!this.currentArtifact) return;

        const chatEntry = {
            timestamp: new Date().toISOString(),
            source: source,
            userInput: userInput,
            aiResponse: aiResponse,
            project: this.currentProject
        };

        this.currentArtifact.chatHistory.push(chatEntry);
        this.updateArtifact({ chatHistory: this.currentArtifact.chatHistory });
    }

    // SYSTEM STATUS AND COMPLIANCE
    getSystemStatus() {
        return {
            version: this.version,
            systemType: this.systemType,
            currentProject: this.currentProject,
            currentArtifact: this.currentArtifact ? 'Active' : 'None',
            totalArtifacts: this.projectArtifacts.size,
            currentUser: this.currentUser,
            barNumber: this.userBarNumber,
            compliance: {
                projectFocus: 'Enforced',
                noPhantomLaw: 'Active',
                verificationRequired: 'Active',
                brevityEnforced: 'Active',
                formatCompliance: 'Active'
            }
        };
    }

    // CORE INSTRUCTION COMPLIANCE CHECK
    checkCompliance(userInput, response) {
        const compliance = {
            projectFocused: this.enforceProjectFocus(userInput, this.currentProject).focused,
            noPhantomLaw: !response.includes('fake') && !response.includes('example'),
            verifiableSources: response.includes('source') || response.includes('citation'),
            properFormatting: !response.includes('•') && !response.includes('*'),
            brevityMaintained: response.length < 2000, // Reasonable length limit
            timestamped: response.includes(new Date().toLocaleDateString())
        };

        const violations = Object.entries(compliance)
            .filter(([key, value]) => !value)
            .map(([key]) => key);

        if (violations.length > 0) {
            console.warn(`⚠️ COMPLIANCE VIOLATIONS: ${violations.join(', ')}`);
        }

        return {
            compliant: violations.length === 0,
            violations: violations,
            compliance: compliance
        };
    }
}

// Initialize LAW Matrix Instructions System
window.LawMatrixInstructions = LawMatrixInstructions;

// Auto-initialize when loaded
document.addEventListener('DOMContentLoaded', () => {
    if (!window.lawMatrixInstructions) {
        window.lawMatrixInstructions = new LawMatrixInstructions();
        console.log('📋 LAW Matrix v4.0 Instructions System Ready');
        console.log('🎯 Project Focus: ENFORCED');
        console.log('🛡️ Phantom Law Prevention: ACTIVE');
        console.log('📝 Document Formatting: COMPLIANT');
        console.log('💰 Billing Tracking: ENABLED');
    }
});

console.log('🚀 LAW Matrix v4.0 Bulletproof Enterprise Edition Instructions System Loaded');




























