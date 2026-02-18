/**
 * Enhanced PracticePanther File Reader
 * Reads real PP data from F:\Dropbox\PracticePanther
 * For ESQs Legal System
 */

const path = require('path');
const fs = require('fs-extra');

class RealPracticePantherReader {
    constructor() {
        this.ppDataPath = process.env.PP_DATA_PATH || 'F:\\Dropbox\\PracticePanther';
        this.dropboxPath = process.env.DROPBOX_PATH || 'F:\\Dropbox';
        console.log('🐾 Real PracticePanther Reader initialized');
        console.log(`📁 PP Data Path: ${this.ppDataPath}`);
        this.indexPPData();
    }

    async indexPPData() {
        try {
            console.log('📊 Indexing real PracticePanther data...');
            
            // Check if PP directory exists
            if (await fs.pathExists(this.ppDataPath)) {
                const structure = await this.analyzePPStructure();
                console.log('✅ PracticePanther data structure analyzed:', structure);
                this.ppStructure = structure;
            } else {
                console.log('⚠️ PracticePanther directory not found at:', this.ppDataPath);
                this.ppStructure = null;
            }
        } catch (error) {
            console.error('❌ Error indexing PracticePanther data:', error);
            this.ppStructure = null;
        }
    }

    async analyzePPStructure() {
        try {
            const contents = await fs.readdir(this.ppDataPath);
            const structure = {
                folders: [],
                files: [],
                clientFolders: [],
                alphabeticalFolders: [],
                exportFiles: [],
                databaseFiles: []
            };

            for (const item of contents) {
                const itemPath = path.join(this.ppDataPath, item);
                const stats = await fs.stat(itemPath);

                if (stats.isDirectory()) {
                    structure.folders.push(item);
                    
                    // Check if it's a single letter folder (A, B, C, etc.)
                    if (/^[A-Z]$/.test(item)) {
                        structure.alphabeticalFolders.push(item);
                        
                        // Look inside alphabetical folders for client folders
                        try {
                            const alphabetContents = await fs.readdir(itemPath);
                            for (const subItem of alphabetContents) {
                                const subItemPath = path.join(itemPath, subItem);
                                const subStats = await fs.stat(subItemPath);
                                
                                if (subStats.isDirectory() && this.looksLikeClientFolder(subItem)) {
                                    structure.clientFolders.push({
                                        name: subItem,
                                        path: subItemPath,
                                        letterFolder: item
                                    });
                                }
                            }
                        } catch (error) {
                            console.log(`Could not read contents of ${item} folder:`, error.message);
                        }
                    }
                    // Also check if it looks like a client folder at root level
                    else if (this.looksLikeClientFolder(item)) {
                        structure.clientFolders.push({
                            name: item,
                            path: itemPath,
                            letterFolder: null
                        });
                    }
                } else {
                    structure.files.push(item);
                    
                    // Check for common PP export files
                    if (item.toLowerCase().includes('export') || 
                        item.endsWith('.csv') || 
                        item.endsWith('.xlsx')) {
                        structure.exportFiles.push(item);
                    }
                    
                    // Check for database files
                    if (item.endsWith('.db') || 
                        item.endsWith('.sqlite') || 
                        item.endsWith('.mdb')) {
                        structure.databaseFiles.push(item);
                    }
                }
            }

            console.log(`✅ Found ${structure.alphabeticalFolders.length} alphabetical folders`);
            console.log(`✅ Found ${structure.clientFolders.length} client folders`);

            return structure;
        } catch (error) {
            console.error('❌ Error analyzing PP structure:', error);
            return null;
        }
    }

    looksLikeClientFolder(folderName) {
        // Skip admin folders
        if (folderName.startsWith('000')) {
            return false;
        }
        
        // Primary pattern: [Last, First] format
        const lastFirstPattern = /^[A-Z][a-z]+,\s*[A-Z][a-z]+$/; // "Stears, Julie"
        if (lastFirstPattern.test(folderName)) {
            return true;
        }
        
        // Alternative patterns for client folders
        const clientPatterns = [
            /^[A-Z][a-z]+ [A-Z][a-z]+$/, // "John Smith"
            /vs?\./, // "Smith v. Jones"
            /^\d{6,}/, // Case numbers
        ];

        return clientPatterns.some(pattern => pattern.test(folderName));
    }

    async searchRealClients(searchText) {
        try {
            console.log(`🔍 Searching real PP data for clients: ${searchText}`);
            
            if (!this.ppStructure) {
                console.log('⚠️ PP structure not available, falling back to sample data');
                return this.getFallbackClients(searchText);
            }

            let clients = [];

            // Search in client folders (now organized in alphabetical folders)
            for (const clientFolder of this.ppStructure.clientFolders) {
                if (clientFolder.name.toLowerCase().includes(searchText.toLowerCase())) {
                    const clientData = await this.extractClientFromFolder(clientFolder.name, clientFolder.path);
                    if (clientData) {
                        clientData.letterFolder = clientFolder.letterFolder;
                        clients.push(clientData);
                    }
                }
            }

            // Also search in export files
            for (const exportFile of this.ppStructure.exportFiles) {
                const exportClients = await this.searchInExportFile(exportFile, searchText);
                clients = clients.concat(exportClients);
            }

            // Remove duplicates
            clients = this.deduplicateClients(clients);

            console.log(`✅ Found ${clients.length} real clients matching "${searchText}"`);
            
            // Log which alphabetical folders were searched
            const searchedFolders = clients.map(c => c.letterFolder).filter(Boolean);
            if (searchedFolders.length > 0) {
                console.log(`📁 Found clients in folders: ${[...new Set(searchedFolders)].join(', ')}`);
            }

            return clients;

        } catch (error) {
            console.error('❌ Error searching real clients:', error);
            return this.getFallbackClients(searchText);
        }
    }

    async searchRealMatters(searchText, clientId = null) {
        try {
            console.log(`🔍 Searching real PP data for matters: ${searchText}`);
            
            if (!this.ppStructure) {
                console.log('⚠️ PP structure not available for matter search');
                return [];
            }

            let matters = [];

            // Search in client folders for matter-related documents
            for (const clientFolder of this.ppStructure.clientFolders) {
                try {
                    const clientDocuments = await this.getClientDocuments(clientFolder.path);
                    
                    // Look for documents that match the search text
                    const matchingDocs = clientDocuments.filter(doc => 
                        doc.name.toLowerCase().includes(searchText.toLowerCase()) ||
                        (doc.type && doc.type.toLowerCase().includes(searchText.toLowerCase()))
                    );
                    
                    if (matchingDocs.length > 0 || 
                        (clientId && clientFolder.name.toLowerCase().includes(searchText.toLowerCase()))) {
                        
                        // Create a matter entry for this client
                        const matter = {
                            id: `real_matter_${clientFolder.name.replace(/[^a-zA-Z0-9]/g, '_')}`,
                            caseNumber: this.extractCaseNumber(clientFolder.name, matchingDocs),
                            clientId: `real_client_${clientFolder.name.replace(/[^a-zA-Z0-9]/g, '_')}`,
                            title: `${clientFolder.name} - Legal Matter`,
                            description: `Case documents for ${clientFolder.name}`,
                            status: 'Active',
                            openDate: await this.inferClientSinceDate(clientFolder.path),
                            court: this.extractCourtInfo(matchingDocs),
                            judge: this.extractJudgeInfo(matchingDocs),
                            opposingCounsel: this.extractOpposingCounsel(matchingDocs),
                            practiceArea: this.inferPracticeArea(matchingDocs),
                            tags: ['real-pp-data', 'file-based'],
                            notes: `Matter based on documents in ${clientFolder.name} folder`,
                            lastActivity: new Date().toISOString().split('T')[0],
                            source: 'practicepanther_files',
                            folderPath: clientFolder.path,
                            letterFolder: clientFolder.letterFolder,
                            documents: matchingDocs
                        };
                        
                        matters.push(matter);
                    }
                } catch (error) {
                    console.error(`Error processing client folder ${clientFolder.name}:`, error);
                }
            }

            // Remove duplicates and filter by clientId if specified
            matters = this.deduplicateMatters(matters);
            
            if (clientId) {
                matters = matters.filter(matter => matter.clientId === clientId);
            }

            console.log(`✅ Found ${matters.length} real matters matching "${searchText}"`);
            return matters;

        } catch (error) {
            console.error('❌ Error searching real matters:', error);
            return [];
        }
    }

    extractCaseNumber(clientName, documents) {
        // Try to extract case number from folder name or documents
        const caseNumberPattern = /\b\d{6,}\b/;
        
        // First check folder name
        const folderMatch = clientName.match(caseNumberPattern);
        if (folderMatch) {
            return folderMatch[0];
        }
        
        // Then check document names
        for (const doc of documents) {
            const docMatch = doc.name.match(caseNumberPattern);
            if (docMatch) {
                return docMatch[0];
            }
        }
        
        // Generate a placeholder case number
        return '244501' + Math.floor(Math.random() * 1000).toString().padStart(3, '0');
    }

    extractCourtInfo(documents) {
        const courtPatterns = [
            /Fifth Judicial District Court/i,
            /District Court/i,
            /Superior Court/i,
            /Family Court/i,
            /Washington County/i,
            /Utah/i
        ];
        
        for (const doc of documents) {
            for (const pattern of courtPatterns) {
                if (pattern.test(doc.name)) {
                    if (doc.name.toLowerCase().includes('washington')) {
                        return 'Fifth Judicial District Court, Washington County';
                    }
                    return 'Fifth Judicial District Court';
                }
            }
        }
        
        return 'Court TBD';
    }

    extractJudgeInfo(documents) {
        const judgePatterns = [
            /Judge\s+(\w+\s+\w+\s+\w+)/i,
            /Hon\.\s+(\w+\s+\w+\s+\w+)/i,
            /Walton/i
        ];
        
        for (const doc of documents) {
            for (const pattern of judgePatterns) {
                const match = doc.name.match(pattern);
                if (match) {
                    if (match[0].toLowerCase().includes('walton')) {
                        return 'John J. Walton';
                    }
                    return match[1] || match[0];
                }
            }
        }
        
        return null;
    }

    extractOpposingCounsel(documents) {
        const counselPatterns = [
            /Graff/i,
            /opposing/i,
            /defendant/i,
            /respondent/i
        ];
        
        for (const doc of documents) {
            for (const pattern of counselPatterns) {
                if (pattern.test(doc.name)) {
                    if (doc.name.toLowerCase().includes('graff')) {
                        return 'K. Jake Graff, Graff Law Firm';
                    }
                }
            }
        }
        
        return null;
    }

    inferPracticeArea(documents) {
        const practiceAreas = {
            'family': ['divorce', 'custody', 'marriage', 'child', 'spousal', 'alimony'],
            'criminal': ['criminal', 'dui', 'arrest', 'charge', 'plea'],
            'business': ['contract', 'business', 'corporate', 'llc', 'partnership'],
            'real estate': ['property', 'deed', 'real estate', 'title', 'mortgage'],
            'personal injury': ['injury', 'accident', 'medical', 'insurance', 'damages']
        };
        
        const allDocNames = documents.map(doc => doc.name.toLowerCase()).join(' ');
        
        for (const [area, keywords] of Object.entries(practiceAreas)) {
            if (keywords.some(keyword => allDocNames.includes(keyword))) {
                return area.charAt(0).toUpperCase() + area.slice(1);
            }
        }
        
        return 'General Practice';
    }

    deduplicateMatters(matters) {
        const seen = new Set();
        return matters.filter(matter => {
            const key = `${matter.clientId}_${matter.caseNumber}`.toLowerCase();
            if (seen.has(key)) {
                return false;
            }
            seen.add(key);
            return true;
        });
    }

    async extractClientFromFolder(folderName, folderPath = null) {
        try {
            // If folderPath not provided, construct it (for backward compatibility)
            if (!folderPath) {
                folderPath = path.join(this.ppDataPath, folderName);
            }
            
            // Try to find client info files
            const possibleInfoFiles = [
                'client_info.json',
                'client.json',
                'info.txt',
                'details.txt'
            ];

            let clientInfo = null;
            
            for (const infoFile of possibleInfoFiles) {
                const infoPath = path.join(folderPath, infoFile);
                if (await fs.pathExists(infoPath)) {
                    try {
                        if (infoFile.endsWith('.json')) {
                            clientInfo = await fs.readJson(infoPath);
                        } else {
                            const content = await fs.readFile(infoPath, 'utf8');
                            clientInfo = this.parseClientInfoText(content);
                        }
                        break;
                    } catch (error) {
                        console.log(`Could not parse ${infoFile}:`, error.message);
                    }
                }
            }

            // If no info file found, extract from folder structure
            if (!clientInfo) {
                clientInfo = this.inferClientFromFolderName(folderName);
            }

            // Check for documents
            const documents = await this.getClientDocuments(folderPath);

            return {
                id: `real_client_${folderName.replace(/[^a-zA-Z0-9]/g, '_')}`,
                name: clientInfo.name || folderName,
                firstName: clientInfo.firstName || this.extractFirstName(folderName),
                lastName: clientInfo.lastName || this.extractLastName(folderName),
                email: clientInfo.email || `${folderName.replace(/[^a-zA-Z0-9]/g, '').toLowerCase()}@example.com`,
                phoneWork: clientInfo.phone || '(435) 555-0000',
                phoneMobile: clientInfo.mobile || null,
                dateOfBirth: clientInfo.dateOfBirth || null,
                clientSince: clientInfo.clientSince || this.inferClientSinceDate(folderPath),
                address: clientInfo.address || 'Address on file',
                fullAddress: clientInfo.fullAddress || {},
                tags: ['real-pp-data', ...(clientInfo.tags || [])],
                source: 'practicepanther_files',
                folderPath: folderPath,
                folderName: folderName,
                documents: documents
            };

        } catch (error) {
            console.error(`❌ Error extracting client from folder ${folderName}:`, error);
            return null;
        }
    }

    async getClientDocuments(clientFolderPath) {
        try {
            const documents = [];
            const contents = await fs.readdir(clientFolderPath);

            for (const item of contents) {
                const itemPath = path.join(clientFolderPath, item);
                const stats = await fs.stat(itemPath);

                if (!stats.isDirectory()) {
                    const ext = path.extname(item).toLowerCase();
                    if (['.pdf', '.doc', '.docx', '.txt', '.xlsx', '.xls'].includes(ext)) {
                        documents.push({
                            name: item,
                            path: itemPath,
                            size: stats.size,
                            modified: stats.mtime,
                            type: this.getDocumentType(item)
                        });
                    }
                }
            }

            return documents;
        } catch (error) {
            console.error('Error getting client documents:', error);
            return [];
        }
    }

    getDocumentType(filename) {
        const lower = filename.toLowerCase();
        if (lower.includes('motion')) return 'motion';
        if (lower.includes('brief')) return 'brief';
        if (lower.includes('memo')) return 'memo';
        if (lower.includes('contract')) return 'contract';
        if (lower.includes('agreement')) return 'agreement';
        if (lower.includes('order')) return 'court_order';
        if (lower.includes('pleading')) return 'pleading';
        return 'document';
    }

    async searchInExportFile(exportFile, searchText) {
        try {
            const filePath = path.join(this.ppDataPath, exportFile);
            
            if (exportFile.endsWith('.csv')) {
                return await this.searchCSVFile(filePath, searchText);
            } else if (exportFile.endsWith('.xlsx') || exportFile.endsWith('.xls')) {
                return await this.searchExcelFile(filePath, searchText);
            }
            
            return [];
        } catch (error) {
            console.error(`Error searching export file ${exportFile}:`, error);
            return [];
        }
    }

    async searchCSVFile(filePath, searchText) {
        try {
            const content = await fs.readFile(filePath, 'utf8');
            const lines = content.split('\n');
            
            if (lines.length < 2) return [];
            
            const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
            const clients = [];

            for (let i = 1; i < lines.length; i++) {
                const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
                
                if (values.join(' ').toLowerCase().includes(searchText.toLowerCase())) {
                    const client = this.parseCSVRowToClient(headers, values, i);
                    if (client) clients.push(client);
                }
            }

            return clients;
        } catch (error) {
            console.error('Error searching CSV file:', error);
            return [];
        }
    }

    parseCSVRowToClient(headers, values, rowIndex) {
        try {
            const client = {
                id: `csv_client_${rowIndex}`,
                source: 'practicepanther_csv',
                tags: ['real-pp-data', 'csv-import']
            };

            // Map common field names
            const fieldMappings = {
                'name': ['name', 'client_name', 'full_name', 'client'],
                'firstName': ['first_name', 'fname', 'first'],
                'lastName': ['last_name', 'lname', 'last'],
                'email': ['email', 'email_address', 'contact_email'],
                'phone': ['phone', 'phone_number', 'work_phone', 'office_phone'],
                'address': ['address', 'street_address', 'addr'],
                'clientSince': ['created_date', 'client_since', 'date_created']
            };

            for (const [clientField, possibleHeaders] of Object.entries(fieldMappings)) {
                for (const header of possibleHeaders) {
                    const index = headers.findIndex(h => h.toLowerCase().includes(header.toLowerCase()));
                    if (index !== -1 && values[index]) {
                        client[clientField] = values[index];
                        break;
                    }
                }
            }

            // Ensure we have at least a name
            if (!client.name && client.firstName && client.lastName) {
                client.name = `${client.firstName} ${client.lastName}`;
            }

            return client.name ? client : null;
        } catch (error) {
            console.error('Error parsing CSV row:', error);
            return null;
        }
    }

    inferClientFromFolderName(folderName) {
        // Try to extract client info from folder name patterns
        const patterns = {
            // "Stears, Julie" (primary format for this user)
            lastFirst: /^([A-Z][a-z]+),\s*([A-Z][a-z]+).*$/,
            // "Smith, John - Case 123456"
            lastFirstCase: /^([A-Z][a-z]+),\s*([A-Z][a-z]+)\s*-\s*(.*)?$/,
            // "John Smith"
            simple: /^([A-Z][a-z]+)\s+([A-Z][a-z]+)$/,
            // "Smith v. Jones"
            versus: /^([A-Z][a-z]+)\s+vs?\.\s+([A-Z][a-z]+)/i
        };

        for (const [patternName, regex] of Object.entries(patterns)) {
            const match = folderName.match(regex);
            if (match) {
                switch (patternName) {
                    case 'lastFirst':
                        return {
                            lastName: match[1],
                            firstName: match[2],
                            name: `${match[2]} ${match[1]}` // Convert to "First Last"
                        };
                    case 'lastFirstCase':
                        return {
                            lastName: match[1],
                            firstName: match[2],
                            name: `${match[2]} ${match[1]}`, // Convert to "First Last"
                            caseInfo: match[3]
                        };
                    case 'simple':
                        return {
                            firstName: match[1],
                            lastName: match[2],
                            name: `${match[1]} ${match[2]}`
                        };
                    case 'versus':
                        return {
                            firstName: match[1],
                            lastName: '',
                            name: match[1],
                            caseType: 'litigation'
                        };
                }
            }
        }

        // Fallback
        return {
            name: folderName,
            firstName: folderName.split(' ')[0] || folderName,
            lastName: folderName.split(' ')[1] || ''
        };
    }

    async inferClientSinceDate(folderPath) {
        try {
            const stats = await fs.stat(folderPath);
            return stats.birthtime.toISOString().split('T')[0];
        } catch (error) {
            return new Date().toISOString().split('T')[0];
        }
    }

    extractFirstName(folderName) {
        // Handle "Last, First" format
        if (folderName.includes(',')) {
            const parts = folderName.split(',');
            return parts[1]?.trim() || 'Unknown';
        }
        // Handle "First Last" format
        return folderName.split(' ')[0] || 'Unknown';
    }

    extractLastName(folderName) {
        // Handle "Last, First" format  
        if (folderName.includes(',')) {
            const parts = folderName.split(',');
            return parts[0]?.trim() || '';
        }
        // Handle "First Last" format
        const parts = folderName.split(' ');
        return parts[1] || '';
    }

    deduplicateClients(clients) {
        const seen = new Set();
        return clients.filter(client => {
            const key = `${client.name}_${client.email}`.toLowerCase();
            if (seen.has(key)) {
                return false;
            }
            seen.add(key);
            return true;
        });
    }

    getFallbackClients(searchText) {
        // Return sample data if real data not available
        const fallbackClients = [
            {
                id: 'fallback_001',
                name: 'Julie Stears',
                firstName: 'Julie',
                lastName: 'Stears',
                email: 'julie.stears@email.com',
                phoneWork: '(435) 555-0123',
                clientSince: '2024-01-15',
                address: 'St. George, UT',
                tags: ['fallback-data'],
                source: 'fallback'
            }
        ];

        return fallbackClients.filter(client => 
            client.name.toLowerCase().includes(searchText.toLowerCase())
        );
    }

    parseClientInfoText(content) {
        // Parse text files for client information
        const info = {};
        const lines = content.split('\n');

        for (const line of lines) {
            const [key, ...valueParts] = line.split(':');
            if (key && valueParts.length > 0) {
                const value = valueParts.join(':').trim();
                const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, '_');
                
                switch (normalizedKey) {
                    case 'name':
                    case 'client_name':
                        info.name = value;
                        break;
                    case 'email':
                    case 'email_address':
                        info.email = value;
                        break;
                    case 'phone':
                    case 'phone_number':
                        info.phone = value;
                        break;
                    case 'address':
                        info.address = value;
                        break;
                    case 'date_of_birth':
                    case 'dob':
                        info.dateOfBirth = value;
                        break;
                }
            }
        }

        return info;
    }

    // Method to sync real PP data with ESQs
    async syncClientToESQs(clientName, ppClientData) {
        try {
            const sanitizedName = clientName.replace(/[^a-zA-Z0-9\s-_]/g, '').trim();
            const esqsClientPath = path.join(process.env.F_DRIVE_PATH || 'F:\\ESQs-Legal-System', 'client_data', sanitizedName);
            
            // Create ESQs client structure
            await fs.ensureDir(esqsClientPath);
            await fs.ensureDir(path.join(esqsClientPath, 'practicepanther_sync'));
            
            // Copy relevant documents from PP to ESQs
            if (ppClientData.documents && ppClientData.documents.length > 0) {
                const esqsDocsPath = path.join(esqsClientPath, 'documents');
                await fs.ensureDir(esqsDocsPath);
                
                for (const doc of ppClientData.documents) {
                    try {
                        const targetPath = path.join(esqsDocsPath, doc.name);
                        await fs.copy(doc.path, targetPath);
                        console.log(`📄 Synced document: ${doc.name}`);
                    } catch (error) {
                        console.error(`❌ Failed to sync document ${doc.name}:`, error);
                    }
                }
            }
            
            // Save PP data reference
            const ppDataFile = path.join(esqsClientPath, 'practicepanther_sync', 'pp_client_data.json');
            await fs.writeJson(ppDataFile, ppClientData, { spaces: 2 });
            
            console.log(`✅ Synced PP client ${clientName} to ESQs`);
            return { success: true, path: esqsClientPath };
            
        } catch (error) {
            console.error(`❌ Error syncing client ${clientName} to ESQs:`, error);
            throw error;
        }
    }
}

// Export for use in main server
module.exports = RealPracticePantherReader;