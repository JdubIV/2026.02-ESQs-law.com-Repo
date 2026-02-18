#!/usr/bin/env python3
"""
LAW Matrix v4.0 - Multi-Source Contextual Awareness System
Advanced user context tracking and proactive assistance
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import logging

class ContextType(Enum):
    UI_STATE = "ui_state"
    USER_ACTIVITY = "user_activity"
    DOCUMENT_STATE = "document_state"
    CASE_CONTEXT = "case_context"
    SESSION_MEMORY = "session_memory"
    HISTORICAL_DATA = "historical_data"
    ENVIRONMENTAL = "environmental"

class ActivityType(Enum):
    DOCUMENT_VIEW = "document_view"
    QUERY_SUBMITTED = "query_submitted"
    SEARCH_PERFORMED = "search_performed"
    TEMPLATE_ACCESSED = "template_accessed"
    CASE_OPENED = "case_opened"
    NAVIGATION = "navigation"
    IDLE = "idle"

@dataclass
class UIState:
    """Represents current UI state and user interface context"""
    current_page: str
    active_tabs: List[str]
    focused_element: str
    scroll_position: Dict[str, int]
    form_data: Dict[str, Any]
    selected_text: Optional[str] = None
    mouse_position: Optional[Tuple[int, int]] = None

@dataclass
class UserActivity:
    """Represents user activity and behavior patterns"""
    activity_type: ActivityType
    timestamp: datetime
    duration: float
    context_data: Dict[str, Any]
    user_intent: Optional[str] = None
    confidence_score: float = 0.0

@dataclass
class DocumentState:
    """Represents current document state and editing context"""
    document_id: str
    document_type: str
    current_section: str
    cursor_position: int
    recent_changes: List[Dict[str, Any]]
    collaborators: List[str]
    version: str

@dataclass
class CaseContext:
    """Represents current case context and legal matter information"""
    case_id: str
    case_title: str
    case_type: str
    jurisdiction: str
    parties: List[Dict[str, str]]
    key_dates: Dict[str, datetime]
    current_phase: str
    assigned_attorney: str
    case_status: str

@dataclass
class SessionMemory:
    """Represents session memory and conversation context"""
    session_id: str
    conversation_history: List[Dict[str, Any]]
    user_preferences: Dict[str, Any]
    current_topics: List[str]
    unresolved_queries: List[str]
    context_switches: List[datetime]

@dataclass
class HistoricalData:
    """Represents historical user data and patterns"""
    user_id: str
    total_sessions: int
    average_session_duration: float
    preferred_features: List[str]
    common_queries: List[Tuple[str, int]]
    document_types_used: Dict[str, int]
    time_patterns: Dict[str, int]  # hour of day -> frequency
    productivity_metrics: Dict[str, float]

@dataclass
class EnvironmentalContext:
    """Represents environmental and system context"""
    time_of_day: str
    day_of_week: str
    system_performance: Dict[str, float]
    network_status: str
    device_type: str
    browser_info: Dict[str, str]
    location_context: Optional[str] = None

@dataclass
class ComprehensiveContext:
    """Comprehensive user context combining all sources"""
    user_id: str
    session_id: str
    timestamp: datetime
    ui_state: UIState
    user_activity: List[UserActivity]
    document_state: Optional[DocumentState]
    case_context: Optional[CaseContext]
    session_memory: SessionMemory
    historical_data: HistoricalData
    environmental_context: EnvironmentalContext
    context_confidence: float
    derived_insights: Dict[str, Any]

class LawMatrixContextualAwarenessSystem:
    """
    Multi-source contextual awareness system for LAW Matrix v4.0
    Provides comprehensive user understanding and proactive assistance
    """
    
    def __init__(self, db_path: str = "lawmatrix_context.db"):
        self.db_path = db_path
        self.logger = self._setup_logging()
        self._initialize_database()
        
        # Context tracking
        self.active_sessions = {}
        self.user_patterns = defaultdict(list)
        self.context_history = deque(maxlen=1000)
        
        # Proactive assistance thresholds
        self.assistance_thresholds = {
            'idle_time': 300,  # 5 minutes
            'query_complexity': 0.8,
            'document_complexity': 0.7,
            'case_urgency': 0.9
        }
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for contextual awareness"""
        
        logger = logging.getLogger('LawMatrixContextualAwareness')
        logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler('lawmatrix_contextual_awareness.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    def _initialize_database(self):
        """Initialize database for contextual data storage"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create comprehensive context table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comprehensive_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                ui_state TEXT NOT NULL,
                user_activity TEXT NOT NULL,
                document_state TEXT,
                case_context TEXT,
                session_memory TEXT NOT NULL,
                historical_data TEXT NOT NULL,
                environmental_context TEXT NOT NULL,
                context_confidence REAL NOT NULL,
                derived_insights TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create user patterns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_patterns (
                user_id TEXT PRIMARY KEY,
                total_sessions INTEGER NOT NULL,
                average_session_duration REAL NOT NULL,
                preferred_features TEXT,
                common_queries TEXT,
                document_types_used TEXT,
                time_patterns TEXT,
                productivity_metrics TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create proactive assistance table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS proactive_assistance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                assistance_type TEXT NOT NULL,
                trigger_context TEXT NOT NULL,
                suggested_action TEXT NOT NULL,
                user_response TEXT,
                effectiveness_score REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ LAW Matrix v4.0 - Contextual awareness database initialized")
        
    def update_context(self, context: ComprehensiveContext):
        """Update comprehensive user context"""
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO comprehensive_context 
            (user_id, session_id, timestamp, ui_state, user_activity, document_state,
             case_context, session_memory, historical_data, environmental_context,
             context_confidence, derived_insights)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            context.user_id,
            context.session_id,
            context.timestamp.isoformat(),
            json.dumps(asdict(context.ui_state)),
            json.dumps([asdict(activity) for activity in context.user_activity]),
            json.dumps(asdict(context.document_state)) if context.document_state else None,
            json.dumps(asdict(context.case_context)) if context.case_context else None,
            json.dumps(asdict(context.session_memory)),
            json.dumps(asdict(context.historical_data)),
            json.dumps(asdict(context.environmental_context)),
            context.context_confidence,
            json.dumps(context.derived_insights)
        ))
        
        conn.commit()
        conn.close()
        
        # Update in-memory tracking
        self.active_sessions[context.session_id] = context
        self.context_history.append(context)
        
        # Log context update
        self.logger.info(f"Context updated for user {context.user_id}, session {context.session_id}")
        
    def capture_ui_state(self, user_id: str, session_id: str, ui_data: Dict[str, Any]) -> UIState:
        """Capture current UI state"""
        
        return UIState(
            current_page=ui_data.get('current_page', ''),
            active_tabs=ui_data.get('active_tabs', []),
            focused_element=ui_data.get('focused_element', ''),
            scroll_position=ui_data.get('scroll_position', {}),
            form_data=ui_data.get('form_data', {}),
            selected_text=ui_data.get('selected_text'),
            mouse_position=ui_data.get('mouse_position')
        )
        
    def capture_user_activity(self, user_id: str, activity_data: Dict[str, Any]) -> UserActivity:
        """Capture user activity and behavior"""
        
        return UserActivity(
            activity_type=ActivityType(activity_data.get('type', 'idle')),
            timestamp=datetime.now(),
            duration=activity_data.get('duration', 0.0),
            context_data=activity_data.get('context', {}),
            user_intent=activity_data.get('intent'),
            confidence_score=activity_data.get('confidence', 0.0)
        )
        
    def analyze_user_intent(self, context: ComprehensiveContext) -> Dict[str, Any]:
        """Analyze user intent based on comprehensive context"""
        
        intent_analysis = {
            'primary_intent': None,
            'confidence': 0.0,
            'supporting_evidence': [],
            'recommended_actions': []
        }
        
        # Analyze recent activities
        recent_activities = [a for a in context.user_activity 
                           if (datetime.now() - a.timestamp).seconds < 300]  # Last 5 minutes
        
        if not recent_activities:
            return intent_analysis
            
        # Pattern recognition for common intents
        activity_types = [a.activity_type for a in recent_activities]
        
        # Document editing intent
        if ActivityType.DOCUMENT_VIEW in activity_types:
            intent_analysis['primary_intent'] = 'document_editing'
            intent_analysis['confidence'] = 0.8
            intent_analysis['supporting_evidence'].append('User viewing documents')
            
            if context.document_state:
                intent_analysis['recommended_actions'].extend([
                    'Provide relevant templates',
                    'Suggest legal precedents',
                    'Offer document formatting assistance'
                ])
                
        # Legal research intent
        if ActivityType.SEARCH_PERFORMED in activity_types or ActivityType.QUERY_SUBMITTED in activity_types:
            intent_analysis['primary_intent'] = 'legal_research'
            intent_analysis['confidence'] = 0.9
            intent_analysis['supporting_evidence'].append('User performing searches')
            
            intent_analysis['recommended_actions'].extend([
                'Provide comprehensive legal analysis',
                'Suggest related case law',
                'Offer jurisdiction-specific guidance'
            ])
            
        # Case management intent
        if ActivityType.CASE_OPENED in activity_types:
            intent_analysis['primary_intent'] = 'case_management'
            intent_analysis['confidence'] = 0.85
            intent_analysis['supporting_evidence'].append('User accessing case information')
            
            if context.case_context:
                intent_analysis['recommended_actions'].extend([
                    'Provide case timeline overview',
                    'Suggest next legal steps',
                    'Offer deadline reminders'
                ])
                
        return intent_analysis
        
    def generate_proactive_assistance(self, context: ComprehensiveContext) -> List[Dict[str, Any]]:
        """Generate proactive assistance suggestions based on context"""
        
        assistance_suggestions = []
        
        # Check for idle time
        if context.user_activity:
            last_activity = max(context.user_activity, key=lambda x: x.timestamp)
            idle_time = (datetime.now() - last_activity.timestamp).seconds
            
            if idle_time > self.assistance_thresholds['idle_time']:
                assistance_suggestions.append({
                    'type': 'idle_assistance',
                    'priority': 'medium',
                    'message': 'Would you like assistance with your current task?',
                    'suggested_actions': [
                        'Continue document editing',
                        'Research related legal topics',
                        'Review case timeline'
                    ],
                    'confidence': 0.7
                })
                
        # Check for complex queries
        if context.session_memory.conversation_history:
            recent_queries = [conv for conv in context.session_memory.conversation_history[-5:]]
            
            for query in recent_queries:
                if len(query.get('input', '')) > 200:  # Complex query
                    assistance_suggestions.append({
                        'type': 'complex_query_assistance',
                        'priority': 'high',
                        'message': 'Complex query detected - would you like me to break this down?',
                        'suggested_actions': [
                            'Provide step-by-step analysis',
                            'Offer related subtopics',
                            'Suggest specific legal areas'
                        ],
                        'confidence': 0.9
                    })
                    
        # Check for case urgency
        if context.case_context and context.case_context.key_dates:
            upcoming_deadlines = []
            for date_name, date_value in context.case_context.key_dates.items():
                if isinstance(date_value, datetime):
                    days_until = (date_value - datetime.now()).days
                    if 0 <= days_until <= 7:  # Within a week
                        upcoming_deadlines.append(date_name)
                        
            if upcoming_deadlines:
                assistance_suggestions.append({
                    'type': 'deadline_reminder',
                    'priority': 'critical',
                    'message': f'Upcoming deadlines: {", ".join(upcoming_deadlines)}',
                    'suggested_actions': [
                        'Review deadline requirements',
                        'Prepare necessary documents',
                        'Set reminder notifications'
                    ],
                    'confidence': 1.0
                })
                
        # Check for document complexity
        if context.document_state and context.document_state.recent_changes:
            change_count = len(context.document_state.recent_changes)
            if change_count > 20:  # Many recent changes
                assistance_suggestions.append({
                    'type': 'document_complexity',
                    'priority': 'medium',
                    'message': 'Document has many recent changes - would you like help organizing?',
                    'suggested_actions': [
                        'Review change history',
                        'Suggest document structure',
                        'Offer formatting assistance'
                    ],
                    'confidence': 0.8
                })
                
        return assistance_suggestions
        
    def update_user_patterns(self, user_id: str, session_data: Dict[str, Any]):
        """Update user behavior patterns"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get existing patterns
        cursor.execute("SELECT * FROM user_patterns WHERE user_id = ?", (user_id,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing patterns
            patterns = {
                'total_sessions': existing[1] + 1,
                'average_session_duration': (existing[2] + session_data.get('duration', 0)) / 2,
                'preferred_features': json.loads(existing[3]) if existing[3] else [],
                'common_queries': json.loads(existing[4]) if existing[4] else [],
                'document_types_used': json.loads(existing[5]) if existing[5] else {},
                'time_patterns': json.loads(existing[6]) if existing[6] else {},
                'productivity_metrics': json.loads(existing[7]) if existing[7] else {}
            }
        else:
            # Create new patterns
            patterns = {
                'total_sessions': 1,
                'average_session_duration': session_data.get('duration', 0),
                'preferred_features': [],
                'common_queries': [],
                'document_types_used': {},
                'time_patterns': {},
                'productivity_metrics': {}
            }
            
        # Update patterns with new data
        if 'features_used' in session_data:
            patterns['preferred_features'].extend(session_data['features_used'])
            
        if 'queries' in session_data:
            patterns['common_queries'].extend(session_data['queries'])
            
        if 'document_types' in session_data:
            for doc_type in session_data['document_types']:
                patterns['document_types_used'][doc_type] = patterns['document_types_used'].get(doc_type, 0) + 1
                
        # Update time patterns
        current_hour = datetime.now().hour
        patterns['time_patterns'][str(current_hour)] = patterns['time_patterns'].get(str(current_hour), 0) + 1
        
        # Store updated patterns
        cursor.execute('''
            INSERT OR REPLACE INTO user_patterns 
            (user_id, total_sessions, average_session_duration, preferred_features,
             common_queries, document_types_used, time_patterns, productivity_metrics)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            patterns['total_sessions'],
            patterns['average_session_duration'],
            json.dumps(patterns['preferred_features']),
            json.dumps(patterns['common_queries']),
            json.dumps(patterns['document_types_used']),
            json.dumps(patterns['time_patterns']),
            json.dumps(patterns['productivity_metrics'])
        ))
        
        conn.commit()
        conn.close()
        
    def get_contextual_recommendations(self, user_id: str, current_context: Dict[str, Any]) -> Dict[str, Any]:
        """Get personalized recommendations based on user context"""
        
        # Get user patterns
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM user_patterns WHERE user_id = ?", (user_id,))
        patterns = cursor.fetchone()
        conn.close()
        
        if not patterns:
            return {'error': 'No user patterns found'}
            
        recommendations = {
            'personalized_features': [],
            'suggested_workflows': [],
            'time_based_suggestions': [],
            'productivity_tips': []
        }
        
        # Parse patterns
        preferred_features = json.loads(patterns[3]) if patterns[3] else []
        common_queries = json.loads(patterns[4]) if patterns[4] else []
        document_types = json.loads(patterns[5]) if patterns[5] else {}
        time_patterns = json.loads(patterns[6]) if patterns[6] else {}
        
        # Generate personalized recommendations
        if 'document_editing' in preferred_features:
            recommendations['personalized_features'].append('Advanced document templates')
            
        if 'legal_research' in preferred_features:
            recommendations['suggested_workflows'].append('Automated case law research workflow')
            
        # Time-based suggestions
        current_hour = datetime.now().hour
        if str(current_hour) in time_patterns and time_patterns[str(current_hour)] > 5:
            recommendations['time_based_suggestions'].append(
                f'You typically work at {current_hour}:00 - consider scheduling important tasks now'
            )
            
        # Document type suggestions
        most_used_type = max(document_types.items(), key=lambda x: x[1]) if document_types else None
        if most_used_type:
            recommendations['productivity_tips'].append(
                f'You frequently use {most_used_type[0]} documents - consider creating custom templates'
            )
            
        return recommendations

class LawMatrixProactiveAssistant:
    """
    Proactive assistant that provides intelligent, context-aware assistance
    """
    
    def __init__(self, contextual_awareness: LawMatrixContextualAwarenessSystem):
        self.contextual_awareness = contextual_awareness
        
    def process_user_interaction(self, user_id: str, session_id: str, interaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process user interaction and provide contextual response"""
        
        # Capture current context
        ui_state = self.contextual_awareness.capture_ui_state(user_id, session_id, interaction_data.get('ui_state', {}))
        user_activity = self.contextual_awareness.capture_user_activity(user_id, interaction_data.get('activity', {}))
        
        # Create comprehensive context
        comprehensive_context = ComprehensiveContext(
            user_id=user_id,
            session_id=session_id,
            timestamp=datetime.now(),
            ui_state=ui_state,
            user_activity=[user_activity],
            document_state=None,  # Would be populated from real data
            case_context=None,    # Would be populated from real data
            session_memory=SessionMemory(
                session_id=session_id,
                conversation_history=interaction_data.get('conversation_history', []),
                user_preferences=interaction_data.get('user_preferences', {}),
                current_topics=interaction_data.get('current_topics', []),
                unresolved_queries=interaction_data.get('unresolved_queries', []),
                context_switches=[]
            ),
            historical_data=HistoricalData(
                user_id=user_id,
                total_sessions=1,
                average_session_duration=0.0,
                preferred_features=[],
                common_queries=[],
                document_types_used={},
                time_patterns={},
                productivity_metrics={}
            ),
            environmental_context=EnvironmentalContext(
                time_of_day=datetime.now().strftime("%H:%M"),
                day_of_week=datetime.now().strftime("%A"),
                system_performance={},
                network_status="good",
                device_type="desktop",
                browser_info={}
            ),
            context_confidence=0.8,
            derived_insights={}
        )
        
        # Analyze intent
        intent_analysis = self.contextual_awareness.analyze_user_intent(comprehensive_context)
        
        # Generate proactive assistance
        assistance_suggestions = self.contextual_awareness.generate_proactive_assistance(comprehensive_context)
        
        # Get personalized recommendations
        recommendations = self.contextual_awareness.get_contextual_recommendations(user_id, interaction_data)
        
        return {
            'intent_analysis': intent_analysis,
            'assistance_suggestions': assistance_suggestions,
            'personalized_recommendations': recommendations,
            'context_confidence': comprehensive_context.context_confidence
        }

if __name__ == "__main__":
    # Initialize contextual awareness system
    contextual_awareness = LawMatrixContextualAwarenessSystem()
    
    # Create sample interaction
    sample_interaction = {
        'ui_state': {
            'current_page': '/case/244501169',
            'active_tabs': ['documents', 'timeline'],
            'focused_element': 'search_input',
            'scroll_position': {'y': 150},
            'form_data': {'search_query': 'custody factors'}
        },
        'activity': {
            'type': 'query_submitted',
            'duration': 2.5,
            'context': {'query_type': 'legal_research'},
            'intent': 'find_custody_guidelines',
            'confidence': 0.9
        },
        'conversation_history': [
            {'input': 'What are the custody factors in Utah?', 'output': 'Utah considers best interests...'}
        ],
        'user_preferences': {'jurisdiction': 'Utah', 'practice_area': 'family_law'},
        'current_topics': ['custody', 'family_law'],
        'unresolved_queries': []
    }
    
    # Process interaction
    proactive_assistant = LawMatrixProactiveAssistant(contextual_awareness)
    result = proactive_assistant.process_user_interaction(
        'user_123', 
        'session_456', 
        sample_interaction
    )
    
    print("✅ LAW Matrix v4.0 - Contextual awareness system operational!")
    print(f"Intent: {result['intent_analysis']['primary_intent']}")
    print(f"Confidence: {result['intent_analysis']['confidence']}")
    print(f"Assistance suggestions: {len(result['assistance_suggestions'])}")
