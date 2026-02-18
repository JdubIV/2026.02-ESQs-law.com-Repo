#!/usr/bin/env python3
"""
LAW Matrix v4.0 - LLM Observability System
Robust monitoring and feedback loop for continuous improvement
"""

import os
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import numpy as np
from collections import defaultdict, Counter
import logging
from enum import Enum

class InteractionType(Enum):
    QUERY = "query"
    RESPONSE = "response"
    ERROR = "error"
    FEEDBACK = "feedback"

class QualityMetric(Enum):
    RELEVANCE = "relevance"
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CLARITY = "clarity"

@dataclass
class InteractionLog:
    """Represents a single user interaction with the AI system"""
    id: str
    user_id: str
    session_id: str
    timestamp: datetime
    interaction_type: InteractionType
    input_data: Dict[str, Any]
    output_data: Dict[str, Any]
    processing_time_ms: float
    tokens_used: int
    model_version: str
    context_data: Dict[str, Any]
    quality_scores: Dict[str, float]
    user_feedback: Optional[Dict[str, Any]] = None
    error_details: Optional[str] = None

@dataclass
class PerformanceMetrics:
    """Aggregated performance metrics"""
    total_interactions: int
    average_response_time: float
    error_rate: float
    user_satisfaction_score: float
    token_efficiency: float
    context_utilization: float
    quality_scores: Dict[str, float]

class LawMatrixObservabilitySystem:
    """
    Comprehensive observability system for LAW Matrix v4.0
    Tracks, analyzes, and improves AI performance continuously
    """
    
    def __init__(self, db_path: str = "lawmatrix_observability.db"):
        self.db_path = db_path
        self.logger = self._setup_logging()
        self._initialize_database()
        
        # Performance tracking
        self.session_metrics = defaultdict(list)
        self.user_patterns = defaultdict(list)
        self.error_patterns = defaultdict(int)
        self.quality_trends = defaultdict(list)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup structured logging for observability"""
        
        logger = logging.getLogger('LawMatrixObservability')
        logger.setLevel(logging.INFO)
        
        # File handler for detailed logs
        file_handler = logging.FileHandler('lawmatrix_observability.log')
        file_handler.setLevel(logging.INFO)
        
        # JSON formatter for structured logging
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
        return logger
        
    def _initialize_database(self):
        """Initialize SQLite database for observability data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create interactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS interactions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                interaction_type TEXT NOT NULL,
                input_data TEXT NOT NULL,
                output_data TEXT NOT NULL,
                processing_time_ms REAL NOT NULL,
                tokens_used INTEGER NOT NULL,
                model_version TEXT NOT NULL,
                context_data TEXT,
                quality_scores TEXT,
                user_feedback TEXT,
                error_details TEXT
            )
        ''')
        
        # Create performance_metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                total_interactions INTEGER NOT NULL,
                average_response_time REAL NOT NULL,
                error_rate REAL NOT NULL,
                user_satisfaction_score REAL NOT NULL,
                token_efficiency REAL NOT NULL,
                context_utilization REAL NOT NULL,
                quality_scores TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create user_patterns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_patterns (
                user_id TEXT PRIMARY KEY,
                total_interactions INTEGER NOT NULL,
                preferred_query_types TEXT,
                average_session_duration REAL,
                satisfaction_trend REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create error_analysis table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS error_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                frequency INTEGER NOT NULL,
                first_occurrence TIMESTAMP NOT NULL,
                last_occurrence TIMESTAMP NOT NULL,
                resolution_status TEXT DEFAULT 'unresolved'
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ LAW Matrix v4.0 - Observability database initialized")
        
    def log_interaction(self, interaction: InteractionLog):
        """Log a user interaction for analysis"""
        
        # Generate unique ID if not provided
        if not interaction.id:
            interaction.id = self._generate_interaction_id(interaction)
            
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO interactions 
            (id, user_id, session_id, timestamp, interaction_type, input_data, output_data,
             processing_time_ms, tokens_used, model_version, context_data, quality_scores,
             user_feedback, error_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            interaction.id,
            interaction.user_id,
            interaction.session_id,
            interaction.timestamp.isoformat(),
            interaction.interaction_type.value,
            json.dumps(interaction.input_data),
            json.dumps(interaction.output_data),
            interaction.processing_time_ms,
            interaction.tokens_used,
            interaction.model_version,
            json.dumps(interaction.context_data),
            json.dumps(interaction.quality_scores),
            json.dumps(interaction.user_feedback) if interaction.user_feedback else None,
            interaction.error_details
        ))
        
        conn.commit()
        conn.close()
        
        # Update in-memory metrics
        self._update_session_metrics(interaction)
        self._update_user_patterns(interaction)
        
        # Log to structured log
        self.logger.info(json.dumps({
            'interaction_id': interaction.id,
            'user_id': interaction.user_id,
            'type': interaction.interaction_type.value,
            'processing_time': interaction.processing_time_ms,
            'tokens_used': interaction.tokens_used,
            'quality_scores': interaction.quality_scores
        }))
        
    def _generate_interaction_id(self, interaction: InteractionLog) -> str:
        """Generate unique interaction ID"""
        
        content = f"{interaction.user_id}_{interaction.session_id}_{interaction.timestamp}_{interaction.interaction_type.value}"
        return hashlib.md5(content.encode()).hexdigest()
        
    def _update_session_metrics(self, interaction: InteractionLog):
        """Update session-level metrics"""
        
        session_key = f"{interaction.user_id}_{interaction.session_id}"
        self.session_metrics[session_key].append(interaction)
        
        # Keep only recent interactions (last 100)
        if len(self.session_metrics[session_key]) > 100:
            self.session_metrics[session_key] = self.session_metrics[session_key][-100:]
            
    def _update_user_patterns(self, interaction: InteractionLog):
        """Update user behavior patterns"""
        
        self.user_patterns[interaction.user_id].append(interaction)
        
        # Keep only recent interactions (last 1000)
        if len(self.user_patterns[interaction.user_id]) > 1000:
            self.user_patterns[interaction.user_id] = self.user_patterns[interaction.user_id][-1000:]
            
    def analyze_performance_trends(self, days: int = 7) -> Dict[str, Any]:
        """Analyze performance trends over specified period"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get interactions from last N days
        cutoff_date = datetime.now() - timedelta(days=days)
        cursor.execute('''
            SELECT * FROM interactions 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC
        ''', (cutoff_date.isoformat(),))
        
        interactions = cursor.fetchall()
        conn.close()
        
        if not interactions:
            return {'error': 'No data available for analysis'}
            
        # Calculate metrics
        total_interactions = len(interactions)
        error_count = sum(1 for i in interactions if i[13])  # error_details column
        
        response_times = [i[7] for i in interactions if i[7] > 0]  # processing_time_ms
        avg_response_time = np.mean(response_times) if response_times else 0
        
        tokens_used = [i[8] for i in interactions]  # tokens_used
        avg_tokens = np.mean(tokens_used) if tokens_used else 0
        
        # Calculate quality scores
        quality_scores = defaultdict(list)
        for interaction in interactions:
            if interaction[11]:  # quality_scores column
                scores = json.loads(interaction[11])
                for metric, score in scores.items():
                    quality_scores[metric].append(score)
                    
        avg_quality = {metric: np.mean(scores) for metric, scores in quality_scores.items()}
        
        # User satisfaction (from feedback)
        satisfaction_scores = []
        for interaction in interactions:
            if interaction[12]:  # user_feedback column
                feedback = json.loads(interaction[12])
                if 'satisfaction_score' in feedback:
                    satisfaction_scores.append(feedback['satisfaction_score'])
                    
        avg_satisfaction = np.mean(satisfaction_scores) if satisfaction_scores else 0
        
        return {
            'period_days': days,
            'total_interactions': total_interactions,
            'error_rate': (error_count / total_interactions) * 100 if total_interactions > 0 else 0,
            'average_response_time_ms': avg_response_time,
            'average_tokens_per_interaction': avg_tokens,
            'average_quality_scores': avg_quality,
            'user_satisfaction_score': avg_satisfaction,
            'trend_analysis': self._analyze_trends(interactions)
        }
        
    def _analyze_trends(self, interactions: List[Tuple]) -> Dict[str, Any]:
        """Analyze trends in interaction data"""
        
        # Group by day
        daily_metrics = defaultdict(list)
        for interaction in interactions:
            date = datetime.fromisoformat(interaction[3]).date()
            daily_metrics[date].append(interaction)
            
        # Calculate daily averages
        daily_trends = {}
        for date, day_interactions in daily_metrics.items():
            response_times = [i[7] for i in day_interactions if i[7] > 0]
            daily_trends[date.isoformat()] = {
                'interaction_count': len(day_interactions),
                'avg_response_time': np.mean(response_times) if response_times else 0,
                'error_count': sum(1 for i in day_interactions if i[13])
            }
            
        return daily_trends
        
    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalous behavior patterns"""
        
        anomalies = []
        
        # Analyze recent interactions
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM interactions 
            WHERE timestamp >= datetime('now', '-1 day')
            ORDER BY timestamp DESC
        ''')
        
        recent_interactions = cursor.fetchall()
        conn.close()
        
        if len(recent_interactions) < 10:
            return anomalies
            
        # Check for response time anomalies
        response_times = [i[7] for i in recent_interactions if i[7] > 0]
        if response_times:
            avg_response_time = np.mean(response_times)
            std_response_time = np.std(response_times)
            
            for interaction in recent_interactions:
                if interaction[7] > avg_response_time + (3 * std_response_time):
                    anomalies.append({
                        'type': 'high_response_time',
                        'interaction_id': interaction[0],
                        'value': interaction[7],
                        'threshold': avg_response_time + (3 * std_response_time),
                        'severity': 'high'
                    })
                    
        # Check for error rate spikes
        error_count = sum(1 for i in recent_interactions if i[13])
        error_rate = (error_count / len(recent_interactions)) * 100
        
        if error_rate > 10:  # More than 10% error rate
            anomalies.append({
                'type': 'high_error_rate',
                'value': error_rate,
                'threshold': 10,
                'severity': 'critical'
            })
            
        return anomalies
        
    def generate_improvement_recommendations(self) -> List[Dict[str, Any]]:
        """Generate recommendations for system improvement"""
        
        recommendations = []
        
        # Analyze performance trends
        trends = self.analyze_performance_trends(days=7)
        
        # Check response time
        if trends.get('average_response_time_ms', 0) > 5000:  # 5 seconds
            recommendations.append({
                'category': 'performance',
                'priority': 'high',
                'recommendation': 'Optimize response time - consider caching or model optimization',
                'current_value': trends['average_response_time_ms'],
                'target_value': 3000
            })
            
        # Check error rate
        if trends.get('error_rate', 0) > 5:  # 5%
            recommendations.append({
                'category': 'reliability',
                'priority': 'critical',
                'recommendation': 'Investigate and resolve error sources',
                'current_value': trends['error_rate'],
                'target_value': 1
            })
            
        # Check user satisfaction
        if trends.get('user_satisfaction_score', 0) < 4.0:  # Scale 1-5
            recommendations.append({
                'category': 'user_experience',
                'priority': 'high',
                'recommendation': 'Improve response quality and relevance',
                'current_value': trends['user_satisfaction_score'],
                'target_value': 4.5
            })
            
        # Check quality scores
        quality_scores = trends.get('average_quality_scores', {})
        for metric, score in quality_scores.items():
            if score < 0.8:  # 80% threshold
                recommendations.append({
                    'category': 'quality',
                    'priority': 'medium',
                    'recommendation': f'Improve {metric} quality',
                    'current_value': score,
                    'target_value': 0.9
                })
                
        return recommendations
        
    def export_training_data(self, output_file: str = "lawmatrix_training_data.jsonl"):
        """Export high-quality interactions for fine-tuning"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get high-quality interactions (good user feedback, no errors)
        cursor.execute('''
            SELECT * FROM interactions 
            WHERE error_details IS NULL 
            AND user_feedback IS NOT NULL
            AND json_extract(user_feedback, '$.satisfaction_score') >= 4
            ORDER BY timestamp DESC
        ''')
        
        interactions = cursor.fetchall()
        conn.close()
        
        # Format for training
        training_data = []
        for interaction in interactions:
            if interaction[12]:  # user_feedback
                feedback = json.loads(interaction[12])
                if feedback.get('satisfaction_score', 0) >= 4:
                    training_data.append({
                        'instruction': interaction[5],  # input_data
                        'response': interaction[6],    # output_data
                        'context': interaction[10],    # context_data
                        'quality_scores': interaction[11],  # quality_scores
                        'timestamp': interaction[3]
                    })
                    
        # Write to JSONL file
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in training_data:
                f.write(json.dumps(item) + '\n')
                
        print(f"✅ LAW Matrix v4.0 - Exported {len(training_data)} high-quality interactions to {output_file}")
        
        return len(training_data)

class LawMatrixFeedbackCollector:
    """
    Collects and processes user feedback for continuous improvement
    """
    
    def __init__(self, observability_system: LawMatrixObservabilitySystem):
        self.observability = observability_system
        
    def collect_feedback(self, interaction_id: str, feedback_data: Dict[str, Any]):
        """Collect user feedback for an interaction"""
        
        # Update interaction with feedback
        conn = sqlite3.connect(self.observability.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE interactions 
            SET user_feedback = ? 
            WHERE id = ?
        ''', (json.dumps(feedback_data), interaction_id))
        
        conn.commit()
        conn.close()
        
        print(f"✅ LAW Matrix v4.0 - Feedback collected for interaction {interaction_id}")
        
    def analyze_feedback_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in user feedback"""
        
        conn = sqlite3.connect(self.observability.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user_feedback FROM interactions 
            WHERE user_feedback IS NOT NULL
        ''')
        
        feedback_data = cursor.fetchall()
        conn.close()
        
        if not feedback_data:
            return {'error': 'No feedback data available'}
            
        # Analyze feedback patterns
        satisfaction_scores = []
        common_issues = Counter()
        
        for feedback_row in feedback_data:
            feedback = json.loads(feedback_row[0])
            
            if 'satisfaction_score' in feedback:
                satisfaction_scores.append(feedback['satisfaction_score'])
                
            if 'issues' in feedback:
                for issue in feedback['issues']:
                    common_issues[issue] += 1
                    
        return {
            'average_satisfaction': np.mean(satisfaction_scores) if satisfaction_scores else 0,
            'total_feedback_responses': len(feedback_data),
            'common_issues': dict(common_issues.most_common(10)),
            'satisfaction_distribution': Counter(satisfaction_scores)
        }

if __name__ == "__main__":
    # Initialize observability system
    observability = LawMatrixObservabilitySystem()
    
    # Create sample interaction
    sample_interaction = InteractionLog(
        id="test_123",
        user_id="user_456",
        session_id="session_789",
        timestamp=datetime.now(),
        interaction_type=InteractionType.QUERY,
        input_data={"query": "What are the custody factors in Utah?"},
        output_data={"response": "Utah considers best interests of child..."},
        processing_time_ms=1250.5,
        tokens_used=150,
        model_version="lawmatrix-v4.0",
        context_data={"current_case": "Stears v. Stears"},
        quality_scores={"relevance": 0.95, "accuracy": 0.88, "completeness": 0.92}
    )
    
    # Log interaction
    observability.log_interaction(sample_interaction)
    
    # Analyze performance
    trends = observability.analyze_performance_trends(days=1)
    print(f"✅ LAW Matrix v4.0 - Observability system operational!")
    print(f"Analyzed {trends.get('total_interactions', 0)} interactions")
