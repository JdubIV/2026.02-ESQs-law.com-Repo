#!/usr/bin/env python3
"""
LAW Matrix v4.0 - Intelligence Flywheel System
Continuous improvement through feedback loops and self-learning
"""

import os
import json
import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import numpy as np
from collections import defaultdict, Counter
import logging
from enum import Enum
import threading
import time

class FeedbackType(Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    CORRECTION = "correction"

class ImprovementTrigger(Enum):
    QUALITY_THRESHOLD = "quality_threshold"
    USER_SATISFACTION = "user_satisfaction"
    ERROR_RATE = "error_rate"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    SCHEDULED = "scheduled"

@dataclass
class FeedbackEntry:
    """Represents user feedback for continuous improvement"""
    id: str
    interaction_id: str
    user_id: str
    feedback_type: FeedbackType
    satisfaction_score: float  # 1-5 scale
    specific_feedback: str
    suggested_improvements: List[str]
    timestamp: datetime
    context_data: Dict[str, Any]

@dataclass
class ImprovementAction:
    """Represents an improvement action to be taken"""
    id: str
    trigger_type: ImprovementTrigger
    priority: str  # critical, high, medium, low
    action_type: str  # retrain, update_knowledge, adjust_thresholds, optimize_prompts
    description: str
    parameters: Dict[str, Any]
    estimated_impact: float
    status: str  # pending, in_progress, completed, failed
    created_at: datetime
    completed_at: Optional[datetime] = None

@dataclass
class PerformanceBaseline:
    """Represents performance baseline for comparison"""
    metric_name: str
    baseline_value: float
    current_value: float
    trend_direction: str  # improving, declining, stable
    confidence_level: float
    measurement_period: int  # days
    last_updated: datetime

class LawMatrixIntelligenceFlywheel:
    """
    Intelligence flywheel system for continuous improvement
    Implements the complete feedback loop for self-improving AI
    """
    
    def __init__(self, db_path: str = "lawmatrix_flywheel.db"):
        self.db_path = db_path
        self.logger = self._setup_logging()
        self._initialize_database()
        
        # Feedback tracking
        self.feedback_queue = []
        self.improvement_queue = []
        self.performance_baselines = {}
        
        # Continuous improvement settings
        self.improvement_settings = {
            'quality_threshold': 0.8,
            'satisfaction_threshold': 4.0,
            'error_rate_threshold': 0.05,
            'feedback_analysis_interval': 24,  # hours
            'retraining_threshold': 100,  # feedback entries
            'performance_monitoring_window': 7  # days
        }
        
        # Background tasks
        self.is_running = False
        self.background_tasks = []
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the intelligence flywheel"""
        
        logger = logging.getLogger('LawMatrixIntelligenceFlywheel')
        logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler('logs/intelligence_flywheel.log')
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "flywheel", "message": "%(message)s"}'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    def _initialize_database(self):
        """Initialize database for intelligence flywheel"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create feedback table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback_entries (
                id TEXT PRIMARY KEY,
                interaction_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                feedback_type TEXT NOT NULL,
                satisfaction_score REAL NOT NULL,
                specific_feedback TEXT,
                suggested_improvements TEXT,
                timestamp TIMESTAMP NOT NULL,
                context_data TEXT,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create improvement actions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS improvement_actions (
                id TEXT PRIMARY KEY,
                trigger_type TEXT NOT NULL,
                priority TEXT NOT NULL,
                action_type TEXT NOT NULL,
                description TEXT NOT NULL,
                parameters TEXT NOT NULL,
                estimated_impact REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                results TEXT
            )
        ''')
        
        # Create performance baselines table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_baselines (
                metric_name TEXT PRIMARY KEY,
                baseline_value REAL NOT NULL,
                current_value REAL NOT NULL,
                trend_direction TEXT NOT NULL,
                confidence_level REAL NOT NULL,
                measurement_period INTEGER NOT NULL,
                last_updated TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create improvement history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS improvement_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_id TEXT NOT NULL,
                metric_before REAL NOT NULL,
                metric_after REAL NOT NULL,
                improvement_percentage REAL NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                details TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ LAW Matrix v4.0 - Intelligence flywheel database initialized")
        
    async def start_continuous_improvement(self):
        """Start the continuous improvement background processes"""
        
        self.is_running = True
        self.logger.info("🚀 Starting LAW Matrix v4.0 Intelligence Flywheel")
        
        # Start background tasks
        tasks = [
            self._feedback_analysis_loop(),
            self._performance_monitoring_loop(),
            self._improvement_execution_loop(),
            self._quality_assurance_loop()
        ]
        
        self.background_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
    async def stop_continuous_improvement(self):
        """Stop the continuous improvement processes"""
        
        self.is_running = False
        self.logger.info("🛑 Stopping LAW Matrix v4.0 Intelligence Flywheel")
        
        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                
    def collect_feedback(self, interaction_id: str, user_id: str, feedback_data: Dict[str, Any]):
        """Collect user feedback for continuous improvement"""
        
        feedback_entry = FeedbackEntry(
            id=self._generate_feedback_id(interaction_id, user_id),
            interaction_id=interaction_id,
            user_id=user_id,
            feedback_type=FeedbackType(feedback_data.get('type', 'neutral')),
            satisfaction_score=feedback_data.get('satisfaction_score', 3.0),
            specific_feedback=feedback_data.get('specific_feedback', ''),
            suggested_improvements=feedback_data.get('suggested_improvements', []),
            timestamp=datetime.now(),
            context_data=feedback_data.get('context', {})
        )
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO feedback_entries 
            (id, interaction_id, user_id, feedback_type, satisfaction_score,
             specific_feedback, suggested_improvements, timestamp, context_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            feedback_entry.id,
            feedback_entry.interaction_id,
            feedback_entry.user_id,
            feedback_entry.feedback_type.value,
            feedback_entry.satisfaction_score,
            feedback_entry.specific_feedback,
            json.dumps(feedback_entry.suggested_improvements),
            feedback_entry.timestamp.isoformat(),
            json.dumps(feedback_entry.context_data)
        ))
        
        conn.commit()
        conn.close()
        
        # Add to processing queue
        self.feedback_queue.append(feedback_entry)
        
        self.logger.info(f"Feedback collected: {feedback_entry.feedback_type.value}, score: {feedback_entry.satisfaction_score}")
        
    async def _feedback_analysis_loop(self):
        """Continuously analyze feedback for improvement opportunities"""
        
        while self.is_running:
            try:
                # Process feedback queue
                if self.feedback_queue:
                    await self._analyze_feedback_batch()
                    
                # Check for improvement triggers
                await self._check_improvement_triggers()
                
                # Sleep before next iteration
                await asyncio.sleep(3600)  # 1 hour
                
            except Exception as e:
                self.logger.error(f"Error in feedback analysis loop: {str(e)}")
                await asyncio.sleep(300)  # 5 minutes on error
                
    async def _analyze_feedback_batch(self):
        """Analyze a batch of feedback entries"""
        
        if not self.feedback_queue:
            return
            
        # Get recent feedback
        recent_feedback = self.feedback_queue[-50:]  # Last 50 entries
        
        # Analyze patterns
        feedback_analysis = self._analyze_feedback_patterns(recent_feedback)
        
        # Generate improvement actions based on analysis
        improvement_actions = self._generate_improvement_actions(feedback_analysis)
        
        # Add to improvement queue
        self.improvement_queue.extend(improvement_actions)
        
        # Clear processed feedback
        self.feedback_queue = []
        
        self.logger.info(f"Analyzed {len(recent_feedback)} feedback entries, generated {len(improvement_actions)} improvement actions")
        
    def _analyze_feedback_patterns(self, feedback_entries: List[FeedbackEntry]) -> Dict[str, Any]:
        """Analyze patterns in feedback data"""
        
        if not feedback_entries:
            return {}
            
        analysis = {
            'total_feedback': len(feedback_entries),
            'average_satisfaction': np.mean([f.satisfaction_score for f in feedback_entries]),
            'feedback_distribution': Counter([f.feedback_type.value for f in feedback_entries]),
            'common_issues': Counter(),
            'improvement_suggestions': Counter(),
            'satisfaction_trend': self._calculate_satisfaction_trend(feedback_entries),
            'quality_issues': []
        }
        
        # Analyze specific feedback
        for entry in feedback_entries:
            if entry.specific_feedback:
                # Simple keyword analysis (in production, use NLP)
                issues = self._extract_issues_from_feedback(entry.specific_feedback)
                for issue in issues:
                    analysis['common_issues'][issue] += 1
                    
            # Collect improvement suggestions
            for suggestion in entry.suggested_improvements:
                analysis['improvement_suggestions'][suggestion] += 1
                
        # Identify quality issues
        low_satisfaction = [f for f in feedback_entries if f.satisfaction_score < 3.0]
        if len(low_satisfaction) > len(feedback_entries) * 0.2:  # More than 20% low satisfaction
            analysis['quality_issues'].append('high_low_satisfaction_rate')
            
        return analysis
        
    def _extract_issues_from_feedback(self, feedback_text: str) -> List[str]:
        """Extract issues from feedback text (simplified NLP)"""
        
        issues = []
        feedback_lower = feedback_text.lower()
        
        # Simple keyword matching for common issues
        issue_keywords = {
            'accuracy': ['wrong', 'incorrect', 'inaccurate', 'mistake', 'error'],
            'relevance': ['irrelevant', 'not helpful', 'off topic', 'unrelated'],
            'completeness': ['incomplete', 'missing', 'partial', 'unfinished'],
            'clarity': ['unclear', 'confusing', 'hard to understand', 'complex'],
            'speed': ['slow', 'delayed', 'takes too long', 'timeout']
        }
        
        for issue_type, keywords in issue_keywords.items():
            if any(keyword in feedback_lower for keyword in keywords):
                issues.append(issue_type)
                
        return issues
        
    def _calculate_satisfaction_trend(self, feedback_entries: List[FeedbackEntry]) -> str:
        """Calculate satisfaction trend over time"""
        
        if len(feedback_entries) < 5:
            return 'insufficient_data'
            
        # Sort by timestamp
        sorted_feedback = sorted(feedback_entries, key=lambda x: x.timestamp)
        
        # Calculate trend
        first_half = sorted_feedback[:len(sorted_feedback)//2]
        second_half = sorted_feedback[len(sorted_feedback)//2:]
        
        first_avg = np.mean([f.satisfaction_score for f in first_half])
        second_avg = np.mean([f.satisfaction_score for f in second_half])
        
        if second_avg > first_avg + 0.2:
            return 'improving'
        elif second_avg < first_avg - 0.2:
            return 'declining'
        else:
            return 'stable'
            
    def _generate_improvement_actions(self, feedback_analysis: Dict[str, Any]) -> List[ImprovementAction]:
        """Generate improvement actions based on feedback analysis"""
        
        actions = []
        
        # Check satisfaction threshold
        if feedback_analysis.get('average_satisfaction', 5.0) < self.improvement_settings['satisfaction_threshold']:
            actions.append(ImprovementAction(
                id=self._generate_action_id('satisfaction'),
                trigger_type=ImprovementTrigger.USER_SATISFACTION,
                priority='high',
                action_type='retrain',
                description=f"Low satisfaction score: {feedback_analysis['average_satisfaction']:.2f}",
                parameters={'target_metric': 'satisfaction', 'threshold': self.improvement_settings['satisfaction_threshold']},
                estimated_impact=0.3,
                status='pending',
                created_at=datetime.now()
            ))
            
        # Check for common issues
        common_issues = feedback_analysis.get('common_issues', Counter())
        for issue, count in common_issues.most_common(3):
            if count > 5:  # Issue mentioned more than 5 times
                actions.append(ImprovementAction(
                    id=self._generate_action_id(f'issue_{issue}'),
                    trigger_type=ImprovementTrigger.QUALITY_THRESHOLD,
                    priority='medium',
                    action_type='optimize_prompts',
                    description=f"Common issue identified: {issue} (mentioned {count} times)",
                    parameters={'issue_type': issue, 'frequency': count},
                    estimated_impact=0.2,
                    status='pending',
                    created_at=datetime.now()
                ))
                
        # Check satisfaction trend
        trend = feedback_analysis.get('satisfaction_trend', 'stable')
        if trend == 'declining':
            actions.append(ImprovementAction(
                id=self._generate_action_id('trend_decline'),
                trigger_type=ImprovementTrigger.PERFORMANCE_DEGRADATION,
                priority='critical',
                action_type='retrain',
                description="Satisfaction trend is declining",
                parameters={'trend': trend},
                estimated_impact=0.4,
                status='pending',
                created_at=datetime.now()
            ))
            
        return actions
        
    async def _check_improvement_triggers(self):
        """Check for various improvement triggers"""
        
        # Check feedback volume trigger
        feedback_count = len(self.feedback_queue)
        if feedback_count >= self.improvement_settings['retraining_threshold']:
            action = ImprovementAction(
                id=self._generate_action_id('volume_trigger'),
                trigger_type=ImprovementTrigger.SCHEDULED,
                priority='medium',
                action_type='retrain',
                description=f"Retraining triggered by feedback volume: {feedback_count}",
                parameters={'feedback_count': feedback_count},
                estimated_impact=0.25,
                status='pending',
                created_at=datetime.now()
            )
            self.improvement_queue.append(action)
            
        # Check performance baselines
        for metric_name, baseline in self.performance_baselines.items():
            if baseline.current_value < baseline.baseline_value * 0.9:  # 10% decline
                action = ImprovementAction(
                    id=self._generate_action_id(f'baseline_{metric_name}'),
                    trigger_type=ImprovementTrigger.PERFORMANCE_DEGRADATION,
                    priority='high',
                    action_type='update_knowledge',
                    description=f"Performance decline in {metric_name}: {baseline.current_value:.3f} vs {baseline.baseline_value:.3f}",
                    parameters={'metric': metric_name, 'current': baseline.current_value, 'baseline': baseline.baseline_value},
                    estimated_impact=0.3,
                    status='pending',
                    created_at=datetime.now()
                )
                self.improvement_queue.append(action)
                
    async def _performance_monitoring_loop(self):
        """Continuously monitor system performance"""
        
        while self.is_running:
            try:
                # Update performance baselines
                await self._update_performance_baselines()
                
                # Sleep before next check
                await asyncio.sleep(1800)  # 30 minutes
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring loop: {str(e)}")
                await asyncio.sleep(300)  # 5 minutes on error
                
    async def _update_performance_baselines(self):
        """Update performance baselines based on recent data"""
        
        # This would integrate with the observability system to get real metrics
        # For now, we'll simulate baseline updates
        
        current_metrics = {
            'response_quality': 0.85,
            'user_satisfaction': 4.2,
            'error_rate': 0.02,
            'response_time': 2.5
        }
        
        for metric_name, current_value in current_metrics.items():
            if metric_name not in self.performance_baselines:
                # Create new baseline
                self.performance_baselines[metric_name] = PerformanceBaseline(
                    metric_name=metric_name,
                    baseline_value=current_value,
                    current_value=current_value,
                    trend_direction='stable',
                    confidence_level=0.8,
                    measurement_period=7,
                    last_updated=datetime.now()
                )
            else:
                # Update existing baseline
                baseline = self.performance_baselines[metric_name]
                baseline.current_value = current_value
                baseline.last_updated = datetime.now()
                
                # Calculate trend
                if current_value > baseline.baseline_value * 1.05:
                    baseline.trend_direction = 'improving'
                elif current_value < baseline.baseline_value * 0.95:
                    baseline.trend_direction = 'declining'
                else:
                    baseline.trend_direction = 'stable'
                    
    async def _improvement_execution_loop(self):
        """Execute improvement actions"""
        
        while self.is_running:
            try:
                if self.improvement_queue:
                    # Process highest priority action
                    action = self._get_next_improvement_action()
                    if action:
                        await self._execute_improvement_action(action)
                        
                # Sleep before next iteration
                await asyncio.sleep(600)  # 10 minutes
                
            except Exception as e:
                self.logger.error(f"Error in improvement execution loop: {str(e)}")
                await asyncio.sleep(300)  # 5 minutes on error
                
    def _get_next_improvement_action(self) -> Optional[ImprovementAction]:
        """Get the next improvement action to execute"""
        
        if not self.improvement_queue:
            return None
            
        # Priority order: critical, high, medium, low
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        
        # Sort by priority and creation time
        sorted_actions = sorted(
            self.improvement_queue,
            key=lambda x: (priority_order.get(x.priority, 4), x.created_at)
        )
        
        return sorted_actions[0] if sorted_actions else None
        
    async def _execute_improvement_action(self, action: ImprovementAction):
        """Execute an improvement action"""
        
        self.logger.info(f"Executing improvement action: {action.action_type} - {action.description}")
        
        action.status = 'in_progress'
        
        try:
            if action.action_type == 'retrain':
                await self._execute_retraining(action)
            elif action.action_type == 'update_knowledge':
                await self._execute_knowledge_update(action)
            elif action.action_type == 'optimize_prompts':
                await self._execute_prompt_optimization(action)
            elif action.action_type == 'adjust_thresholds':
                await self._execute_threshold_adjustment(action)
                
            action.status = 'completed'
            action.completed_at = datetime.now()
            
            # Record improvement results
            await self._record_improvement_results(action)
            
            self.logger.info(f"Completed improvement action: {action.id}")
            
        except Exception as e:
            action.status = 'failed'
            action.completed_at = datetime.now()
            self.logger.error(f"Failed to execute improvement action {action.id}: {str(e)}")
            
        # Remove from queue
        self.improvement_queue.remove(action)
        
    async def _execute_retraining(self, action: ImprovementAction):
        """Execute model retraining"""
        
        self.logger.info("Starting model retraining...")
        
        # In production, this would trigger actual retraining
        # For now, we'll simulate the process
        
        await asyncio.sleep(2)  # Simulate retraining time
        
        self.logger.info("Model retraining completed")
        
    async def _execute_knowledge_update(self, action: ImprovementAction):
        """Execute knowledge base update"""
        
        self.logger.info("Updating knowledge base...")
        
        # In production, this would update the RAG system
        await asyncio.sleep(1)  # Simulate update time
        
        self.logger.info("Knowledge base updated")
        
    async def _execute_prompt_optimization(self, action: ImprovementAction):
        """Execute prompt optimization"""
        
        self.logger.info("Optimizing prompts...")
        
        # In production, this would optimize prompt templates
        await asyncio.sleep(1)  # Simulate optimization time
        
        self.logger.info("Prompts optimized")
        
    async def _execute_threshold_adjustment(self, action: ImprovementAction):
        """Execute threshold adjustments"""
        
        self.logger.info("Adjusting thresholds...")
        
        # In production, this would adjust system thresholds
        await asyncio.sleep(1)  # Simulate adjustment time
        
        self.logger.info("Thresholds adjusted")
        
    async def _record_improvement_results(self, action: ImprovementAction):
        """Record the results of an improvement action"""
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO improvement_actions 
            (id, trigger_type, priority, action_type, description, parameters,
             estimated_impact, status, created_at, completed_at, results)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            action.id,
            action.trigger_type.value,
            action.priority,
            action.action_type,
            action.description,
            json.dumps(action.parameters),
            action.estimated_impact,
            action.status,
            action.created_at.isoformat(),
            action.completed_at.isoformat() if action.completed_at else None,
            json.dumps({'execution_time': (action.completed_at - action.created_at).total_seconds() if action.completed_at else None})
        ))
        
        conn.commit()
        conn.close()
        
    async def _quality_assurance_loop(self):
        """Quality assurance and validation loop"""
        
        while self.is_running:
            try:
                # Validate improvement results
                await self._validate_improvement_results()
                
                # Sleep before next check
                await asyncio.sleep(3600)  # 1 hour
                
            except Exception as e:
                self.logger.error(f"Error in quality assurance loop: {str(e)}")
                await asyncio.sleep(300)  # 5 minutes on error
                
    async def _validate_improvement_results(self):
        """Validate the results of improvement actions"""
        
        # Get recent completed actions
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM improvement_actions 
            WHERE status = 'completed' 
            AND completed_at >= datetime('now', '-24 hours')
        ''')
        
        recent_actions = cursor.fetchall()
        conn.close()
        
        for action_data in recent_actions:
            action_id = action_data[0]
            
            # Validate improvement (simplified)
            # In production, this would measure actual performance improvements
            
            validation_result = {
                'action_id': action_id,
                'validation_status': 'passed',
                'improvement_verified': True,
                'validation_timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Validated improvement action: {action_id}")
            
    def _generate_feedback_id(self, interaction_id: str, user_id: str) -> str:
        """Generate unique feedback ID"""
        
        import hashlib
        content = f"{interaction_id}_{user_id}_{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()
        
    def _generate_action_id(self, action_type: str) -> str:
        """Generate unique action ID"""
        
        import hashlib
        content = f"{action_type}_{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()
        
    def get_flywheel_status(self) -> Dict[str, Any]:
        """Get current status of the intelligence flywheel"""
        
        return {
            'is_running': self.is_running,
            'feedback_queue_size': len(self.feedback_queue),
            'improvement_queue_size': len(self.improvement_queue),
            'performance_baselines': {name: asdict(baseline) for name, baseline in self.performance_baselines.items()},
            'improvement_settings': self.improvement_settings,
            'background_tasks_running': len([t for t in self.background_tasks if not t.done()])
        }

if __name__ == "__main__":
    # Example usage
    async def main():
        # Create intelligence flywheel
        flywheel = LawMatrixIntelligenceFlywheel()
        
        # Start continuous improvement
        await flywheel.start_continuous_improvement()
        
        # Simulate feedback collection
        flywheel.collect_feedback(
            interaction_id="test_123",
            user_id="user_456",
            feedback_data={
                'type': 'positive',
                'satisfaction_score': 4.5,
                'specific_feedback': 'Very helpful and accurate response',
                'suggested_improvements': ['Could be faster', 'More examples would help'],
                'context': {'case_type': 'family_law'}
            }
        )
        
        # Run for a short time to demonstrate
        await asyncio.sleep(5)
        
        # Get status
        status = flywheel.get_flywheel_status()
        print("✅ LAW Matrix v4.0 - Intelligence Flywheel operational!")
        print(f"Status: {status}")
        
        # Stop the flywheel
        await flywheel.stop_continuous_improvement()
        
    # Run the example
    asyncio.run(main())
