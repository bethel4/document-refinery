"""
ProvenanceChain Pydantic model for tracking document processing history.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

from pydantic import BaseModel, Field


class ProcessingStep(str, Enum):
    """Processing step types."""
    INGESTION = "ingestion"
    TRIAGE = "triage"
    EXTRACTION = "extraction"
    CHUNKING = "chunking"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    QUALITY_CHECK = "quality_check"
    TRANSFORMATION = "transformation"
    EXPORT = "export"


class AgentType(str, Enum):
    """Agent types that perform processing."""
    HUMAN = "human"
    AUTOMATED = "automated"
    HYBRID = "hybrid"
    EXTERNAL = "external"


class ProcessingStatus(str, Enum):
    """Processing status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class AgentInfo(BaseModel):
    """Information about the processing agent."""
    agent_id: str
    agent_type: AgentType
    agent_name: str
    version: str
    configuration: Dict[str, Any] = Field(default_factory=dict)
    
    def is_automated(self) -> bool:
        """Check if agent is automated."""
        return self.agent_type == AgentType.AUTOMATED
    
    def is_human(self) -> bool:
        """Check if agent is human."""
        return self.agent_type == AgentType.HUMAN


class ProcessingMetrics(BaseModel):
    """Metrics for a processing step."""
    duration_seconds: float
    cpu_usage_percent: Optional[float] = Field(None, ge=0.0, le=100.0)
    memory_usage_mb: Optional[float] = Field(None, ge=0.0)
    pages_processed: int = 0
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    error_count: int = 0
    warning_count: int = 0
    
    def get_efficiency_score(self) -> float:
        """Calculate efficiency score (pages per second)."""
        if self.duration_seconds > 0:
            return self.pages_processed / self.duration_seconds
        return 0.0
    
    def has_errors(self) -> bool:
        """Check if processing had errors."""
        return self.error_count > 0


class ProcessingStep(BaseModel):
    """Single processing step in the provenance chain."""
    
    # Step identification
    step_id: str
    step_type: ProcessingStep
    step_name: str
    sequence_order: int
    
    # Agent information
    agent: AgentInfo
    
    # Timing
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    
    # Input/Output
    input_artifacts: List[str] = Field(default_factory=list)
    output_artifacts: List[str] = Field(default_factory=list)
    input_parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Results and metrics
    metrics: Optional[ProcessingMetrics] = None
    result_summary: Optional[str] = None
    error_message: Optional[str] = None
    
    # Quality and validation
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    validation_results: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        
    def is_completed(self) -> bool:
        """Check if step is completed."""
        return self.status == ProcessingStatus.COMPLETED
    
    def is_failed(self) -> bool:
        """Check if step failed."""
        return self.status == ProcessingStatus.FAILED
    
    def get_duration(self) -> float:
        """Get step duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        elif self.metrics:
            return self.metrics.duration_seconds
        return 0.0
    
    def has_quality_issues(self) -> bool:
        """Check if step has quality issues."""
        return (
            self.quality_score is not None and self.quality_score < 0.7
        ) or self.metrics.has_errors()


class ProvenanceChain(BaseModel):
    """Complete provenance chain for document processing."""
    
    # Chain identification
    chain_id: str
    document_id: str
    chain_version: str = "1.0"
    
    # Chain metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: str
    
    # Processing steps
    steps: List[ProcessingStep] = Field(default_factory=list)
    
    # Chain-level metrics
    total_duration: float = 0.0
    total_cost: Optional[float] = None
    overall_quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    # Chain status
    status: ProcessingStatus = ProcessingStatus.PENDING
    completion_percentage: float = Field(ge=0.0, le=100.0)
    
    class Config:
        use_enum_values = True
        
    def add_step(self, step: ProcessingStep):
        """Add a processing step to the chain."""
        self.steps.append(step)
        self._update_chain_metrics()
        self.updated_at = datetime.now()
    
    def get_step_by_type(self, step_type: ProcessingStep) -> Optional[ProcessingStep]:
        """Get the first step of a specific type."""
        for step in self.steps:
            if step.step_type == step_type:
                return step
        return None
    
    def get_completed_steps(self) -> List[ProcessingStep]:
        """Get all completed steps."""
        return [step for step in self.steps if step.is_completed()]
    
    def get_failed_steps(self) -> List[ProcessingStep]:
        """Get all failed steps."""
        return [step for step in self.steps if step.is_failed()]
    
    def get_steps_by_agent(self, agent_id: str) -> List[ProcessingStep]:
        """Get all steps performed by a specific agent."""
        return [step for step in self.steps if step.agent.agent_id == agent_id]
    
    def calculate_completion_percentage(self) -> float:
        """Calculate completion percentage based on step statuses."""
        if not self.steps:
            return 0.0
        
        completed = len(self.get_completed_steps())
        return (completed / len(self.steps)) * 100.0
    
    def has_errors(self) -> bool:
        """Check if any step has errors."""
        return len(self.get_failed_steps()) > 0
    
    def get_total_processing_time(self) -> float:
        """Get total processing time across all steps."""
        return sum(step.get_duration() for step in self.steps)
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get quality summary across all steps."""
        quality_scores = [
            step.quality_score for step in self.steps 
            if step.quality_score is not None
        ]
        
        return {
            "average_quality": sum(quality_scores) / len(quality_scores) if quality_scores else 0.0,
            "min_quality": min(quality_scores) if quality_scores else 0.0,
            "max_quality": max(quality_scores) if quality_scores else 0.0,
            "steps_with_quality": len(quality_scores),
            "total_steps": len(self.steps)
        }
    
    def _update_chain_metrics(self):
        """Update chain-level metrics."""
        self.total_duration = self.get_total_processing_time()
        self.completion_percentage = self.calculate_completion_percentage()
        
        # Update status based on steps
        if self.steps:
            failed_count = len(self.get_failed_steps())
            completed_count = len(self.get_completed_steps())
            
            if failed_count > 0:
                self.status = ProcessingStatus.FAILED
            elif completed_count == len(self.steps):
                self.status = ProcessingStatus.COMPLETED
            else:
                self.status = ProcessingStatus.RUNNING
    
    def to_summary(self) -> Dict[str, Any]:
        """Get chain summary for reporting."""
        return {
            "chain_id": self.chain_id,
            "document_id": self.document_id,
            "status": self.status.value,
            "total_steps": len(self.steps),
            "completed_steps": len(self.get_completed_steps()),
            "failed_steps": len(self.get_failed_steps()),
            "completion_percentage": self.completion_percentage,
            "total_duration": self.total_duration,
            "overall_quality": self.overall_quality_score,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
