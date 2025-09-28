from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PromptComponent(BaseModel):
    id: str
    priority: int
    content: str
    condition: Optional[str] = None


class WorkflowStep(BaseModel):
    type: str
    params: Optional[Dict[str, Any]] = Field(default_factory=dict)


class WorldConfig(BaseModel):
    world_name: str
    initial_state: Dict[str, Any]
    prompts: List[PromptComponent]
    main_workflow: List[WorkflowStep]


class Session(BaseModel):
    session_id: str
    config: WorldConfig
    messages: List[Dict[str, str]] = Field(default_factory=list)


class InteractRequest(BaseModel):
    session_id: str
    user_input: str


class InteractResponse(BaseModel):
    session_id: str
    response: str
    state_snapshot: Dict[str, Any]
