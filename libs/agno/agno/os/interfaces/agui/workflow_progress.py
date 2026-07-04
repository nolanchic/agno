"""Workflow structural events -> native AG-UI STATE (workflow_progress) + STEP.

A workflow's structural WorkflowRunEvents (started, step_*, loop/parallel/condition/
router_*, pause/continue, cancelled) surface as a flat steps[] progress object in
shared STATE -- the one channel the default AG-UI client auto-renders -- plus native
STEP_STARTED/STEP_FINISHED at flat step boundaries (emitted for protocol consistency;
they render nothing themselves). No CustomEvent: the author's own custom_event keeps
its passthrough via the RunEvent.custom_event handler.

v1 is intentionally FLAT: container events (loop/parallel/condition/router) are no-ops
here -- their inner steps emit their own step_started/completed and populate the list.
Grouping the topology is a deferred follow-up. Pause/cancel surface as a status only;
interactive resume is out of scope.

The workflow status uses canonical uppercase RunStatus values ("RUNNING"/"COMPLETED"/...);
per-step status is a lowercase vocabulary ("running"/"completed"/"skipped"/"error"/"paused"),
since there is no RunStatus value for a skipped step.
"""

import copy
from typing import Any, Dict, List, Optional

from ag_ui.core import (
    BaseEvent,
    EventType,
    StateDeltaEvent,
    StateSnapshotEvent,
    StepFinishedEvent,
    StepStartedEvent,
)

from agno.os.interfaces.agui.state import StreamState
from agno.os.interfaces.agui.workflow_handlers import _event_value, _render_content
from agno.run.base import BaseRunOutputEvent, RunStatus
from agno.run.workflow import WorkflowRunEvent

_E = WorkflowRunEvent
_MAX_OUTPUT = 500  # cap step output stored in STATE so deltas stay small

_PAUSE_STEP = frozenset({_E.step_paused.value, _E.step_executor_paused.value, _E.step_output_review.value})
_PAUSE_WORKFLOW = frozenset({_E.workflow_paused.value, _E.router_paused.value})
_CONTINUE = frozenset({_E.step_continued.value, _E.step_executor_continued.value})
# condition_paused ("ConditionPaused") is intentionally absent: it is a vestigial enum
# value in agno core with no event class, never emitted -- so it needs no handling here.


def _progress(state: StreamState) -> Dict[str, Any]:
    if state.run_state is None:
        state.run_state = {}
    return state.run_state.setdefault("workflow_progress", {"status": RunStatus.running.value, "steps": []})


def _baseline(state: StreamState) -> List[BaseEvent]:
    """First touch with no caller-supplied state: router.py emitted no initial snapshot
    and stream.py set no delta baseline -> establish one here so the workflow_progress
    deltas below have a reference to diff against."""
    if state.run_state is not None:
        return []
    state.run_state = {}
    state.set_state_snapshot(state.run_state)
    return [StateSnapshotEvent(type=EventType.STATE_SNAPSHOT, snapshot=copy.deepcopy(state.run_state))]


def _emit_delta(state: StreamState) -> List[BaseEvent]:
    if state.run_state is None:
        return []
    ops = state.compute_state_delta(state.run_state)
    if not ops:
        return []
    state.set_state_snapshot(state.run_state)
    return [StateDeltaEvent(type=EventType.STATE_DELTA, delta=ops)]


def _short(content: Any) -> Optional[str]:
    if content is None:
        return None
    text = _render_content(content)
    return text[:_MAX_OUTPUT] if text else None


def _open_step(steps: List[Dict[str, Any]], step_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Most recent not-yet-finished entry for this event's step, matched by its unique step_id.
    On the async run path AG-UI drives, every step event that finds its entry
    (step_completed/step_error/pause/continue) carries a step_id, distinct per step instance --
    keying by it, not the step_index that nested Parallel siblings share, is what keeps concurrent
    siblings from mis-attributing each other's completions. (The sync StepCompletedEvent omits
    step_id, but the sync stream driver has no runtime callers, so this stays async-scoped.)"""
    for step in reversed(steps):
        if step["status"] in ("running", "paused") and step["id"] == step_id:
            return step
    return None


def mark_completed(state: StreamState) -> None:
    """Promote a still-running workflow to 'completed' at terminal time (never clobbering a
    'cancelled' / 'error' / 'paused' status already set by a structural event). A step skipped
    via on_error=skip emits no terminal event, so in a COMPLETED run any leftover 'running' step
    was skipped -- surface it as 'skipped' instead of leaving it stuck on 'running'."""
    progress = state.workflow_progress
    if progress is None:
        return
    if progress.get("status") == RunStatus.running.value:
        progress["status"] = RunStatus.completed.value
    if progress.get("status") == RunStatus.completed.value:
        for step in progress["steps"]:
            if step["status"] == "running":
                step["status"] = "skipped"


def progress_handler(chunk: BaseRunOutputEvent, state: StreamState) -> List[BaseEvent]:
    """Map one structural WorkflowRunEvent to a workflow_progress mutation (+ STEP)."""
    events = _baseline(state)
    value = _event_value(chunk)
    progress = _progress(state)
    state.workflow_progress = progress
    steps = progress["steps"]
    name = getattr(chunk, "step_name", None)
    step_id = getattr(chunk, "step_id", None)

    if value == _E.step_started.value:
        steps.append({"id": step_id, "name": name, "status": "running", "output": None})
        if name:
            events.append(StepStartedEvent(type=EventType.STEP_STARTED, step_name=name))
    elif value == _E.step_completed.value:
        step = _open_step(steps, step_id)
        if step is not None:
            step.update(status="completed", output=_short(getattr(chunk, "content", None)))
        if name:
            events.append(StepFinishedEvent(type=EventType.STEP_FINISHED, step_name=name))
    elif value == _E.step_error.value:
        step = _open_step(steps, step_id)
        if step is not None:
            step.update(status="error", output=_short(getattr(chunk, "error", None)))
        if name:
            events.append(StepFinishedEvent(type=EventType.STEP_FINISHED, step_name=name))
    elif value in _PAUSE_STEP:
        step = _open_step(steps, step_id)
        if step is not None:
            step["status"] = "paused"
    elif value in _PAUSE_WORKFLOW:
        progress["status"] = RunStatus.paused.value
    elif value in _CONTINUE:
        step = _open_step(steps, step_id)
        if step is not None:
            step["status"] = "running"
    elif value == _E.workflow_cancelled.value:
        progress["status"] = RunStatus.cancelled.value
        state.cancelled = True
    # workflow_started initialises progress above; container/agent events are no-ops (their inner
    # step_started/completed populate the flat list). step_output carries no step_id and only restates
    # content the immediately-following step_completed sets authoritatively, so it needs no branch here.
    return events + _emit_delta(state)
