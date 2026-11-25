"""Atomic shared/exclusive resource reservations for one asyncio event loop."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from core.models import AccessMode, LockMode, LockScope, ProcessSpec, ResourceRule, TaskRun

if TYPE_CHECKING:
    from core.storage import Storage


@dataclass(frozen=True)
class _Resource:
    kind: str
    name: str
    partition: str | None
    mode: LockMode

    @property
    def key(self) -> str:
        prefix = "READ" if self.mode == LockMode.SHARED else "WRITE"
        return f"{prefix}:{self.kind}:{self.name}:{self.partition or 'GLOBAL'}"

    @property
    def resource_type(self) -> str:
        if self.kind != "SOURCE":
            return "PROCESS_STAGING"
        return "SOURCE_DATE" if self.partition is not None else "SOURCE_GLOBAL"

    def conflicts(self, other: _Resource) -> bool:
        return (
            self.kind == other.kind and self.name == other.name
            and (self.partition is None or other.partition is None or self.partition == other.partition)
            and LockMode.EXCLUSIVE in (self.mode, other.mode)
        )


class LockManager:
    def __init__(self, storage: Storage | None = None) -> None:
        self._guard = asyncio.Lock()
        self._held: dict[int, tuple[_Resource, ...]] = {}
        self._tasks: dict[int, TaskRun] = {}
        self._storage = storage

    @staticmethod
    def _resources(task: TaskRun, spec: ProcessSpec) -> tuple[_Resource, ...]:
        rules = spec.resource_rules or tuple(
            ResourceRule(src, AccessMode.WRITE if src in spec.write_sources else AccessMode.READ)
            for src in dict.fromkeys((*spec.read_sources, *spec.write_sources))
        )
        merged: dict[tuple[str, str | None], _Resource] = {}
        for rule in rules:
            partition = task.target_date if rule.lock_scope == LockScope.BY_DATE else None
            key = (rule.source_id, partition)
            mode = (LockMode.SHARED
                    if rule.access_mode == AccessMode.READ and rule.source_id not in spec.write_sources
                    else LockMode.EXCLUSIVE)
            if key in merged and merged[key].mode == LockMode.EXCLUSIVE:
                continue
            merged[key] = _Resource("SOURCE", rule.source_id, partition, mode)
        resources = list(merged.values())
        if not spec.allow_parallel_dates:
            resources.append(_Resource("PROCESS", spec.process_id, None, LockMode.EXCLUSIVE))
        if spec.uses_shared_staging:
            if not spec.staging_key:
                raise ValueError("Shared staging requires staging_key")
            resources.append(_Resource("STAGING", spec.staging_key, None, LockMode.EXCLUSIVE))
        return tuple(resources)

    def _audit(self, task: TaskRun, resources: tuple[_Resource, ...], action: str,
               blockers: list[int] | None = None) -> None:
        if self._storage:
            with self._storage.transaction():
                for resource in resources:
                    self._storage.create_lock_event(
                        task_id=task.task_id, run_id=task.run_id, lock_key=resource.key,
                        lock_mode=resource.mode.value, resource_type=resource.resource_type,
                        action=action, blocking_task_ids=blockers,
                    )

    async def try_acquire(self, task: TaskRun, spec: ProcessSpec) -> tuple[bool, str | None]:
        requested = self._resources(task, spec)
        async with self._guard:
            if task.task_id in self._held:
                if self._held[task.task_id] != requested:
                    raise ValueError("Cannot change resources of an active owner")
                return True, None
            conflicts = [
                (resource, owner)
                for resource in requested for owner, held in self._held.items()
                if any(resource.conflicts(other) for other in held)
            ]
            if conflicts:
                resource, owner = conflicts[0]
                reason = f"Ожидает ресурс {resource.key}, удерживаемый задачей #{owner}"
                if task.blocked_by != reason:
                    self._audit(task, tuple(dict.fromkeys(r for r, _ in conflicts)),
                                "WAIT_CONFLICT", sorted({owner for _, owner in conflicts}))
                return False, reason
            self._audit(task, requested, "ACQUIRED")
            self._held[task.task_id] = requested
            self._tasks[task.task_id] = task
            return True, None

    async def release(self, task_id: int, *, expired: bool = False) -> None:
        """Caller must first await the actual termination of the resource owner."""
        async with self._guard:
            resources = self._held.get(task_id)
            if resources is None:
                return
            self._audit(self._tasks[task_id], resources, "EXPIRED_TTL" if expired else "RELEASED")
            del self._held[task_id]
            del self._tasks[task_id]

    def get_snapshot(self) -> dict[str, list[int]]:
        result: dict[str, list[int]] = {}
        for owner, resources in self._held.items():
            for resource in resources:
                result.setdefault(resource.key, []).append(owner)
        return result

    def reset(self) -> None:
        if self._held:
            raise RuntimeError("Stop and release all resource owners before reset")
        self._tasks.clear()


RWLockManager = LockManager
