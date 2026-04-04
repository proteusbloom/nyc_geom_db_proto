from __future__ import annotations

from collections.abc import Iterable

from imagery_pipeline.models import Bounds4326, ResolvedBuilding


class GeometryResolver:
    """Interface boundary for a separate BBL-to-geometry resolver."""

    def resolve_many(self, bbls: Iterable[str]) -> list[ResolvedBuilding]:
        raise NotImplementedError


class StubGeometryResolver(GeometryResolver):
    def __init__(self, mapping: dict[str, Bounds4326]) -> None:
        self.mapping = mapping

    def resolve_many(self, bbls: Iterable[str]) -> list[ResolvedBuilding]:
        missing = [bbl for bbl in bbls if bbl not in self.mapping]
        if missing:
            raise KeyError(f"Missing geometry for BBLs: {missing}")
        return [ResolvedBuilding(bbl=bbl, bounds=self.mapping[bbl]) for bbl in bbls]
