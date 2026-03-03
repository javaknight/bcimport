"""Mapper registry: resolves a vendor name to an instantiated mapper.

Usage:
    from mappers import get_mapper
    mapper = get_mapper("lutron")
    payload = mapper.map_row(row)
"""
from mappers.lutron_mapper import LutronMapper

_REGISTRY: dict[str, type] = {
    "lutron": LutronMapper,
}


def get_mapper(vendor: str) -> LutronMapper:  # return type broadens as vendors are added
    """Return an instantiated mapper for the given vendor name (case-insensitive)."""
    cls = _REGISTRY.get(vendor.lower())
    if cls is None:
        raise ValueError(
            f"Unknown vendor {vendor!r}. Registered vendors: {list(_REGISTRY)}"
        )
    return cls()
