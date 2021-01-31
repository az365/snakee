from enum import Enum
import gc


MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'flux_{}.tmp'
TMP_FILES_ENCODING = 'utf8'


try:  # Assume we're a sub-module in a package.
    from streams.simple.any_stream import AnyFlux
    from streams.simple.line_stream import LinesFlux
    from streams.simple.row_stream import RowsFlux
    from streams.pairs.key_value_stream import PairsFlux
    from streams.typed.schema_stream import SchemaFlux
    from streams.simple.record_stream import RecordsFlux
    from streams.typed.pandas_stream import PandasFlux
    from utils import arguments as arg
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .simple.any_stream import AnyFlux
    from .simple.line_stream import LinesFlux
    from .simple.row_stream import RowsFlux
    from .pairs.key_value_stream import PairsFlux
    from .typed.schema_stream import SchemaFlux
    from .simple.record_stream import RecordsFlux
    from .typed.pandas_stream import PandasFlux
    from ..utils import arguments as arg
    from ..schema import schema_classes as sh


class FluxType(Enum):
    AnyFlux = 'AnyFlux'
    LinesFlux = 'LinesFlux'
    RowsFlux = 'RowsFlux'
    PairsFlux = 'PairsFlux'
    SchemaFlux = 'SchemaFlux'
    RecordsFlux = 'RecordsFlux'
    PandasFlux = 'PandasFlux'


def get_class(flux_type):
    if isinstance(flux_type, str):
        flux_type = FluxType(flux_type)
    assert isinstance(flux_type, FluxType), TypeError(
        'flux_type must be an instance of FluxType (but {} as type {} received)'.format(flux_type, type(flux_type))
    )
    if flux_type == FluxType.AnyFlux:
        return AnyFlux
    elif flux_type == FluxType.LinesFlux:
        return LinesFlux
    elif flux_type == FluxType.RowsFlux:
        return RowsFlux
    elif flux_type == FluxType.PairsFlux:
        return PairsFlux
    elif flux_type == FluxType.SchemaFlux:
        return SchemaFlux
    elif flux_type == FluxType.RecordsFlux:
        return RecordsFlux
    elif flux_type == FluxType.PandasFlux:
        return PandasFlux


def is_flux(obj):
    return isinstance(
        obj,
        (AnyFlux, LinesFlux, RowsFlux, PairsFlux, SchemaFlux, RecordsFlux, PandasFlux),
    )


def is_row(item):
    return RowsFlux.is_valid_item(item)


def is_record(item):
    return RecordsFlux.is_valid_item(item)


def is_schema_row(item):
    return isinstance(item, sh.SchemaRow)


def concat(*iter_fluxes):
    iter_fluxes = arg.update(iter_fluxes)
    result = None
    for cur_flux in iter_fluxes:
        if result is None:
            result = cur_flux
        else:
            result = result.add_flux(cur_flux)
        gc.collect()
    return result
