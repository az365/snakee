try:  # Assume we're a submodule in a package.
    from context import SnakeeContext
    from functions.secondary import all_secondary_functions as fs
    from utils.algo import JoinType
    from content.fields.field_classes import struct
    from content.terms.term_classes import ProcessTerm, ObjectTerm, HierarchicTerm, TermRelation
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...context import SnakeeContext
    from ...functions.secondary import all_secondary_functions as fs
    from ...utils.algo import JoinType
    from ..fields.field_classes import struct
    from .term_classes import ProcessTerm, ObjectTerm, HierarchicTerm, TermRelation

# Terms
SEPULING = ProcessTerm(
    'sepuling',
    caption='an activity of Ardrites from the planet of Enteropia',
)
SEPULKA = ObjectTerm(
    'sepulka',
    caption='a prominent element of the civilization of Ardrites from the planet of Enteropia',
    relations={SEPULING: TermRelation.Process},
)
SEPULKARIUM = ObjectTerm(
    'sepulkarium',
    caption='establishments used for sepuling',
    relations={SEPULKA: TermRelation.OneToMany, SEPULING: TermRelation.Process},
)
LOCATION = HierarchicTerm(
    'location',
    caption='location of sepulka',
    levels=['planet', 'region', 'sepulkarium', 'slot'],
)

# Fields
SEPULKA_ID = SEPULKA.get_id_field()
SEPULKARIUM_ID = SEPULKARIUM.get_id_field()
SEPULING_SHARE = SEPULING.get_share_field()

# Structs
struct_sepulka_master = struct(
    SEPULKA.get_id_field(),  # sepulka_id
    SEPULKA.get_name_field(),  # sepulka_name
    SEPULKARIUM.get_id_field(),  # sepulkarium_id
    name='SEPULKA_MASTER',
    caption='sepulka master-data',
)
struct_sepulka_state = struct(
    SEPULKA.get_id_field(),  # sepulka_id
    SEPULING.get_share_field(),  # sepuling_share
    name='SEPULKARIUM_STATE',
    caption='current state of sepuling process',
)
struct_sepulkarium_state = struct(
    SEPULKARIUM.get_id_field(),  # sepulkarium_id
    SEPULKARIUM.get_name_field(),  # sepulkarium_name
    SEPULING.get_share_field(),  # sepuling_share
    SEPULKA.get_count_field(),  # sepulka_count
    name='SEPULKARIUM_STATE',
    caption='current state of sepuling process',
)

# Connectors
cx = SnakeeContext()
tsv_sepulka_master = cx.get_job_folder().folder('test_tmp').file(
    'sepulka_master.tsv',
    struct=struct_sepulka_master,
    caption=struct_sepulka_master.get_caption(),
)
tsv_sepulka_state = cx.find_job_folder('test_tmp').folder('test_tmp').file(
    'sepulka_state.tsv',
    struct=struct_sepulka_state,
    caption=struct_sepulka_state.get_caption(),
)

# Test data
DATA_SEPULKA_MASTER = [  # sepulka_id, sepulka_name, sepulkarium_id
    (1, 'sepulka_01', 10),
    (2, 'sepulka_02', 20),
    (3, 'sepulka_03', 10),
    (4, 'sepulka_04', 20),
    (5, 'sepulka_05', 10),
]
DATA_SEPULKA_STATE = [  # sepulka_id, sepuling_share
    (1, 0.9),
    (2, 0.8),
    (3, 0.7),
    (4, 0.6),
    (5, 0.5),
]


def test_term():
    initial_stream_sepulka_master = cx.sm.RegularStream(DATA_SEPULKA_MASTER, item_type=cx.sm.ItemType.Row)
    initial_stream_data_sepulka_state = cx.sm.RegularStream(DATA_SEPULKA_STATE, item_type=cx.sm.ItemType.Row)
    tsv_sepulka_master.write_stream(initial_stream_sepulka_master)  # (1, 'sepulka_01', 10), ..20..10..20..10)
    tsv_sepulka_state.write_stream(initial_stream_data_sepulka_state)  # (1, 0.9), (2, 0.8), ..0.7..0.6), (5, 0.5)
    stream_sepulkarium_state = tsv_sepulka_state.to_record_stream().join(
        tsv_sepulka_master.to_record_stream(),
        key=SEPULKA_ID,
        how=JoinType.Left,
    ).group_by(
        SEPULKARIUM.get_id_field(),  # sepulcarium_id
        values=[SEPULKA_ID, SEPULING_SHARE],
        step=None,
    ).select(
        '*',
        SEPULING_SHARE.map(fs.mean(round_digits=2)).to(SEPULING_SHARE),
        SEPULKA_ID.map(fs.count()).to(SEPULKA.get_count_field())
    ).collect()
    dict_sepulkarium_state = stream_sepulkarium_state.get_dict(SEPULKARIUM_ID, SEPULING_SHARE)
    dict_sepulka_count_by_sepulkarium = stream_sepulkarium_state.get_dict(SEPULKARIUM_ID, SEPULKA.get_count_field())
    expected_state = {10: 0.7, 20: 0.7}
    assert dict_sepulkarium_state == expected_state, f'{dict_sepulkarium_state} != {expected_state}'
    assert dict_sepulka_count_by_sepulkarium == {10: 3, 20: 2}, dict_sepulka_count_by_sepulkarium


def main():
    test_term()


if __name__ == '__main__':
    main()
