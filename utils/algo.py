from enum import Enum
from typing import Optional, Callable, Iterable, Iterator, Union


JOIN_TYPES = ('left', 'right', 'full', 'inner', 'outer')  # deprecated


class JoinType(Enum):
    Left = 'left'
    Right = 'right'
    Full = 'full'
    Inner = 'inner'
    Outer = 'outer'


def topologically_sorted(  # Kahn's algorithm
        nodes: Union[list, tuple],
        edges: dict,
        ignore_cycles: bool = False,
        logger=None,
):
    if len(nodes) < 2:
        return nodes
    unordered_nodes = nodes.copy()
    unresolved_dependencies = edges.copy()
    ordered_nodes = list()
    while unordered_nodes:
        ordered_count = len(ordered_nodes)
        for node in unordered_nodes:
            if not unresolved_dependencies[node]:
                unordered_nodes.remove(node)
                ordered_nodes.append(node)
                for f in unordered_nodes:
                    if node in unresolved_dependencies[f]:
                        unresolved_dependencies[f].remove(node)
        has_progress = ordered_count != len(ordered_nodes)
        if not has_progress:
            message = "Kahn's algorithm is not converging. "
            message += 'Probably given graph has cyclic dependencies or missing nodes. '
            message += 'Unordered nodes: {} '.format(unordered_nodes)
            if ignore_cycles:
                if hasattr(logger, 'warning'):
                    # logger.log(msg=message + 'skipped.', level=30)
                    logger.warning(message + 'skipped.')
                break
            else:
                raise OverflowError(message)
    return ordered_nodes


def merge_iter(
        iterables: Union[list, tuple],
        key_function: Callable,
        reverse: bool = False,
        post_action: Optional[Callable] = None,
):
    iterators_count = len(iterables)
    finished = [False] * iterators_count
    take_next = [True] * iterators_count
    item_from = [None] * iterators_count
    key_from = [None] * iterators_count
    choice_function = max if reverse else min
    while not min(finished):
        for n in range(iterators_count):
            if take_next[n] and not finished[n]:
                try:
                    item_from[n] = next(iterables[n])
                    key_from[n] = key_function(item_from[n])
                    take_next[n] = False
                except StopIteration:
                    finished[n] = True
        if not min(finished):
            chosen_key = choice_function([k for f, k in zip(finished, key_from) if not f])
            for n in range(iterators_count):
                if key_from[n] == chosen_key and not finished[n]:
                    yield item_from[n]
                    take_next[n] = True
    if post_action:
        post_action()


def map_side_join(
        iter_left: Iterable,
        iter_right: Iterable,
        key_function: Callable,
        merge_function: Callable,  # it.merge_two_items
        dict_function: Callable,  # it.items_to_dict
        how: JoinType = JoinType.Left,
        uniq_right: bool = False,
):
    if not isinstance(how, JoinType):
        how = JoinType(how)
    dict_right = dict_function(iter_right, key_function=key_function, of_lists=not uniq_right)
    keys_used = set()
    for left_part in iter_left:
        cur_key = key_function(left_part)
        right_part = dict_right.get(cur_key)
        if how in (JoinType.Right, JoinType.Full):
            keys_used.add(cur_key)
        if right_part:
            if uniq_right:
                out_items = [merge_function(left_part, right_part)]
            elif isinstance(right_part, (list, tuple)):
                out_items = [merge_function(left_part, i) for i in right_part]
            else:
                message = 'right part must be list or tuple while using uniq_right option (got {})'
                raise ValueError(message.format(type(right_part)))
        else:
            if how in (JoinType.Right, JoinType.Inner):
                out_items = []
            else:
                out_items = [left_part]
        if right_part or how != JoinType.Inner:
            yield from out_items
    if how in (JoinType.Right, JoinType.Full):
        for k in dict_right:
            if k not in keys_used:
                if uniq_right:
                    yield merge_function(None, dict_right[k])
                else:
                    yield from [merge_function(None, i) for i in dict_right[k]]


def sorted_join(
        iter_left: Iterator,
        iter_right: Iterator,
        key_function: Callable,
        merge_function: Callable,  # fs.merge_two_items()
        order_function: Callable,  # fs.is_ordered(reverse=sorting_is_reversed, including=True)
        how: JoinType = JoinType.Left,
):
    if not isinstance(how, JoinType):
        how = JoinType(how)
    left_finished, right_finished = False, False
    take_next_left, take_next_right = True, True
    cur_left, cur_right = None, None
    group_left, group_right = list(), list()
    left_key, right_key = None, None
    prev_left_key, prev_right_key = None, None

    while not (left_finished and right_finished):
        if take_next_left and not left_finished:
            try:
                cur_left = next(iter_left)
                left_key = key_function(cur_left)
            except StopIteration:
                left_finished = True
        if take_next_right and not right_finished:
            try:
                cur_right = next(iter_right)
                right_key = key_function(cur_right)
            except StopIteration:
                right_finished = True
        left_key_changed = left_finished or left_key != prev_left_key
        right_key_changed = right_finished or right_key != prev_right_key

        if left_key_changed and right_key_changed:
            if prev_left_key == prev_right_key:
                if how != JoinType.Outer:
                    for out_left in group_left:
                        for out_right in group_right:
                            yield merge_function(out_left, out_right)
            else:
                if how in (JoinType.Left, JoinType.Full, JoinType.Outer):
                    for out_left in group_left:
                        yield merge_function(out_left, None)
                if how in (JoinType.Right, JoinType.Full, JoinType.Outer):
                    for out_right in group_right:
                        yield merge_function(None, out_right)
            group_left, group_right = list(), list()

        if left_key == right_key:
            take_next_left, take_next_right = True, True
            prev_left_key, prev_right_key = left_key, right_key
            if take_next_left and not left_finished:
                group_left.append(cur_left)
            if take_next_right and not right_finished:
                group_right.append(cur_right)
        elif order_function(left_key, right_key) or right_finished:
            take_next_left, take_next_right = True, False
            assert order_function(prev_left_key, left_key) or left_finished, 'left stream must be sorted'
            prev_left_key = left_key
            if take_next_left and not left_finished:
                group_left.append(cur_left)
        else:  # next is right
            take_next_left, take_next_right = False, True
            assert order_function(prev_right_key, right_key) or right_finished, 'right stream must be sorted'
            prev_right_key = right_key
            if take_next_right and not right_finished:
                group_right.append(cur_right)

        if (left_finished and not take_next_right) or (right_finished and not take_next_left):
            break
