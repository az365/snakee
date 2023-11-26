from typing import Iterator, Optional

try:  # Assume we're a submodule in a package.
    from base.constants.chars import CROP_SUFFIX, SPACE, DOT, EMPTY, PIPE
    from base.constants.text import DEFAULT_LINE_LEN
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import CROP_SUFFIX, SPACE, DOT, EMPTY, PIPE
    from ...base.constants.text import DEFAULT_LINE_LEN


def get_cropped_line(line: str, char_count: int, crop: str = CROP_SUFFIX, centred: bool = True) -> str:
    line = str(line)
    full_len = len(line)
    if full_len > char_count:
        crop_len = len(crop)
        if centred:
            half_len = int((char_count - crop_len) / 2)
            if half_len > 0:
                right_half_len = char_count - half_len - crop_len
                right_pos = full_len - right_half_len
                return line[:half_len] + crop + line[right_pos:]
            elif crop_len < char_count:
                return crop
            else:
                return crop[:char_count]
        else:
            part_len = char_count - crop_len
            if part_len > 0:
                return line[:part_len]
            elif crop_len < char_count:
                return crop
            else:
                return crop[:char_count]
    else:
        return line


def get_fit_line(
        line: str,
        line_len: int,
        delimiter: str = CROP_SUFFIX,
        centred: bool = True,
        align_left: bool = False,
        align_right: bool = False,
        spacer: str = SPACE,
) -> str:
    cropped_line = get_cropped_line(line, line_len, delimiter, centred=centred)
    cropped_line_len = len(cropped_line)
    if cropped_line_len == line_len:
        return cropped_line
    elif cropped_line_len < line_len:
        spacer_count = line_len - cropped_line_len
        align_center = align_left == align_right
        if align_center:
            spacer_left_count = int(spacer_count / 2)
            spacer_right_count = line_len - cropped_line_len - spacer_left_count
            return spacer_left_count * spacer + cropped_line + spacer_right_count * spacer
        elif align_left:
            return cropped_line + spacer_count * spacer
        elif align_right:
            return spacer_count * spacer + cropped_line
    else:
        raise ValueError(f'"{cropped_line}" ({cropped_line_len}) > {line_len}')


def get_filled_line(line_len: int = DEFAULT_LINE_LEN, sequence=DOT) -> str:
    sequence_count = int(line_len / len(sequence)) + 1
    line = sequence * sequence_count
    return get_fit_line(line, line_len=line_len, delimiter=EMPTY)


def get_empty_line(line_len: int = DEFAULT_LINE_LEN) -> str:
    return get_filled_line(line_len=line_len, sequence=SPACE)


def get_united_lines(first: Iterator[str], second: Iterator[str], invert=False, delimiter: str = PIPE):
    lines = list()
    for f, s in zip(first, second):
        if invert:
            lines += [s + delimiter + f]
        else:
            lines += [f + delimiter + s]
    return lines


def get_compact_pair_repr(left_part: str, right_part: str, delimiter: str = SPACE, line_len: Optional[int] = None):
    line = str(left_part) + delimiter + str(right_part)
    if line_len is not None:
        line = get_fit_line(line, line_len=line_len, centred=False, align_left=True, align_right=False)
    return line


def get_centred_pair_repr(
        left_part: str,
        right_part: str,
        delimiter: str = SPACE,
        line_len: int = DEFAULT_LINE_LEN,
) -> str:
    delimiter_len = len(delimiter)
    left_part_len = int((line_len - delimiter_len) / 2)
    right_part_len = line_len - left_part_len - delimiter_len
    left_part_repr = get_fit_line(left_part, left_part_len, centred=True, align_right=True)
    right_part_repr = get_fit_line(right_part, right_part_len, centred=False, align_left=True)
    return left_part_repr + delimiter + right_part_repr
