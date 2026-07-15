import re

from helper import  get_relative_path

NUM_OPS: int = 43
NUM_PLATFORMS: int = 5
ID_POSITION: int = 0
NULL_OPERATOR_ID: int = 0
NULL_PLATFORM_POSITION: int = 0

def is_null_operator(values: list[int]) -> bool:
    return sum(values) == 0

def add_null_class(line: str) -> str:
    regex_pattern = r'\(((?:[+,-]?\d+(?:,[+,-]?\d+)*)(?:\s*,\s*\(.*?\))*)\)'
    matches_iterator = re.finditer(regex_pattern, line)

    new_line = line

    for match in matches_iterator:
        in_paranthesis = match.group()
        find = in_paranthesis.strip('(').strip(')')
        values = [int(num.strip()) for num in find.split(',')]
        old_platforms = values[NUM_OPS:NUM_OPS+NUM_PLATFORMS-1]

        new_platforms = [0] + old_platforms

        # Add null platform for null operators
        if is_null_operator(values):
            new_platforms[NULL_PLATFORM_POSITION] = 1

        new_values = values[ID_POSITION:NUM_OPS] + [0] + new_platforms + values[NUM_OPS+len(old_platforms):len(values)]
        print(len(values))
        print(len(new_values))

        assert len(values) + 2 == len(new_values)

        replacement = ','.join(map(str, new_values))
        new_line = new_line.replace(find, f"{replacement}", 1)

    return new_line



def main():
    file_path = get_relative_path("train.txt", "Data/splits/stats")
    out_file_path = get_relative_path("train.txt", "Data/splits/stats/amended")

    amended_lines = []

    with open(file_path) as file:
        for line in file:
            amended_lines.append(add_null_class(line))

    with open(out_file_path, 'a') as out_file:
        out_file.writelines(amended_lines)


if __name__ == "__main__":
    main()
