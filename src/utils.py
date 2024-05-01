def read_str_file(path):
    with open(path, 'r') as f:
        lines = f.read().splitlines()
    return lines
