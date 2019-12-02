"""
"""
import argparse
import os
import json


def conv_export_to_json(filename):
    ret = {}
    with open(filename, 'r') as fp:
        for cnt, line in enumerate(fp):
            if not line:
                print(f'Line skipped {cnt}: {line}')
                continue

            try:
                # line example
                # export ENVIRONMENT=demo4
                key_values = line.split(' ')[-1].split('=')
                ret[key_values[0].strip()] = key_values[1].strip()
            except Exception:
                print(f'Line Failed {cnt}: {line}')

    return ret


def main():
    home = os.getenv('HOME')
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='file path',
                        default=os.path.join(home, '.agentiq/demo4.env'))
    args = parser.parse_args()

    ret = conv_export_to_json(args.input_file)
    print(json.dumps(ret, indent=4))


if __name__ == "__main__":
    main()
