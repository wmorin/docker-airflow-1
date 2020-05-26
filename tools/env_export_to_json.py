"""
"""
import argparse
import os
import json


def load_variables_from_env(variables_file):
    variables = None
    with open(variables_file) as f:
        variables = json.load(f)

        for k, v in variables.items():
            if k == 'JWT_TOKEN':
                # TODO: remove this one off case
                variables[k] = os.getenv('BASE_API_TOKEN')
            else:
                if os.getenv(k):
                    variables[k] = os.getenv(k)

    return variables


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='file path',
                        default='variables.json')
    args = parser.parse_args()

    # ret = conv_export_to_json(args.input_file)
    ret = load_variables_from_env(args.input_file)
    print(json.dumps(ret, indent=4))


if __name__ == "__main__":
    main()
