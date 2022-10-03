import argparse

from system_tests.tests import test_connector


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', required=True)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--azure_container', required=True)
    args = parser.parse_args()

    try:
        test_connector(args.topic, args.directory, args.azure_container)
    except Exception as e:
        print('Something went wrong:', e)
