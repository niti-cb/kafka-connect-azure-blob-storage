import argparse

from kafka_client.consumer import wait_until_consumption_stops


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', required=True)
    parser.add_argument('--consumer_group', required=True)
    args = parser.parse_args()

    try:
        wait_until_consumption_stops(args.topic, args.consumer_group)
    except Exception as e:
        print('Something went wrong:',  e)
    else:
        print('Done.')
