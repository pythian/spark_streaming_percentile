from csv import writer
import argparse

from numpy.random import random_integers


MESSAGE_TYPES = ['profile.picture.like', 'profile.view', 'message.private']
START_INDEX = 1000
NUM_USERS = 100000
NUM_ROWS = 10000000


def generate_data(start_index=START_INDEX, num_users=NUM_USERS,
                  num_rows=NUM_ROWS):
    users = random_integers(start_index, start_index+num_users, num_rows)
    activity = random_integers(0, len(MESSAGE_TYPES) - 1, num_rows)
    activity_name = [MESSAGE_TYPES[i] for i in activity]
    user_activity = zip(users, activity_name)
    return user_activity


def write_csv(user_activity, file_name):
    with open(file_name, 'w') as sample_data:
        csv_writer = writer(sample_data)
        csv_writer.writerows(user_activity)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', default='sample_data.csv')
    parser.add_argument('-s', '--start', default=START_INDEX)
    parser.add_argument('-u', '--users', default=NUM_USERS)
    parser.add_argument('-n', '--rows', default=NUM_ROWS)

    args = parser.parse_args()

    user_activity = generate_data(args.start, args.users, args.rows)
    write_csv(user_activity, args.file)
