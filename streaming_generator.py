import random


MESSAGE_TYPES = ['profile.picture.like', 'profile.view', 'message.private']
START_INDEX = 1000
END_INDEX = 101000


def gen_random_message(start_index=START_INDEX, end_index=END_INDEX):
    return (random.randint(start_index, end_index),
            random.choice(MESSAGE_TYPES))
