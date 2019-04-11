import os
import csv

_EVENTS_INPUT_FILE_URI = "./data/events.csv"
_AGE_GROUPS = [1, 5, 8]
_AS_OF_TIME = 10


def calculate_frequency_standard_python(data, age_groups, as_of_time):
    """
    Return a list of integers representing how many events fall into each of the
    age ranges relative to the given point in time.

    :param data: list of dictionaries representing event rows
    :type data: list

    :param age_groups: list of integers representing Unix time deltas
    :type age_groups: list

    :param as_of_time: Unix stamp; serves as upper bound for each interval
    :type as_of_time: int or float

    :return: list of event totals; order should correspond to `time_deltas`
    """

    # For each age-group, pre-calculate the minimum time an event must have in
    # order to fall into that bucket. Assuming the number of event is *large*
    # doing the same calculation repeatedly is wasteful.
    # If the bucket age > max_time, then the bucket will be skipped by
    # setting the minimum time of the bucket > max_time
    # for example: 10, [1, 5, 8, 13] ==> [9, 5, 2, 11]
    age_range_start_times = [
        as_of_time - i if i <= as_of_time else as_of_time + 1 for
        i in age_groups]

    # Initialize result variable with frequencies set to 0
    frequencies = [0] * len(age_range_start_times)

    # Loop over each event, and increment the frequency for each age group
    for event in data:
        event_time = int(event.get("Timestamp"))
        if event_time is None:
            continue
        if event_time > as_of_time:
            continue
        for group_index, min_group_time in enumerate(age_range_start_times):
            if event_time >= min_group_time:
                frequencies[group_index] = frequencies[group_index] + 1
    return frequencies


if __name__ == '__main__':
    try:
        with open(_EVENTS_INPUT_FILE_URI) as csvfile:
            events = list(csv.DictReader(csvfile))
    except FileNotFoundError:
        print('Point the "_EVENTS_INPUT_FILE_URI" variable in recency_calc.py '
              'to a csv file of event data starting with a header row naming '
              'two columns: Category, Timestamp.\n'
              'This must be a path relative to {}'.format(os.getcwd()))
    else:
        print(f'Reading inputs from {_EVENTS_INPUT_FILE_URI}')
        print(calculate_frequency_standard_python(events,
                                                  _AGE_GROUPS, _AS_OF_TIME))
