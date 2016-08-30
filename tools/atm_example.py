"""Test data generator for ATM transactions.

Run without arguments to generate events on standard output. Run with --help for more options.
"""
import argparse
import random
import time

random.seed(0)
parser = argparse.ArgumentParser()
parser.add_argument(
    '--people',
    help='Number of people.',
    type=int,
    default=1000)
parser.add_argument(
    '--friends',
    help='Number of pairs that co-occur often.',
    type=int,
    default=1000)
parser.add_argument(
    '--friendship',
    help='Number total friendly co-occurrences.',
    type=int,
    default=10000)
parser.add_argument('--atms', help='Number of ATMs.', type=int, default=1000)
parser.add_argument(
    '--events',
    help='Number of events.',
    type=int,
    default=1000000)

time_start = time.mktime(time.strptime('2015', '%Y'))
time_end = time.mktime(time.strptime('2016', '%Y'))

rnd = random.randrange


def main():
  args = parser.parse_args()
  events = []
  # Non-friendly events.
  for i in range(args.events - 2 * args.friendship):
    timestamp = rnd(time_start, time_end)
    atm_id = rnd(args.atms)
    person_id = rnd(args.people)
    events.append((timestamp, atm_id, person_id))
  # Friendly events.
  friends = {}
  for i in range(args.friends):
    friends[rnd(args.people)] = rnd(args.people)
  for i in range(args.friendship):
    timestamp = rnd(time_start, time_end)
    atm_id = rnd(args.atms)
    person_id = random.choice(friends.keys())
    events.append((timestamp, atm_id, person_id))
    events.append((timestamp + rnd(-120, 120), atm_id, friends[person_id]))
  # Output.
  random.shuffle(events)
  print 'timestamp,atm_id,person_id'
  for timestamp, atm_id, person_id in events:
    print '{},A-{},P-{}'.format(timestamp, atm_id, person_id)

if __name__ == '__main__':
  main()
