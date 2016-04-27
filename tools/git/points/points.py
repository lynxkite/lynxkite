'''Tracks GitHub issue "points" on a graph.

Requisites:

  pip install matplotlib  # or:  sudo apt install python-matplotlib
  pip install PyGithub
  pip install pystache

Running:

  python tools/points/points.py        # Generate graphs and quit.
  python tools/points/points.py serve  # Periodically generate graphs and serve them over HTTP.
'''

class Milestone(object):
  def __init__(self, title, updated_at):
    self.title = title
    self.updated_at = updated_at
    self.total = 0
    self.timeline_score = []
    self.timeline_datetime = []
    self.timeline_velocity = []


def parse_label(label):
  score = 0
  if label.endswith('P'):
    try:
      score = float(label[:-1])
    except ValueError:
      pass
  return score


def get_data():
  from github import Github
  import datetime
  import os
  # lynx-read-only token.
  token = os.environ['LYNX_READONLY_TOKEN']
  gh = Github(token)
  repo = gh.get_repo('biggraph/biggraph')
  zero = datetime.datetime.fromtimestamp(0)
  seconds_in_day = 24 * 3600
  milestones = []
  for milestone in repo.get_milestones(state='open'):
    m = Milestone(milestone.title, milestone.updated_at)
    print m.title
    issues = list(repo.get_issues(milestone, state='all'))
    issues.sort(key=lambda i: i.closed_at or zero)
    closed_score = 0
    last_closed = zero
    for issue in issues:
      score = 0
      for label in issue.labels:
        score += parse_label(label.name)
      if score:
        m.total += score
        if issue.state == 'closed':
          closed_score += score
          m.timeline_score.append(closed_score)
          m.timeline_datetime.append(issue.closed_at)
          delta = (issue.closed_at - last_closed).total_seconds() / seconds_in_day
          m.timeline_velocity.append(score / delta)
          last_closed = issue.closed_at
      print ' ', issue.number, issue.title, issue.state, issue.closed_at, score
    if m.timeline_score:
      milestones.append(m)
  milestones.sort(key=lambda m: m.updated_at, reverse=True)
  return milestones


def save_plot(milestone):
  import matplotlib.pyplot as plt
  fig, ax1 = plt.subplots()
  fig.suptitle(milestone.title)
  ax1.set_ylabel('Score', color='b')
  ax1.plot(milestone.timeline_datetime, milestone.timeline_score, 'b-', drawstyle='steps')
  ax2 = ax1.twinx()
  ax2.set_ylabel('Velocity', color='g')
  ax2.plot(milestone.timeline_datetime, milestone.timeline_velocity, 'g:')
  fig.autofmt_xdate()
  fn = '{}.png'.format(milestone.title)
  fig.savefig(fn)
  print 'saved', fn


def generate_html(milestones):
  import pystache
  with file('../index.html.mustache') as f:
    template = f.read()
  html = pystache.render(template, {'milestones': [m.title for m in milestones]})
  fn = 'index.html'
  with file(fn, 'w') as f:
    f.write(html)
  print 'saved', fn


def generate_graphs():
  data = get_data()
  for milestone in data:
    save_plot(milestone)
  generate_html(data)


def start_scheduler():
  import sched, time
  sc = sched.scheduler(time.time, time.sleep)
  def sc_generate_graphs(sc):
    generate_graphs()
    sc.enter(600, 1, sc_generate_graphs, (sc,))
  sc.enter(1, 1, sc_generate_graphs, (sc,))
  sc.run()


def start_http():
  import SimpleHTTPServer
  import SocketServer
  PORT = 8000
  Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
  class ReusingTCPServer(SocketServer.TCPServer):
    allow_reuse_address = True
  server = ReusingTCPServer(('', PORT), Handler)
  print 'serving at port', PORT
  server.serve_forever()


def main(args):
  if len(args) > 1 and args[1] == 'serve':
    import thread
    thread.start_new_thread(start_scheduler, ())
    start_http()
  else:
    generate_graphs()


if __name__ == '__main__':
  import os
  import sys
  os.chdir(os.path.dirname(os.path.realpath(__file__)))
  if not os.path.exists('output'):
    os.mkdir('output')
  os.chdir('output')
  main(sys.argv)
