import luigi
import lynx


class ProjectTarget(luigi.target.Target):

  def __init__(self, project_name):
    self.project_name = project_name

  def exists(self):
    entry = lynx.get_directory_entry(self.project_name)
    if entry.exists and not entry.isProject:
      raise Exception('project expected but non-project entry found ', entry)
    return entry.exists

  def load(self):
    return lynx.Project.load(self.project_name)

  def save(self, project):
    project.save(self.project_name)


class LynxTask(luigi.Task):
  pass
