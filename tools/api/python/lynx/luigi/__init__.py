import luigi
import lynx


class ProjectTarget(luigi.target.Target):

  def __init__(self, project_name):
    self.project_name = project_name

  def exists(self):
    return lynx.Project.exists(self.project_name)

  def load(self):
    return lynx.Project.load(self.project_name)

  def save(self, project):
    project.save(self.project_name)


class LynxTask(luigi.Task):
  pass
